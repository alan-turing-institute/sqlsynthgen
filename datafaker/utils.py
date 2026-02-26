"""Utility functions."""
import ast
import gzip
import importlib.util
import io
import json
import logging
import random
import re
import string
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Callable,
    Final,
    Generator,
    Generic,
    Iterable,
    Optional,
    Type,
    TypeVar,
    Union,
)

import psycopg2
import sqlalchemy
import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from sqlalchemy import Connection, Engine, ForeignKey, create_engine, event, select
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.schema import (
    AddConstraint,
    DropConstraint,
    ForeignKeyConstraint,
    MetaData,
    Table,
)

# Define some types used repeatedly in the code base
MaybeAsyncEngine = Union[Engine, AsyncEngine]

# After every how many rows of vocab table downloading do we see a
# progres update
MAKE_VOCAB_PROGRESS_REPORT_EVERY = 10000

CONFIG_SCHEMA_PATH: Final[Path] = (
    Path(__file__).parent / "json_schemas/config_schema.json"
)

T = TypeVar("T")


class Empty(Generic[T]):
    """Generic empty sequences for default arguments."""

    @classmethod
    def iterable(cls) -> Iterable[T]:
        """Get an empty iterable."""
        e: list[T] = []
        return (x for x in e)


def read_config_file(path: str) -> dict:
    """Read a config file, warning if it is invalid.

    Args:
        path: The path to a YAML-format config file.

    Returns:
        The config file as a dictionary.
    """
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)

    assert isinstance(config, dict)

    schema_config = json.loads(CONFIG_SCHEMA_PATH.read_text(encoding="UTF-8"))
    try:
        validate(config, schema_config)
    except ValidationError as e:
        logger.error("The config file is invalid: %s", e.message)

    return config


def import_file(file_path: str) -> ModuleType:
    """Import a file.

    This utility function returns file_path imported as a module.

    Args:
        file_path (str): The path of a file to import.

    Returns:
        ModuleType
    """
    spec = importlib.util.spec_from_file_location("df", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No loadable module at {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def open_file(file_name: str | Path) -> io.BufferedWriter:
    """Open a file for writing."""
    return Path(file_name).open("wb")


def open_compressed_file(file_name: str | Path) -> gzip.GzipFile:
    """
    Open a gzip-compressed file for writing.

    :param file_name: The name of the file to open.
    :return: A file object; it can be written to as a normal uncompressed
    file and it will do the compression.
    """
    return gzip.GzipFile(file_name, "wb")


def table_row_count(table: Table, conn: Connection) -> int:
    """
    Count the rows in the table.

    :param table: The table to count.
    :param conn: The connection to the database.
    :return: The number of rows in the table.
    """
    return conn.execute(
        # pylint: disable=not-callable
        select(sqlalchemy.func.count()).select_from(
            sqlalchemy.table(
                table.name,
                *[
                    sqlalchemy.column(col.name)
                    for col in table.primary_key.columns.values()
                ],
            )
        )
    ).scalar_one()


def download_table(
    table: Table,
    engine: Engine,
    yaml_file_name: Union[str, Path],
    compress: bool,
) -> None:
    """Download a Table and store it as a .yaml file."""
    open_fn = open_compressed_file if compress else open_file
    with engine.connect().execution_options(yield_per=1000) as conn:
        with open_fn(yaml_file_name) as yamlfile:
            stmt = select(table)
            rowcount = table_row_count(table, conn)
            count = 0
            for row in conn.execute(stmt).mappings():
                result = {str(col_name): value for (col_name, value) in row.items()}
                yamlfile.write(yaml.dump([result]).encode())
                count += 1
                if count % MAKE_VOCAB_PROGRESS_REPORT_EVERY == 0:
                    logger.info(
                        "written row %d of %d, %.1f%%",
                        count,
                        rowcount,
                        100 * count / rowcount,
                    )


def get_sync_engine(engine: MaybeAsyncEngine) -> Engine:
    """Given an SQLAlchemy engine that may or may not be async return one that isn't."""
    if isinstance(engine, AsyncEngine):
        return engine.sync_engine
    return engine


def create_db_engine(
    db_dsn: str,
    schema_name: Optional[str] = None,
    use_asyncio: bool = False,
    **kwargs: Any,
) -> MaybeAsyncEngine:
    """Create a SQLAlchemy Engine."""
    if use_asyncio:
        async_dsn = db_dsn.replace("postgresql://", "postgresql+asyncpg://")
        engine: MaybeAsyncEngine = create_async_engine(async_dsn, **kwargs)
    else:
        engine = create_engine(db_dsn, **kwargs)

    if schema_name is not None:
        event_engine = get_sync_engine(engine)

        @event.listens_for(event_engine, "connect", insert=True)
        def connect(dbapi_connection: DBAPIConnection, _: Any) -> None:
            set_search_path(dbapi_connection, schema_name)

    return engine


def create_db_engine_dst(
    db_dsn: str,
    schema_name: Optional[str] = None,
    use_asyncio: bool = False,
) -> MaybeAsyncEngine:
    """
    Create a SQLAlchemy Engine suitable for output.

    This prevents DuckDB from reading any parquet files avoiding any
    possible leakage from existing source files into the destination database.
    :param db_dsn: The database connection string.
    :param schema_name: The name of the schema within the database to use.
    :param use_asyncio: True if an asynchronous connection is required.
    :return: The ``Engine`` or ``AsyncEngine``.
    """
    if db_dsn.startswith("duckdb:"):
        return create_db_engine(
            db_dsn,
            schema_name,
            use_asyncio,
            connect_args={
                "config": {
                    "enable_external_access": False,
                }
            },
        )
    return create_db_engine(db_dsn, schema_name, use_asyncio)


def set_search_path(connection: DBAPIConnection, schema: str) -> None:
    """Set the SEARCH_PATH for a PostgreSQL connection."""
    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path
    existing_autocommit = connection.autocommit
    connection.autocommit = True

    cursor = connection.cursor()
    # Parametrised queries don't work with asyncpg, hence the f-string.
    cursor.execute(f"SET search_path TO {schema};")
    cursor.close()

    connection.autocommit = existing_autocommit


def get_orm_metadata(
    orm_module: ModuleType, tables_config: Mapping[str, Any]
) -> MetaData:
    """Get the SQLAlchemy Metadata object from an ORM module.

    Drop all tables from the metadata that are marked with `ignore` in `tables_config`.
    """
    metadata: MetaData = orm_module.Base.metadata
    # The call to tuple makes a copy of the iterable, allowing us to mutate the original
    # within the loop.
    for table_name, table in tuple(metadata.tables.items()):
        ignore = tables_config.get(table_name, {}).get("ignore", False)
        if ignore:
            metadata.remove(table)
    return metadata


# This is the main logger that the other modules of datafaker should use for output.
# conf_logger() should be called once, as early as possible, to configure this logger.
logger = logging.getLogger("datafaker")


def info_or_lower(record: logging.LogRecord) -> bool:
    """Allow records with level of INFO or lower."""
    return record.levelno in (logging.DEBUG, logging.INFO)


def warning_or_higher(record: logging.LogRecord) -> bool:
    """Allow records with level of WARNING or higher."""
    return record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL)


class StdoutHandler(logging.Handler):
    """
    A handler that writes to stdout.

    We aren't using StreamHandler because that confuses typer.testing.CliRunner
    """

    def flush(self) -> None:
        """Flush the buffer."""
        self.acquire()
        try:
            sys.stdout.flush()
        finally:
            self.release()

    def emit(self, record: Any) -> None:
        """Write the record."""
        try:
            msg = self.format(record)
            sys.stdout.write(msg + "\n")
            sys.stdout.flush()
        except RecursionError:
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            self.handleError(record)


class StderrHandler(logging.Handler):
    """
    A handler that writes to stderr.

    We aren't using StreamHandler because that confuses typer.testing.CliRunner
    """

    def flush(self) -> None:
        """Flush the buffer."""
        self.acquire()
        try:
            sys.stderr.flush()
        finally:
            self.release()

    def emit(self, record: Any) -> None:
        """Write the record."""
        try:
            msg = self.format(record)
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except RecursionError:
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            self.handleError(record)


def conf_logger(verbose: bool) -> None:
    """Configure the logger."""
    # Note that this function modifies the global `logger`.
    log_format = "%(message)s"

    # info will always be printed to stdout
    # debug will be printed to stdout only if verbose=True
    stdout_handler = StdoutHandler()
    stdout_handler.setFormatter(logging.Formatter(log_format))
    stdout_handler.addFilter(info_or_lower)

    # warning and error will always be printed to stderr
    stderr_handler = StderrHandler()
    stderr_handler.setFormatter(logging.Formatter(log_format))
    stderr_handler.addFilter(warning_or_higher)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        handlers=[stdout_handler, stderr_handler],
        force=True,
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("blib2to3.pgen2.driver").setLevel(logging.WARNING)


def get_flag(maybe_dict: Any, key: Any) -> bool:
    """
    Get a boolean from a mapping, or False if that does not make sense.

    :param maybe_dict: A mapping, or possibly not.
    :param key: A key in ``maybe_dict``, or possibly not.
    :return: True only if ``maybe_dict`` is a mapping, ``maybe_dict[key]``
    exists and ``maybe_dict[key]`` is truthy.
    """
    return isinstance(maybe_dict, Mapping) and maybe_dict.get(key, False)


def get_property(maybe_dict: Any, key: Any, required_type: Type[T], default: T) -> T:
    """
    Get a specific property from a dict or a default if that does not exist.

    :param maybe_dict: A mapping, or possibly not.
    :param key: A key in ``maybe_dict``, or possibly not.
    :param required_type: The type ``maybe_dict[key]`` needs to be an instance of.
    :param default: The return value if ``maybe_dict`` is not a mapping,
    or if ``key`` is not a key of ``maybe_dict``.
    :return: ``maybe_dict[key]`` if this makes sense, or ``default`` if not.
    """
    if not isinstance(maybe_dict, Mapping):
        return default
    v = maybe_dict.get(key, default)
    return v if isinstance(v, required_type) else default


def fk_refers_to_ignored_table(fk: ForeignKey) -> bool:
    """
    Test if this foreign key refers to an ignored table.

    :param fk: The foreign key to test.
    :return: True if the table referred to is ignored in ``config.yaml``.
    """
    try:
        fk.column
    except sqlalchemy.exc.NoReferencedTableError:
        return True
    return False


def fk_constraint_refers_to_ignored_table(fk: ForeignKeyConstraint) -> bool:
    """
    Test if the constraint refers to a table marked as ignored in ``config.yaml``.

    :param fk: The foreign key constraint.
    :return: True if ``fk`` refers to an ignored table.
    """
    try:
        fk.referred_table
    except sqlalchemy.exc.NoReferencedTableError:
        return True
    return False


def get_related_table_names(table: Table) -> set[str]:
    """
    Get the names of all tables for which there exist foreign keys from this table.

    :param table: SQLAlchemy table object.
    :return: The set of the names of the tables referred to by foreign keys
    in ``table``.
    """
    return {
        str(fk.referred_table.name)
        for fk in table.foreign_key_constraints
        if not fk_constraint_refers_to_ignored_table(fk)
    }


def table_is_private(config: Mapping, table_name: str) -> bool:
    """
    Test if the named table is private.

    :param config: The ``config.yaml`` object.
    :param table_name: The name of the table to test.
    :return: True if the table is marked as private in ``config``.
    """
    ts = config.get("tables", {})
    if not isinstance(ts, Mapping):
        return False
    t = ts.get(table_name, {})
    ret = t.get("primary_private", False)
    return ret if isinstance(ret, bool) else False


def primary_private_fks(config: Mapping, table: Table) -> list[str]:
    """
    Get the list of columns in the table that refer to primary private tables.

    A table that is not primary private but has a non-empty list of
    primary_private_fks is secondary private.

    :param config: The ``config.yaml`` object.
    :param table: The table to examine.
    :return: A list of names of columns that refer to private tables.
    """
    return [
        str(fk.referred_table.name)
        for fk in table.foreign_key_constraints
        if not fk_constraint_refers_to_ignored_table(fk)
        if table_is_private(config, str(fk.referred_table.name))
    ]


def get_vocabulary_table_names(config: Mapping) -> set[str]:
    """Extract the table names with a vocabulary_table: true property."""
    return {
        table_name
        for (table_name, table_config) in config.get("tables", {}).items()
        if get_flag(table_config, "vocabulary_table")
    }


def get_ignored_table_names(config: Mapping) -> set[str]:
    """Extract the table names with a ignore: true property."""
    return {
        table_name
        for (table_name, table_config) in config.get("tables", {}).items()
        if get_flag(table_config, "ignore")
    }


def get_columns_assigned(
    row_generator_config: Mapping[str, Any]
) -> Generator[str, None, None]:
    """
    Get the columns assigned in a ``row_generators[n]`` stanza.

    :param generator_config: The ``row_generators[n]`` stanza itself.
    """
    ca = row_generator_config.get("columns_assigned", None)
    if ca is None:
        return
    if isinstance(ca, str):
        yield ca
        return
    if not hasattr(ca, "__iter__"):
        return
    for c in ca:
        yield str(c)


def get_row_generators(
    table_config: Mapping[str, Any],
) -> Generator[tuple[str, Mapping[str, Any]], None, None]:
    """
    Get the row generators from a table configuration.

    :param table_config: The element from the ``tables:`` stanza of ``config.xml``.
    :return: Pair of (name, row generator config).
    """
    rgs = table_config.get("row_generators", None)
    if isinstance(rgs, str) or not hasattr(rgs, "__iter__"):
        return
    for rg in rgs:
        name = rg.get("name", None)
        if name:
            yield (name, rg)


_alphanumeric_re = re.compile(r"[^a-zA-Z0-9]")


def normalize_table_name(table_name: str) -> str:
    """Remove non alphanumeric characters from table name."""
    name = _alphanumeric_re.sub("_", table_name)
    if not name or not name[0].isalpha():
        return "_" + name
    return name


def make_foreign_key_name(table_name: str, col_name: str) -> str:
    """Make a suitable foreign key name."""
    ntn = normalize_table_name(table_name)
    name = f"{ntn}_{col_name}_fkey"
    # really this should be max_identifier_length in the sqlalchemy dialect
    if len(name) < 64:
        return name
    rand = "".join(random.choice(string.ascii_letters) for _ in range(6))
    return f"{ntn[:24]}_{col_name[:24]}_{rand}_fkey"


def make_primary_key_name(table_name: str) -> str:
    """Make a suitable primary key name."""
    return f"{normalize_table_name(table_name)}_primary_key"


def remove_vocab_foreign_key_constraints(
    metadata: MetaData,
    config: Mapping[str, Any],
    dst_engine: Union[Connection, Engine],
) -> None:
    """
    Remove the foreign key constraints from vocabulary tables.

    This allows vocabulary tables to be loaded without worrying about
    topologically sorting them or circular dependencies.

    :param metadata: The SQLAlchemy metadata from ``orm.yaml``.
    :param config: The ``config.yaml`` object.
    :param dst_engine: The destination database or a connection to it.
    """
    vocab_tables = get_vocabulary_table_names(config)
    for vocab_table_name in vocab_tables:
        vocab_table = metadata.tables[vocab_table_name]
        for fk in vocab_table.foreign_key_constraints:
            logger.debug(
                "Dropping constraint %s from table %s", fk.name, vocab_table_name
            )
            with Session(dst_engine) as session:
                session.begin()
                try:
                    session.execute(DropConstraint(fk))
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    logger.exception(
                        "Dropping table %s key constraint %s failed:",
                        vocab_table_name,
                        fk.name,
                    )
                except ProgrammingError as e:
                    session.rollback()
                    # pylint: disable=no-member
                    if isinstance(e.orig, psycopg2.errors.UndefinedObject):
                        logger.debug("Constraint does not exist")
                    else:
                        raise e


def reinstate_vocab_foreign_key_constraints(
    metadata: MetaData,
    meta_dict: Mapping[str, Any],
    config: Mapping[str, Any],
    dst_engine: Union[Connection, Engine],
) -> None:
    """
    Put the removed foreign keys back into the destination database.

    :param metadata: The SQLAlchemy metadata for the destination database.
    :param meta_dict: The ``orm.yaml`` configuration that ``metadata`` was
    created from.
    :param config: The ``config.yaml`` data.
    :param dst_engine: The connection to the destination database.
    """
    vocab_tables = get_vocabulary_table_names(config)
    for vocab_table_name in vocab_tables:
        vocab_table = metadata.tables[vocab_table_name]
        try:
            for column_name, column_dict in meta_dict["tables"][vocab_table_name][
                "columns"
            ].items():
                fk_targets = column_dict.get("foreign_keys", [])
                if fk_targets:
                    fk = ForeignKeyConstraint(
                        columns=[column_name],
                        name=make_foreign_key_name(vocab_table_name, column_name),
                        refcolumns=fk_targets,
                    )
                    logger.debug("Restoring foreign key constraint %s", fk.name)
                    with Session(dst_engine) as session:
                        session.begin()
                        vocab_table.append_constraint(fk)
                        session.execute(AddConstraint(fk))
                        session.commit()
        except IntegrityError:
            logger.exception(
                "Restoring table %s foreign keys failed:", vocab_table_name
            )


def stream_yaml(yaml_file_handle: io.TextIOBase) -> Generator[Any, None, None]:
    """
    Stream a yaml list into an iterator.

    Used instead of yaml.load(yaml_path) when the file is
    known to be a list and the file might be too long to
    be decoded in memory.
    """
    buf = ""
    while True:
        line = yaml_file_handle.readline()
        if not line or line.startswith("-"):
            if buf:
                yl = yaml.load(buf, yaml.Loader)
                assert isinstance(yl, Sequence) and len(yl) == 1
                yield yl[0]
            if not line:
                return
            buf = ""
        buf += line


def topological_sort(
    input_nodes: Iterable[T], get_dependencies_fn: Callable[[T], set[T]]
) -> tuple[list[T], list[list[T]]]:
    """
    Topoligically sort input_nodes and find any cycles.

    Returns a pair ``(sorted, cycles)``.

    ``sorted`` is a list of all the elements of input_nodes sorted
    so that dependencies returned by get_dependencies_fn
    come after nodes that depend on them. Cycles are
    arbitrarily broken for this.

    ``cycles`` is a list of lists of dependency cycles.

    :param input_nodes: an iterator of nodes to sort. Duplicates
    are discarded.
    :param get_dependencies_fn: a function that takes an input
    node and returns a list of its dependencies. Any
    dependencies not in the input_nodes list are ignored.
    """
    # input nodes
    white = set(input_nodes)
    # output nodes
    black = []
    # list of cycles
    cycles = []
    while white:
        w = white.pop()
        # stack of dependencies under consideration
        grey = [w]
        # nextss[i] are the dependencies of grey[i] yet to be considered
        nextss = [get_dependencies_fn(w)]
        while grey:
            if not nextss[-1]:
                black.append(grey.pop())
                nextss.pop()
            else:
                n = nextss[-1].pop()
                if n in white:
                    # n is unconsidered, move it to the grey stack
                    white.remove(n)
                    grey.append(n)
                    nextss.append(get_dependencies_fn(n))
                elif n in grey:
                    # n is in a cycle
                    cycle_start = grey.index(n)
                    cycles.append(grey[cycle_start : len(grey)])
    return (black, cycles)


def sorted_non_vocabulary_tables(metadata: MetaData, config: Mapping) -> list[Table]:
    """
    Get the list of non-vocabulary tables, topologically sorted.

    :param metadata: SQLAlchemy database description.
    :param config: The ``config.yaml`` object.
    :return: The list of non-vocabulary tables, ordered such that the targets
    of all the foreign keys come before their sources.
    """
    table_names = set(metadata.tables.keys()).difference(
        get_vocabulary_table_names(config)
    )
    (sorted_tables, cycles) = topological_sort(
        table_names, lambda tn: get_related_table_names(metadata.tables[tn])
    )
    for cycle in cycles:
        logger.warning("Cycle detected between tables: %s", cycle)
    return [metadata.tables[tn] for tn in sorted_tables]


def generated_tables(metadata: MetaData, config: Mapping) -> list[Table]:
    """
    Get all the non-ignored, non-vocabulary tables.

    :param metadata: MetaData of the database.
    :param config: Mapping from `config.yaml`.
    :return: All the non-ignored, non-vocabulary tables.
    """
    not_for_output = get_vocabulary_table_names(config) | get_ignored_table_names(
        config
    )
    return [
        table for table in metadata.tables.values() if table.name not in not_for_output
    ]


def underline_error(e: SyntaxError) -> str:
    r"""
    Make an underline for this error.

    :return: string beginning ``\n`` then spaces then ``^^^^``
    underlining the error, or a null string if this was not possible.
    """
    start = e.offset
    if start is None:
        return ""
    end = e.end_offset
    if end is None or end <= start:
        end = start + 1
    return "\n" + " " * start + "^" * (end - start)


def generators_require_stats(config: Mapping) -> bool:
    """
    Test if the generator references ``SRC_STATS``.

    :param config: ``config.yaml`` object.
    :return: True if any of the arguments for any of the generators
    reference ``SRC_STATS``.
    """
    ois = {
        f"object_instantiation.{k}": call
        for k, call in config.get("object_instantiation", {}).items()
    }
    sgs = {
        f"story_generators[{n}]": call
        for n, call in enumerate(config.get("story_generators", []))
    }
    table_calls = {
        f"tables.{table_name}.{call_type}[{n}]": call
        for table_name, table in config.get("tables", {}).items()
        for call_type in ("row_generators", "missingness_generators")
        for n, call in enumerate(table.get(call_type, []))
    }
    errors = []
    stats_required = False
    for where, call in (ois | sgs | table_calls).items():
        for n, arg in enumerate(call.get("args", [])):
            try:
                names = (
                    node.id
                    for node in ast.walk(ast.parse(arg))
                    if isinstance(node, ast.Name)
                )
                if any(name == "SRC_STATS" for name in names):
                    stats_required = True
            except SyntaxError as e:
                errors.append(
                    (
                        "Syntax error in argument %d of %s: %s\n%s%s",
                        n + 1,
                        where,
                        e.msg,
                        arg,
                        underline_error(e),
                    )
                )
        for k, arg in call.get("kwargs", {}).items():
            if isinstance(arg, str):
                try:
                    names = (
                        node.id
                        for node in ast.walk(ast.parse(arg))
                        if isinstance(node, ast.Name)
                    )
                    if any(name == "SRC_STATS" for name in names):
                        stats_required = True
                except SyntaxError as e:
                    errors.append(
                        (
                            "Syntax error in argument %s of %s: %s\n%s%s",
                            k,
                            where,
                            e.msg,
                            arg,
                            underline_error(e),
                        )
                    )
    for error in errors:
        logger.error(*error)
    return stats_required


def split_column_full_name(col_fullname: str) -> tuple[str, str]:
    """
    Split a column fullname into table and column.

    :param col_fn: The string, such as ``artist.artist_id`` or ``artist.parquet.artist_id``.
    :return: A pair of strings; the table name and the column name. For example
    ``("artist.parquet", "artist_id")``. If there is no ``.`` in ``col_fullname``
    ``(None, col_fullname)`` will be returned.
    """
    name_parts = col_fullname.split(".")
    return (
        ".".join(name_parts[:-1]),
        name_parts[-1],
    )
