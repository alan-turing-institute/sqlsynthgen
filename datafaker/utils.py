"""Utility functions."""
import json
import logging
import sys
from importlib import import_module
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Mapping, Optional, Union
import gzip

import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from psycopg2.errors import UndefinedObject
import sqlalchemy
from sqlalchemy import (
    Connection,
    Engine,
    create_engine,
    event,
    select,
)
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
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def open_file(file_name):
    return Path(file_name).open("wb")


def open_compressed_file(file_name):
    return gzip.GzipFile(file_name, "wb")


def table_row_count(table: Table, conn: Connection) -> int:
    return conn.execute(
        select(sqlalchemy.func.count()).select_from(sqlalchemy.table(
            table.name,
            *[sqlalchemy.column(col.name) for col in table.primary_key.columns.values()],
        ))
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
                result = {
                    str(col_name): value
                    for (col_name, value) in row.items()
                }
                yamlfile.write(yaml.dump([result]).encode())
                count += 1
                if count % MAKE_VOCAB_PROGRESS_REPORT_EVERY == 0:
                    logger.info(
                        "written row %d of %d, %.1f%%",
                        count,
                        rowcount,
                        100*count/rowcount,
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
logger = logging.getLogger(__name__)


def info_or_lower(record: logging.LogRecord) -> bool:
    """Allow records with level of INFO or lower."""
    return record.levelno in (logging.DEBUG, logging.INFO)


def warning_or_higher(record: logging.LogRecord) -> bool:
    """Allow records with level of WARNING or higher."""
    return record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL)


def conf_logger(verbose: bool) -> None:
    """Configure the logger."""
    # Note that this function modifies the global `logger`.
    log_format = "%(message)s"

    # info will always be printed to stdout
    # debug will be printed to stdout only if verbose=True
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter(log_format))
    stdout_handler.addFilter(info_or_lower)

    # warning and error will always be printed to stderr
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter(log_format))
    stderr_handler.addFilter(warning_or_higher)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        handlers=[stdout_handler, stderr_handler],
    )


def get_flag(maybe_dict, key):
    """Returns maybe_dict[key] or False if that doesn't exist"""
    return type(maybe_dict) is dict and maybe_dict.get(key, False)


def get_property(maybe_dict, key, default):
    """Returns maybe_dict[key] or default if that doesn't exist"""
    return maybe_dict.get(key, default) if type(maybe_dict) is dict else default


def get_related_table_names(table: Table) -> set[str]:
    """
    Get the names of all tables for which there exist foreign keys from this table.
    """
    return {
        str(fk.referred_table.name)
        for fk in table.foreign_key_constraints
    }


def table_is_private(config: Mapping, table_name: str) -> bool:
    """
    Return True if the table with name table_name is a primary private table
    according to config.
    """
    ts = config.get("tables", {})
    if type(ts) is not dict:
        return False
    t = ts.get(table_name, {})
    return t.get("primary_private", False)


def primary_private_fks(config: Mapping, table: Table) -> list[str]:
    """
    Returns the list of columns in the table that refer to primary private tables.

    A table that is not primary private but has a non-empty list of
    primary_private_fks is secondary private.
    """
    return [
        str(fk.referred_table.name)
        for fk in table.foreign_key_constraints
        if table_is_private(config, str(fk.referred_table.name))
    ]


def get_vocabulary_table_names(config: Mapping) -> set[str]:
    """
    Extract the table names with a vocabulary_table: true property.
    """
    return {
        table_name
        for (table_name, table_config) in config.get("tables", {}).items()
        if get_flag(table_config, "vocabulary_table")
    }


def make_foreign_key_name(table_name: str, col_name: str) -> str:
    return f"{table_name}_{col_name}_fkey"


def remove_vocab_foreign_key_constraints(metadata, config, dst_engine):
    vocab_tables = get_vocabulary_table_names(config)
    for vocab_table_name in vocab_tables:
        vocab_table = metadata.tables[vocab_table_name]
        for fk in vocab_table.foreign_key_constraints:
            logger.debug("Dropping constraint %s from table %s", fk.name, vocab_table_name)
            with Session(dst_engine) as session:
                session.begin()
                try:
                    session.execute(DropConstraint(fk))
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    logger.exception("Dropping table %s key constraint %s failed:", vocab_table_name, fk.name)
                except ProgrammingError as e:
                    session.rollback()
                    if type(e.orig) is UndefinedObject:
                        logger.debug("Constraint does not exist")
                    else:
                        raise e


def reinstate_vocab_foreign_key_constraints(metadata, meta_dict, config, dst_engine):
    vocab_tables = get_vocabulary_table_names(config)
    for vocab_table_name in vocab_tables:
        vocab_table = metadata.tables[vocab_table_name]
        try:
            for (column_name, column_dict) in meta_dict["tables"][vocab_table_name]["columns"].items():
                fk_targets = column_dict.get("foreign_keys", [])
                if fk_targets:
                    fk = ForeignKeyConstraint(
                        columns=[column_name],
                        name=make_foreign_key_name(vocab_table_name, column_name),
                        refcolumns=fk_targets,
                    )
                    logger.debug(f"Restoring foreign key constraint {fk.name}")
                    with Session(dst_engine) as session:
                        session.begin()
                        vocab_table.append_constraint(fk)
                        session.execute(AddConstraint(fk))
                        session.commit()
        except IntegrityError:
            logger.exception("Restoring table %s foreign keys failed:", vocab_table_name)


def stream_yaml(yaml_file_handle):
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
                assert type(yl) is list and len(yl) == 1
                yield yl[0]
            if not line:
                return
            buf = ""
        buf += line


def topological_sort(input_nodes, get_dependencies_fn):
    """
    Topoligically sort input_nodes and find any cycles.

    Returns a pair (sorted, cycles).
    
    'sorted' is a list of all the elements of input_nodes sorted
    so that dependencies returned by get_dependencies_fn
    come after nodes that depend on them. Cycles are
    arbitrarily broken for this.

    'cycles' is a list of lists of dependency cycles.

    arguments:
    input_nodes: an iterator of nodes to sort. Duplicates
    are discarded.
    get_dependencies_fn: a function that takes an input
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
                    cycles.append(grey[cycle_start:len(grey)])
    return (black, cycles)


def sorted_non_vocabulary_tables(metadata: MetaData, config: Mapping) -> list[Table]:
    table_names = set(
        metadata.tables.keys()
    ).difference(
        get_vocabulary_table_names(config)
    )
    (sorted, cycles) = topological_sort(
        table_names,
        lambda tn: get_related_table_names(metadata.tables[tn])
    )
    for cycle in cycles:
        logger.warning(f"Cycle detected between tables: {cycle}")
    return [ metadata.tables[tn] for tn in sorted ]
