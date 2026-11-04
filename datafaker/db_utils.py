"""Utility functions."""
import gzip
import io
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Optional, Union

import sqlalchemy.dialects
import yaml

# pylint: disable=no-name-in-module
from psycopg2.errors import UndefinedObject  # ty: ignore[unresolved-import]
from sqlalchemy import Connection, Engine, ForeignKey, create_engine, event, select
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import (
    IntegrityError,
    NoReferencedTableError,
    NoSuchModuleError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.schema import (
    AddConstraint,
    ColumnCollectionConstraint,
    DropConstraint,
    ForeignKeyConstraint,
    MetaData,
    Table,
)
from sqlalchemy.types import Integer, Numeric, TypeEngine
from typer import Exit

from datafaker.parquet2orm import ParquetTable, find_parquet_tables
from datafaker.utils import (
    T,
    get_ignored_table_names,
    get_vocabulary_table_names,
    logger,
    make_async_dsn,
    make_foreign_key_name,
    unqualify_fk_target,
)

# Define some types used repeatedly in the code base
MaybeAsyncEngine = Union[Engine, AsyncEngine]

# After every how many rows of vocab table downloading do we see a
# progres update
MAKE_VOCAB_PROGRESS_REPORT_EVERY = 10000


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


def get_dialect(dsn: str) -> sqlalchemy.engine.interfaces.Dialect:
    """Get the SQLAlchemy dialect from a DSN string."""
    url = sqlalchemy.engine.make_url(dsn)
    backend = url.get_backend_name()
    dialect_cls: type[
        sqlalchemy.engine.interfaces.Dialect
    ] = sqlalchemy.dialects.registry.load(backend)
    url.get_backend_name()
    return dialect_cls()


def get_sync_engine(engine: MaybeAsyncEngine) -> Engine:
    """Given an SQLAlchemy engine that may or may not be async return one that isn't."""
    if isinstance(engine, AsyncEngine):
        return engine.sync_engine
    return engine


def create_db_engine(
    db_dsn: str,
    schema_name: Optional[str] = None,
    use_asyncio: bool = False,
    parquet_dir: Optional[Union[Path, str]] = None,
    **kwargs: Any,
) -> MaybeAsyncEngine:
    """Create a SQLAlchemy Engine."""
    kwargs.setdefault("pool_pre_ping", True)
    try:
        if use_asyncio:
            engine: MaybeAsyncEngine = create_async_engine(
                make_async_dsn(db_dsn), **kwargs
            )
        else:
            engine = create_engine(db_dsn, **kwargs)
    except NoSuchModuleError as exc:
        logger.error("Failed to connect to the database: %s", exc)
        logger.error("Perhaps the dialect '%s' is invalid.", db_dsn.split(":")[0])
        raise Exit(1) from exc
    except ModuleNotFoundError as exc:
        logger.error("Failed to connect to the database: %s", exc)
        raise Exit(1) from exc
    except ValueError as exc:
        logger.error("DSN %s is malformed: %s", db_dsn, exc)
        raise Exit(1) from exc

    settings: list[str] = []
    dialect_name = get_sync_engine(engine).dialect.name
    if schema_name is not None:
        match dialect_name:
            case "mssql":
                engine = engine.execution_options(
                    schema_translate_map={None: schema_name}
                )
            case "snowflake":
                settings.append(f"USE SCHEMA {schema_name};")
            case _:
                settings.append(f"SET search_path TO {schema_name};")
    dataset_views: list[str] = []
    if parquet_dir is not None:
        match dialect_name:
            case "duckdb":
                tables = find_parquet_tables(parquet_dir)
                # double up single quotes
                dj = str(parquet_dir).replace("'", "''")
                settings.append(f"SET file_search_path TO '{dj}';")
                dataset_views = [_dataset_view_sql(t) for t in tables if t.hive_files()]
            case _:
                tables = []
                logger.error(
                    "--parquet-dir cannot be used with %s, only duckdb.", dialect_name
                )

    if settings or dataset_views:
        event_engine = get_sync_engine(engine)

        @event.listens_for(event_engine, "connect", insert=True)
        def connect(dbapi_connection: DBAPIConnection, _: Any) -> None:
            set_db_settings(dbapi_connection, settings)
            create_dataset_views(dbapi_connection, dataset_views)

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


def get_metadata(
    engine: Engine,
    schema_name: Optional[str],
    views_to_tables: bool,
) -> MetaData:
    """Get the MetaData object associated with the engine passed."""
    md = MetaData()
    try:
        md.reflect(engine, schema=schema_name, views=views_to_tables)
    except OperationalError as exc:
        logger.error("Cannot connect to database: %s", exc)
        raise Exit(1) from exc
    return md


def _sql_quote(text: str, quote: str) -> str:
    """Enclose ``text`` in ``quote``, doubling any ``quote`` inside it."""
    return f"{quote}{text.replace(quote, quote * 2)}{quote}"


def _dataset_view_sql(table: ParquetTable) -> str:
    """Make the SQL that exposes a directory of parquet files as one table."""
    files = ",".join(_sql_quote(str(f), "'") for f in table.hive_files())
    return (
        f"CREATE OR REPLACE TEMP VIEW {_sql_quote(table.name, chr(34))} AS "
        f"SELECT * FROM read_parquet([{files}], "
        "hive_partitioning=true, union_by_name=true)"
    )


def create_dataset_views(
    connection: DBAPIConnection, view_statements: Iterable[str]
) -> None:
    """
    Make each partitioned parquet dataset available as a table.

    The views are temporary so nothing is written into the source database.
    """
    cursor = connection.cursor()
    for statement in view_statements:
        cursor.execute(statement)
    cursor.close()


def set_db_settings(connection: DBAPIConnection, settings: list[str]) -> None:
    """Run commands."""
    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path
    existing_autocommit = connection.autocommit
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("\n".join(settings))
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


def is_numeric_sql_type(column_type: TypeEngine | None) -> bool:
    """Whether a SQLAlchemy column type is Numeric or Integer."""
    return isinstance(column_type, (Numeric, Integer))


def fk_refers_to_ignored_table(fk: ForeignKey) -> bool:
    """
    Test if this foreign key refers to an ignored table.

    :param fk: The foreign key to test.
    :return: True if the table referred to is ignored in ``config.yaml``.
    """
    try:
        fk.column
    except NoReferencedTableError:
        return True
    return False


def constraint_name(constraint: ColumnCollectionConstraint) -> str:
    """Get the constraint name, synthesising it if it does not exist explicitly."""
    name = constraint.name
    if isinstance(name, str):
        return name
    joined = "_".join(constraint.columns.keys())
    kind = constraint.__visit_name__.split("_", 1)[0]
    return f"{joined}_{kind}"


def fk_constraint_refers_to_ignored_table(fk: ForeignKeyConstraint) -> bool:
    """
    Test if the constraint refers to a table marked as ignored in ``config.yaml``.

    :param fk: The foreign key constraint.
    :return: True if ``fk`` refers to an ignored table.
    """
    try:
        fk.referred_table
    except NoReferencedTableError:
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
                    if isinstance(e.orig, UndefinedObject):
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
                    table_names = frozenset(meta_dict.get("tables", {}).keys())
                    fk = ForeignKeyConstraint(
                        columns=[column_name],
                        name=make_foreign_key_name(vocab_table_name, column_name),
                        refcolumns=[
                            unqualify_fk_target(t, table_names) for t in fk_targets
                        ],
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
    Get the list of non-vocabulary non-ignored tables, topologically sorted.

    :param metadata: SQLAlchemy database description.
    :param config: The ``config.yaml`` object.
    :return: The list of non-vocabulary non-ignored tables, ordered such that
      the targets of all the foreign keys come before their sources.
    """
    table_names = set(metadata.tables.keys()).difference(
        get_vocabulary_table_names(config) | get_ignored_table_names(config)
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
