"""Utility functions."""
import json
import logging
import os
import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Mapping, Optional, Union

import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from sqlalchemy import Engine, create_engine, event, select
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.schema import MetaData, Table

# Define some types used repeatedly in the code base
MaybeAsyncEngine = Union[Engine, AsyncEngine]


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
    module_name = os.path.splitext(os.path.basename(file_path))[0]

    sys.path.append(os.path.dirname(os.path.abspath(file_path)))

    try:
        module = import_module(module_name)
    finally:
        sys.path.pop()

    return module


def download_table(
    table: Table, engine: Engine, yaml_file_name: Union[str, Path]
) -> None:
    """Download a Table and store it as a .yaml file."""
    stmt = select(table)
    with engine.connect() as conn:
        result = [dict(row) for row in conn.execute(stmt).mappings()]

    with Path(yaml_file_name).open("w", newline="", encoding="utf-8") as yamlfile:
        yamlfile.write(yaml.dump(result))


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


# This is the main logger that the other modules of sqlsynthgen should use for output.
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
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
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

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
