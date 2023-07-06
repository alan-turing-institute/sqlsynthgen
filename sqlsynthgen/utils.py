"""Utility functions."""
import os
import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Optional

import yaml
from sqlalchemy import create_engine, event, select
from sqlalchemy.ext.asyncio import create_async_engine


def read_yaml_file(path: str) -> Any:
    """Read a yaml file in to dictionary, given a path."""
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
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


def download_table(table: Any, engine: Any, yaml_file_name: str) -> None:
    """Download a Table and store it as a .yaml file."""
    stmt = select([table])
    with engine.connect() as conn:
        result = [dict(row) for row in conn.execute(stmt)]

    with Path(yaml_file_name).open("w", newline="", encoding="utf-8") as yamlfile:
        yamlfile.write(yaml.dump(result))


def create_db_engine(
    db_dsn: str,
    schema_name: Optional[str] = None,
    use_asyncio: bool = False,
    **kwargs: dict,
) -> Any:
    """Create a SQLAlchemy Engine."""
    if use_asyncio:
        async_dsn = db_dsn.replace("postgresql://", "postgresql+asyncpg://")
        engine = create_async_engine(async_dsn, **kwargs)
        event_engine = engine.sync_engine
    else:
        engine = create_engine(db_dsn, **kwargs)
        event_engine = engine

    if schema_name is not None:

        @event.listens_for(event_engine, "connect", insert=True)
        def connect(dbapi_connection: Any, _: Any) -> None:
            set_search_path(dbapi_connection, schema_name)  # type: ignore

    return engine


def set_search_path(connection: Any, schema: str) -> None:
    """Set the SEARCH_PATH for a PostgreSQL connection."""
    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path
    existing_autocommit = connection.autocommit
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute("SET search_path to %s;", (schema,))
    cursor.close()

    connection.autocommit = existing_autocommit
