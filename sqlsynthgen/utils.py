"""Utility functions."""
import os
import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml
from pydantic import PostgresDsn
from sqlalchemy import create_engine, event, select


def read_yaml_file(path: str) -> Any:
    """Read a yaml file in to dictionary, given a path."""
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
    return config


def import_file(file_name: str) -> ModuleType:
    """Import a file.

    This utility function returns file_name imported as a module.

    Args:
        file_name (str): The name of a file in the current working directory.

    Returns:
        ModuleType
    """
    module_name = file_name[:-3]

    sys.path.append(os.getcwd())

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


def create_engine_with_search_path(postgres_dsn: PostgresDsn, schema_name: str) -> Any:
    """Create a SQLAlchemy Engine with an explicitly set schema."""
    engine = create_engine(postgres_dsn)

    @event.listens_for(engine, "connect", insert=True)
    def connect(dbapi_connection: Any, _: Any) -> None:
        set_search_path(dbapi_connection, schema_name)

    return engine


def set_search_path(connection: Any, schema: str) -> None:
    """Set the SEARCH_PATH for a PostgreSQL connection."""
    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path
    existing_autocommit = connection.autocommit
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute(f'SET search_path to "{schema}";')
    cursor.close()

    connection.autocommit = existing_autocommit
