"""Utility functions."""
import csv
import os
import sys
from importlib import import_module
from pathlib import Path
from sys import stderr
from types import ModuleType
from typing import Any, Optional

import yaml
from sqlalchemy import select


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


def download_table(table: Any, engine: Any, schema: Optional[Any] = None) -> None:
    """Download a Table and store it as a .csv file."""
    csv_file_name = table.fullname + ".csv"
    csv_file_path = Path(csv_file_name)
    if csv_file_path.exists():
        print(f"{str(csv_file_name)} already exists. Exiting...", file=stderr)
        sys.exit(1)

    stmt = select([table])
    with engine.connect() as conn:
        if schema:
            conn.execute(f"SET SEARCH_PATH TO {schema}")
        result = list(conn.execute(stmt))

    with csv_file_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow([x.name for x in table.columns])
        for row in result:
            writer.writerow(row)


def set_search_path(connection: Any, schema: str) -> None:
    """Set the SEARCH_PATH for a PostgreSQL connection."""
    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path
    existing_autocommit = connection.autocommit
    connection.autocommit = True

    cursor = connection.cursor()
    cursor.execute(f"SET search_path to {schema};")
    cursor.close()

    connection.autocommit = existing_autocommit
