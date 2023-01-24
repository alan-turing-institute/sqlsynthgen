"""Entrypoint for the SQLSynthGen package."""
import sys
from importlib import import_module
from pathlib import Path
from subprocess import CalledProcessError, run
from sys import stderr
from types import ModuleType
from typing import Any, Optional

import typer
import yaml

from sqlsynthgen.create import create_db_data, create_db_tables
from sqlsynthgen.make import make_generators_from_tables
from sqlsynthgen.settings import get_settings

app = typer.Typer()


def import_file(file_path: str) -> ModuleType:
    """Import a file given a relative path."""
    file_path_path = Path(file_path)
    module_path = ".".join(file_path_path.parts[:-1] + (file_path_path.stem,))
    return import_module(module_path)


def read_yaml_file(path: Optional[str]) -> Any:
    """Read a yaml file in to dictionary, given a path.

    If the argument is None, return {}.
    """
    if path is None:
        return {}
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)
    return config


@app.command()
def create_data(
    orm_file: str = typer.Argument(...),
    ssg_file: str = typer.Argument(...),
    num_rows: int = typer.Argument(...),
) -> None:
    """Fill tables with synthetic data."""
    orm_module = import_file(orm_file)
    ssg_module = import_file(ssg_file)
    create_db_data(
        orm_module.Base.metadata.sorted_tables, ssg_module.sorted_generators, num_rows
    )


@app.command()
def create_tables(orm_file: str = typer.Argument(...)) -> None:
    """Create tables using the SQLAlchemy file."""
    orm_module = import_file(orm_file)
    create_db_tables(orm_module.Base.metadata)


@app.command()
def make_generators(
    orm_file: str = typer.Argument(...),
    config_file: Optional[str] = typer.Argument(None),
) -> None:
    """Make a SQLSynthGen file of generator classes."""
    orm_module = import_file(orm_file)
    provider_config = read_yaml_file(config_file)
    result = make_generators_from_tables(orm_module, provider_config)
    print(result)


@app.command()
def make_tables() -> None:
    """Make a SQLAlchemy file of Table classes."""
    settings = get_settings()

    command = ["sqlacodegen"]

    if settings.src_schema:
        command.append(f"--schema={settings.src_schema}")

    command.append(str(get_settings().src_postgres_dsn))

    try:
        completed_process = run(
            command, capture_output=True, encoding="utf-8", check=True
        )
    except CalledProcessError as e:
        print(e.stderr, file=stderr)
        sys.exit(e.returncode)

    print(completed_process.stdout)


if __name__ == "__main__":
    app()
