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

from sqlsynthgen.create import create_db_data, create_db_tables, create_db_vocab
from sqlsynthgen.make import make_generators_from_tables
from sqlsynthgen.settings import get_settings

app = typer.Typer()


def import_file(file_path: str) -> ModuleType:
    """Import a file given a relative path."""
    file_path_path = Path(file_path)
    module_path = ".".join(file_path_path.parts[:-1] + (file_path_path.stem,))
    return import_module(module_path)


def read_yaml_file(path: str) -> Any:
    """Read a yaml file in to dictionary, given a path."""
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
def create_vocab(ssg_file: str = typer.Argument(...)) -> None:
    """Create tables using the SQLAlchemy file."""
    ssg_module = import_file(ssg_file)
    create_db_vocab(ssg_module.sorted_vocab)


@app.command()
def create_tables(orm_file: str = typer.Argument(...)) -> None:
    # """Create tables using the SQLAlchemy file."""
    """
    Create tables using the SQLAlchemy file.

    :param orm_file: Mandatory path to SQLAlchemy file
    :type kind: str
    :raise lumache.InvalidKindError: If the kind is invalid.
    :return: None
    :rtype: None

    """
    orm_module = import_file(orm_file)
    create_db_tables(orm_module.Base.metadata)


@app.command()
def make_generators(
    orm_file: str = typer.Argument(...),
    config_file: Optional[str] = typer.Argument(None),
) -> None:
    """Make a SQLSynthGen file of generator classes."""
    orm_module = import_file(orm_file)
    generator_config = read_yaml_file(config_file) if config_file is not None else {}
    result = make_generators_from_tables(orm_module, generator_config)
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

@app.command()
def print_int(
    phone: int = typer.Argument(...),
) -> None:
    """
    Print argument on screen

    :param message: Text to be printed
    :type message: str
    :raise TypeError: If the phone is invalid.
    :return: None
    :rtype: None

    """
    if type(phone) is int:
        print("My phone: [{}]".format(phone))
    else:
        raise TypeError("Argument `phone` has to be of type integer")

if __name__ == "__main__":
    app()
