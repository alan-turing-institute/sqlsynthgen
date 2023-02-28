"""Entrypoint for the SQLSynthGen package."""
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Optional

import typer
import yaml

from sqlsynthgen.create import create_db_data, create_db_tables, create_db_vocab
from sqlsynthgen.make import make_generators_from_tables, make_tables_file
from sqlsynthgen.settings import get_settings

app = typer.Typer()


def import_file(file_path: str) -> ModuleType:
    """Import a file.

    This utility function returns
    the file at file_path as a module

    Args:
        file_path (str): Path to file to be imported

    Returns:
        ModuleType
    """
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
    num_passes: int = typer.Argument(...),
) -> None:
    """Populate schema with synthetic data.

    This CLI command generates synthetic data for
    Python table structures, and inserts these rows
    into a destination schema.

    Also takes as input object relational model as represented
    by file containing Python classes and its attributes.

    Takes as input sqlsynthgen output as represented by Python
    classes, its attributes and methods for generating values
    for those attributes.

    Final input is the number of rows required.

    Example:
        $ sqlsynthgen create-data example_orm.py expected_ssg.py 100

    Args:
        orm_file (str): Path to object relational model.
        ssg_file (str): Path to sqlsynthgen output.
        num_passes (int): Number of passes to make.

    Returns:
        None
    """
    orm_module = import_file(orm_file)
    ssg_module = import_file(ssg_file)
    create_db_data(
        orm_module.Base.metadata.sorted_tables, ssg_module.sorted_generators, num_passes
    )


@app.command()
def create_vocab(ssg_file: str = typer.Argument(...)) -> None:
    """Create tables using the SQLAlchemy file."""
    ssg_module = import_file(ssg_file)
    create_db_vocab(ssg_module.sorted_vocab)


@app.command()
def create_tables(orm_file: str = typer.Argument(...)) -> None:
    """Create schema from Python classes.

    This CLI command creates Postgresql schema using object relational model
    declared as Python tables. (eg.)

    Example:
        $ sqlsynthgen create-tables example_orm.py

    Args:
        orm_file (str): Path to Python tables file.

    Returns:
        None

    """
    orm_module = import_file(orm_file)
    create_db_tables(orm_module.Base.metadata)


@app.command()
def make_generators(
    orm_file: str = typer.Argument(...),
    config_file: Optional[str] = typer.Argument(None),
) -> None:
    """Make a SQLSynthGen file of generator classes.

    This CLI command takes an object relation model output by sqlcodegen and
    returns a set of synthetic data generators for each attribute

    Example:
        $ sqlsynthgen make-generators example_orm.py

    Args:
        orm_file (str): Path to Python tables file.
        config_file (str): Path to configuration file.
    """
    orm_module = import_file(orm_file)
    generator_config = read_yaml_file(config_file) if config_file is not None else {}
    result = make_generators_from_tables(orm_module, generator_config)
    print(result)


@app.command()
def make_tables() -> None:
    """Make a SQLAlchemy file of Table classes.

    This CLI command deploys sqlacodegen to discover a
    schema structure, and generates an object relational model declared
    as Python classes.

    Example:
        $ sqlsynthgen make_tables
    """
    settings = get_settings()

    make_tables_file(str(settings.src_postgres_dsn), settings.src_schema)


if __name__ == "__main__":
    app()
