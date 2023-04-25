"""Entrypoint for the SQLSynthGen package."""
import sys
from pathlib import Path
from sys import stderr
from typing import Final, Optional

import typer
import yaml

from sqlsynthgen.create import create_db_data, create_db_tables, create_db_vocab
from sqlsynthgen.make import (
    make_generators_from_tables,
    make_src_stats,
    make_tables_file,
)
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import import_file, read_yaml_file

ORM_FILENAME: Final[str] = "orm.py"
SSG_FILENAME: Final[str] = "ssg.py"
STATS_FILENAME: Final[str] = "src-stats.yaml"

app = typer.Typer()


@app.command()
def create_data(
    orm_file: str = typer.Option(ORM_FILENAME),
    ssg_file: str = typer.Option(SSG_FILENAME),
    num_passes: int = typer.Option(1),
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
        $ sqlsynthgen create-data

    Args:
        orm_file (str): Name of Python ORM file.
          Must be in the current working directory.
        ssg_file (str): Name of generators file.
          Must be in the current working directory.
        num_passes (int): Number of passes to make.
    """
    orm_module = import_file(orm_file)
    ssg_module = import_file(ssg_file)
    create_db_data(
        orm_module.Base.metadata.sorted_tables, ssg_module.generator_dict, num_passes
    )


@app.command()
def create_vocab(ssg_file: str = typer.Option(SSG_FILENAME)) -> None:
    """Create tables using the SQLAlchemy file.

    Example:
        $ sqlsynthgen create-vocab

    Args:
        ssg_file (str): Name of generators file.
          Must be in the current working directory.
    """
    ssg_module = import_file(ssg_file)
    create_db_vocab(ssg_module.vocab_dict)


@app.command()
def create_tables(orm_file: str = typer.Option(ORM_FILENAME)) -> None:
    """Create schema from Python classes.

    This CLI command creates Postgresql schema using object relational model
    declared as Python tables. (eg.)

    Example:
        $ sqlsynthgen create-tables

    Args:
        orm_file (str): Name of Python ORM file.
          Must be in the current working directory.
    """
    orm_module = import_file(orm_file)
    create_db_tables(orm_module.Base.metadata)


@app.command()
def make_generators(
    orm_file: str = typer.Option(ORM_FILENAME),
    ssg_file: str = typer.Option(SSG_FILENAME),
    config_file: Optional[str] = typer.Option(None),
    stats_file: Optional[str] = typer.Option(None),
    force: bool = typer.Option(False, "--force", "--f"),
) -> None:
    """Make a SQLSynthGen file of generator classes.

    This CLI command takes an object relation model output by sqlcodegen and
    returns a set of synthetic data generators for each attribute

    Example:
        $ sqlsynthgen make-generators

    Args:
        orm_file (str): Name of Python ORM file.
          Must be in the current working directory.
        ssg_file (str): Path to write the generators file to.
        config_file (str): Path to configuration file.
        stats_file (str): Path to source stats file (output of make-stats).
    """
    ssg_file_path = Path(ssg_file)
    if ssg_file_path.exists() and not force:
        print(f"{ssg_file} should not already exist. Exiting...", file=stderr)
        sys.exit(1)

    orm_module = import_file(orm_file)
    generator_config = read_yaml_file(config_file) if config_file is not None else {}
    result = make_generators_from_tables(orm_module, generator_config, stats_file)

    ssg_file_path.write_text(result, encoding="utf-8")


@app.command()
def make_stats(
    config_file: str = typer.Option(...),
    stats_file: str = typer.Option(STATS_FILENAME),
) -> None:
    """Compute summary statistics from the source database, write them to a YAML file.

    Example:
        $ sqlsynthgen make_stats --config-file=example_config.yaml
    """
    stats_file_path = Path(stats_file)
    if stats_file_path.exists():
        print(f"{stats_file} should not already exist. Exiting...", file=stderr)
        sys.exit(1)
    settings = get_settings()
    generator_config = read_yaml_file(config_file) if config_file is not None else {}
    src_dsn = settings.src_postgres_dsn
    if src_dsn is None:
        raise ValueError("Missing source database connection details.")
    src_stats = make_src_stats(src_dsn, generator_config)
    stats_file_path.write_text(yaml.dump(src_stats), encoding="utf-8")


@app.command()
def make_tables(
    orm_file: str = typer.Option(ORM_FILENAME),
) -> None:
    """Make a SQLAlchemy file of Table classes.

    This CLI command deploys sqlacodegen to discover a
    schema structure, and generates an object relational model declared
    as Python classes.

    Example:
        $ sqlsynthgen make_tables

    Args:
        orm_file (str): Path to write the Python ORM file.
    """
    orm_file_path = Path(orm_file)
    if orm_file_path.exists():
        print(f"{orm_file} should not already exist. Exiting...", file=stderr)
        sys.exit(1)

    settings = get_settings()

    content = make_tables_file(settings.src_postgres_dsn, settings.src_schema)  # type: ignore
    orm_file_path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    app()
