"""Entrypoint for the SQLSynthGen package."""
import asyncio
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Final, Optional

import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from typer import Option, Typer, echo

from sqlsynthgen.create import create_db_data, create_db_tables, create_db_vocab
from sqlsynthgen.make import make_src_stats, make_table_generators, make_tables_file
from sqlsynthgen.remove import remove_db_data, remove_db_tables, remove_db_vocab
from sqlsynthgen.settings import Settings, get_settings
from sqlsynthgen.utils import import_file, read_yaml_file

# pylint: disable=too-many-arguments

ORM_FILENAME: Final[str] = "orm.py"
SSG_FILENAME: Final[str] = "ssg.py"
STATS_FILENAME: Final[str] = "src-stats.yaml"
CONFIG_SCHEMA_PATH: Final[Path] = (
    Path(__file__).parent / "json_schemas/config_schema.json"
)

app = Typer(no_args_is_help=True)


def _check_file_non_existence(file_path: Path) -> None:
    """Check that a given file does not exist. Exit with an error message if it does."""
    if file_path.exists():
        echo(f"{file_path} should not already exist. Exiting...", err=True)
        sys.exit(1)


def _require_src_db_dsn(settings: Settings) -> str:
    """Return the source DB DSN.

    Check that source DB details have been set. Exit with error message if not.
    """
    if (src_dsn := settings.src_dsn) is None:
        echo("Missing source database connection details.", err=True)
        sys.exit(1)
    return src_dsn


@app.command()
def create_data(
    orm_file: str = Option(ORM_FILENAME),
    ssg_file: str = Option(SSG_FILENAME),
    num_passes: int = Option(1),
    verbose: bool = Option(False, "--verbose", "-v"),
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
        verbose (bool): Be verbose. Default to False.
    """
    if verbose:
        echo("Creating data.")
    orm_module = import_file(orm_file)
    ssg_module = import_file(ssg_file)
    create_db_data(
        orm_module.Base.metadata.sorted_tables,
        ssg_module.table_generator_dict,
        ssg_module.story_generator_list,
        num_passes,
    )
    if verbose:
        echo(f"Data created in {num_passes} passes.")


@app.command()
def create_vocab(
    ssg_file: str = Option(SSG_FILENAME),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Create tables using the SQLAlchemy file.

    Example:
        $ sqlsynthgen create-vocab

    Args:
        ssg_file (str): Name of generators file.
          Must be in the current working directory.
    """
    if verbose:
        echo("Loading vocab.")
    ssg_module = import_file(ssg_file)
    create_db_vocab(ssg_module.vocab_dict)
    if verbose:
        num_vocabs = len(ssg_module.vocab_dict)
        echo(f"{num_vocabs} {'table' if num_vocabs == 1 else 'tables'} loaded.")


@app.command()
def create_tables(
    orm_file: str = Option(ORM_FILENAME),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Create schema from Python classes.

    This CLI command creates the destination schema using object
    relational model declared as Python tables.

    Example:
        $ sqlsynthgen create-tables

    Args:
        orm_file (str): Name of Python ORM file.
          Must be in the current working directory.
    """
    if verbose:
        echo("Creating tables.")

    orm_module = import_file(orm_file)
    create_db_tables(orm_module.Base.metadata)

    if verbose:
        echo("Tables created.")


@app.command()
def make_generators(
    orm_file: str = Option(ORM_FILENAME),
    ssg_file: str = Option(SSG_FILENAME),
    config_file: Optional[str] = Option(None),
    stats_file: Optional[str] = Option(None),
    force: bool = Option(False, "--force", "-f"),
    verbose: bool = Option(False, "--verbose", "-v"),
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
        force (bool): Overwrite the ORM file if exists. Default to False.
        verbose (bool): Be verbose. Default to False.
    """
    if verbose:
        echo(f"Making {ssg_file}.")

    ssg_file_path = Path(ssg_file)
    if not force:
        _check_file_non_existence(ssg_file_path)
    settings = get_settings()
    # Check that src_dsn is set, even though we don't need it here.
    _require_src_db_dsn(settings)

    orm_module: ModuleType = import_file(orm_file)
    generator_config = read_yaml_file(config_file) if config_file is not None else {}
    result: str = make_table_generators(
        orm_module, generator_config, stats_file, overwrite_files=force
    )

    ssg_file_path.write_text(result, encoding="utf-8")

    if verbose:
        echo(f"{ssg_file} created.")


@app.command()
def make_stats(
    config_file: str = Option(...),
    stats_file: str = Option(STATS_FILENAME),
    force: bool = Option(False, "--force", "-f"),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Compute summary statistics from the source database.

    Writes the statistics to a YAML file.

    Example:
        $ sqlsynthgen make_stats --config-file=example_config.yaml
    """
    if verbose:
        echo(f"Creating {stats_file}.")

    stats_file_path = Path(stats_file)
    if not force:
        _check_file_non_existence(stats_file_path)

    config = read_yaml_file(config_file) if config_file is not None else {}

    settings = get_settings()
    src_dsn: str = _require_src_db_dsn(settings)

    src_stats = asyncio.get_event_loop().run_until_complete(
        make_src_stats(src_dsn, config, settings.src_schema)
    )
    stats_file_path.write_text(yaml.dump(src_stats), encoding="utf-8")

    if verbose:
        echo(f"{stats_file} created.")


@app.command()
def make_tables(
    orm_file: str = Option(ORM_FILENAME),
    force: bool = Option(False, "--force", "-f"),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Make a SQLAlchemy file of Table classes.

    This CLI command deploys sqlacodegen to discover a
    schema structure, and generates an object relational model declared
    as Python classes.

    Example:
        $ sqlsynthgen make_tables

    Args:
        orm_file (str): Path to write the Python ORM file.
        force (bool): Overwrite ORM file, if exists. Default to False.
        verbose (bool): Be verbose. Default to False.
    """
    if verbose:
        echo(f"Creating {orm_file}.")

    orm_file_path = Path(orm_file)
    if not force:
        _check_file_non_existence(orm_file_path)

    settings = get_settings()
    src_dsn: str = _require_src_db_dsn(settings)

    content = make_tables_file(src_dsn, settings.src_schema)
    orm_file_path.write_text(content, encoding="utf-8")

    if verbose:
        echo(f"{orm_file} created.")


@app.command()
def validate_config(
    config_file: Path,
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Validate the format of a config file."""
    if verbose:
        echo(f"Validating config file: {config_file}.")

    config = yaml.load(config_file.read_text(encoding="UTF-8"), Loader=yaml.SafeLoader)
    schema_config = json.loads(CONFIG_SCHEMA_PATH.read_text(encoding="UTF-8"))
    try:
        validate(config, schema_config)
    except ValidationError as e:
        echo(e, err=True)
        sys.exit(1)

    if verbose:
        echo("Config file is valid.")


@app.command()
def remove_data(
    orm_file: str = Option(ORM_FILENAME),
    ssg_file: str = Option(SSG_FILENAME),
    yes: bool = Option(False, "--yes", prompt="Are you sure?"),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Truncate non-vocabulary tables in the destination schema."""
    if yes:
        if verbose:
            echo("Truncating non-vocabulary tables.")

        orm_module = import_file(orm_file)
        ssg_module = import_file(ssg_file)
        remove_db_data(orm_module, ssg_module)

        if verbose:
            echo("Non-vocabulary tables truncated.")
    else:
        echo("Would truncate non-vocabulary tables if called with --yes.")


@app.command()
def remove_vocab(
    orm_file: str = Option(ORM_FILENAME),
    ssg_file: str = Option(SSG_FILENAME),
    yes: bool = Option(False, "--yes", prompt="Are you sure?"),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Truncate vocabulary tables in the destination schema."""
    if yes:
        if verbose:
            echo("Truncating vocabulary tables.")

        orm_module = import_file(orm_file)
        ssg_module = import_file(ssg_file)
        remove_db_vocab(orm_module, ssg_module)

        if verbose:
            echo("Vocabulary tables truncated.")
    else:
        echo("Would truncate vocabulary tables if called with --yes.")


@app.command()
def remove_tables(
    orm_file: str = Option(ORM_FILENAME),
    yes: bool = Option(False, "--yes", prompt="Are you sure?"),
    verbose: bool = Option(False, "--verbose", "-v"),
) -> None:
    """Drop all tables in the destination schema.

    Does not drop the schema itself.
    """
    if yes:
        if verbose:
            echo("Dropping tables.")

        orm_module = import_file(orm_file)
        remove_db_tables(orm_module)

        if verbose:
            echo("Tables dropped.")
    else:
        echo("Would remove tables if called with --yes.")


if __name__ == "__main__":
    app()
