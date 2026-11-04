"""Entrypoint for the datafaker package."""
import asyncio
import importlib.metadata
import io
import json
import sys
from collections.abc import Iterable
from enum import Enum
from pathlib import Path
from typing import Any, Final, Optional

import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import InternalError, OperationalError
from typer import Argument, Exit, Option, Typer

from datafaker.create import create_db_data, create_db_tables, create_db_vocab
from datafaker.db_utils import generated_tables, sorted_non_vocabulary_tables
from datafaker.dump import (
    CsvTableWriter,
    ParquetTableWriter,
    TableWriter,
    get_parquet_table_writer,
)
from datafaker.interactive import (
    update_config_generators,
    update_config_tables,
    update_missingness,
)
from datafaker.interactive.base import DbCmd
from datafaker.make import make_src_stats, make_tables_file, make_vocabulary_tables
from datafaker.remove import remove_db_data, remove_db_tables, remove_db_vocab
from datafaker.serialize_metadata import dict_to_metadata, should_ignore_fk
from datafaker.settings import (
    SettingsError,
    get_destination_dsn,
    get_destination_schema,
    get_source_dsn,
    get_source_schema,
)
from datafaker.theme import ThemeEntry, set_active_theme
from datafaker.utils import (
    CONFIG_SCHEMA_PATH,
    conf_logger,
    generators_require_stats,
    get_flag,
    logger,
    read_config_file,
)

# pylint: disable=too-many-arguments

ORM_FILENAME: Final[str] = "orm.yaml"
CONFIG_FILENAME: Final[str] = "config.yaml"
STATS_FILENAME: Final[str] = "src-stats.yaml"
DF_FILENAME: Final[str] = "df.py"

app = Typer(no_args_is_help=True)


def datafaker() -> None:
    """Run the app and catch internal exceptions."""
    try:
        app()
    except OperationalError as exc:
        logger.error(str(exc))
        if (
            type(exc.orig).__module__ == "pyodbc"
            and isinstance(exc.orig, BaseException)
            and 0 < len(exc.orig.args)
            and exc.orig.args[0] == "HYT00"
        ):
            logger.error(
                "Please ensure that the ODBC driver is installed and registered,"
                " and that the database server is available at the location specified."
            )
            logger.error("(see the installation instructions)")
        # Outside of app() typer.Exit(1) doesn't work
        sys.exit(1)
    except SettingsError as exc:
        logger.error(str(exc))
        sys.exit(1)


def _check_file_non_existence(file_path: Path) -> None:
    """Check that a given file does not exist. Exit with an error message if it does."""
    if file_path.exists():
        logger.error("%s should not already exist. Exiting...", file_path)
        raise Exit(1)


def load_metadata_config(
    orm_file_path: Path, config: dict | None = None
) -> dict[str, Any]:
    """
    Load the ``orm.yaml`` file, returning a dict representation.

    :param orm_file_name: The name of the file to load.
    :param config: The ``config.yaml`` file object. Ignored tables will be
        excluded from the output.
    :return: A dict representing the ``orm.yaml`` file, with the tables
        the ``config`` says to ignore removed.
    """
    with orm_file_path.open(encoding="utf-8") as orm_fh:
        meta_dict = yaml.load(orm_fh, yaml.Loader)
        if not isinstance(meta_dict, dict):
            return {}
        tables_dict = meta_dict.get("tables", {})
        if config is not None and "tables" in config:
            tables_config = config["tables"]
            # Remove ignored tables
            for name, table_config in tables_config.items():
                if get_flag(table_config, "ignore"):
                    tables_dict.pop(name, None)
            # Remove foreign keys to ignored tables
            for table_meta in tables_dict.values():
                for column_meta in table_meta.get("columns", {}).values():
                    if isinstance(column_meta, dict) and "foreign_keys" in column_meta:
                        filtered_fks = [
                            fk
                            for fk in column_meta["foreign_keys"]
                            if not should_ignore_fk(tables_config, fk)
                        ]
                        column_meta["foreign_keys"] = filtered_fks
        return meta_dict


def load_metadata(orm_file_path: Path, config: dict | None = None) -> MetaData:
    """
    Load metadata from ``orm.yaml``.

    :param orm_file_name: ``orm.yaml`` or alternative name to load metadata from.
    :param config: Used to exclude tables that are marked as ``ignore: true``.
    :return: SQLAlchemy MetaData object representing the database described by the loaded file.
    """
    meta_dict = load_metadata_config(orm_file_path, config)
    return dict_to_metadata(meta_dict, None)


def load_metadata_for_output(
    orm_file_path: Path, config: dict | None = None
) -> MetaData:
    """Load metadata excluding any foreign keys pointing to ignored tables."""
    meta_dict = load_metadata_config(orm_file_path, config)
    return dict_to_metadata(meta_dict, config)


@app.callback()
def main(
    verbose: bool = Option(False, "--verbose", "-v", help="Print more information."),
    theme: ThemeEntry = Option(
        ThemeEntry.DARK,
        help="The colour scheme",
        envvar="DATAFAKER_THEME",
    ),
) -> None:
    """Set the global parameters."""
    conf_logger(verbose)
    set_active_theme(theme)


@app.command(rich_help_panel="Configure and Extract")
def make_tables(
    orm_file: Path = Option(ORM_FILENAME, help="Path to write the ORM yaml file to"),
    force: bool = Option(
        False, "--force", "-f", help="Overwrite any existing orm yaml file."
    ),
    parquet_dir: Optional[Path] = Option(
        None,
        help=(
            "Directory of Parquet files to consider part of the database."
            " This can be useful when using DuckDB."
            " The .parquet files or directories containing partitioned tables"
            " must all be directly inside this named directory."
            " Make sure you check the output!"
        ),
        file_okay=False,
        dir_okay=True,
    ),
    views_to_tables: bool = Option(
        False,
        help=("Generate all views as tables in the synthetic version."),
    ),
) -> None:
    """Make a YAML file representing the tables in the schema.

    Example:
        $ datafaker make_tables
    """
    logger.debug("Creating %s.", orm_file)

    orm_file_path = Path(orm_file)
    if not force:
        _check_file_non_existence(orm_file_path)

    content = make_tables_file(
        get_source_dsn(),
        get_source_schema(),
        views_to_tables=views_to_tables,
        parquet_dir=parquet_dir,
    )
    orm_file_path.write_text(content, encoding="utf-8")
    logger.debug("%s created.", orm_file)


@app.command(rich_help_panel="Configure and Extract")
def make_vocab(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
    force: bool = Option(
        False,
        "--force/--no-force",
        "-f/+f",
        help="Overwrite any existing vocabulary file.",
    ),
    compress: bool = Option(False, help="Compress file to .gz"),
    only: list[str] = Option([], help="Only download this table."),
) -> None:
    """Make files of vocabulary tables.

    Each table marked in the configuration file as "vocabulary_table: true"

    Example:
        $ datafaker make-vocab --config-file config.yml
    """
    generator_config = read_config_file(config_file) if config_file is not None else {}
    orm_metadata = load_metadata(orm_file, generator_config)
    make_vocabulary_tables(
        orm_metadata,
        generator_config,
        overwrite_files=force,
        compress=compress,
        table_names=set(only) if only else None,
    )


@app.command(rich_help_panel="Configure and Extract")
def configure_tables(
    config_file: Path = Option(
        CONFIG_FILENAME,
        help="Path to write the configuration file to",
        dir_okay=False,
    ),
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
) -> None:
    """Interactively set tables to ignored, vocabulary or primary private."""
    logger.debug("Configuring tables in %s.", config_file)
    config = {}
    if config_file.exists():
        config = yaml.load(
            config_file.read_text(encoding="UTF-8"), Loader=yaml.SafeLoader
        )
    # we don't pass config here so that no tables are ignored
    meta_dict = load_metadata_config(orm_file)
    metadata = dict_to_metadata(meta_dict, None)
    config_updated = update_config_tables(
        get_source_dsn(),
        get_source_schema(),
        metadata,
        config,
        Path(meta_dict["parquet-dir"]) if "parquet-dir" in meta_dict else None,
    )
    if config_updated is None:
        logger.debug("Cancelled")
        return
    content = yaml.dump(config_updated)
    config_file.write_text(content, encoding="utf-8")
    logger.debug("Tables configured in %s.", config_file)


@app.command(rich_help_panel="Configure and Extract")
def configure_generators(
    config_file: Path = Option(
        CONFIG_FILENAME,
        help="Path of the configuration file to alter",
        dir_okay=False,
    ),
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    spec: Path = Option(
        None,
        help=(
            "CSV file (headerless) with fields table-name,"
            " column-name, generator-name to set non-interactively"
        ),
    ),
) -> None:
    """Interactively set generators for column data."""
    logger.debug("Configuring generators in %s.", config_file)
    config = {}
    if config_file.exists():
        config = yaml.load(
            config_file.read_text(encoding="UTF-8"), Loader=yaml.SafeLoader
        )
    meta_dict = load_metadata_config(orm_file)
    metadata = dict_to_metadata(meta_dict, None)
    config_updated = update_config_generators(
        DbCmd.Settings(
            get_source_dsn(),
            get_source_schema(),
            config,
            metadata,
            meta_dict.get("parquet-dir", None),
        ),
        spec_path=spec,
    )
    if config_updated is None:
        logger.debug("Cancelled")
        return
    content = yaml.dump(config_updated)
    config_file.write_text(content, encoding="utf-8")
    logger.debug("Generators configured in %s.", config_file)


@app.command(rich_help_panel="Configure and Extract")
def configure_missing(
    config_file: Path = Option(
        CONFIG_FILENAME,
        help="Path to write the configuration file to",
        dir_okay=False,
    ),
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
) -> None:
    """Interactively set the missingness of the generated data."""
    logger.debug("Configuring missingness in %s.", config_file)
    config: dict[str, Any] = {}
    if config_file.exists():
        config_any = yaml.load(
            config_file.read_text(encoding="UTF-8"), Loader=yaml.SafeLoader
        )
        if isinstance(config_any, dict):
            config = config_any
    meta_dict = load_metadata_config(orm_file, config)
    metadata = dict_to_metadata(meta_dict, None)
    config_updated = update_missingness(
        get_source_dsn(),
        get_source_schema(),
        metadata,
        config,
        Path(meta_dict["parquet-dir"]) if "parquet-dir" in meta_dict else None,
    )
    if config_updated is None:
        logger.debug("Cancelled")
        return
    content = yaml.dump(config_updated)
    config_file.write_text(content, encoding="utf-8")
    logger.debug("Generators missingness in %s.", config_file)


@app.command(rich_help_panel="Configure and Extract")
def make_stats(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
    stats_file: Path = Option(STATS_FILENAME),
    force: bool = Option(
        False, "--force", "-f", help="Overwrite any existing vocabulary file."
    ),
) -> None:
    """Compute summary statistics from the source database."""
    logger.debug("Creating %s.", stats_file)

    if not force:
        _check_file_non_existence(stats_file)

    config = read_config_file(config_file) if config_file is not None else {}
    meta_dict = load_metadata_config(orm_file, config)

    src_stats = asyncio.get_event_loop().run_until_complete(
        make_src_stats(
            get_source_dsn(),
            config,
            get_source_schema(),
            parquet_dir=meta_dict.get("parquet-dir", None),
        )
    )
    stats_file.write_text(yaml.dump(src_stats), encoding="utf-8")
    logger.debug("%s created.", stats_file)


@app.command(rich_help_panel="Create Synthetic Database")
def create_tables(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
) -> None:
    """Create schema from the ORM YAML file.

    This CLI command creates the destination schema using object
    relational model declared as Python tables.

    Example:
        $ datafaker create-tables
    """
    logger.debug("Creating tables.")
    config = read_config_file(config_file) if config_file is not None else {}
    orm_metadata = load_metadata_for_output(orm_file, config)
    create_db_tables(orm_metadata)
    logger.debug("Tables created.")


@app.command(rich_help_panel="Create Synthetic Database")
def create_vocab(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Path = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
) -> None:
    """Import vocabulary data into the target database.

    Example:
        $ datafaker create-vocab
    """
    logger.debug("Loading vocab.")
    config = read_config_file(config_file) if config_file is not None else {}
    meta_dict = load_metadata_config(orm_file, config)
    orm_metadata = dict_to_metadata(meta_dict, config)
    vocabs_loaded = create_db_vocab(orm_metadata, meta_dict, config)
    num_vocabs = len(vocabs_loaded)
    logger.debug("%s %s loaded.", num_vocabs, "table" if num_vocabs == 1 else "tables")


@app.command(rich_help_panel="Create Synthetic Database")
def create_data(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
    ),
    stats_file: Optional[Path] = Option(
        None,
        help=(
            "Statistics file (output of make-stats); default is src-stats.yaml if the "
            "config file references SRC_STATS, or None otherwise."
        ),
        show_default=False,
        dir_okay=False,
    ),
    num_passes: int = Option(1, help="Number of passes (rows or stories) to make"),
) -> None:
    """Populate the schema in the target directory with synthetic data.

    This CLI command generates synthetic data for
    Python table structures, and inserts these rows
    into a destination schema.

    Also takes as input object relational model as represented
    by file containing Python classes and its attributes.

    Takes as input datafaker output as represented by Python
    classes, its attributes and methods for generating values
    for those attributes.

    Final input is the number of rows required.

    Example:
        $ datafaker create-data
    """
    logger.debug("Creating data.")
    config = read_config_file(config_file) if config_file is not None else {}
    if stats_file is None and generators_require_stats(config):
        stats_file = Path(STATS_FILENAME)
    orm_metadata = load_metadata_for_output(orm_file, config)
    try:
        row_counts = create_db_data(
            sorted_non_vocabulary_tables(orm_metadata, config),
            config,
            stats_file,
            num_passes,
            orm_metadata,
        )
        logger.debug(
            "Data created in %s %s.",
            num_passes,
            "pass" if num_passes == 1 else "passes",
        )
        for table_name, row_count in row_counts.items():
            logger.debug(
                "%s: %s %s created.",
                table_name,
                row_count,
                "row" if row_count == 1 else "rows",
            )
        return
    except RuntimeError as e:
        logger.error(e.args[0])
    except SettingsError as e:
        logger.error(str(e))
    raise Exit(1)


def convert_table_names_to_tables(
    table_names: list[str], metadata: MetaData
) -> list[Table]:
    """
    Convert a list of table names to SQLAlchemy Tables.

    :param table_names: List of names of tables
    :param metadata: Metadata of the database
    :return: List of tables with names matching ``table_names``
    """
    failed_count = 0
    results: list[Table] = []
    for name in table_names:
        table = metadata.tables.get(name, None)
        if table is not None:
            results.append(table)
        else:
            failed_count += 1
            logger.error(
                "%s is not the name of a table in the destination database", name
            )
    if failed_count:
        raise Exit(1)
    return results


def _dump_csv_to_stdout(
    table: Table,
    metadata: MetaData,
    dsn: str,
    schema_name: str | None,
) -> None:
    """Dump the table to stdout if possible."""
    if isinstance(sys.stdout, io.TextIOBase):
        csv_writer = CsvTableWriter(metadata, dsn, schema_name)
        csv_writer.write_io(table, sys.stdout)


def _get_writer(
    parquet: bool,
    output: Path | None,
    metadata: MetaData,
    dsn: str,
    schema_name: str | None,
) -> TableWriter:
    if parquet or output and output.suffix == ParquetTableWriter.EXTENSION:
        return get_parquet_table_writer(metadata, dsn, schema_name)
    return CsvTableWriter(metadata, dsn, schema_name)


def _dump_tables_to_directory(
    writer: TableWriter,
    directory: Path,
    tables: Iterable[Table],
) -> None:
    count_written = 0
    failed: list[str] = []
    for mtable in tables:
        if writer.write(mtable, directory):
            count_written += 1
        else:
            failed.append(mtable.name)
    logger.info("%d files successfully written", count_written)
    if failed:
        logger.warning("%d files failed:", len(failed))
        for f in failed:
            logger.warning("Failed to write %s", f)


@app.command(rich_help_panel="Inspect and Export")
def dump_data(
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="Path of the configuration file to use",
        dir_okay=False,
    ),
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    table: list[str] = Option(
        default=[],
        help="The tables to dump (default is all non-ignored, non-vocabulary tables)",
    ),
    output: Path
    | None = Option(
        None,
        help=(
            "Output CSV or Parquet file name,"
            " directory to write into or - to output to the console"
        ),
        file_okay=True,
        dir_okay=True,
    ),
    parquet: bool = Option(
        False,
        help="Use Parquet format (default use CSV unless --output specifies a .parquet file)",
    ),
) -> None:
    """Dump a whole table as a CSV file (or to the console) from the destination database."""
    directory = Path(".")
    if output:
        if Path(output).is_dir():
            directory = Path(output)
            output = None
        elif len(table) != 1:
            logger.error(
                "Must specify exactly one table if the output name is"
                " specified, or specify an existing directory"
            )
            raise Exit(1)
    dst_dsn = get_destination_dsn()
    schema_name = get_destination_schema()
    config = read_config_file(config_file) if config_file is not None else {}
    metadata = load_metadata_for_output(orm_file, config)
    mtables = convert_table_names_to_tables(table, metadata)
    if not mtables:
        mtables = generated_tables(metadata, config)
    if output is not None and output.name == "-":
        _dump_csv_to_stdout(mtables[0], metadata, dst_dsn, schema_name)
        return
    writer = _get_writer(parquet, output, metadata, dst_dsn, schema_name)
    if output:
        mtable = mtables[0]
        if not writer.write_file(mtable, directory / output):
            logger.error("Could not write table %s to file %s", mtable.name, output)
        return
    _dump_tables_to_directory(writer, directory, mtables)


@app.command(rich_help_panel="Inspect and Export")
def validate_config(
    config_file: Path = Argument(help="The configuration file to validate"),
) -> None:
    """Validate the format of a config file."""
    logger.debug("Validating config file: %s.", config_file)

    config = yaml.load(config_file.read_text(encoding="UTF-8"), Loader=yaml.SafeLoader)
    schema_config = json.loads(CONFIG_SCHEMA_PATH.read_text(encoding="UTF-8"))
    try:
        validate(config, schema_config)
    except ValidationError as e:
        logger.error(e)
        raise Exit(1) from e
    logger.debug("Config file is valid.")


@app.command(rich_help_panel="Remove Destination Data")
def remove_data(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
    yes: bool = Option(
        False, "--yes", prompt="Are you sure?", help="Just remove, don't ask first"
    ),
) -> None:
    """Truncate non-vocabulary tables in the destination schema."""
    if yes:
        logger.debug("Truncating non-vocabulary tables.")
        config = read_config_file(config_file) if config_file is not None else {}
        metadata = load_metadata_for_output(orm_file, config)
        remove_db_data(metadata, config)
        logger.debug("Non-vocabulary tables truncated.")
    else:
        logger.info("Would truncate non-vocabulary tables if called with --yes.")


@app.command(rich_help_panel="Remove Destination Data")
def remove_vocab(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
    yes: bool = Option(
        False, "--yes", prompt="Are you sure?", help="Just remove, don't ask first"
    ),
) -> None:
    """Truncate vocabulary tables in the destination schema."""
    if yes:
        logger.debug("Truncating vocabulary tables.")
        config = read_config_file(config_file) if config_file is not None else {}
        meta_dict = load_metadata_config(orm_file, config)
        orm_metadata = dict_to_metadata(meta_dict, config)
        remove_db_vocab(orm_metadata, meta_dict, config)
        logger.debug("Vocabulary tables truncated.")
    else:
        logger.info("Would truncate vocabulary tables if called with --yes.")


@app.command(rich_help_panel="Remove Destination Data")
def remove_tables(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Path = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
    # pylint: disable=redefined-builtin
    all: bool = Option(
        False,
        help="Don't use the ORM file, delete all tables in the destination schema",
    ),
    yes: bool = Option(
        False, "--yes", prompt="Are you sure?", help="Just remove, don't ask first"
    ),
) -> None:
    """Drop all tables in the destination schema.

    Does not drop the schema itself.
    """
    if yes:
        logger.debug("Dropping tables.")
        if all:
            remove_db_tables(None)
        else:
            config = read_config_file(config_file)
            metadata = load_metadata_for_output(orm_file, config)
            try:
                remove_db_tables(metadata)
            except InternalError as exc:
                logger.error("Failed to drop tables: %s", exc)
                logger.error("Please try again using the --all option.")
                raise Exit(1) from exc
        logger.debug("Tables dropped.")
    else:
        logger.info("Would remove tables if called with --yes.")


class TableType(str, Enum):
    """Types of tables for the ``list-tables`` command."""

    ALL = "all"
    VOCAB = "vocab"
    GENERATED = "generated"


@app.command(rich_help_panel="Inspect and Export")
def list_tables(
    orm_file: Path = Option(
        ORM_FILENAME,
        help="The name of the ORM yaml file",
        dir_okay=False,
    ),
    config_file: Optional[Path] = Option(
        CONFIG_FILENAME,
        help="The configuration file",
        dir_okay=False,
    ),
    tables: TableType = Option(TableType.GENERATED, help="Which tables to list"),
) -> None:
    """List the names of tables described in the metadata file."""
    config = read_config_file(config_file) if config_file is not None else {}
    orm_metadata = load_metadata(orm_file, config)
    all_table_names = set(orm_metadata.tables.keys())
    vocab_table_names = {
        table_name
        for (table_name, table_config) in config.get("tables", {}).items()
        if get_flag(table_config, "vocabulary_table")
    }
    if tables == TableType.ALL:
        names = all_table_names
    elif tables == TableType.GENERATED:
        names = all_table_names - vocab_table_names
    else:
        names = vocab_table_names
    for name in sorted(names):
        print(name)


@app.command(rich_help_panel="Inspect and Export")
def version() -> None:
    """Display version information."""
    assert __package__ is not None
    logger.info(
        "%s version %s",
        __package__,
        importlib.metadata.version(__package__),
    )


if __name__ == "__main__":
    datafaker()
