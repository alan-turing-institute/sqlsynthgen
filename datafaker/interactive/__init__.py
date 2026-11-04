"""Interactive configuration commands."""
import csv
import itertools
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData

from datafaker.interactive.base import DbCmd
from datafaker.interactive.generators import GeneratorCmd, try_setting_generator
from datafaker.interactive.missingness import MissingnessCmd
from datafaker.interactive.table import TableCmd
from datafaker.utils import logger

# Monkey patch pyreadline3 v3.5 so that it works with Python 3.13
# Windows users can install pyreadline3 to get tab completion working.
# See https://github.com/pyreadline3/pyreadline3/issues/37
try:
    import readline

    if not hasattr(readline, "backend"):
        setattr(readline, "backend", "readline")
except ImportError:
    pass


def update_config_tables(
    src_dsn: str,
    src_schema: str | None,
    metadata: MetaData,
    config: MutableMapping,
    parquet_dir: Path | None,
) -> Mapping[str, Any]:
    """Ask the user to specify what should happen to each table."""
    settings = DbCmd.Settings(src_dsn, src_schema, config, metadata, parquet_dir)
    with TableCmd(settings) as tc:
        tc.cmdloop()
        return tc.config


def update_missingness(
    src_dsn: str,
    src_schema: str | None,
    metadata: MetaData,
    config: MutableMapping[str, Any],
    parquet_dir: Path | None,
) -> Mapping[str, Any]:
    """
    Ask the user to update the missingness information in ``config.yaml``.

    :param src_dsn: The connection string for the source database.
    :param src_schema: The name of the source database schema (or None
    for the default).
    :param metadata: The SQLAlchemy metadata object from ``orm.yaml``.
    :param config: The starting configuration,
    :return: The updated configuration.
    """
    settings = DbCmd.Settings(src_dsn, src_schema, config, metadata, parquet_dir)
    with MissingnessCmd(settings) as mc:
        mc.cmdloop()
        return mc.config


def update_config_generators(
    settings: DbCmd.Settings,
    spec_path: Path | None,
) -> Mapping[str, Any]:
    """
    Update configuration with the specification from a CSV file.

    The specification is a headerless CSV file with columns: Table name,
    Column name (or space-separated list of column names), Generator
    name required, Second choice generator name, Third choice generator
    name, etcetera.
    :param settings: Source database settings.
    :param spec_path: The path of the CSV file containing the specification
    :return: Updated configuration.
    """
    with GeneratorCmd(settings) as gc:
        if spec_path is None:
            gc.cmdloop()
            return gc.config
        with spec_path.open() as spec:
            line_no = 0
            for line in csv.reader(spec):
                line_no += 1
                if line:
                    if len(line) < 3:
                        logger.error(
                            "line %d of file %s has fewer than three values",
                            line_no,
                            spec_path,
                        )
                    cols = line[1].split(maxsplit=1)
                    if gc.go_to(f"{line[0]}.{cols[0]}"):
                        if len(cols) == 1 or gc.set_merged_columns(cols[0], cols[1]):
                            try_setting_generator(gc, itertools.islice(line, 2, None))
                    else:
                        logger.warning("no such column %s[%s]", line[0], line[1])
            gc.do_quit("yes")
            return gc.config
