"""Base table generator classes."""
import gzip
import os
import random
from collections.abc import Callable
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import Connection, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import Table

from datafaker.db_utils import MAKE_VOCAB_PROGRESS_REPORT_EVERY, table_row_count
from datafaker.utils import logger, stream_yaml


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Table

    def _load_existing_file(
        self, connection: Connection, file_size: int, opener: Callable[[], Any]
    ) -> None:
        count = 0
        with opener() as fh:
            rows = stream_yaml(fh)
            for row in rows:
                stmt = insert(self.table).values(row)
                connection.execute(stmt)
                connection.commit()
                count += 1
                if count % MAKE_VOCAB_PROGRESS_REPORT_EVERY == 0:
                    logger.info(
                        "inserted row %d of %s, %.1f%%",
                        count,
                        self.table.name,
                        100 * fh.tell() / file_size,
                    )

    def load(self, connection: Connection, base_path: Path = Path(".")) -> None:
        """Load the data from file."""
        yaml_file = base_path / Path(self.table.fullname + ".yaml")
        if yaml_file.exists():

            def opener() -> TextIOWrapper:
                return open(yaml_file, mode="r", encoding="utf-8")

        else:
            yaml_file = base_path / Path(self.table.fullname + ".yaml.gz")
            if yaml_file.exists():

                def opener() -> TextIOWrapper:
                    return gzip.open(yaml_file, mode="rt")

            else:
                logger.warning("File %s not found. Skipping...", yaml_file)
                return
        if 0 < table_row_count(self.table, connection):
            logger.warning(
                (
                    "Table %s already contains data"
                    " (consider running 'datafaker remove-vocab'), skipping..."
                ),
                self.table.name,
            )
            return
        try:
            file_size = os.path.getsize(yaml_file)
            self._load_existing_file(connection, file_size, opener)
        except yaml.YAMLError as e:
            logger.warning("Error reading YAML file %s: %s", yaml_file, e)
            return
        except SQLAlchemyError as e:
            logger.warning(
                "Error inserting rows into table %s: %s", self.table.fullname, e
            )


class ColumnPresence:
    """Object for generators to use for missingness completely at random."""

    def sampled(self, patterns: list[dict[str, Any]]) -> set[str]:
        """
        Select a random pattern and output the non-null columns.

        :param patterns: List of outputs from missingness SQL queries.
        Columns in each output: ``row_count`` is the number of rows
        with this missingness pattern, then for each column
        ``<column>`` there is a boolean called ``missingness__is_null``.
        :return: All the names of the columns no make non-null.
        """
        total = 0
        for pattern in patterns:
            total += pattern.get("row_count", 0)
        s = random.randrange(total)
        for pattern in patterns:
            s -= pattern.get("row_count", 0)
            if s < 0:
                cs = set()
                for column, nullness in pattern.items():
                    if not nullness and column.endswith("__is_null"):
                        cs.add(column[:-9])
                return cs
        logger.error("failed to sample patterns")
        return set()
