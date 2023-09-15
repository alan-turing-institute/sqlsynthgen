"""Base table generator classes."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import Connection, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import Table

from sqlsynthgen.utils import logger


class TableGenerator(ABC):
    """Abstract base class for table generator classes."""

    num_rows_per_pass: int = 1

    @abstractmethod
    def __call__(self, dst_db_conn: Connection) -> dict[str, Any]:
        """Return, as a dictionary, a new row for the table that we are generating.

        The only argument, `dst_db_conn`, should be a database connection to the
        database to which the data is being written. Most generators won't use it, but
        some do, and thus it's required by the interface.

        The return value should be a dictionary with column names as strings for keys,
        and the values being the values for the new row.
        """


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Table

    def load(self, connection: Connection) -> None:
        """Load the data from file."""
        yaml_file = Path(self.table.fullname + ".yaml")
        if not yaml_file.exists():
            logger.warning("File %s not found. Skipping...", yaml_file)
            return
        try:
            with yaml_file.open("r", newline="", encoding="utf-8") as yamlfile:
                rows = yaml.load(yamlfile, Loader=yaml.Loader)
        except yaml.YAMLError as e:
            logger.warning("Error reading YAML file %s: %s", yaml_file, e)
            return

        if not rows:
            logger.warning("No rows in %s. Skipping...", yaml_file)
            return

        try:
            stmt = insert(self.table).values(list(rows))
            connection.execute(stmt)
            connection.commit()
        except SQLAlchemyError as e:
            logger.warning(
                "Error inserting rows into table %s: %s", self.table.fullname, e
            )
