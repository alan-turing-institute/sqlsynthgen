"""Base generator classes."""
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import insert


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Any

    def load(self, connection: Any) -> None:
        """Load the data from file."""
        with Path(self.table.fullname + ".csv").open(
            "r", newline="", encoding="utf-8"
        ) as csvfile:
            reader = csv.DictReader(csvfile)
            stmt = insert(self.table).values(list(reader))
        connection.execute(stmt)
