"""Base generator classes."""
import json
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
        with Path(self.table.fullname + ".json").open(
            "r", newline="", encoding="utf-8"
        ) as jsonfile:
            rows = json.load(jsonfile)
            stmt = insert(self.table).values(list(rows))
        connection.execute(stmt)
