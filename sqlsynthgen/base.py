"""Base generator classes."""
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import insert


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Any

    def load(self, connection: Any) -> None:
        """Load the data from file."""
        with Path(self.table.fullname + ".yaml").open(
            "r", newline="", encoding="utf-8"
        ) as yamlfile:
            rows = yaml.load(yamlfile, Loader=yaml.Loader)
            if not rows:
                return
            stmt = insert(self.table).values(list(rows))
        connection.execute(stmt)
