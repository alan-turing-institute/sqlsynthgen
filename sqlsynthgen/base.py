"""Base table generator classes."""
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Any

    def load(self, connection: Any) -> None:
        """Load the data from file."""
        yaml_file = Path(self.table.fullname + ".yaml")
        if not yaml_file.exists():
            logging.warning("File %s not found. Skipping...", yaml_file)
            return
        try:
            with yaml_file.open("r", newline="", encoding="utf-8") as yamlfile:
                rows = yaml.safe_load(yamlfile)
        except yaml.YAMLError as e:
            logging.warning("Error reading YAML file %s: %s", yaml_file, e)
            return

        if not rows:
            logging.warning("No rows in %s. Skipping...", yaml_file)
            return

        try:
            stmt = insert(self.table).values(list(rows))
            connection.execute(stmt)
        except SQLAlchemyError as e:
            logging.warning(
                "Error inserting rows into table %s: %s", self.table.fullname, e
            )
