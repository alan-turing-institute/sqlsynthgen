"""Base generator classes."""
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Any
    file_name: Optional[Path] = None

    def load(self) -> None:
        """Load the data from file."""
