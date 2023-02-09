"""Tests for the main module."""
from unittest import TestCase
from unittest.mock import MagicMock

from sqlsynthgen.base import FileUploader


class VocabTests(TestCase):
    """Module test case."""

    def test_load(self) -> None:
        """Test the load method."""
        mock_table = MagicMock()
        vocab_gen = FileUploader(mock_table)
        vocab_gen.load()
