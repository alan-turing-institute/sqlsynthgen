"""Tests for the base module."""
from sqlalchemy.schema import MetaData
from tests.utils import RequiresDBTestCase
from unittest.mock import MagicMock, call, patch

from datafaker.dump import dump_db_tables

class DumpTests(RequiresDBTestCase):
    """Testing configure-tables."""
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    @patch("datafaker.dump._make_csv_writer")
    def test_dump_data(self, make_csv_writer: MagicMock) -> None:
        """ Test dump-data. """
        TEST_OUTPUT_FILE = "test_output_file_object"
        metadata = MetaData()
        metadata.reflect(self.engine)
        dump_db_tables(metadata, self.dsn, self.schema_name, "player", TEST_OUTPUT_FILE)
        make_csv_writer.assert_called_once_with(TEST_OUTPUT_FILE)
        make_csv_writer.assert_has_calls([
            call().writerow(["id", "given_name", "family_name"]),
            call().writerow((1, 'Mark', 'Samson')),
            call().writerow((2, 'Tim', 'Friedman')),
            call().writerow((3, 'Pierre', 'Marchmont')),
        ])
