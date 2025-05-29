"""Tests for the remove module."""
from unittest.mock import MagicMock, patch

from datafaker.remove import remove_db_data, remove_db_tables, remove_db_vocab
from datafaker.serialize_metadata import metadata_to_dict
from datafaker.settings import Settings
from sqlalchemy import func, inspect, select
from tests.utils import RequiresDBTestCase


class RemoveThingsTestCase(RequiresDBTestCase):
    """ Tests for ``remove-`` commands. """
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def count_rows(self, connection, table_name: str) -> int | None:
        return connection.execute(select(
            func.count()
        ).select_from(
            self.metadata.tables[table_name]
        )).scalar()

    @patch("datafaker.remove.get_settings")
    def test_remove_data(self, mock_get_settings: MagicMock):
        mock_get_settings.return_value = Settings(
            src_dsn=self.dsn,
            dst_dsn=self.dsn,
            _env_file=None,
        )
        remove_db_data(self.metadata, {
            "tables": {
                "manufacturer": { "vocabulary_table": True },
                "model": { "vocabulary_table": True },
            }
        })
        with self.engine.connect() as conn:
            self.assertGreater(self.count_rows(conn, "manufacturer"), 0)
            self.assertGreater(self.count_rows(conn, "model"), 0)
            self.assertEqual(self.count_rows(conn, "player"), 0)
            self.assertEqual(self.count_rows(conn, "string"), 0)
            self.assertEqual(self.count_rows(conn, "signature_model"), 0)

    @patch("datafaker.remove.get_settings")
    def test_remove_data_raises(self, mock_get_settings: MagicMock) -> None:
        """ Test that remove-data raises if dst DSN is missing. """
        mock_get_settings.return_value = Settings(
            src_dsn=self.dsn,
            dst_dsn=None,
            _env_file=None,
        )
        with self.assertRaises(AssertionError) as context_manager:
            remove_db_data(self.metadata, {
                "tables": {
                    "manufacturer": { "vocabulary_table": True },
                    "model": { "vocabulary_table": True },
                }
            })
        self.assertEqual(
            context_manager.exception.args[0], "Missing destination database settings"
        )

    @patch("datafaker.remove.get_settings")
    def test_remove_vocab(self, mock_get_settings: MagicMock):
        mock_get_settings.return_value = Settings(
            src_dsn=self.dsn,
            dst_dsn=self.dsn,
            _env_file=None,
        )
        meta_dict = metadata_to_dict(self.metadata, self.schema_name, self.engine)
        config = {
            "tables": {
                "manufacturer": { "vocabulary_table": True },
                "model": { "vocabulary_table": True },
            }
        }
        remove_db_data(self.metadata, config)
        remove_db_vocab(self.metadata, meta_dict, config)
        with self.engine.connect() as conn:
            self.assertEqual(self.count_rows(conn, "manufacturer"), 0)
            self.assertEqual(self.count_rows(conn, "model"), 0)
            self.assertEqual(self.count_rows(conn, "player"), 0)
            self.assertEqual(self.count_rows(conn, "string"), 0)
            self.assertEqual(self.count_rows(conn, "signature_model"), 0)

    @patch("datafaker.remove.get_settings")
    def test_remove_vocab_raises(self, mock_get_settings: MagicMock) -> None:
        """ Test that remove-vocab raises if dst DSN is missing. """
        mock_get_settings.return_value = Settings(
            src_dsn=self.dsn,
            dst_dsn=None,
            _env_file=None,
        )
        with self.assertRaises(AssertionError) as context_manager:
            meta_dict = metadata_to_dict(self.metadata, self.schema_name, self.engine)
            remove_db_vocab(self.metadata, meta_dict, {
                "tables": {
                    "manufacturer": { "vocabulary_table": True },
                    "model": { "vocabulary_table": True },
                }
            })
        self.assertEqual(
            context_manager.exception.args[0], "Missing destination database settings"
        )

    @patch("datafaker.remove.get_settings")
    def test_remove_tables(self, mock_get_settings: MagicMock):
        mock_get_settings.return_value = Settings(
            src_dsn=self.dsn,
            dst_dsn=self.dsn,
            _env_file=None,
        )
        self.assertTrue(inspect(self.engine).has_table("player"))
        remove_db_tables(self.metadata)
        self.assertFalse(inspect(self.engine).has_table("manufacturer"))
        self.assertFalse(inspect(self.engine).has_table("model"))
        self.assertFalse(inspect(self.engine).has_table("player"))
        self.assertFalse(inspect(self.engine).has_table("string"))
        self.assertFalse(inspect(self.engine).has_table("signature_model"))

    @patch("datafaker.remove.get_settings")
    def test_remove_tables_raises(self, mock_get_settings: MagicMock) -> None:
        """ Test that remove-vocab raises if dst DSN is missing. """
        mock_get_settings.return_value = Settings(
            src_dsn=self.dsn,
            dst_dsn=None,
            _env_file=None,
        )
        with self.assertRaises(AssertionError) as context_manager:
            remove_db_tables(self.metadata)
        self.assertEqual(
            context_manager.exception.args[0], "Missing destination database settings"
        )
