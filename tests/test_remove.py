"""Tests for the remove module."""
from unittest.mock import MagicMock, patch

from sqlalchemy import func, inspect, select
from sqlalchemy.engine import Connection

from datafaker.remove import remove_db_data, remove_db_tables, remove_db_vocab
from datafaker.serialize_metadata import metadata_to_dict
from tests.utils import RequiresDBTestCase, get_test_settings


class RemoveThingsTestCase(RequiresDBTestCase):
    """Tests for ``remove-`` commands."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def count_rows(self, connection: Connection, table_name: str) -> int | None:
        """Count the rows in a table."""
        return connection.execute(
            # pylint: disable=not-callable.
            select(func.count()).select_from(self.metadata.tables[table_name])
        ).scalar()

    @patch("datafaker.settings.get_settings")
    def test_remove_data(self, mock_get_settings: MagicMock) -> None:
        """Test that data can be removed from non-vocabulary tables."""
        mock_get_settings.return_value = get_test_settings(
            src_dsn=self.dsn,
            dst_dsn=self.dsn,
        )
        remove_db_data(
            self.metadata,
            {
                "tables": {
                    "manufacturer": {"vocabulary_table": True},
                    "model": {"vocabulary_table": True},
                }
            },
        )
        with self.sync_engine.connect() as conn:
            self.assert_greater_and_not_none(self.count_rows(conn, "manufacturer"), 0)
            self.assert_greater_and_not_none(self.count_rows(conn, "model"), 0)
            self.assertEqual(self.count_rows(conn, "player"), 0)
            self.assertEqual(self.count_rows(conn, "string"), 0)
            self.assertEqual(self.count_rows(conn, "signature_model"), 0)

    @patch("datafaker.settings.get_settings")
    def test_remove_vocab(self, mock_get_settings: MagicMock) -> None:
        """Test that vocabulary tables can be removed."""
        mock_get_settings.return_value = get_test_settings(
            src_dsn=self.dsn,
            dst_dsn=self.dsn,
        )
        meta_dict = metadata_to_dict(
            self.metadata,
            self.schema_name,
            self.sync_engine,
            parquet_dir=None,
        )
        config = {
            "tables": {
                "manufacturer": {"vocabulary_table": True},
                "model": {"vocabulary_table": True},
            }
        }
        remove_db_data(self.metadata, config)
        remove_db_vocab(self.metadata, meta_dict, config)
        with self.sync_engine.connect() as conn:
            self.assertEqual(self.count_rows(conn, "manufacturer"), 0)
            self.assertEqual(self.count_rows(conn, "model"), 0)
            self.assertEqual(self.count_rows(conn, "player"), 0)
            self.assertEqual(self.count_rows(conn, "string"), 0)
            self.assertEqual(self.count_rows(conn, "signature_model"), 0)

    @patch("datafaker.settings.get_settings")
    def test_remove_tables(self, mock_get_settings: MagicMock) -> None:
        """Test that destination tables can be removed."""
        mock_get_settings.return_value = get_test_settings(
            src_dsn=self.dsn,
            dst_dsn=self.dsn,
        )
        engine_in = inspect(self.engine)
        assert engine_in is not None
        assert hasattr(engine_in, "has_table")
        self.assertTrue(engine_in.has_table("player"))
        remove_db_tables(self.metadata)
        engine_out = inspect(self.engine)
        assert engine_out is not None
        assert hasattr(engine_out, "has_table")
        self.assertFalse(engine_out.has_table("manufacturer"))
        self.assertFalse(engine_out.has_table("model"))
        self.assertFalse(engine_out.has_table("player"))
        self.assertFalse(engine_out.has_table("string"))
        self.assertFalse(engine_out.has_table("signature_model"))
