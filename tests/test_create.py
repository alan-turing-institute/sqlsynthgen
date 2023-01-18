"""Tests for the main module."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from sqlsynthgen.create import create_db_data, create_db_tables, populate
from tests.utils import get_test_settings


class MyTestCase(TestCase):
    """Module test case."""

    def test_create_db_data(self) -> None:
        """Test the generate function."""
        with patch("sqlsynthgen.create.populate") as mock_populate, patch(
            "sqlsynthgen.create.get_settings"
        ) as mock_get_settings, patch(
            "sqlsynthgen.create.create_engine"
        ) as mock_create_engine:
            mock_get_settings.return_value = get_test_settings()

            create_db_data([], [], 0)

            mock_populate.assert_called_once()
            mock_create_engine.assert_called()

    def test_create_db_tables(self) -> None:
        """Test the create_tables function."""
        mock_meta = MagicMock()

        with patch("sqlsynthgen.create.create_engine") as mock_create_engine, patch(
            "sqlsynthgen.create.get_settings"
        ) as mock_get_settings:

            create_db_tables(mock_meta)
            mock_get_settings.assert_called_once()
            mock_create_engine.assert_called_once_with(
                mock_get_settings.return_value.dst_postgres_dsn
            )

    def test_populate(self) -> None:
        """Test the populate function."""
        with patch("sqlsynthgen.create.insert") as mock_insert:
            mock_src_conn = MagicMock()
            mock_dst_conn = MagicMock()
            mock_gen = MagicMock()
            tables = [None]
            generators = [mock_gen]
            populate(mock_src_conn, mock_dst_conn, tables, generators, 1)

            mock_gen.assert_called_once_with(mock_src_conn, mock_dst_conn)
            mock_insert.return_value.values.assert_called_once_with(
                mock_gen.return_value.__dict__
            )
            mock_dst_conn.execute.assert_called_once_with(
                mock_insert.return_value.values.return_value
            )
