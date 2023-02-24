"""Tests for the main module."""
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from sqlsynthgen.create import (
    create_db_data,
    create_db_tables,
    create_db_vocab,
    populate,
)
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
            mock_gen.num_rows_per_pass = 2
            tables = [None]
            generators = [mock_gen]
            populate(mock_src_conn, mock_dst_conn, tables, generators, 1)

            mock_gen.assert_has_calls([call(mock_src_conn, mock_dst_conn)] * 2)
            mock_insert.return_value.values.assert_has_calls(
                [call(mock_gen.return_value.__dict__)] * 2
            )
            mock_dst_conn.execute.assert_has_calls(
                [call(mock_insert.return_value.values.return_value)] * 2
            )

    def test_populate_diff_length(self) -> None:
        """Test when generators and tables differ in length."""
        mock_dst_conn = MagicMock()
        mock_gen_two = MagicMock()
        mock_gen_three = MagicMock()
        tables = [1, 2, 3]
        generators = [mock_gen_two, mock_gen_three]

        with patch("sqlsynthgen.create.insert") as mock_insert:
            populate(2, mock_dst_conn, tables, generators, 1)
            self.assertListEqual([call(2), call(3)], mock_insert.call_args_list)

        mock_gen_two.assert_called_once()
        mock_gen_three.assert_called_once()

    def test_create_db_vocab(self) -> None:
        """Test the create_db_vocab function."""
        with patch("sqlsynthgen.create.create_engine") as mock_create_engine, patch(
            "sqlsynthgen.create.get_settings"
        ) as mock_get_settings:
            vocab_list = [MagicMock()]
            create_db_vocab(vocab_list)
            vocab_list[0].load.assert_called_once_with(
                mock_create_engine.return_value.connect.return_value.__enter__.return_value
            )
            mock_create_engine.assert_called_once_with(
                mock_get_settings.return_value.dst_postgres_dsn
            )
