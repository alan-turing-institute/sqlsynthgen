"""Tests for the create module."""
from unittest.mock import MagicMock, call, patch

from sqlsynthgen.create import (
    create_db_data,
    create_db_tables,
    create_db_vocab,
    populate,
)
from tests.utils import SSGTestCase, get_test_settings


class MyTestCase(SSGTestCase):
    """Module test case."""

    @patch("sqlsynthgen.create.create_engine")
    @patch("sqlsynthgen.create.get_settings")
    @patch("sqlsynthgen.create.populate")
    def test_create_db_data(
        self,
        mock_populate: MagicMock,
        mock_get_settings: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """Test the generate function."""
        mock_get_settings.return_value = get_test_settings()

        create_db_data([], [], 0)

        mock_populate.assert_called_once()
        mock_create_engine.assert_called()

    @patch("sqlsynthgen.create.get_settings")
    @patch("sqlsynthgen.create.create_engine")
    def test_create_db_tables(
        self, mock_create_engine: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test the create_tables function."""
        mock_get_settings.return_value.dst_schema = None
        mock_meta = MagicMock()

        create_db_tables(mock_meta)
        mock_create_engine.assert_called_once_with(
            mock_get_settings.return_value.dst_postgres_dsn
        )
        mock_meta.create_all.assert_called_once_with(mock_create_engine.return_value)

    @patch("sqlsynthgen.create.insert")
    def test_populate(self, mock_insert: MagicMock) -> None:
        """Test the populate function."""
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

    @patch("sqlsynthgen.create.insert")
    def test_populate_diff_length(self, mock_insert: MagicMock) -> None:
        """Test when generators and tables differ in length."""
        mock_dst_conn = MagicMock()
        mock_gen_two = MagicMock()
        mock_gen_three = MagicMock()
        tables = [1, 2, 3]
        generators = [mock_gen_two, mock_gen_three]

        populate(2, mock_dst_conn, tables, generators, 1)
        self.assertListEqual([call(2), call(3)], mock_insert.call_args_list)

        mock_gen_two.assert_called_once()
        mock_gen_three.assert_called_once()

    @patch("sqlsynthgen.create.create_engine")
    @patch("sqlsynthgen.create.get_settings")
    def test_create_db_vocab(
        self, mock_get_settings: MagicMock, mock_create_engine: MagicMock
    ) -> None:
        """Test the create_db_vocab function."""
        mock_get_settings.return_value = get_test_settings()
        vocab_list = [MagicMock()]
        create_db_vocab(vocab_list)
        vocab_list[0].load.assert_called_once_with(
            mock_create_engine.return_value.connect.return_value.__enter__.return_value
        )
        mock_create_engine.assert_called_once_with(
            mock_get_settings.return_value.dst_postgres_dsn
        )
