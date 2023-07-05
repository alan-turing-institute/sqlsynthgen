"""Tests for the create module."""
import itertools as itt
from typing import Any, Generator, Tuple
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

    @patch("sqlsynthgen.utils.create_engine")
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

        num_passes = 23
        create_db_data([], {}, [], num_passes)

        self.assertEqual(len(mock_populate.call_args_list), num_passes)
        mock_create_engine.assert_called()

    @patch("sqlsynthgen.create.get_settings")
    @patch("sqlsynthgen.utils.create_engine")
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
        table_name = "table_name"

        def story() -> Generator[Tuple[str, dict], None, None]:
            """Mock story."""
            yield "table_name", {}

        def mock_story_gen(_: Any) -> Generator[Tuple[str, dict], None, None]:
            """A function that returns mock stories."""
            return story()

        for num_stories_per_pass, num_rows_per_pass in itt.product([0, 2], [0, 3]):
            mock_dst_conn = MagicMock()
            mock_dst_conn.execute.return_value.returned_defaults = {}
            mock_table = MagicMock()
            mock_table.name = table_name
            mock_gen = MagicMock()
            mock_gen.num_rows_per_pass = num_rows_per_pass
            mock_gen.return_value = {}

            tables = [mock_table]
            row_generators = {table_name: mock_gen}
            story_generators = (
                [{"name": mock_story_gen, "num_stories_per_pass": num_stories_per_pass}]
                if num_stories_per_pass > 0
                else []
            )
            populate(mock_dst_conn, tables, row_generators, story_generators)

            mock_gen.assert_has_calls(
                [call(mock_dst_conn)] * (num_stories_per_pass + num_rows_per_pass)
            )
            mock_insert.return_value.values.assert_has_calls(
                [call(mock_gen.return_value)]
                * (num_stories_per_pass + num_rows_per_pass)
            )
            mock_dst_conn.execute.assert_has_calls(
                [call(mock_insert.return_value.values.return_value)]
                * (num_stories_per_pass + num_rows_per_pass)
            )

    @patch("sqlsynthgen.create.insert")
    def test_populate_diff_length(self, mock_insert: MagicMock) -> None:
        """Test when generators and tables differ in length."""
        mock_dst_conn = MagicMock()
        mock_gen_two = MagicMock()
        mock_gen_three = MagicMock()
        mock_table_one = MagicMock()
        mock_table_one.name = "one"
        mock_table_two = MagicMock()
        mock_table_two.name = "two"
        mock_table_three = MagicMock()
        mock_table_three.name = "three"
        tables = [mock_table_one, mock_table_two, mock_table_three]
        row_generators = {"two": mock_gen_two, "three": mock_gen_three}

        populate(mock_dst_conn, tables, row_generators, [])
        self.assertListEqual(
            [call(mock_table_two), call(mock_table_three)], mock_insert.call_args_list
        )

        mock_gen_two.assert_called_once()
        mock_gen_three.assert_called_once()

    @patch("sqlsynthgen.utils.create_engine")
    @patch("sqlsynthgen.create.get_settings")
    def test_create_db_vocab(
        self, mock_get_settings: MagicMock, mock_create_engine: MagicMock
    ) -> None:
        """Test the create_db_vocab function."""
        mock_get_settings.return_value = get_test_settings()
        vocab_list = {"table_name": MagicMock()}
        create_db_vocab(vocab_list)
        vocab_list["table_name"].load.assert_called_once_with(
            mock_create_engine.return_value.connect.return_value.__enter__.return_value
        )
        mock_create_engine.assert_called_once_with(
            mock_get_settings.return_value.dst_postgres_dsn
        )
        # Running the same insert twice should be fine.
        create_db_vocab(vocab_list)
