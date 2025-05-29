"""Tests for the create module."""
import itertools as itt
from collections import Counter
from pathlib import Path
from typing import Any, Generator, Tuple
from unittest.mock import MagicMock, call, patch

from sqlalchemy import Column, Connection, Integer, create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import Table

from datafaker.base import FileUploader, TableGenerator
from datafaker.create import (
    Story,
    StoryIterator,
    create_db_data,
    create_db_tables,
    create_db_vocab,
    populate,
)
from tests.utils import RequiresDBTestCase, DatafakerTestCase, get_test_settings


class MyTestCase(DatafakerTestCase):
    """Module test case."""

    @patch("datafaker.utils.create_engine")
    @patch("datafaker.create.get_settings")
    @patch("datafaker.create.populate")
    def test_create_db_data(
        self,
        mock_populate: MagicMock,
        mock_get_settings: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """Test the generate function."""
        mock_get_settings.return_value = get_test_settings()
        mock_populate.return_value = {}

        num_passes = 23
        row_counts = create_db_data([], {}, [], num_passes)

        self.assertEqual(len(mock_populate.call_args_list), num_passes)
        self.assertEqual(row_counts, {})
        mock_create_engine.assert_called()

    @patch("datafaker.create.get_settings")
    @patch("datafaker.utils.create_engine")
    def test_create_db_tables(
        self, mock_create_engine: MagicMock, mock_get_settings: MagicMock
    ) -> None:
        """Test the create_tables function."""
        mock_get_settings.return_value.dst_schema = None
        mock_meta = MagicMock()

        create_db_tables(mock_meta)
        mock_create_engine.assert_called_once_with(
            mock_get_settings.return_value.dst_dsn
        )
        mock_meta.create_all.assert_called_once_with(mock_create_engine.return_value)

    def test_populate(self) -> None:
        """Test the populate function."""
        table_name = "table_name"

        def story() -> Generator[Tuple[str, dict], None, None]:
            """Mock story."""
            yield table_name, {}

        def mock_story_gen(_: Any) -> Generator[Tuple[str, dict], None, None]:
            """A function that returns mock stories."""
            return story()

        for num_stories_per_pass, num_rows_per_pass, num_initial_rows in itt.product(
            [0, 2], [0, 3], [0, 17]
        ):
            with patch("datafaker.create.insert") as mock_insert:
                mock_values = mock_insert.return_value.values
                mock_dst_conn = MagicMock(spec=Connection)
                mock_dst_conn.execute.return_value.returned_defaults = {}
                mock_table = MagicMock(spec=Table)
                mock_table.name = table_name
                mock_gen = MagicMock(spec=TableGenerator)
                mock_gen.num_rows_per_pass = num_rows_per_pass
                mock_gen.return_value = {}
                row_counts = Counter(
                    {table_name: num_initial_rows} if num_initial_rows > 0 else {}
                )

                story_generators: list[dict[str, Any]] = (
                    [
                        {
                            "function": mock_story_gen,
                            "num_stories_per_pass": num_stories_per_pass,
                            "name": "mock_story_gen",
                        }
                    ]
                    if num_stories_per_pass > 0
                    else []
                )
                row_counts += populate(
                    mock_dst_conn,
                    [mock_table],
                    {table_name: mock_gen},
                    story_generators,
                )

                expected_row_count = (
                    num_stories_per_pass + num_rows_per_pass + num_initial_rows
                )
                self.assertEqual(
                    Counter(
                        {table_name: expected_row_count}
                        if expected_row_count > 0
                        else {}
                    ),
                    row_counts,
                )
                self.assertListEqual(
                    [call(mock_dst_conn)] * (num_stories_per_pass + num_rows_per_pass),
                    mock_gen.call_args_list,
                )
                self.assertListEqual(
                    [call(mock_gen.return_value)]
                    * (num_stories_per_pass + num_rows_per_pass),
                    mock_values.call_args_list,
                )
                self.assertListEqual(
                    (
                        [call(mock_values.return_value.return_defaults.return_value)]
                        * num_stories_per_pass
                    )
                    + ([call(mock_values.return_value)] * num_rows_per_pass),
                    mock_dst_conn.execute.call_args_list,
                )

    @patch("datafaker.create.insert")
    def test_populate_diff_length(self, mock_insert: MagicMock) -> None:
        """Test when generators and tables differ in length."""
        mock_dst_conn = MagicMock(spec=Connection)
        mock_gen_two = MagicMock(spec_set=TableGenerator)
        mock_gen_three = MagicMock(spec_set=TableGenerator)
        mock_table_one = MagicMock(spec=Table)
        mock_table_one.name = "one"
        mock_table_two = MagicMock(spec=Table)
        mock_table_two.name = "two"
        mock_table_three = MagicMock(spec=Table)
        mock_table_three.name = "three"
        tables: list[Table] = [mock_table_one, mock_table_two, mock_table_three]
        row_generators: dict[str, TableGenerator] = {
            "two": mock_gen_two,
            "three": mock_gen_three,
        }

        row_counts = populate(mock_dst_conn, tables, row_generators, [])
        self.assertEqual(row_counts, {"two": 1, "three": 1})
        self.assertListEqual(
            [call(mock_table_two), call(mock_table_three)], mock_insert.call_args_list
        )

        mock_gen_two.assert_called_once()
        mock_gen_three.assert_called_once()

    @patch("datafaker.utils.create_engine")
    @patch("datafaker.create.get_settings")
    def test_create_db_vocab(
        self, mock_get_settings: MagicMock, mock_create_engine: MagicMock
    ) -> None:
        """Test the create_db_vocab function."""
        mock_get_settings.return_value = get_test_settings()

        mock_load = MagicMock()
        mock_table = MagicMock()
        mock_table.name = "Mock table"
        mock_file_uploader = MagicMock(spec=FileUploader)
        mock_file_uploader.load = mock_load
        mock_file_uploader.table = mock_table
        vocab_list: dict[str, FileUploader] = {mock_table.name: mock_file_uploader}

        create_db_vocab(vocab_list)

        mock_load.assert_called_once_with(
            mock_create_engine.return_value.connect.return_value.__enter__.return_value
        )
        mock_create_engine.assert_called_once_with(
            mock_get_settings.return_value.dst_dsn
        )
        # Running the same insert twice should be fine.
        create_db_vocab(vocab_list)


class TestStoryDefaults(RequiresDBTestCase):
    """Test that we can handle column defaults in stories."""

    dump_file_path = "dst.dump"
    # pylint: disable=invalid-name
    Base = declarative_base()
    # pylint: enable=invalid-name
    metadata = Base.metadata

    class ColumnDefaultsTable(Base):  # type: ignore
        """A SQLAlchemy model."""

        __tablename__ = "column_defaults"
        someval = Column(Integer, primary_key=True)
        otherval = Column(Integer, server_default="8")

    def test_populate(self) -> None:
        """Check that we can populate a table that has column defaults."""
        self.metadata.create_all(self.engine)

        def my_story() -> Story:
            """A story generator."""
            first_row = yield "column_defaults", {}
            self.assertEqual(1, first_row["someval"])
            self.assertEqual(8, first_row["otherval"])

        story_iterator = StoryIterator([my_story()], dict(self.metadata.tables), {}, conn)
        with self.engine.connect() as conn:
            with conn.begin():
                while not story_iterator.is_ended():
                    story_iterator.insert()
                    story_iterator.next()
