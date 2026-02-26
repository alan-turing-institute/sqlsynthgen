"""Tests for the create module."""
import itertools as itt
import os
import random
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Generator, Mapping, Tuple
from unittest.mock import MagicMock, call, patch

import duckdb
import pandas as pd
from sqlalchemy import Connection, Engine, select
from sqlalchemy.schema import MetaData, Table

from datafaker.base import TableGenerator
from datafaker.create import (
    create_db_data_into,
    create_db_tables,
    create_db_vocab,
    populate,
)
from datafaker.serialize_metadata import metadata_to_dict
from tests.utils import DatafakerTestCase, GeneratesDBTestCase


class TestCreate(GeneratesDBTestCase):
    """Test the make_table_generators function."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def test_create_vocab(self) -> None:
        """Test the create_db_vocab function."""
        with patch.dict(
            os.environ,
            {"DST_DSN": self.dst_dsn},
            clear=True,
        ):
            config = {
                "tables": {
                    "player": {
                        "vocabulary_table": True,
                    }
                },
            }
            self.set_configuration(config)
            meta_dict = metadata_to_dict(
                self.metadata, self.schema_name, self.sync_engine
            )
            create_db_tables(self.metadata)
            create_db_vocab(self.metadata, meta_dict, config, Path("./tests/examples"))
        with self.dst_sync_engine.connect() as conn:
            stmt = select(self.metadata.tables["player"])
            rows = list(conn.execute(stmt).mappings().fetchall())
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0].id, 1)
            self.assertEqual(rows[0].given_name, "Jock")
            self.assertEqual(rows[0].family_name, "Spring")
            self.assertEqual(rows[1].id, 2)
            self.assertEqual(rows[1].given_name, "Jennifer")
            self.assertEqual(rows[1].family_name, "Jenny")
            self.assertEqual(rows[2].id, 3)
            self.assertEqual(rows[2].given_name, "Mus")
            self.assertEqual(rows[2].family_name, "Al-Said")

    def test_make_table_generators(self) -> None:
        """Test that we can handle column defaults in stories."""
        random.seed(56)
        config: Mapping[str, Any] = {}
        self.generate_data(config, num_passes=2)
        with self.dst_sync_engine.connect() as conn:
            stmt = select(self.metadata.tables["string"])
            rows = list(conn.execute(stmt).mappings().fetchall())
            a = rows[0]
            b = rows[1]
            self.assertEqual(a.id, 1)
            self.assertEqual(b.id, 2)
            self.assertIs(type(a.frequency), float)
            self.assertIs(type(b.frequency), float)
            self.assertNotEqual(a.frequency, b.frequency)
            self.assertIs(type(a.position), int)
            self.assertIs(type(b.position), int)
            self.assertNotEqual(a.position, b.position)
            self.assertEqual(a.model_id, 1)
            self.assertIn(b.model_id, {1, 2})
            stmt = select(self.metadata.tables["player"])
            rows = list(conn.execute(stmt).mappings().fetchall())
            c = rows[0]
            d = rows[1]
            self.assertIs(type(c.given_name), str)
            self.assertIs(type(d.given_name), str)


class TestPopulate(DatafakerTestCase):
    """Test create.populate."""

    # pylint: disable=too-many-locals
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
                mock_metadata = MagicMock(spec=MetaData)
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
                    mock_metadata,
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
                    [call(mock_gen.return_value)]
                    * (num_stories_per_pass + num_rows_per_pass),
                    mock_values.call_args_list,
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
        mock_metadata = MagicMock(spec=MetaData)
        tables: list[Table] = [mock_table_one, mock_table_two, mock_table_three]
        row_generators: dict[str, TableGenerator] = {
            "two": mock_gen_two,
            "three": mock_gen_three,
        }

        row_counts = populate(mock_dst_conn, tables, row_generators, [], mock_metadata)
        self.assertEqual(row_counts, {"two": 1, "three": 1})
        self.assertListEqual(
            [call(mock_table_two), call(mock_table_three)], mock_insert.call_args_list
        )

        mock_gen_two.assert_called_once()
        mock_gen_three.assert_called_once()


class MockFunctionUsingConnection:
    """Base mock callable that should not be permitted to read parquet files."""

    @classmethod
    def is_parquet_permitted(cls, connection: Any) -> bool:
        """Test if a normal DuckDB can access the ``fruit.parquet`` file."""
        try:
            connection.execute("SELECT * FROM fruit.parquet")
        except duckdb.PermissionException:
            return False
        return True

    def __init__(self) -> None:
        """Initialize as uncalled."""
        self.called = False

    def do_call(self, connection: Any) -> None:
        """Test for parquet access not being permitted."""
        assert not self.is_parquet_permitted(connection)
        self.called = True


class CreateReadsNoParquetTestCase(DatafakerTestCase):
    """
    Output to the database should not have access to parquet files.

    Otherwise there is a risk of leakage of source data.
    """

    examples_dir = Path("tests/examples/duckdb")
    parquet_name = "fruit.parquet"

    def setUp(self) -> None:
        """Go to the directory where there are parquet files."""
        super().setUp()
        self.start_dir = os.getcwd()
        self.parquet_dir = Path(tempfile.mkdtemp("parq"))
        os.chdir(self.parquet_dir)
        self.write_parquet()
        assert MockFunctionUsingConnection.is_parquet_permitted(duckdb.connect())

    def tearDown(self) -> None:
        """Return to the start directory."""
        os.chdir(self.start_dir)
        return super().tearDown()

    def write_parquet(self) -> None:
        """Write a parquet file into the current directory."""
        fruit: dict[str, list[Any]] = {
            "id": [1, 2, 3],
            "orange": [True, True, False],
            "banana": ["one", "two", "three"],
        }
        pd.DataFrame.from_dict(fruit).to_parquet(self.parquet_name)

    class MockCreateAll(MockFunctionUsingConnection):
        """Mock for the MetaData.create_all function."""

        def __call__(self, engine: Engine) -> None:
            self.do_call(engine.raw_connection())

    def test_create_db_tables_cannot_access_parquet(self) -> None:
        """Test the database connection cannot access parquet file."""
        meta_data = MagicMock()
        meta_data.create_all = self.MockCreateAll()
        with patch.dict(
            os.environ,
            {"DST_DSN": "duckdb:///:memory:tables"},
            clear=True,
        ):
            create_db_tables(meta_data)
        assert meta_data.create_all.called

    def test_create_db_tables_cannot_access_parquet_with_schema(self) -> None:
        """
        Test the database connection cannot access parquet file.

        We use a schema because this activates a different code path.
        """
        meta_data = MagicMock()
        meta_data.create_all = self.MockCreateAll()
        testdb = duckdb.connect("./test.db")
        testdb.execute("CREATE SCHEMA fruity")
        testdb.close()
        with patch.dict(
            os.environ,
            {"DST_SCHEMA": "fruity", "DST_DSN": "duckdb:///./test.db"},
            clear=True,
        ):
            create_db_tables(meta_data)
        assert meta_data.create_all.called

    @patch("datafaker.create.populate")
    def test_create_db_data_cannot_access_parquet(
        self, mock_populate: MagicMock
    ) -> None:
        """Test the database connection cannot access parquet file while creating data."""

        class MockPopulate(MockFunctionUsingConnection):
            """Mock ``populate`` function."""

            def __call__(
                self, connection: Connection, _a2: Any, _a3: Any, _a4: Any, _a5: Any
            ) -> dict[str, Any]:
                super().do_call(connection.connection.dbapi_connection)
                return {"vocab1": 1}

        mock_populate.side_effect = MockPopulate()
        create_db_data_into(
            [MagicMock()],
            MagicMock(),
            1,
            "duckdb:///:memory:data",
            None,
            MagicMock(),
        )
        assert mock_populate.side_effect.called

    @patch("datafaker.create.FileUploader")
    def test_create_db_vocab_cannot_access_parquet(
        self, file_uploader: MagicMock
    ) -> None:
        """Test we cannot access parquet file while populating vocabulary tables."""

        class MockLoader(MockFunctionUsingConnection):
            """Mock ``FileUploader.load`` function."""

            def __call__(self, connection: Connection, base_path: Path) -> None:
                assert str(base_path) == "base"
                super().do_call(connection.connection.dbapi_connection)

        file_uploader.return_value.load = MockLoader()
        assert not file_uploader.return_value.load.called
        meta_data = MetaData()
        Table("table1", meta_data)
        with patch.dict(
            os.environ,
            {"DST_DSN": "duckdb:///:memory:vocab"},
            clear=True,
        ):
            create_db_vocab(
                meta_data,
                {"tables": {"table1": {"columns": {}}}},
                {"tables": {"table1": {"vocabulary_table": True}}},
                base_path=Path("base"),
            )
        assert file_uploader.return_value.load.called
