"""Tests for MS-SQL driver support helpers in datafaker.utils."""
import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.engine import make_url

from datafaker.db_utils import create_db_engine, get_metadata, get_sync_engine
from datafaker.utils import make_async_dsn
from tests.utils import DatafakerTestCase, MsSqlTestDb


class TestMakeAsyncDsn(unittest.TestCase):
    """Tests for make_async_dsn."""

    def _call(self, dsn: str) -> str:
        return make_async_dsn(dsn)

    def test_postgresql_bare_dialect(self) -> None:
        """postgresql:// is rewritten to use asyncpg."""
        result = self._call("postgresql://user:pass@host:5432/db")
        self.assertTrue(
            result.startswith("postgresql+asyncpg://"),
            f"Expected asyncpg driver, got: {result}",
        )

    def test_postgresql_with_existing_driver(self) -> None:
        """postgresql+psycopg2:// is also rewritten to asyncpg."""
        result = self._call("postgresql+psycopg2://user:pass@host:5432/db")
        self.assertTrue(result.startswith("postgresql+asyncpg://"))

    def test_postgresql_preserves_credentials_and_path(self) -> None:
        """Host, port and database name are preserved (password is masked in repr)."""

        result_url = make_url(self._call("postgresql://alice:secret@dbhost:5433/mydb"))
        self.assertEqual(result_url.host, "dbhost")
        self.assertEqual(result_url.port, 5433)
        self.assertEqual(result_url.database, "mydb")
        self.assertEqual(result_url.username, "alice")

    def test_mssql_bare_dialect(self) -> None:
        """mssql:// is rewritten to use aioodbc."""
        result = self._call("mssql://user:pass@host:1433/db")
        self.assertTrue(
            result.startswith("mssql+aioodbc://"),
            f"Expected aioodbc driver, got: {result}",
        )

    def test_mssql_with_existing_driver(self) -> None:
        """mssql+pyodbc:// is rewritten to aioodbc."""
        result = self._call("mssql+pyodbc://user:pass@host:1433/db")
        self.assertTrue(result.startswith("mssql+aioodbc://"))

    def test_unknown_dialect_raises(self) -> None:
        """An unknown dialect raises ValueError rather than silently producing a bad DSN."""
        with self.assertRaises(ValueError) as ctx:
            self._call("oracle://user:pass@host:1521/db")
        self.assertIn("oracle", str(ctx.exception))

    def test_duckdb_raises(self) -> None:
        """DuckDB DSNs are not async-capable and should raise."""
        with self.assertRaises(ValueError):
            self._call("duckdb:///path/to/file.db")


class TestSchemaTranslateMap(DatafakerTestCase):
    """Tests for the cross-dialect schema routing in create_db_engine."""

    def _make_engine(self, dsn: str, schema_name: str | None = None):
        return get_sync_engine(create_db_engine(dsn, schema_name=schema_name))

    def test_no_schema_no_translate_map(self) -> None:
        """Without a schema_name, schema_translate_map is absent from execution options."""
        engine = self._make_engine("duckdb:///:memory:")
        opts = engine.get_execution_options()
        self.assertNotIn("schema_translate_map", opts)

    def test_schema_sets_translate_map(self) -> None:
        """When schema_name is given, MSSQL uses schema_translate_map (not search_path)."""
        try:
            engine = self._make_engine(
                MsSqlTestDb.get_test_db_dsn(), schema_name="myschema"
            )
        except Exception:  # pylint: disable=W0718
            self.skipTest("mssql+pyodbc driver not available in this environment")
        opts = engine.get_execution_options()
        self.assertIn("schema_translate_map", opts)
        self.assertEqual(opts["schema_translate_map"], {None: "myschema"})

    def test_duckdb_parquet_dir_sets_search_path(self) -> None:
        """For DuckDB, parquet_dir is applied via file_search_path session setting."""

        parq_dir = self.get_abs_example_dir() / "duckdb"
        with patch("datafaker.db_utils.set_db_settings") as mock_set:
            engine = get_sync_engine(
                create_db_engine(
                    "duckdb:///:memory:", schema_name="myschema", parquet_dir=parq_dir
                )
            )
            # Force a connection so the connect-event handler fires
            with engine.connect() as conn:
                conn.execute(__import__("sqlalchemy").text("SELECT 1"))

        calls = mock_set.call_args_list
        self.assertTrue(calls, "set_db_settings should have been called at least once")
        settings_passed = (
            calls[0].args[1]
            if len(calls[0].args) > 1
            else calls[0].kwargs.get("settings", {})
        )
        all_settings = "\n".join(settings_passed)
        self.assertIn("file_search_path", all_settings)
        self.assertIn(f"SET file_search_path TO '{parq_dir}';", all_settings)

    def test_mssql_dsn_schema_sets_translate_map(self) -> None:
        """schema_translate_map is set even for an MS-SQL DSN (engine creation, no connect)."""

        # create_engine with mssql+pyodbc does not connect at construction time,
        # so this is safe to run even without an ODBC driver installed.
        try:
            engine = get_sync_engine(
                create_db_engine(
                    "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server",
                    schema_name="dbo",
                )
            )
        except Exception:  # pylint: disable=W0718
            self.skipTest("mssql+pyodbc driver not available in this environment")

        opts = engine.get_execution_options()
        self.assertEqual(opts.get("schema_translate_map"), {None: "dbo"})


class TestGetMetadataSchema(unittest.TestCase):
    """Tests for the schema_name parameter on get_metadata."""

    def test_reflect_called_with_schema(self) -> None:
        """get_metadata passes schema_name to MetaData.reflect."""

        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("datafaker.db_utils.MetaData") as mock_meta_data:
            mock_md = MagicMock()
            mock_meta_data.return_value = mock_md
            mock_md.reflect.return_value = None

            get_metadata(mock_engine, schema_name="myschema", views_to_tables=False)

            mock_md.reflect.assert_called_once_with(
                mock_engine, schema="myschema", views=False
            )

    def test_reflect_called_without_schema_when_none(self) -> None:
        """get_metadata passes schema=None to reflect when no schema_name is given."""

        mock_engine = MagicMock()

        with patch("datafaker.db_utils.MetaData") as mock_meta_data:
            mock_md = MagicMock()
            mock_meta_data.return_value = mock_md
            mock_md.reflect.return_value = None

            get_metadata(mock_engine, schema_name=None, views_to_tables=False)

            mock_md.reflect.assert_called_once_with(
                mock_engine, schema=None, views=False
            )
