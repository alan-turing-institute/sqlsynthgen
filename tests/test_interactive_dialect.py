"""Tests for dialect-correct SQL in interactive shell methods."""
# pylint: disable=protected-access
import unittest
from unittest.mock import MagicMock

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.dialects import mssql, postgresql

from datafaker.interactive.base import DbCmd
from datafaker.interactive.generators import GeneratorCmd
from datafaker.interactive.table import TableCmd


def _make_engine(dialect) -> MagicMock:
    """Return a mock engine whose dialect is the given SQLAlchemy dialect instance."""
    engine = MagicMock()
    engine.dialect = dialect
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    executed = []

    def capture(stmt, *_args, **_kwargs):
        executed.append(stmt)
        result = MagicMock()
        result.keys.return_value = []
        result.fetchmany.return_value = []
        result.all.return_value = []
        return result

    conn.execute.side_effect = capture
    engine.connect.return_value = conn
    engine._executed = executed
    return engine


def _make_table(schema=None) -> Table:
    meta = MetaData()
    return Table("person", meta, Column("gender_concept_id", Integer()), schema=schema)


def _compiled(stmt, dialect) -> str:
    return str(
        stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    ).upper()


class TestPeekDialect(unittest.TestCase):
    """DbCmd.do_peek() uses NEWID/TOP on MS-SQL and RANDOM/LIMIT on PostgreSQL."""

    def _run_peek(self, dialect_instance, col_names=None, schema=None):
        engine = _make_engine(dialect_instance)
        shell = MagicMock(spec=DbCmd)
        shell.sync_engine = engine
        shell.table_index = 0
        shell._table_entries = [MagicMock()]
        shell.table_name.return_value = "person"
        shell._get_column_names.return_value = col_names or ["gender_concept_id"]
        shell.table_metadata.return_value = _make_table(schema=schema)
        shell.print_table = MagicMock()
        shell.print = MagicMock()

        DbCmd.do_peek(shell, " ".join(col_names) if col_names else "")
        return engine._executed, dialect_instance

    def test_mssql_peek_uses_rand_and_top(self) -> None:
        """MS-SQL do_peek compiles to NEWID() with TOP."""
        executed, dialect = self._run_peek(mssql.dialect())
        self.assertEqual(len(executed), 1)
        sql = _compiled(executed[0], dialect)
        self.assertIn(" TOP ", sql)
        self.assertIn("NEWID()", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertNotIn("RANDOM()", sql)

    def test_postgresql_peek_uses_random_and_limit(self) -> None:
        """PostgreSQL do_peek compiles to RANDOM() … LIMIT."""
        executed, dialect = self._run_peek(postgresql.dialect())
        self.assertEqual(len(executed), 1)
        sql = _compiled(executed[0], dialect)
        self.assertIn("RANDOM()", sql)
        self.assertIn("LIMIT", sql)
        self.assertNotIn("NEWID()", sql)
        self.assertNotIn("RAND()", sql)
        self.assertNotIn(" TOP ", sql)
        self.assertNotIn("ROW_NUMBER()", sql)

    def test_schema_appears_in_from(self) -> None:
        """Schema-qualified table name appears in the FROM clause on both dialects."""
        for dialect in (mssql.dialect(), postgresql.dialect()):
            with self.subTest(dialect=dialect.name):
                executed, d = self._run_peek(dialect, schema="myschema")
                sql = _compiled(executed[0], d)
                self.assertIn("MYSCHEMA", sql)


class TestGetColumnDataDialect(unittest.TestCase):
    """Check that GeneratorCmd._get_column_data() produces the correct dialect."""

    def _run_get_column_data(self, dialect_instance, schema=None):
        engine = _make_engine(dialect_instance)
        shell = MagicMock(spec=GeneratorCmd)
        shell.sync_engine = engine
        shell.table_name.return_value = "person"
        shell._get_column_names.return_value = ["gender_concept_id"]
        shell.table_metadata.return_value = _make_table(schema=schema)

        GeneratorCmd._get_column_data(shell, 5)
        return engine._executed, dialect_instance

    def test_mssql_uses_rand_and_top(self) -> None:
        """MS-SQL _get_column_data compiles to TOP … NEWID()."""
        executed, dialect = self._run_get_column_data(mssql.dialect())
        self.assertEqual(len(executed), 1)
        sql = _compiled(executed[0], dialect)
        self.assertIn(" TOP ", sql)
        self.assertIn("NEWID()", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertNotIn("RANDOM()", sql)

    def test_postgresql_uses_random_and_limit(self) -> None:
        """PostgreSQL _get_column_data compiles to RANDOM() … LIMIT."""
        executed, dialect = self._run_get_column_data(postgresql.dialect())
        self.assertEqual(len(executed), 1)
        sql = _compiled(executed[0], dialect)
        self.assertIn("RANDOM()", sql)
        self.assertIn("LIMIT", sql)
        self.assertNotIn("NEWID()", sql)
        self.assertNotIn("RAND()", sql)
        self.assertNotIn(" TOP ", sql)
        self.assertNotIn("ROW_NUMBER()", sql)

    def test_schema_appears_in_from(self) -> None:
        """Schema-qualified table name appears in the FROM clause on both dialects."""
        for dialect in (mssql.dialect(), postgresql.dialect()):
            with self.subTest(dialect=dialect.name):
                executed, d = self._run_get_column_data(dialect, schema="myschema")
                sql = _compiled(executed[0], d)
                self.assertIn("MYSCHEMA", sql)


class TestPrintColumnDataDialect(unittest.TestCase):
    """TableCmd.print_column_data() uses NEWID/TOP on MS-SQL and RANDOM/LIMIT on PostgreSQL."""

    def _run_print_column_data(self, dialect_instance, schema=None):
        engine = _make_engine(dialect_instance)
        shell = MagicMock(spec=TableCmd)
        shell.sync_engine = engine
        shell.table_name.return_value = "person"
        shell.table_metadata.return_value = _make_table(schema=schema)
        shell.columnize = MagicMock()

        TableCmd.print_column_data(shell, "gender_concept_id", 10, 0)
        return engine._executed, dialect_instance

    def test_mssql_uses_rand_and_top(self) -> None:
        """MS-SQL print_column_data compiles to TOP … NEWID()."""
        executed, dialect = self._run_print_column_data(mssql.dialect())
        self.assertEqual(len(executed), 1)
        sql = _compiled(executed[0], dialect)
        self.assertIn(" TOP ", sql)
        self.assertIn("NEWID()", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertNotIn("RANDOM()", sql)

    def test_postgresql_uses_random_and_limit(self) -> None:
        """PostgreSQL print_column_data compiles to RANDOM() … LIMIT."""
        executed, dialect = self._run_print_column_data(postgresql.dialect())
        self.assertEqual(len(executed), 1)
        sql = _compiled(executed[0], dialect)
        self.assertIn("RANDOM()", sql)
        self.assertIn("LIMIT", sql)
        self.assertNotIn("RAND()", sql)
        self.assertNotIn("NEWID()", sql)
        self.assertNotIn(" TOP ", sql)
        self.assertNotIn("ROW_NUMBER()", sql)

    def test_schema_appears_in_from(self) -> None:
        """Schema-qualified table name appears in the FROM clause on both dialects."""
        for dialect in (mssql.dialect(), postgresql.dialect()):
            with self.subTest(dialect=dialect.name):
                executed, d = self._run_print_column_data(dialect, schema="myschema")
                sql = _compiled(executed[0], d)
                self.assertIn("MYSCHEMA", sql)


class TestCountsDialect(unittest.TestCase):
    """DbCmd.do_counts() compiles a schema-qualified COUNT query."""

    def _run_counts(self, dialect_instance, schema=None):
        engine = _make_engine(dialect_instance)
        tbl = _make_table(schema=schema)
        shell = MagicMock(spec=DbCmd)
        shell.sync_engine = engine
        shell._table_entries = [MagicMock()]
        shell.table_index = 0
        shell.table_name.return_value = "person"
        shell.get_nullable_columns.return_value.name = ["gender_concept_id"]
        shell.table_metadata.return_value = tbl
        shell.print = MagicMock()
        shell.print_table = MagicMock()

        DbCmd.do_counts(shell, "")
        return engine._executed, dialect_instance

    def test_counts_schema_appears_in_from(self) -> None:
        """do_counts FROM clause includes the schema when one is set."""
        for dialect in (mssql.dialect(), postgresql.dialect()):
            with self.subTest(dialect=dialect.name):
                executed, d = self._run_counts(dialect, schema="myschema")
                self.assertEqual(len(executed), 1)
                sql = _compiled(executed[0], d)
                self.assertIn("MYSCHEMA", sql)

    def test_counts_no_schema_omits_qualifier(self) -> None:
        """do_counts FROM clause has no schema prefix when schema is None."""
        executed, d = self._run_counts(postgresql.dialect(), schema=None)
        sql = _compiled(executed[0], d)
        self.assertIn("FROM PERSON", sql)
        self.assertNotIn("FROM MYSCHEMA", sql)
