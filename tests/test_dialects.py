"""Tests for the dialect helpers in datafaker.dialects."""

import unittest

from sqlalchemy import column, literal_column, select, table
from sqlalchemy.dialects import mssql, postgresql
from sqlalchemy.engine import Dialect

from datafaker.dialects import random_sample


class TestRandomSample(unittest.TestCase):
    """Tests for random_sample."""

    def _sql(self, query, dialect: Dialect) -> str:
        """Compile a query to SQL text for the given dialect."""
        return str(
            query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        )

    def _simple(self, n=5):
        """A random sample of 'n' rows of column x from table t."""
        return random_sample(select(column("x")).select_from(table("t")), n)

    def test_postgresql_orders_randomly_then_limits(self) -> None:
        """PostgreSQL: ORDER BY RANDOM() comes before LIMIT."""
        sql = self._sql(self._simple(), postgresql.dialect())
        self.assertIn("ORDER BY RANDOM()", sql)
        self.assertIn("LIMIT 5", sql)
        self.assertLess(sql.index("ORDER BY"), sql.index("LIMIT"))

    def test_mssql_uses_top_and_newid(self) -> None:
        """MS-SQL: TOP n with ORDER BY NEWID()."""
        sql = self._sql(self._simple(), mssql.dialect())
        self.assertIn("TOP 5", sql)
        self.assertIn("ORDER BY NEWID()", sql)

    def test_keeps_existing_where_clause(self) -> None:
        """A WHERE clause on the query is kept, before the random ordering."""
        query = random_sample(
            select(column("x")).select_from(table("t")).where(column("x").isnot(None)),
            5,
        )
        sql = self._sql(query, postgresql.dialect())
        self.assertIn("WHERE x IS NOT NULL", sql)
        self.assertLess(sql.index("WHERE"), sql.index("ORDER BY"))

    def test_accepts_sql_expression_as_limit(self) -> None:
        """The limit can be an SQL expression, as missingness.py passes."""
        query = random_sample(
            select(column("x")).select_from(table("t")), literal_column("1000")
        )
        sql = self._sql(query, postgresql.dialect())
        self.assertIn("LIMIT 1000", sql)
