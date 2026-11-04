"""Unit tests for proposers."""
import re
from pathlib import Path

import duckdb_sqlalchemy
import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    select,
    text,
)

from datafaker.db_utils import create_db_engine, get_sync_engine
from datafaker.interactive.generators import get_aggregate_query
from datafaker.proposers import ProposerFactory, everything_factory
from datafaker.proposers.base import Proposer
from tests.utils import DatafakerTestCase

select_re = re.compile(
    r"SELECT\s+([A-Za-z0-9_.\"]+)\s+FROM\s+([\"A-Za-z0-9_.]+)\s+AS\s+([\"A-Za-z0-9_.]+)",
    re.IGNORECASE,
)


class ProposerUnitTests(DatafakerTestCase):
    """Proposer test case."""

    def test_duckdb_workaround(self) -> None:
        """Test the duckdb workaround where Tables always get aliased."""
        tabname = "tab1"
        colname = "col1"
        metadata = MetaData()
        table = Table(tabname, metadata)
        column = Column(colname, Text())
        table.append_column(column)
        stmt = select(column)
        ddbd = duckdb_sqlalchemy.Dialect()
        sql = stmt.compile(dialect=ddbd, compile_kwargs={"literal_binds": True})
        grps = select_re.match(str(sql))
        assert grps is not None
        tcs = grps.group(1).split(".")
        self.assertEqual(len(tcs), 2)
        self.assertEqual(tcs[0], grps.group(3))
        self.assertEqual(tcs[1], colname)
        self.assertEqual(grps.group(2), tabname)


class DuckDbProposerQueriesUnitTests(DatafakerTestCase):
    """Proposer's src-stats queries test case."""

    use_temporary_cwd = True

    def test_parquet_queries(self) -> None:
        """Test that DuckDB can run src-stats queries against parquet files."""
        filename = "source.parquet"
        pd.DataFrame(
            {
                "real": [1.0, 2.0, 2.3, 1.4, 2.1, 1.7],
                "integer": [5, 18, 17, 10, 11, 6],
                "timestamp": [
                    pd.Timestamp("20200101"),
                    pd.Timestamp("20200204"),
                    pd.Timestamp("20200403"),
                    pd.Timestamp("20200318"),
                    pd.Timestamp("20200421"),
                    pd.Timestamp("20200213"),
                ],
                "name": ["Alice", "Alice", "Charlie", "Alice", "Charlie", "Charlie"],
            }
        ).to_parquet(filename)
        ma_engine = create_db_engine("duckdb:///:memory:", parquet_dir=Path("."))
        engine = get_sync_engine(ma_engine)
        col_real = Column("real", Float)
        col_int = Column("integer", Integer)
        col_time = Column("timestamp", DateTime)
        col_name = Column("name", String)
        metadata = MetaData()
        tab = Table("source.parquet", metadata)
        tab.append_column(col_real)
        tab.append_column(col_int)
        tab.append_column(col_time)
        tab.append_column(col_name)
        factory = everything_factory({}, metadata)
        columnss: list[list[Column]] = [
            [col_real],
            [col_int],
            [col_time],
            [col_name],
            [col_real, col_int],
            [col_real, col_time],
            [col_real, col_name],
            [col_real, col_time, col_name],
        ]
        for columns in columnss:
            self.try_queries_from_columns(columns, filename, factory, engine)

    def try_queries_from_columns(
        self,
        columns: list[Column],
        table_name: str,
        factory: ProposerFactory,
        engine: Engine,
    ) -> None:
        """
        Attempt to run custom and aggregate queries for this set of columns.

        Throw an exception if any of them fails.
        """
        proposers = factory.get_proposers(columns, engine)
        agg_sql = get_aggregate_query(proposers, table_name, engine)
        if agg_sql is not None:
            # Test that the aggregate query SQL succeeds
            with engine.connect() as conn:
                try:
                    conn.execute(text(agg_sql)).fetchall()
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    self.fail(f"Aggregate query failed:\n{agg_sql}\n{exc}")
        for proposer in proposers:
            self.try_each_custom_query(proposer, engine)

    def try_each_custom_query(self, proposer: Proposer, engine: Engine) -> None:
        """
        Attempt to run each custom query in the proposer.

        Throw an exception if any of them fails.
        """
        for name, query_desc in proposer.custom_queries().items():
            query = query_desc["query"]
            with engine.connect() as conn:
                try:
                    conn.execute(text(query)).fetchall()
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    self.fail(
                        f"Custom query from {proposer.name()} {name} failed:\n{query}\n{exc}"
                    )
