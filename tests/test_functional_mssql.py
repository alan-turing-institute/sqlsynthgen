"""End-to-end tests for the MS-SQL Server dialect.

These tests require a running SQL Server instance.  Set the ``MSSQL_TEST_DSN``
environment variable to a ``mssql+pyodbc://`` connection string to enable them:

    export MSSQL_TEST_DSN="mssql+pyodbc://sa:Datafaker!Test123@\
    localhost:1433/master?driver=ODBC+Driver+18+for+SQL+Server\
    &TrustServerCertificate=yes"

With docker-compose:

    docker compose up -d mssql
    # wait ~30 s for SQL Server to start
"""
import asyncio
import os
from tempfile import mkstemp

import yaml
from sqlalchemy import Column, MetaData, Table, Text
from sqlalchemy import create_engine as sa_create_engine
from sqlalchemy import text
from sqlalchemy.dialects import mssql as mssql_dialect  # noqa: PLC0415
from sqlalchemy.schema import CreateTable

from datafaker.make import make_src_stats, make_tables_file
from datafaker.proposers.choice import ZipfChoiceProposer  # noqa: PLC0415
from tests.utils import DatafakerTestCase, GeneratesDBTestCase, MsSqlTestDb

_EXPECTED_TABLES = frozenset(
    {"manufacturer", "model", "string", "player", "signature_model"}
)


# ---------------------------------------------------------------------------
# Test case
# ---------------------------------------------------------------------------


class MSSQLFunctionalTestCase(GeneratesDBTestCase):
    """End-to-end tests exercising the full datafaker pipeline against SQL Server."""

    database_type = MsSqlTestDb
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = None
    dst_schema_name = "dst"

    def setUp(self) -> None:
        super().setUp()

        # Write orm.yaml so generate_data() has the file it expects.
        (self.orm_fd, self.orm_file_path) = mkstemp(".yaml", "orm_", text=True)
        with os.fdopen(self.orm_fd, "w", encoding="utf-8") as fh:
            fh.write(
                make_tables_file(
                    self.dsn, self.schema_name, False, engine=self.sync_engine
                )
            )

    def tearDown(self) -> None:
        # Dispose connection pools so the next setUp can drop these databases.
        if hasattr(self, "sync_engine"):
            self.sync_engine.dispose()
        if hasattr(self, "dst_engine") and self.dst_engine is not None:
            self.dst_engine.dispose()
        if self.database is not None:
            self.database.close()
        if self.dst_database is not None:
            self.dst_database.close()
        DatafakerTestCase.tearDown(self)

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_smoke_connect(self) -> None:
        """ODBC driver can connect and run a trivial query."""
        engine = sa_create_engine(self.dsn)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT 1 AS n")).fetchone()
        assert row is not None
        self.assertEqual(row[0], 1)

    def test_make_tables(self) -> None:
        """make_tables_file produces an orm.yaml listing the expected tables."""
        # setUp already called make_tables_file and wrote the result to orm_file_path
        with open(self.orm_file_path, encoding="utf-8") as fh:
            orm = yaml.safe_load(fh)
        # orm["tables"] is a dict keyed by table name
        table_names = set(orm.get("tables", {}).keys())
        self.assert_subset(_EXPECTED_TABLES, table_names)

    def test_make_stats(self) -> None:
        """make_src_stats runs without error against SQL Server.

        With an empty config there are no src-stats query blocks to run, so the
        function returns an empty dict — that is the correct behaviour.
        """
        loop = asyncio.new_event_loop()
        try:
            src_stats = loop.run_until_complete(
                make_src_stats(self.dsn, {}, self.schema_name)
            )
        finally:
            loop.close()
        self.assertIsInstance(src_stats, dict)

    def test_create_data(self) -> None:
        """Full pipeline: make-stats → create-tables → create-data inserts rows."""
        self.generate_data({})

        # Verify that at least the manufacturer table received rows.
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {self.dst_schema_name}.manufacturer")
            ).scalar()
        self.assertGreater(count, 0, "Expected rows in manufacturer after create-data")

    def test_dialect_rand(self) -> None:
        """ChoiceProposer compiles its query with NEWID() not RANDOM() for mssql."""

        dialect = mssql_dialect.dialect()
        table = Table("manufacturer", MetaData(), Column("name", Text()))
        proposer = ZipfChoiceProposer(
            column=table.c.name,
            values=["Blender", "Gibbs"],
            counts=[5, 5],
            sample_count=2,
            dialect=dialect,
        )
        self.assertIn("newid()", proposer._query.lower())  # pylint: disable=W0212
        self.assertNotIn("random()", proposer._query.lower())  # pylint: disable=W0212

    def test_cascade_stripped(self) -> None:
        """The @compiles(CreateTable, 'mssql') hook strips ON DELETE CASCADE."""

        model_table = self.metadata.tables["model"]
        ddl = str(
            CreateTable(model_table).compile(
                dialect=mssql_dialect.dialect(), compile_kwargs={"literal_binds": True}
            )
        )
        self.assertIn("FOREIGN KEY", ddl)
        self.assertNotIn("ON DELETE CASCADE", ddl)
