"""Unit tests for the UUID proposer."""
import uuid as uuid_lib

from sqlalchemy import Column, MetaData, String, Table, create_engine
from sqlalchemy.dialects import mssql, postgresql
from sqlalchemy.types import Uuid

from datafaker.proposers.uuid import UuidProposer, UuidProposerFactory
from tests.utils import DatafakerTestCase


class UuidProposerFactoryUnitTests(DatafakerTestCase):
    """Tests for ``UuidProposerFactory``."""

    def setUp(self) -> None:
        """Set up an in-memory engine the factory can actually query."""
        super().setUp()
        self.engine = create_engine("sqlite:///:memory:")
        self.factory = UuidProposerFactory()

    def _make_table(self, *columns: Column) -> Table:
        """Build a bare (not physically created) table for factory-only checks."""
        meta = MetaData()
        return Table("t", meta, *columns)

    def _make_real_table(self, *columns: Column) -> Table:
        """Build and physically create a table, for tests that insert real rows."""
        table = self._make_table(*columns)
        table.metadata.create_all(self.engine)
        return table

    def _insert(self, table: Table, column: Column, values: list[str]) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                table.insert(), [{column.name: value} for value in values]
            )

    # -- Native UUID SQL types: never need to query data --------------------

    def test_native_uuid_type_is_proposed(self) -> None:
        """A column with SQLAlchemy's own ``Uuid`` type is always proposed."""
        table = self._make_table(Column("id", Uuid()))
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    def test_postgresql_uuid_type_is_proposed(self) -> None:
        """PostgreSQL's ``UUID`` type is recognised (via ``as_generic``)."""
        table = self._make_table(Column("id", postgresql.UUID()))
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    def test_mssql_uniqueidentifier_type_is_proposed(self) -> None:
        """MS-SQL's ``UNIQUEIDENTIFIER`` type is recognised (via ``as_generic``)."""
        table = self._make_table(Column("id", mssql.UNIQUEIDENTIFIER()))
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    # -- String primary keys: decided from real sampled data -----------------

    def test_string_pk_with_real_uuid_data_is_proposed(self) -> None:
        """A String PK whose real values are UUID-shaped is proposed, by content."""
        table = self._make_real_table(Column("id", String(36), primary_key=True))
        self._insert(table, table.c.id, [str(uuid_lib.uuid4()) for _ in range(20)])
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    def test_string_pk_with_unhyphenated_uuid_data_is_proposed(self) -> None:
        """A String PK holding hex-only (no hyphen) UUIDs is also recognised."""
        table = self._make_real_table(Column("id", String(32), primary_key=True))
        self._insert(table, table.c.id, [uuid_lib.uuid4().hex for _ in range(20)])
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    def test_unconstrained_length_string_pk_with_uuid_data_is_proposed(self) -> None:
        """A length-less String PK (e.g. Snowflake's unconstrained VARCHAR) is still
        recognised from its actual content, not its (absent) declared length."""
        table = self._make_real_table(Column("id", String(), primary_key=True))
        self._insert(table, table.c.id, [str(uuid_lib.uuid4()) for _ in range(20)])
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    def test_coincidentally_uuid_length_natural_key_is_not_proposed(self) -> None:
        """A 36-char natural key that isn't UUID-shaped is correctly rejected.

        This is exactly the false positive a length-only check would produce.
        """
        table = self._make_real_table(Column("id", String(36), primary_key=True))
        self._insert(
            table,
            table.c.id,
            [f"NATURAL-KEY-{i:020d}-NOT-A-UUID!!" for i in range(20)],
        )
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(proposers, [])

    def test_non_pk_uuid_data_is_not_proposed(self) -> None:
        """A UUID-shaped String column that is not a primary key is not proposed."""
        table = self._make_real_table(Column("id", String(36)))
        self._insert(table, table.c.id, [str(uuid_lib.uuid4()) for _ in range(20)])
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(proposers, [])

    # -- No real data to sample: fall back to the length heuristic -----------

    def test_empty_table_with_uuid_length_pk_falls_back_to_length(self) -> None:
        """With no rows to sample, a canonical-length String PK is still proposed."""
        table = self._make_real_table(Column("id", String(36), primary_key=True))
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(len(proposers), 1)
        self.assertIsInstance(proposers[0], UuidProposer)

    def test_empty_table_with_wrong_length_pk_is_not_proposed(self) -> None:
        """With no rows to sample, a non-canonical length is not proposed."""
        table = self._make_real_table(Column("id", String(10), primary_key=True))
        proposers = self.factory.get_proposers([table.c.id], self.engine)
        self.assertEqual(proposers, [])

    def test_multi_column_is_not_proposed(self) -> None:
        """Composite keys are out of scope for this proposer."""
        table = self._make_table(
            Column("a", String(36), primary_key=True),
            Column("b", String(36), primary_key=True),
        )
        proposers = self.factory.get_proposers([table.c.a, table.c.b], self.engine)
        self.assertEqual(proposers, [])
