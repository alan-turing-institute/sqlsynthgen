"""Tests for the providers module."""
import datetime as dt
from collections import Counter
from typing import Any
from unittest.mock import MagicMock

from sqlalchemy import Column, Integer, MetaData, Text, insert
from sqlalchemy.dialects import mssql, postgresql
from sqlalchemy.orm import declarative_base

from datafaker import providers
from tests.utils import DatafakerTestCase, RequiresDBTestCase

# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata: MetaData = Base.metadata


class Person(Base):
    """A SQLAlchemy table."""

    __tablename__ = "person"
    person_id = Column(
        Integer,
        primary_key=True,
    )
    # We don't actually need a foreign key constraint to test this
    sex = Column(Text)


class BinaryProviderTestCase(DatafakerTestCase):
    """Tests for the BytesProvider class."""

    def test_bytes(self) -> None:
        """Test the bytes method."""
        self.assertTrue(providers.BytesProvider().bytes().decode("utf-8") != "")


class ColumnValueProviderTestCase(RequiresDBTestCase):
    """Tests for the ColumnValueProvider class."""

    dump_file_path = "providers.dump"

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()
        metadata.create_all(self.sync_engine)

    def test_column_value_present(self) -> None:
        """Test the key method."""
        # pylint: disable=invalid-name

        with self.sync_engine.connect() as conn:
            stmt = insert(Person).values(sex="M")
            conn.execute(stmt)

            provider = providers.ColumnValueProvider()
            key = provider.column_value(conn, Person, "sex")

        self.assertEqual("M", key)

    def test_column_value_missing(self) -> None:
        """Test the generator when there are no values in the source table."""

        with self.sync_engine.connect() as connection:
            provider: providers.ColumnValueProvider = providers.ColumnValueProvider()
            generated_value: Any = provider.column_value(connection, Person, "sex")

        self.assertIsNone(generated_value)


class ColumnValueRandomFunctionTestCase(DatafakerTestCase):
    """column_value uses the correct random function for each dialect."""

    def _make_connection(self, dialect_name: str) -> MagicMock:
        conn = MagicMock()
        conn.dialect.name = dialect_name
        conn.execute.return_value.first.return_value = None
        return conn

    def _get_order_by_sql(self, dialect_name: str) -> str:
        conn = self._make_connection(dialect_name)
        providers.ColumnValueProvider.column_value(conn, Person, "sex")
        query = conn.execute.call_args[0][0]

        dialect = mssql.dialect() if dialect_name == "mssql" else postgresql.dialect()
        return str(
            query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        )

    def test_mssql_uses_rand(self) -> None:
        """Test that the column provider uses RAND for the Postgres dialect."""
        sql = self._get_order_by_sql("mssql")
        self.assertIn("newid()", sql.lower())
        self.assertNotIn("random()", sql.lower())

    def test_postgresql_uses_random(self) -> None:
        """Test that the column provider uses RANDOM for the Postgres dialect."""
        sql = self._get_order_by_sql("postgresql")
        self.assertIn("random()", sql.lower())
        self.assertNotIn("newid()", sql.lower())
        self.assertNotIn("rand()", sql.lower())


class TimedeltaProvider(DatafakerTestCase):
    """Tests for TimedeltaProvider"""

    def test_timedelta(self) -> None:
        """Test the timedelta method."""
        min_dt = dt.timedelta(days=1)
        max_dt = dt.timedelta(days=2)
        delta = providers.TimedeltaProvider().timedelta(min_dt=min_dt, max_dt=max_dt)
        self.assertIsInstance(delta, dt.timedelta)
        self.assertLessEqual(min_dt, delta)
        self.assertLessEqual(delta, max_dt)


class TimespanProvider(DatafakerTestCase):
    """Tests for TimespanProvider."""

    def test_timespan(self) -> None:
        """Test the timespan method"""
        earliest_start_year = 1917
        last_start_year = 1923
        min_dt = dt.timedelta(seconds=2)
        max_dt = dt.timedelta(days=10000)
        start, end, delta = providers.TimespanProvider().timespan(
            earliest_start_year, last_start_year, min_dt, max_dt
        )
        self.assertIsInstance(start, dt.datetime)
        self.assertIsInstance(end, dt.datetime)
        self.assertIsInstance(delta, dt.timedelta)
        self.assertLessEqual(earliest_start_year, start.year)
        self.assertLessEqual(start.year, last_start_year)
        self.assertLessEqual(min_dt, delta)
        self.assertLessEqual(delta, max_dt)
        self.assertEqual(end - start, delta)


class DistributionProviderChoiceTestCase(DatafakerTestCase):
    """Tests for DistributionProvider's weighted_choice/zipf_choice_direct."""

    def test_weighted_choice_follows_counts(self) -> None:
        """weighted_choice should pick values in proportion to their counts."""
        prov = providers.DistributionProvider(seed=0)
        a = [
            {"value": "rare", "count": 1},
            {"value": "common", "count": 9},
            {"value": "never", "count": 0},
        ]
        counts = Counter(prov.weighted_choice(a) for _ in range(20000))
        self.assertNotIn("never", counts)
        observed_odds = counts["common"] / counts["rare"]
        self.assertLess(abs(observed_odds / 9 - 1.0), 0.15)

    def test_weighted_choice_empty(self) -> None:
        """weighted_choice on an empty list returns None."""
        prov = providers.DistributionProvider(seed=0)
        self.assertIsNone(prov.weighted_choice([]))

    def test_weighted_choice_caches_per_list_identity(self) -> None:
        """Repeated/alternating calls with distinct lists must not cross-contaminate.

        Regression test for the id(a)-keyed cache added to speed up repeated
        calls with the same list object: interleaving two different lists
        must not return the wrong (stale) values/weights for either.
        """
        prov = providers.DistributionProvider(seed=0)
        a = [{"value": "a-only", "count": 1}]
        b = [{"value": "b-only", "count": 1}]
        seen = {prov.weighted_choice(a) for _ in range(5)}
        seen |= {prov.weighted_choice(b) for _ in range(5)}
        seen |= {prov.weighted_choice(a) for _ in range(5)}
        self.assertEqual(seen, {"a-only", "b-only"})

    def test_zipf_choice_direct_decreasing_frequency(self) -> None:
        """zipf_choice_direct should favour earlier (more frequent) values."""
        prov = providers.DistributionProvider(seed=0)
        values = list(range(10))
        counts = Counter(prov.zipf_choice_direct(values) for _ in range(20000))
        frequencies = [counts[v] for v in values]
        self.assertEqual(frequencies, sorted(frequencies, reverse=True))


class TestWeightedBooleanProvider(DatafakerTestCase):
    """Tests for WeightedBooleanProvider."""

    def test_bool(self) -> None:
        """Test the bool method"""
        self.assertFalse(providers.WeightedBooleanProvider().bool(0.0))
        self.assertTrue(providers.WeightedBooleanProvider().bool(1.0))
        seed = 0
        num_repeats = 10000
        prov = providers.WeightedBooleanProvider(seed=seed)
        for probability in (0.1, 0.5, 0.9):
            bools = [prov.bool(probability) for _ in range(num_repeats)]
            trues = sum(bools)
            falses = sum(not x for x in bools)
            expected_odds = probability / (1 - probability)
            observed_odds = trues / falses
            self.assertLess(abs(observed_odds / expected_odds - 1.0), 0.1)
