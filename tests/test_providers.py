"""Tests for the providers module."""
import datetime as dt
from pathlib import Path
from typing import Any

from sqlalchemy import Column, Integer, Text, create_engine, insert
from sqlalchemy.ext.declarative import declarative_base

from datafaker import providers
from tests.utils import RequiresDBTestCase, DatafakerTestCase

# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata = Base.metadata


class Person(Base):  # type: ignore
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
        metadata.create_all(self.engine)

    def test_column_value_present(self) -> None:
        """Test the key method."""
        # pylint: disable=invalid-name

        with self.engine.connect() as conn:
            stmt = insert(Person).values(sex="M")
            conn.execute(stmt)

            provider = providers.ColumnValueProvider()
            key = provider.column_value(conn, Person, "sex")

        self.assertEqual("M", key)

    def test_column_value_missing(self) -> None:
        """Test the generator when there are no values in the source table."""

        with self.engine.connect() as connection:
            provider: providers.ColumnValueProvider = providers.ColumnValueProvider()
            generated_value: Any = provider.column_value(connection, Person, "sex")

        self.assertIsNone(generated_value)


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
