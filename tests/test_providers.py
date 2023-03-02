"""Tests for the providers module."""
import datetime as dt
from pathlib import Path

from sqlalchemy import Column, Integer, Text, create_engine, insert
from sqlalchemy.ext.declarative import declarative_base

from sqlsynthgen import providers
from tests.utils import RequiresDBTestCase, SSGTestCase, run_psql

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


class BinaryProviderTestCase(SSGTestCase):
    """Tests for the BytesProvider class."""

    def test_bytes(self) -> None:
        """Test the bytes method."""
        self.assertTrue(providers.BytesProvider().bytes().decode("utf-8") != "")


class ColumnValueProviderTestCase(RequiresDBTestCase):
    """Tests for the ColumnValueProvider class."""

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql(Path("tests/examples/providers.dump"))

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers",
        )
        metadata.create_all(self.engine)

    def test_column_value(self) -> None:
        """Test the key method."""
        # pylint: disable=invalid-name

        with self.engine.connect() as conn:
            stmt = insert(Person).values(sex="M")
            conn.execute(stmt)

            provider = providers.ColumnValueProvider()
            key = provider.column_value(conn, Person, "sex")

        self.assertEqual("M", key)


class TimedeltaProvider(SSGTestCase):
    """Tests for TimedeltaProvider"""

    def test_timedelta(self) -> None:
        """Test the timedelta method."""
        min_dt = dt.timedelta(days=1)
        max_dt = dt.timedelta(days=2)
        delta = providers.TimedeltaProvider().timedelta(min_dt=min_dt, max_dt=max_dt)
        assert isinstance(delta, dt.timedelta)
        assert min_dt <= delta <= max_dt


class TimespanProvider(SSGTestCase):
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
        assert isinstance(start, dt.datetime)
        assert isinstance(end, dt.datetime)
        assert isinstance(delta, dt.timedelta)
        assert earliest_start_year <= start.year <= last_start_year
        assert min_dt <= delta <= max_dt
        assert end - start == delta


class TestWeightedBooleanProvider(SSGTestCase):
    """Tests for WeightedBooleanProvider."""

    def test_bool(self) -> None:
        """Test the bool method"""
        assert not providers.WeightedBooleanProvider().bool(0.0)
        assert providers.WeightedBooleanProvider().bool(1.0)
        seed = 0
        num_repeats = 10000
        prov = providers.WeightedBooleanProvider(seed)
        for probability in (0.1, 0.5, 0.9):
            bools = [prov.bool(probability) for _ in range(num_repeats)]
            trues = sum(bools)
            falses = sum(not x for x in bools)
            expected_odds = probability / (1 - probability)
            observed_odds = trues / falses
            assert abs(observed_odds / expected_odds - 1.0) < 0.1
