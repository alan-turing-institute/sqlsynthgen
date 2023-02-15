"""Tests for the providers module."""
import datetime as dt
import os
from unittest import TestCase, skipUnless

from sqlalchemy import Column, Integer, Text, create_engine, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool

from sqlsynthgen import providers
from tests.utils import run_psql

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


class BinaryProviderTestCase(TestCase):
    """Tests for the BytesProvider class."""

    def test_bytes(self) -> None:
        """Test the bytes method."""
        self.assertTrue(providers.BytesProvider().bytes().decode("utf-8") != "")


@skipUnless(
    os.environ.get("FUNCTIONAL_TESTS") == "1", "Set 'FUNCTIONAL_TESTS=1' to enable."
)
class ColumnValueProviderTestCase(TestCase):
    """Tests for the ColumnValueProvider class."""

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql("providers.dump")

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers",
            # pool=NullPool
            poolclass=NullPool,
        )
        metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_column_value(self) -> None:
        """Test the key method."""
        # pylint: disable=invalid-name

        with self.engine.connect() as conn:
            stmt = insert(Person).values(sex="M")
            conn.execute(stmt)

            provider = providers.ColumnValueProvider()
            key = provider.column_value(conn, Person, "sex")

        self.assertEqual("M", key)
        # pass


class TimedeltaProvider(TestCase):
    """Tests for TimedeltaProvider"""

    def test_timedelta(self) -> None:
        """Test the timedelta method."""
        min_dt = dt.timedelta(days=1)
        max_dt = dt.timedelta(days=2)
        delta = providers.TimedeltaProvider().timedelta(min_dt=min_dt, max_dt=max_dt)
        assert isinstance(delta, dt.timedelta)
        assert min_dt <= delta <= max_dt


class TimespanProvider(TestCase):
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
