"""Tests for the providers module."""
import os
from unittest import TestCase, skipUnless

from sqlalchemy import Column, Integer, Text, create_engine, insert
from sqlalchemy.ext.declarative import declarative_base

from sqlsynthgen.providers import BytesProvider, ColumnValueProvider
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
        BytesProvider().bytes().decode("utf-8")


@skipUnless(
    os.environ.get("FUNCTIONAL_TESTS") == "1", "Set 'FUNCTIONAL_TESTS=1' to enable."
)
class ColumnValueProviderTestCase(TestCase):
    """Tests for the ColumnValueProvider class."""

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql("providers.dump")

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers"
        )
        metadata.create_all(self.engine)

    def test_column_value(self) -> None:
        """Test the key method."""
        # pylint: disable=invalid-name

        with self.engine.connect() as conn:
            stmt = insert(Person).values(sex="M")
            conn.execute(stmt)

            provider = ColumnValueProvider()
            key = provider.column_value(conn, "public", "person", "sex")

        self.assertEqual("M", key)
