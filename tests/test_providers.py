"""Tests for the providers module."""
import os
from unittest import TestCase, skipUnless

from sqlalchemy import Column, Integer, Text, create_engine, insert
from sqlalchemy.ext.declarative import declarative_base

from sqlsynthgen.providers import BinaryProvider, ForeignKeyProvider
from tests.utils import run_psql


class BinaryProviderTestCase(TestCase):
    """Tests for the BinaryProvider class."""

    def test_bytes(self) -> None:
        BinaryProvider().bytes().decode("utf-8")


@skipUnless(
    os.environ.get("FUNCTIONAL_TESTS") == "1", "Set 'FUNCTIONAL_TESTS=1' to enable."
)
class ForeignKeyProviderTestCase(TestCase):
    """Tests for the ForeignKeyProvider class."""

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql("providers.dump")

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers"
        )

    def test_key(self) -> None:
        """Test the key method."""
        # pylint: disable=invalid-name

        Base = declarative_base()
        metadata = Base.metadata

        class Person(Base):  # type: ignore
            """A SQLAlchemy table."""

            __tablename__ = "person"
            person_id = Column(
                Integer,
                primary_key=True,
            )
            sex = Column(Text)

        metadata.create_all(self.engine)

        with self.engine.connect() as conn:
            stmt = insert(Person).values(sex="M")
            conn.execute(stmt)

            fkp = ForeignKeyProvider()
            key = fkp.key(conn, "public", "person", "sex")

        self.assertEqual("M", key)
