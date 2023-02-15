"""Tests for the main module."""
import os
from unittest import TestCase

from sqlalchemy import Column, Integer, create_engine, select
from sqlalchemy.orm import declarative_base

from sqlsynthgen.base import FileUploader
from tests.utils import run_psql

# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata = Base.metadata


class BaseTable(Base):  # type: ignore
    """A SQLAlchemy table."""

    __tablename__ = "basetable"
    id = Column(
        Integer,
        primary_key=True,
    )


class VocabTests(TestCase):
    """Module test case."""

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql("providers.dump")

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers"
        )
        metadata.create_all(self.engine)
        os.chdir("tests/examples")

    def tearDown(self) -> None:
        os.chdir("../..")

    def test_load(self) -> None:
        """Test the load method."""
        vocab_gen = FileUploader(BaseTable.__table__)

        with self.engine.connect() as conn:
            vocab_gen.load(conn)
            statement = select([BaseTable])
            rows = list(conn.execute(statement))
        self.assertEqual(3, len(rows))
