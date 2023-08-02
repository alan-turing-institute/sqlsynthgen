"""Tests for the base module."""
import os
from pathlib import Path

from sqlalchemy import Column, Integer, create_engine, select
from sqlalchemy.orm import declarative_base

from sqlsynthgen.base import FileUploader
from tests.utils import RequiresDBTestCase, run_psql

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


class VocabTests(RequiresDBTestCase):
    """Module test case."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql(Path("tests/examples/providers.dump"))

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers"
        )
        metadata.create_all(self.engine)
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)

    def test_load(self) -> None:
        """Test the load method."""
        vocab_gen = FileUploader(BaseTable.__table__)

        with self.engine.connect() as conn:
            vocab_gen.load(conn)
            statement = select(BaseTable)
            rows = list(conn.execute(statement))
        self.assertEqual(3, len(rows))
