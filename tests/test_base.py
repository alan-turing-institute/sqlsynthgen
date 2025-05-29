"""Tests for the base module."""
import os
from pathlib import Path

from sqlalchemy import Column, Integer, select
from sqlalchemy.orm import declarative_base

from datafaker.base import FileUploader
from tests.utils import RequiresDBTestCase

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

    dump_file_path = "providers.dump"
    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()

        metadata.create_all(self.engine)
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)
        super().tearDown()

    def test_load(self) -> None:
        """Test the load method."""
        vocab_gen = FileUploader(BaseTable.__table__)

        with self.engine.connect() as conn:
            vocab_gen.load(conn)
            statement = select(BaseTable)
            rows = list(conn.execute(statement))
        self.assertEqual(3, len(rows))
