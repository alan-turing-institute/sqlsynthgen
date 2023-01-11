"""Tests for the providers module."""
import os
from pathlib import Path
from subprocess import run
from unittest import TestCase, skipUnless

from sqlalchemy import Column, Integer, Text, create_engine, insert
from sqlalchemy.ext.declarative import declarative_base

from sqlsynthgen.providers import BinaryProvider, ForeignKeyProvider


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

        env = os.environ.copy()
        env = {**env, "PGPASSWORD": "password"}

        # Clear and re-create the test database
        completed_process = run(
            [
                "psql",
                "--host=localhost",
                "--username=postgres",
                "--file=" + str(Path("tests/examples/providers.dump")),
            ],
            capture_output=True,
            env=env,
            check=True,
        )

        # psql doesn't always return != 0 if it fails
        assert completed_process.stderr == b"", completed_process.stderr

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
