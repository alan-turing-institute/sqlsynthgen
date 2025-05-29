"""Tests for the unique_generator module."""
from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    Text,
    UniqueConstraint,
    create_engine,
    insert,
)
from sqlalchemy.ext.declarative import declarative_base

from datafaker.unique_generator import UniqueGenerator
from tests.utils import RequiresDBTestCase

# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata = Base.metadata


class TestTable(Base):  # type: ignore
    """A test SQLAlchemy table."""

    __tablename__ = "test_table"
    __table_args__ = (UniqueConstraint("a", "b", name="ab_uniq"),)
    id = Column(Integer, primary_key=True)
    a = Column(Boolean)
    b = Column(Boolean)
    c = Column(Text, unique=True)


class UniqueGeneratorTestCase(RequiresDBTestCase):
    """Tests for the UniqueGenerator class.

    The tests utilise the table defined above called test_table. It has three columns, a
    and b which are boolean, and c which is a text column. There is a joint unique
    constraint on a and b, and a separate unique constraint on c.
    """
    dump_file_path = "unique_generator.dump"

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()
        metadata.create_all(self.engine)

    def test_unique_generator_empty_table(self) -> None:
        """Test finding non-conflicting values for an empty database."""

        table_name = TestTable.__tablename__
        uniq_ab = UniqueGenerator(["a", "b"], table_name)
        uniq_c = UniqueGenerator(["c"], table_name, max_tries=10)

        with self.engine.connect() as conn:
            # Find a couple of different values that could be inserted, then try to do
            # one duplicate.
            test_ab1 = [True, False]
            test_ab2 = [False, False]
            self.assertEqual(uniq_ab(conn, ["a", "b"], lambda: test_ab1), test_ab1)
            self.assertEqual(uniq_ab(conn, ["a", "b"], lambda: test_ab2), test_ab2)
            self.assertRaises(RuntimeError, uniq_ab, conn, ["a", "b"], lambda: test_ab2)

            # Same for the string column
            string1 = "String 1"
            string2 = "String 2"
            self.assertEqual(uniq_c(conn, ["c"], lambda: string1), string1)
            self.assertEqual(uniq_c(conn, ["c"], lambda: string2), string2)
            self.assertRaises(RuntimeError, uniq_c, conn, ["c"], lambda: string2)

    def test_unique_generator_nonempty_table(self) -> None:
        """Test finding non-conflicting values for a prepopulated database.

        Write some data into the database and then create UniqueGenerators, test that
        they can handle catch conflicts with the prepopulated values. This simulates
        running create-data when there already is data in the database.
        """

        table_name = TestTable.__tablename__
        uniq_ab = UniqueGenerator(["a", "b"], table_name)
        uniq_c = UniqueGenerator(["c"], table_name, max_tries=10)

        with self.engine.connect() as conn:
            test_ab1 = [True, False]
            test_ab2 = [False, False]
            string1 = "String 1"
            string2 = "String 2"
            conn.execute(
                insert(TestTable).values(a=test_ab1[0], b=test_ab1[1], c=string1)
            )
            # First check a value that doesn't conflict with the one we just wrote, then
            # the one that does.
            self.assertEqual(uniq_ab(conn, ["a", "b"], lambda: test_ab2), test_ab2)
            self.assertRaises(RuntimeError, uniq_ab, conn, ["a", "b"], lambda: test_ab1)

            # Same for the string column.
            self.assertEqual(uniq_c(conn, ["c"], lambda: string2), string2)
            self.assertRaises(RuntimeError, uniq_c, conn, ["c"], lambda: string1)

    def test_unique_generator_multivalue_generator(self) -> None:
        """Test that UniqueGenerator can handle row generators that return multiple
        values.
        """

        table_name = TestTable.__tablename__
        uniq_ab = UniqueGenerator(["a", "b"], table_name)
        uniq_c = UniqueGenerator(["c"], table_name, max_tries=10)

        with self.engine.connect() as conn:
            test_val1 = (True, False, "String 1")
            test_val2 = (True, False, "String 2")  # Conflicts on (a, b)
            test_val3 = (False, False, "String 1")  # Conflicts on c
            self.assertEqual(
                uniq_ab(conn, ["a", "b", "c"], lambda: test_val1), test_val1
            )
            self.assertEqual(
                uniq_ab(conn, ["a", "b", "c"], lambda: test_val3), test_val3
            )
            self.assertRaises(
                RuntimeError, uniq_ab, conn, ["a", "b", "c"], lambda: test_val2
            )

            self.assertEqual(
                uniq_c(conn, ["a", "b", "c"], lambda: test_val1), test_val1
            )
            self.assertEqual(
                uniq_c(conn, ["a", "b", "c"], lambda: test_val2), test_val2
            )
            self.assertRaises(
                RuntimeError, uniq_c, conn, ["a", "b", "c"], lambda: test_val3
            )

    def test_unique_generator_max_tries(self) -> None:
        """Test that UniqueGenerator the max_tries argument is respected."""

        max_tries = 23
        table_name = TestTable.__tablename__
        uniq_ab = UniqueGenerator(["a", "b"], table_name, max_tries=max_tries)
        mock_generator = MagicMock()
        test_val = (True, False, "String 1")
        mock_generator.return_value = test_val

        with self.engine.connect() as conn:
            self.assertEqual(uniq_ab(conn, ["a", "b", "c"], mock_generator), test_val)
            self.assertRaises(
                RuntimeError, uniq_ab, conn, ["a", "b", "c"], mock_generator
            )
            self.assertEqual(len(mock_generator.mock_calls), max_tries + 1)
