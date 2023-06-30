"""Tests for the unique_generator module."""
from pathlib import Path

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

from sqlsynthgen.unique_generator import UniqueGenerator
from tests.utils import RequiresDBTestCase, run_psql

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
    """Tests for the UniqueGenerator class."""

    def setUp(self) -> None:
        """Pre-test setup."""
        run_psql(Path("tests/examples/unique_generator.dump"))
        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/unique_generator_test",
        )
        metadata.create_all(self.engine)

    def test_unique_generator(self) -> None:
        """Test the key method.

        This utilises table made defined above called test_table. It has three columns,
        a and b which are boolean, and c which is a text column. There is a join unique
        constraint on a and b, and a separate unique constraint on c.
        """

        table_name = TestTable.__tablename__
        uniq_ab = UniqueGenerator(["a", "b"], table_name)
        uniq_c = UniqueGenerator(["c"], table_name, max_tries=10)

        with self.engine.connect() as conn:
            # Find a couple of different values that could be inserted, then try to do
            # one duplicate.
            test_ab1 = [True, False]
            return_value = uniq_ab(conn, [0, 1], lambda: test_ab1)
            self.assertEqual(return_value, test_ab1)

            test_ab2 = [False, False]
            return_value = uniq_ab(conn, [0, 1], lambda: test_ab2)
            self.assertEqual(return_value, test_ab2)
            self.assertRaises(RuntimeError, uniq_ab, conn, [0, 1], lambda: test_ab2)

            # Same for the string column
            string1 = "String 1"
            return_value = uniq_c(conn, None, lambda: string1)
            self.assertEqual(return_value, string1)

            string2 = "String 2"
            return_value = uniq_c(conn, None, lambda: string2)
            self.assertEqual(return_value, string2)
            self.assertRaises(RuntimeError, uniq_c, conn, None, lambda: string2)

            # Now actually write some data into the database and then create new
            # UniqueGenerators for these same columns and tables, and test that they can
            # handle things like above. This simulates running create-data when there
            # already is data in the database.
            conn.execute(
                insert(TestTable).values(a=test_ab1[0], b=test_ab1[1], c=string1)
            )
            uniq_ab2 = UniqueGenerator(["a", "b"], table_name)
            uniq_c2 = UniqueGenerator(["c"], table_name, max_tries=10)

            return_value = uniq_ab2(conn, [0, 1], lambda: test_ab2)
            self.assertEqual(return_value, test_ab2)
            self.assertRaises(RuntimeError, uniq_ab2, conn, [0, 1], lambda: test_ab1)

            return_value = uniq_c2(conn, None, lambda: string2)
            self.assertEqual(return_value, string2)
            self.assertRaises(RuntimeError, uniq_c2, conn, None, lambda: string1)
