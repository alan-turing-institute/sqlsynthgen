"""Tests for the main module."""
from unittest import TestCase

from sqlsynthgen import make
from tests.examples import example_orm


class MyTestCase(TestCase):
    """Module test case."""

    def test_make_generators_from_tables(self) -> None:
        """Check that we can make a generators file from a tables module."""

        with open(
            "tests/examples/expected_ssg.py", encoding="utf-8"
        ) as expected_output:
            expected = expected_output.read()

        actual = make.make_generators_from_tables(example_orm)
        self.assertEqual(expected, actual)
