"""Tests for the main module."""
from unittest import TestCase

from sqlsynthgen import make


class MyTestCase(TestCase):
    """Module test case."""

    def test_generators_from_tables(self) -> None:
        """Check that we can create a generators file from a tables file."""
        with open(
            "tests/examples/expected_output.py", encoding="utf-8"
        ) as expected_output:
            expected = expected_output.read()

        actual = make.make_generators_from_tables("tests.examples.example_tables")
        self.assertEqual(expected, actual)
