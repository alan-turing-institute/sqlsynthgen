"""Tests for the main module."""
from unittest import TestCase
from unittest.mock import patch

from sqlsynthgen import main


class MyTestCase(TestCase):
    """Main module test case."""

    def test_main(self) -> None:
        """Check that the main function works."""
        with patch("sqlsynthgen.main.populate") as mock_populate:
            main.main()

        mock_populate.assert_called_once()

    def test_generators_from_tables(self) -> None:
        """Check that we can create a generators file from a tables file."""
        with open(
            "tests/examples/expected_output.py", encoding="utf-8"
        ) as expected_output:
            expected = expected_output.read()

        actual = main.create_generators_from_tables("tests.examples.example_tables")
        self.assertEqual(expected, actual)
