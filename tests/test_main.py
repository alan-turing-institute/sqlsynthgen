"""Tests for the main module."""
from functools import lru_cache
from unittest import TestCase
from unittest.mock import patch

from sqlsynthgen import main, settings


@lru_cache(1)
def get_test_settings() -> settings.Settings:
    """Get a Settings object that ignores .env files and environment variables."""
    return settings.Settings(
        db_host_name="db_host_name",
        db_user_name="db_user_name",
        db_password="db_password",
        db_name="db_name",
        _env_file=None,
    )


class MyTestCase(TestCase):
    """Main module test case."""

    def test_main(self) -> None:
        """Check that the main function works."""
        with patch("sqlsynthgen.main.populate") as mock_populate, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value = get_test_settings()
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
