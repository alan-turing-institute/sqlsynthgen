"""Tests for the main module."""
from functools import lru_cache
from unittest import TestCase
from unittest.mock import MagicMock, patch

from sqlsynthgen import main, settings
from sqlsynthgen.main import create_tables


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
    """Module test case."""

    def test_main(self) -> None:
        """Test the main function."""
        with patch("sqlsynthgen.main.populate"), patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value = get_test_settings()
            with self.assertRaises(NotImplementedError):
                main.main()

    def test_generate(self) -> None:
        """Test the generate function."""
        with patch("sqlsynthgen.main.populate") as mock_populate, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings, patch(
            "sqlsynthgen.main.create_engine"
        ) as mock_create_engine:
            mock_get_settings.return_value = get_test_settings()

            main.generate([], [])

            mock_populate.assert_called_once()
            mock_create_engine.assert_called_once()

    def test_create_tables(self) -> None:
        """Test the create_tables function."""
        mock_meta = MagicMock()

        with patch("sqlsynthgen.main.create_engine") as mock_create_engine, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings:

            create_tables(mock_meta)
            mock_get_settings.assert_called_once()
            mock_create_engine.assert_called_once_with(
                mock_get_settings.return_value.postgres_dsn
            )
