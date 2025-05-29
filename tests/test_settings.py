"""Tests for the settings module."""
from pydantic import ValidationError

from datafaker.settings import Settings
from tests.utils import DatafakerTestCase


class TestSettings(DatafakerTestCase):
    """Tests for the Settings class."""

    def test_minimal_settings(self) -> None:
        """Test the minimal settings."""
        settings = Settings(
            # To stop any local .env files influencing the test
            # The mypy ignore can be removed once we upgrade to pydantic 2.
            _env_file=None,  # type: ignore[call-arg]
        )
        self.assertIsNone(settings.src_dsn)
        self.assertIsNone(settings.src_schema)

        self.assertIsNone(settings.dst_dsn)
        self.assertIsNone(settings.dst_schema)

    def test_maximal_settings(self) -> None:
        """Test the full settings."""
        Settings(
            src_dsn="postgresql://user:password@host:port/db_name?sslmode=require",
            src_schema="dst_schema",
            dst_dsn="postgresql://user:password@host:port/db_name?sslmode=require",
            dst_schema="src_schema",
            # To stop any local .env files influencing the test
            # The mypy ignore can be removed once we upgrade to pydantic 2.
            _env_file=None,  # type: ignore[call-arg]
        )

    def test_validation(self) -> None:
        """Schema settings aren't compatible with MariaDB."""
        with self.assertRaises(ValidationError):
            Settings(
                src_dsn="mariadb+pymysql://myuser@localhost:3306/testdb", src_schema=""
            )

        with self.assertRaises(ValidationError):
            Settings(
                dst_dsn="mariadb+pymysql://myuser@localhost:3306/testdb", dst_schema=""
            )
