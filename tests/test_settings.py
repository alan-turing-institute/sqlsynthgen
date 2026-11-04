"""Tests for the settings module."""
from unittest import mock

from datafaker.settings import SettingsError, get_destination_dsn, get_source_dsn
from tests.utils import DatafakerTestCase, get_test_settings


class TestSettings(DatafakerTestCase):
    """Tests for the Settings class."""

    def test_maximal_settings(self) -> None:
        """Test the full settings."""
        get_test_settings(
            src_dsn="postgresql://user:password@host:port/db_name?sslmode=require",
            src_schema="dst_schema",
            dst_dsn="postgresql://user:password@host:port/db_name?sslmode=require",
            dst_schema="src_schema",
        )

    def test_validation(self) -> None:
        """Schema settings aren't compatible with MariaDB."""
        with self.assertRaises(SettingsError):
            get_test_settings(
                src_dsn="mariadb+pymysql://myuser@localhost:3306/testdb", src_schema=""
            )

        with self.assertRaises(SettingsError):
            get_test_settings(
                dst_dsn="mariadb+pymysql://myuser@localhost:3306/testdb", dst_schema=""
            )

    @mock.patch("datafaker.settings.get_settings")
    def test_get_destination_dsn_raises_if_no_dsn(
        self, mock_get_settings: mock.MagicMock
    ) -> None:
        """Test that get_destination_dsn raises if dst DSN is missing."""
        mock_get_settings.return_value = get_test_settings(
            src_dsn="mariadb+pymysql://myuser@localhost:3306/testdb",
            dst_dsn=None,
        )
        with self.assertRaises(SettingsError) as context_manager:
            get_destination_dsn()
        self.assertEqual(context_manager.exception.args[0], "Missing DST_DSN setting")

    @mock.patch("datafaker.settings.get_settings")
    def test_get_source_dsn_raises_if_no_dsn(
        self, mock_get_settings: mock.MagicMock
    ) -> None:
        """Test that get_destination_dsn raises if src DSN is missing."""
        mock_get_settings.return_value = get_test_settings(
            src_dsn=None,
            dst_dsn="mariadb+pymysql://myuser@localhost:3306/testdb",
        )
        with self.assertRaises(SettingsError) as context_manager:
            get_source_dsn()
        self.assertEqual(context_manager.exception.args[0], "Missing SRC_DSN setting")
