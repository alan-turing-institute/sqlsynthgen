"""Tests for the settings module."""
from sqlsynthgen.settings import Settings
from tests.utils import SSGTestCase


class TestSettings(SSGTestCase):
    """Tests for the Settings class."""

    def test_default_settings(self) -> None:
        """Test the minimal settings."""
        settings = Settings(
            src_host_name="shost",
            src_user_name="suser",
            src_password="spassword",
            src_db_name="sdbname",
            dst_host_name="dhost",
            dst_user_name="duser",
            dst_password="dpassword",
            dst_db_name="ddbname",
            # To stop any local .env files influencing the test
            _env_file=None,
        )

        self.assertEqual(
            "postgresql://suser:spassword@shost:5432/sdbname",
            str(settings.src_postgres_dsn),
        )
        self.assertIsNone(settings.src_schema)

        self.assertEqual(
            "postgresql://duser:dpassword@dhost:5432/ddbname",
            str(settings.dst_postgres_dsn),
        )

    def test_maximal_settings(self) -> None:
        """Test the full settings."""
        settings = Settings(
            src_host_name="shost",
            src_port=1234,
            src_user_name="suser",
            src_password="spassword",
            src_db_name="sdbname",
            src_ssl_required=True,
            dst_host_name="dhost",
            dst_port=4321,
            dst_user_name="duser",
            dst_password="dpassword",
            dst_db_name="ddbname",
            dst_ssl_required=True,
            # To stop any local .env files influencing the test
            _env_file=None,
        )

        self.assertEqual(
            "postgresql://suser:spassword@shost:1234/sdbname?sslmode=require",
            str(settings.src_postgres_dsn),
        )

        self.assertEqual(
            "postgresql://duser:dpassword@dhost:4321/ddbname?sslmode=require",
            str(settings.dst_postgres_dsn),
        )
