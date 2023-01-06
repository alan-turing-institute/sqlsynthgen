"""Utilities for testing."""
from functools import lru_cache

from sqlsynthgen import settings


@lru_cache(1)
def get_test_settings() -> settings.Settings:
    """Get a Settings object that ignores .env files and environment variables."""

    return settings.Settings(
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
