"""Utilities for testing."""
from functools import lru_cache

from sqlsynthgen import settings


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
