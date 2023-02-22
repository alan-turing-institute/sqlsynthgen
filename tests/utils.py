"""Utilities for testing."""
import os
from functools import lru_cache
from pathlib import Path
from subprocess import run
from unittest import TestCase, skipUnless

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


def run_psql(dump_file_name: str) -> None:
    """Run psql and"""

    # If you need to update a .dump file, use
    # pg_dump -d DBNAME -h localhost -U postgres -C -c > tests/examples/FILENAME.dump

    env = os.environ.copy()
    env = {**env, "PGPASSWORD": "password"}

    # Clear and re-create the test database
    completed_process = run(
        [
            "psql",
            "--host=localhost",
            "--username=postgres",
            "--file=" + str(Path(f"tests/examples/{dump_file_name}")),
        ],
        capture_output=True,
        env=env,
        check=True,
    )
    # psql doesn't always return != 0 if it fails
    assert completed_process.stderr == b"", completed_process.stderr


@skipUnless(os.environ.get("REQUIRES_DB") == "1", "Set 'REQUIRES_DB=1' to enable.")
class RequiresDBTestCase(TestCase):
    """A test case that only runs if REQUIRES_DB has been set to true."""

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass
