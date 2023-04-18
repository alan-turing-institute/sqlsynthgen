"""Utilities for testing."""
import os
from functools import lru_cache
from pathlib import Path
from subprocess import run
from typing import Any
from unittest import TestCase, skipUnless

from sqlsynthgen import settings


class SysExit(Exception):
    """To force the function to exit as sys.exit() would."""


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


def run_psql(dump_file: Path) -> None:
    """Run psql and pass dump_file_name as the --file option."""

    # If you need to update a .dump file, use
    # PGPASSWORD=password pg_dump \
    # --host=localhost \
    # --port=5432 \
    # --dbname=src \
    # --username=postgres \
    # --no-password \
    # --clean \
    # --create \
    # --insert \
    # --if-exists > tests/examples/FILENAME.dump

    env = os.environ.copy()
    env = {**env, "PGPASSWORD": "password"}

    # Clear and re-create the test database
    completed_process = run(
        ["psql", "--host=localhost", "--username=postgres", f"--file={dump_file}"],
        capture_output=True,
        env=env,
        check=True,
    )
    # psql doesn't always return != 0 if it fails
    assert completed_process.stderr == b"", completed_process.stderr


class SSGTestCase(TestCase):
    """Parent class for all TestCases in SqlSynthGen."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize an instance of SSGTestCase."""
        self.maxDiff = None  # pylint: disable=invalid-name
        super().__init__(*args, **kwargs)

    def assertSuccess(self, result: Any) -> None:  # pylint: disable=invalid-name
        """Give details for a subprocess result and raise if the result isn't good."""
        code = result.exit_code if hasattr(result, "exit_code") else result.returncode
        if code != 0:
            print(result.stdout)
            print(result.stderr)
            self.assertEqual(0, code)


@skipUnless(os.environ.get("REQUIRES_DB") == "1", "Set 'REQUIRES_DB=1' to enable.")
class RequiresDBTestCase(SSGTestCase):
    """A test case that only runs if REQUIRES_DB has been set to 1."""

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass
