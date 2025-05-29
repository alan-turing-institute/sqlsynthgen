"""Utilities for testing."""
import os
from functools import lru_cache
from pathlib import Path
import shutil
from subprocess import run
import testing.postgresql
from typing import Any
from unittest import TestCase, skipUnless

from datafaker import settings
from datafaker.utils import create_db_engine

class SysExit(Exception):
    """To force the function to exit as sys.exit() would."""


@lru_cache(1)
def get_test_settings() -> settings.Settings:
    """Get a Settings object that ignores .env files and environment variables."""

    return settings.Settings(
        src_dsn="postgresql://suser:spassword@shost:5432/sdbname",
        dst_dsn="postgresql://duser:dpassword@dhost:5432/ddbname",
        # To stop any local .env files influencing the test
        # The mypy ignore can be removed once we upgrade to pydantic 2.
        _env_file=None,  # type: ignore[call-arg]
    )


class DatafakerTestCase(TestCase):
    """Parent class for all TestCases in datafaker."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize an instance of DatafakerTestCase."""
        self.maxDiff = None  # pylint: disable=invalid-name
        super().__init__(*args, **kwargs)

    def assertReturnCode(  # pylint: disable=invalid-name
        self, result: Any, expected_code: int
    ) -> None:
        """Give details for a subprocess result and raise if it's not as expected."""
        code = result.exit_code if hasattr(result, "exit_code") else result.returncode
        if code != expected_code:
            print(result.stdout)
            print(result.stderr)
            self.assertEqual(expected_code, code)

    def assertSuccess(self, result: Any) -> None:  # pylint: disable=invalid-name
        """Give details for a subprocess result and raise if the result isn't good."""
        self.assertReturnCode(result, 0)

    def assertFailure(self, result: Any) -> None:  # pylint: disable=invalid-name
        """Give details for a subprocess result and raise if the result isn't bad."""
        self.assertReturnCode(result, 1)


@skipUnless(shutil.which("psql"), "need to find 'psql': install PostgreSQL to enable")
class RequiresDBTestCase(DatafakerTestCase):
    """A test case that only runs if PostgreSQL is installed."""
    schema_name = None
    use_asyncio = False
    examples_dir = "tests/examples"
    dump_file_path = None
    database_name = None
    Postgresql = None

    @classmethod
    def setUpClass(cls):
        cls.Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

    @classmethod
    def tearDownClass(cls):
        cls.Postgresql.clear_cache()

    def setUp(self) -> None:
        super().setUp()
        self.postgresql = self.Postgresql()
        if self.dump_file_path is not None:
            self.run_psql(Path(self.examples_dir) / Path(self.dump_file_path))
        self.engine = create_db_engine(
            self.dsn,
            schema_name=self.schema_name,
            use_asyncio=self.use_asyncio,
        )

    def tearDown(self) -> None:
        self.postgresql.stop()
        super().tearDown()

    @property
    def dsn(self):
        if self.database_name:
            return self.postgresql.url(database=self.database_name)
        return self.postgresql.url()

    def run_psql(self, dump_file: Path) -> None:
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

        # Clear and re-create the test database
        completed_process = run(
            ["psql", "-d", self.postgresql.url(), "-f", dump_file],
            capture_output=True,
            check=True,
        )
        # psql doesn't always return != 0 if it fails
        assert completed_process.stderr == b"", completed_process.stderr
