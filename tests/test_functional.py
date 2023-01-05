"""Tests for the main module."""
import os
from pathlib import Path
from subprocess import run
from unittest import TestCase, skipUnless


@skipUnless(
    os.environ.get("FUNCTIONAL_TESTS") == "1", "Set 'FUNCTIONAL_TESTS=1' to enable."
)
class FunctionalTests(TestCase):
    """End-to-end tests."""

    def setUp(self) -> None:
        """Pre-test setup."""

        env = os.environ.copy()
        env = {**env, "PGPASSWORD": "password"}
        # Clear and re-create the destination database
        completed_process = run(
            [
                "psql",
                "--host=localhost",
                "--username=postgres",
                "--file=" + str(Path("tests/examples/dst.dump")),
            ],
            capture_output=True,
            env=env,
            check=True,
        )
        # psql doesn't always return != 0 if it fails
        assert completed_process.stderr == b""

    @staticmethod
    def test_workflow() -> None:
        """Test the recommended CLI workflow runs without errors."""

        # Export example databases
        # pg_dump -d src -h localhost -U postgres -C > tests/examples/src.dump
        # pg_dump -d dst -h localhost -U postgres -C > tests/examples/dst.dump

        # Restore databases
        # psql --host localhost --username postgres --file="tests/examples/src.dump"
        # psql --host localhost --username postgres --file="tests/examples/dst.dump"

        env = os.environ.copy()
        env = {
            **env,
            "src_host_name": "localhost",
            "src_user_name": "postgres",
            "src_password": "password",
            "src_db_name": "src",
            "src_schema": "",
            "dst_host_name": "localhost",
            "dst_user_name": "postgres",
            "dst_password": "password",
            "dst_db_name": "dst",
        }

        orm_file_path = str(Path("tests/tmp/orm.py"))
        with open(orm_file_path, "wb") as file:
            run(["sqlsynthgen", "make-tables"], stdout=file, env=env, check=True)

        ssg_file_path = str(Path("tests/tmp/ssg.py"))
        with open(ssg_file_path, "wb") as file:
            run(
                ["sqlsynthgen", "make-generators", orm_file_path],
                stdout=file,
                env=env,
                check=True,
            )

        run(["sqlsynthgen", "create-tables", orm_file_path], env=env, check=True)
        run(
            ["sqlsynthgen", "create-data", orm_file_path, ssg_file_path],
            env=env,
            check=True,
        )
