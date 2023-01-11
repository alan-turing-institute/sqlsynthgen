"""Tests for the CLI."""
import os
from pathlib import Path
from subprocess import run
from unittest import TestCase, skipUnless


@skipUnless(
    os.environ.get("FUNCTIONAL_TESTS") == "1", "Set 'FUNCTIONAL_TESTS=1' to enable."
)
class FunctionalTests(TestCase):
    """End-to-end tests."""

    orm_file_path = Path("tests/tmp/orm.py")
    ssg_file_path = Path("tests/tmp/ssg.py")

    def setUp(self) -> None:
        """Pre-test setup."""
        self.orm_file_path.unlink(missing_ok=True)
        self.ssg_file_path.unlink(missing_ok=True)

        # If you need to update src.dump or dst.dump, use
        # pg_dump -d src|dst -h localhost -U postgres -C -c > tests/examples/src|dst.dump

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
        assert completed_process.stderr == b"", completed_process.stderr

    def test_workflow(self) -> None:
        """Test the recommended CLI workflow runs without errors."""

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

        with open(self.orm_file_path, "wb") as file:
            run(["sqlsynthgen", "make-tables"], stdout=file, env=env, check=True)

        with open(self.ssg_file_path, "wb") as file:
            run(
                ["sqlsynthgen", "make-generators", self.orm_file_path],
                stdout=file,
                env=env,
                check=True,
            )

        run(["sqlsynthgen", "create-tables", self.orm_file_path], env=env, check=True)
        run(
            ["sqlsynthgen", "create-data", self.orm_file_path, self.ssg_file_path, "1"],
            env=env,
            check=True,
        )
