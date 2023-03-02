"""Tests for the CLI."""
import os
from pathlib import Path
from subprocess import run

from tests.utils import RequiresDBTestCase, run_psql

# pylint: disable=subprocess-run-check


class FunctionalTestCase(RequiresDBTestCase):
    """End-to-end tests."""

    orm_file_path = Path("orm.py")
    ssg_file_path = Path("ssg.py")

    alt_orm_file_path = Path("my_orm.py")
    alt_ssg_file_path = Path("my_ssg.py")

    concept_file_path = Path("concept.csv")
    config_file_path = Path("../examples/functional_conf.yaml")
    stats_file_path = Path("example_stats.yaml")

    test_dir = Path("tests/workspace")
    start_dir = os.getcwd()

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

    def setUp(self) -> None:
        """Pre-test setup."""

        # Create a blank destination database
        run_psql(Path("tests/examples/dst.dump"))

        os.chdir(self.test_dir)

        for file_path in (
            self.orm_file_path,
            self.ssg_file_path,
            self.alt_orm_file_path,
            self.alt_ssg_file_path,
            self.concept_file_path,
        ):
            file_path.unlink(missing_ok=True)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)

    def test_workflow_minimal_args(self) -> None:
        """Test the recommended CLI workflow runs without errors."""
        completed_process = run(
            ["sqlsynthgen", "make-tables"],
            capture_output=True,
            env=self.env,
        )
        self.assertSuccess(completed_process.returncode)

        completed_process = run(
            ["sqlsynthgen", "make-generators"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            ["sqlsynthgen", "create-tables"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            ["sqlsynthgen", "create-vocab"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            ["sqlsynthgen", "create-data"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

    def test_workflow_maximal_args(self) -> None:
        """Test the CLI workflow runs with optional arguments."""

        completed_process = run(
            [
                "sqlsynthgen",
                "make-tables",
                f"--orm-file={self.alt_orm_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual(
            "WARNING: Table without PK detected. sqlsynthgen may not be able to continue.\n",
            completed_process.stderr.decode("utf-8"),
        )
        self.assertSuccess(completed_process)

        completed_process = run(
            [
                "sqlsynthgen",
                "make-stats",
                f"--stats-file={self.stats_file_path}",
                f"--config-file={self.config_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertSuccess(completed_process)

        completed_process = run(
            [
                "sqlsynthgen",
                "make-generators",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                f"--stats-file={self.stats_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            [
                "sqlsynthgen",
                "create-tables",
                f"--orm-file={self.alt_orm_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            [
                "sqlsynthgen",
                "create-vocab",
                f"--ssg-file={self.alt_ssg_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            [
                "sqlsynthgen",
                "create-data",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                "--num-passes=2",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
