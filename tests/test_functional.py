"""Tests for the CLI."""
import os
import shutil
from pathlib import Path
from subprocess import run

from tests.utils import RequiresDBTestCase, run_psql

# pylint: disable=subprocess-run-check


class FunctionalTestCase(RequiresDBTestCase):
    """End-to-end tests."""

    test_dir = Path("tests/workspace")
    examples_dir = Path("tests/examples")

    orm_file_path = Path("orm.py")
    ssg_file_path = Path("ssg.py")

    alt_orm_file_path = Path("my_orm.py")
    alt_ssg_file_path = Path("my_ssg.py")

    vocabulary_file_paths = tuple(
        map(Path, ("concept.yaml", "concept_type.yaml", "mitigation_type.yaml"))
    )
    generator_file_paths = tuple(
        map(Path, ("story_generators.py", "row_generators.py"))
    )
    dump_file_path = Path("dst.dump")
    config_file_path = Path("example_config.yaml")
    stats_file_path = Path("example_stats.yaml")

    start_dir = os.getcwd()

    env = os.environ.copy()
    env = {
        **env,
        "src_host_name": "localhost",
        "src_user_name": "postgres",
        "src_password": "password",
        "src_db_name": "src",
        "dst_host_name": "localhost",
        "dst_user_name": "postgres",
        "dst_password": "password",
        "dst_db_name": "dst",
    }

    def setUp(self) -> None:
        """Pre-test setup."""

        # Create a blank destination database
        run_psql(self.examples_dir / self.dump_file_path)

        # Copy some of the example files over to the workspace.
        for file in self.generator_file_paths + (self.config_file_path,):
            src = self.examples_dir / file
            dst = self.test_dir / file
            dst.unlink(missing_ok=True)
            shutil.copy(src, dst)

        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)

    def test_workflow_minimal_args(self) -> None:
        """Test the recommended CLI workflow runs without errors."""
        completed_process = run(
            ["sqlsynthgen", "make-tables", "--force"],
            capture_output=True,
            env=self.env,
        )
        self.assertSuccess(completed_process)

        completed_process = run(
            ["sqlsynthgen", "make-generators", "--force"],
            capture_output=True,
            env=self.env,
        )
        # The database has some multi-column unique constraints, but the minimal
        # configuration here generates values for each column individually. In principle
        # this could mean that we might accidentally violate the constraints. In
        # practice this won't happen because we only write one row to an empty table.
        self.assertEqual(
            "WARNING:root:A unique constraint (ab_uniq) isn't fully covered by one "
            "row generator (['a']). Enforcement of the constraint may not work.\n"
            "WARNING:root:A unique constraint (ab_uniq) isn't fully covered by one "
            "row generator (['b']). Enforcement of the constraint may not work.\n"
            "WARNING:root:A unique constraint (abc_uniq2) isn't fully covered by one "
            "row generator (['a']). Enforcement of the constraint may not work.\n"
            "WARNING:root:A unique constraint (abc_uniq2) isn't fully covered by one "
            "row generator (['b']). Enforcement of the constraint may not work.\n"
            "WARNING:root:A unique constraint (abc_uniq2) isn't fully covered by one "
            "row generator (['c']). Enforcement of the constraint may not work.\n",
            completed_process.stderr.decode("utf-8"),
        )
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

        completed_process = run(
            ["sqlsynthgen", "remove-data"],
            capture_output=True,
            env=self.env,
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            ["sqlsynthgen", "remove-vocab"],
            capture_output=True,
            env=self.env,
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        completed_process = run(
            ["sqlsynthgen", "remove-tables"],
            capture_output=True,
            env=self.env,
            input=b"\n",  # To select the default prompt option
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
                "--force",
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
                "--force",
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
                "--force",
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
        self.assertEqual(
            "WARNING:root:No rows in empty_vocabulary.yaml. Skipping...\n",
            completed_process.stderr.decode("utf-8"),
        )
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

        completed_process = run(
            [
                "sqlsynthgen",
                "remove-data",
                "--yes",
                f"--orm-file={self.alt_orm_file_path}",
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
                "remove-vocab",
                "--yes",
                f"--orm-file={self.alt_orm_file_path}",
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
                "remove-tables",
                "--yes",
                f"--orm-file={self.alt_orm_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

    def test_unique_constraint_fail(self) -> None:
        """Test that the unique constraint is triggered correctly.

        In the database there is a table called unique_constraint_test, which has a
        unique constraint on two boolean columns, so that exactly 4 rows can be written
        to the table until it becomes impossible to fulfill the constraint. We test that
        a) we can write 4 rows,
        b) trying to write a 5th row results in an error, a failure to find a new row to
        fulfill the constraint.

        We also deliberately call create-data multiple times to make sure that the
        loading of existing keys from the database at start up works as expected.
        """

        # This is all exactly the same stuff we run in test_workflow_maximal_args.
        completed_process = run(
            [
                "sqlsynthgen",
                "make-tables",
                f"--orm-file={self.alt_orm_file_path}",
                "--force",
            ],
            capture_output=True,
            env=self.env,
        )
        completed_process = run(
            [
                "sqlsynthgen",
                "make-stats",
                f"--stats-file={self.stats_file_path}",
                f"--config-file={self.config_file_path}",
                "--force",
            ],
            capture_output=True,
            env=self.env,
        )
        completed_process = run(
            [
                "sqlsynthgen",
                "make-generators",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                f"--stats-file={self.stats_file_path}",
                "--force",
            ],
            capture_output=True,
            env=self.env,
        )
        completed_process = run(
            [
                "sqlsynthgen",
                "create-tables",
                f"--orm-file={self.alt_orm_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        completed_process = run(
            [
                "sqlsynthgen",
                "create-vocab",
                f"--ssg-file={self.alt_ssg_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )

        # First a couple of successful create-data calls. Note the num-passes, which add
        # up to 4.
        completed_process = run(
            [
                "sqlsynthgen",
                "create-data",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                "--num-passes=1",
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
                "--num-passes=3",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        # Writing one more row should fail.
        completed_process = run(
            [
                "sqlsynthgen",
                "create-data",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                "--num-passes=1",
            ],
            capture_output=True,
            env=self.env,
        )
        expected_error = (
            "RuntimeError: Failed to generate a value that satisfies unique constraint "
            "for ['a', 'b'] in unique_constraint_test after 50 attempts."
        )
        self.assertIn(expected_error, completed_process.stderr.decode("utf-8"))
        self.assertFailure(completed_process)
