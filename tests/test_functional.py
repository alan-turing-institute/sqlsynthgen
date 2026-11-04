"""Tests for the CLI."""
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

import yaml
from sqlalchemy import create_engine, inspect
from typer.testing import CliRunner, Result

from datafaker.main import app
from tests.utils import DuckTestDb, MsSqlTestDb, RequiresDBTestCase

# pylint: disable=subprocess-run-check


class DBFunctionalTestCaseBase(RequiresDBTestCase):
    """Base class for test that call the CLI and require a database."""

    examples_dir = Path("examples")

    orm_file_path = Path("orm.yaml")

    generator_file_paths = tuple(
        map(Path, ("story_generators.py", "row_generators.py")),
    )
    config_file_path = Path("example_config2.yaml")
    stats_file_path = Path("example_stats.yaml")

    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()
        self.make_destination_database("dst")
        self.env = {
            "SRC_DSN": self.dsn,
            "SRC_SCHEMA": self.schema_name,
            "DST_DSN": self.dst_dsn,
            "DST_SCHEMA": self.dst_schema_name,
        }
        self.runner = CliRunner(
            mix_stderr=False,
            env=self.env,
        )

        # Copy some of the example files over to the workspace.
        self.test_dir = Path(tempfile.mkdtemp(prefix="df-"))
        for file in self.generator_file_paths + (self.config_file_path,):
            src = self.get_abs_example_dir() / file
            dst = self.test_dir / file
            dst.unlink(missing_ok=True)
            shutil.copy(src, dst)

        (self.test_dir / "config.yaml").unlink(missing_ok=True)

        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)
        super().tearDown()

    def invoke(
        self,
        *args: Any,
        expected_error: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> Result:
        """
        Run datafaker with the given arguments and environment.

        :param args: Arguments to provide to datafaker.
        :param expected_error: If None, will assert that the invocation
        passes successfully without throwing an exception. Otherwise,
        the suggested error must be present in the standard error stream.
        :param env: The environment variables to be set during invocation.
        """
        res = self.runner.invoke(app, args, env=env)
        if expected_error is None:
            self.assertNoException(res)
            self.assertSuccess(res)
        else:
            self.assertIn(expected_error, res.stderr)
        return res

    def assert_silent_success(self, completed_process: Result) -> None:
        """Assert that the process completed successfully without producing output."""
        self.assertNoException(completed_process)
        self.assertSuccess(completed_process)
        self.assertEqual(completed_process.stderr, "")
        self.assertEqual(completed_process.stdout, "")


class DBFunctionalTestCasePg(DBFunctionalTestCaseBase):
    """End-to-end tests that require a database, but can't work with DuckDB."""

    dump_file_path = "src.dump"
    database_name = "src"
    alt_orm_file_path = Path("my_orm.yaml")
    config_file_path = Path("example_config.yaml")

    def test_create_schema(self) -> None:
        """Check that we create a destination schema if it doesn't exist."""
        env = {"dst_schema": "doesntexistyetschema"}

        dst_dsn = self.env["DST_DSN"]
        assert dst_dsn is not None

        engine = create_engine(dst_dsn)
        inspector = inspect(engine)
        self.assertFalse(inspector.has_schema(env["dst_schema"]))

        self.invoke(
            "make-tables",
            "--force",
            env=env,
        )

        completed_process = self.invoke(
            "create-tables",
            f"--config-file={self.config_file_path}",
            env=env,
        )
        self.assertEqual("", completed_process.stderr)

        engine = create_engine(dst_dsn)
        inspector = inspect(engine)
        self.assertTrue(inspector.has_schema(env["dst_schema"]))

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
        self.invoke(
            "make-tables",
            f"--orm-file={self.alt_orm_file_path}",
            "--force",
        )
        self.invoke(
            "make-vocab",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
            "--force",
        )
        self.invoke(
            "make-stats",
            f"--orm-file={self.alt_orm_file_path}",
            f"--stats-file={self.stats_file_path}",
            f"--config-file={self.config_file_path}",
            "--force",
        )
        self.invoke(
            "create-tables",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )
        self.invoke(
            "create-vocab",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )

        # First a couple of successful create-data calls. Note the num-passes, which add
        # up to 4.
        completed_process = self.invoke(
            "create-data",
            f"--config-file={self.config_file_path}",
            f"--orm-file={self.alt_orm_file_path}",
            f"--stats-file={self.stats_file_path}",
            "--num-passes=1",
        )
        self.assertEqual(
            "Generating data for story 'story_generators.short_story'\n"
            "Generating data for story 'story_generators.full_row_story'\n"
            "Generating data for story 'story_generators.long_story'\n",
            completed_process.stdout,
        )

        completed_process = self.invoke(
            "create-data",
            f"--config-file={self.config_file_path}",
            f"--orm-file={self.alt_orm_file_path}",
            f"--stats-file={self.stats_file_path}",
            "--num-passes=3",
        )
        self.assertEqual(
            (
                "Generating data for story 'story_generators.short_story'\n"
                "Generating data for story 'story_generators.full_row_story'\n"
                "Generating data for story 'story_generators.long_story'\n"
            )
            * 3,
            completed_process.stdout,
        )

        # Writing one more row should fail.
        completed_process = self.invoke(
            "create-data",
            f"--config-file={self.config_file_path}",
            f"--orm-file={self.alt_orm_file_path}",
            f"--stats-file={self.stats_file_path}",
            "--num-passes=1",
            expected_error=(
                "Failed to satisfy unique constraints for table unique_constraint_test"
            ),
        )
        self.assertFailure(completed_process)
        self.assertIn("after 50 attempts", completed_process.stderr)

    def test_workflow_minimal_args(self) -> None:
        """Test the recommended CLI workflow runs without errors."""
        shutil.copy(self.config_file_path, "config.yaml")
        completed_process = self.invoke(
            "make-tables",
            "--force",
        )
        self.assert_silent_success(completed_process)

        completed_process = self.invoke(
            "make-vocab",
            "--force",
        )
        self.assert_silent_success(completed_process)

        completed_process = self.invoke(
            "make-stats",
            "--force",
        )
        self.assert_silent_success(completed_process)

        completed_process = self.invoke(
            "create-tables",
        )
        self.assert_silent_success(completed_process)

        completed_process = self.invoke(
            "create-vocab",
        )
        self.assert_silent_success(completed_process)

        completed_process = self.invoke(
            "make-stats",
            "--force",
        )
        self.assert_silent_success(completed_process)

        completed_process = self.invoke(
            "create-data",
        )
        self.assertNoException(completed_process)
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Generating data for story 'story_generators.short_story'\n"
            "Generating data for story 'story_generators.full_row_story'\n"
            "Generating data for story 'story_generators.long_story'\n",
            completed_process.stdout,
        )

        completed_process = self.runner.invoke(
            app,
            ["remove-data"],
            input="\n",  # To select the default prompt option
        )
        self.assertNoException(completed_process)
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Are you sure? [y/N]: \n"
            "Would truncate non-vocabulary tables if called with --yes.\n",
            completed_process.stdout,
        )

        completed_process = self.runner.invoke(
            app,
            ["remove-vocab"],
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Are you sure? [y/N]: \n"
            "Would truncate vocabulary tables if called with --yes.\n",
            completed_process.stdout,
        )

        completed_process = self.runner.invoke(
            app,
            ["remove-tables"],
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Are you sure? [y/N]: \nWould remove tables if called with --yes.\n",
            completed_process.stdout,
        )


class DBFunctionalTestCase(DBFunctionalTestCaseBase):
    """End-to-end tests that require a database and work with DuckDB."""

    dump_file_path = "src2.dump"
    database_name = "src"
    schema_name = "public"
    alt_orm_file_path = Path("my_orm.yaml")

    def test_workflow_maximal_args(self) -> None:
        """Test the CLI workflow runs with optional arguments."""
        completed_process = self.invoke(
            "--verbose",
            "make-tables",
            f"--orm-file={self.alt_orm_file_path}",
            "--force",
        )
        self.assertNoException(completed_process)
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            f"Creating {self.alt_orm_file_path}.\n{self.alt_orm_file_path} created.\n",
            completed_process.stdout,
        )

        completed_process = self.invoke(
            "--verbose",
            "make-stats",
            f"--orm-file={self.alt_orm_file_path}",
            f"--stats-file={self.stats_file_path}",
            f"--config-file={self.config_file_path}",
            "--force",
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            f"Creating {self.stats_file_path}.\n"
            "Executing query count_names\n"
            "Executing query avg_person_id\n"
            "Executing query count_opt_outs\n"
            "Executing query auto__hospital_visit__visit_type_concept_id\n"
            f"{self.stats_file_path} created.\n",
            completed_process.stdout,
        )

        completed_process = self.invoke(
            "--verbose",
            "make-vocab",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
            "--force",
        )
        self.assertSetEqual(
            {
                "Downloading vocabulary table concept_type",
                "Downloading vocabulary table unignorable_table",
                "Downloading vocabulary table ref_to_unignorable_table",
                "Downloading vocabulary table empty_vocabulary",
                "Downloading vocabulary table mitigation_type",
            },
            set(completed_process.stdout.split("\n")) - {""},
        )

        completed_process = self.invoke(
            "--verbose",
            "create-tables",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Creating tables.\nTables created.\n",
            completed_process.stdout,
        )

        completed_process = self.invoke(
            "--verbose",
            "create-vocab",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        if self.dsn.startswith("duckdb:"):
            # DuckDB does not seem to know about constraints
            constraint_messages = set()
        else:
            constraint_messages = {
                (
                    "Dropping constraint ref_to_unignorable_table_ref_fkey from "
                    "table ref_to_unignorable_table"
                ),
                "Dropping constraint concept_type_mitigation_type_id_fkey from table concept_type",
                "Restoring foreign key constraint ref_to_unignorable_table_ref_fkey",
                "Restoring foreign key constraint concept_type_mitigation_type_id_fkey",
            }
        self.assertSetEqual(
            constraint_messages
            | {
                "Loading vocab.",
                "Loading vocabulary table empty_vocabulary",
                "Loading vocabulary table mitigation_type",
                "Loading vocabulary table ref_to_unignorable_table",
                "Loading vocabulary table unignorable_table",
                "Loading vocabulary table concept_type",
                "5 tables loaded.",
            },
            set(completed_process.stdout.split("\n")) - {""},
        )

        completed_process = self.invoke(
            "--verbose",
            "create-data",
            f"--orm-file={self.alt_orm_file_path}",
            f"--stats-file={self.stats_file_path}",
            f"--config-file={self.config_file_path}",
            "--num-passes=2",
        )
        self.assertSetEqual(
            {
                "Creating data.",
                "Generating data for story 'story_generators.short_story'",
                "Generating data for story 'story_generators.full_row_story'",
                "Generating data for table 'data_type_test'",
                "Generating data for table 'no_pk_test'",
                "Generating data for table 'person'",
                "Generating data for table 'unique_constraint_test'",
                "Generating data for table 'unique_constraint_test2'",
                "Generating data for table 'test_entity'",
                "Generating data for table 'hospital_visit'",
                "Data created in 2 passes.",
                f"person: {2*(3+1+2)} rows created.",
                "hospital_visit: 6 rows created.",
                "data_type_test: 2 rows created.",
                "no_pk_test: 2 rows created.",
                "unique_constraint_test: 2 rows created.",
                "unique_constraint_test2: 2 rows created.",
                "test_entity: 2 rows created.",
                "",
            },
            set(completed_process.stdout.split("\n")),
        )

        completed_process = self.invoke(
            "--verbose",
            "remove-data",
            "--yes",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )
        self.assertEqual("", completed_process.stderr)
        self.assertEqual(
            {
                "Truncating non-vocabulary tables.",
                'Truncating table "hospital_visit".',
                'Truncating table "test_entity".',
                'Truncating table "unique_constraint_test2".',
                'Truncating table "unique_constraint_test".',
                'Truncating table "person".',
                'Truncating table "no_pk_test".',
                'Truncating table "data_type_test".',
                "Non-vocabulary tables truncated.",
            },
            set(completed_process.stdout.split("\n")) - {""},
        )

        completed_process = self.invoke(
            "--verbose",
            "remove-vocab",
            "--yes",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            constraint_messages
            | {
                "Truncating vocabulary tables.",
                'Truncating vocabulary table "concept_type".',
                'Truncating vocabulary table "ref_to_unignorable_table".',
                'Truncating vocabulary table "unignorable_table".',
                'Truncating vocabulary table "mitigation_type".',
                'Truncating vocabulary table "empty_vocabulary".',
                "Vocabulary tables truncated.",
            },
            set(completed_process.stdout.split("\n")) - {""},
        )

        completed_process = self.invoke(
            "--verbose",
            "remove-tables",
            "--yes",
            f"--orm-file={self.alt_orm_file_path}",
            f"--config-file={self.config_file_path}",
        )
        self.assertEqual("", completed_process.stderr)
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Dropping tables.\nTables dropped.\n",
            completed_process.stdout,
        )

    def test_story_incorrect_name(self) -> None:
        """Test we get a proper error message if the story generator module does not exist."""
        config_file = "config_story_incorrect.yaml"
        config = {
            "story_generators_module": "incorrect_module",
        }
        with Path(config_file).open("w", encoding="utf-8") as fh:
            fh.write(yaml.dump(config))
        self.invoke(
            "make-tables",
            "--force",
        )
        self.invoke(
            "create-tables",
            "--config-file",
            config_file,
        )
        completed_process = self.invoke(
            "create-data",
            "--config-file",
            config_file,
            expected_error="No module found 'incorrect_module",
        )
        self.assertReturnCode(completed_process, 1)

    def test_story_hyphens_in_name(self) -> None:
        """Test hyphens in story generator names cause an error to be emitted."""
        config_file = "config_story_hyphens.yaml"
        config = {
            "story_generators_module": "story-generators",
        }
        with Path(config_file).open("w", encoding="utf-8") as fh:
            fh.write(yaml.dump(config))
        self.invoke(
            "make-tables",
            "--force",
        )
        completed_process = self.invoke(
            "create-data",
            "--config-file",
            config_file,
            expected_error="hyphen",
        )
        self.assertReturnCode(completed_process, 1)

    def test_row_hyphens_in_name(self) -> None:
        """Test hyphens in row generator names cause an error to be emitted."""
        config_file = "config_row_hyphens.yaml"
        config = {
            "row_generators_module": "row-generators",
        }
        with Path(config_file).open("w", encoding="utf-8") as fh:
            fh.write(yaml.dump(config))
        self.invoke(
            "make-tables",
            "--force",
        )
        completed_process = self.invoke(
            "create-data",
            "--config-file",
            config_file,
            expected_error="hyphen",
        )
        self.assertReturnCode(completed_process, 1)


class DuckDbFunctionalTestCase(DBFunctionalTestCase):
    """End-to-end tests for the DuckDB workflow."""

    database_type = DuckTestDb


class MsSqlFunctionalTestCase(DBFunctionalTestCase):
    """End-to-end tests for the MsSql workflow."""

    schema_name = None
    database_type = MsSqlTestDb
