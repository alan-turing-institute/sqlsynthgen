"""Tests for the CLI."""
import os
import shutil
from pathlib import Path
from subprocess import run

from sqlalchemy import create_engine, inspect

from tests.utils import RequiresDBTestCase

# pylint: disable=subprocess-run-check


class FunctionalTestCase(RequiresDBTestCase):
    """End-to-end tests that don't require a database."""

    def test_version_command(self) -> None:
        """Check that the version command works."""
        completed_process = run(
            ["sqlsynthgen", "version"],
            capture_output=True,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertRegex(
            completed_process.stdout.decode("utf-8"),
            r"sqlsynthgen version [0-9]+\.[0-9]+\.[0-9]+",
        )


class DBFunctionalTestCase(RequiresDBTestCase):
    """End-to-end tests that require a database."""

    test_dir = Path("tests/workspace")
    examples_dir = Path("tests/examples")

    orm_file_path = Path("orm.py")
    ssg_file_path = Path("ssg.py")

    alt_orm_file_path = Path("my_orm.py")
    alt_ssg_file_path = Path("my_ssg.py")

    vocabulary_file_paths = tuple(
        map(Path, ("concept.yaml", "concept_type.yaml", "mitigation_type.yaml")),
    )
    generator_file_paths = tuple(
        map(Path, ("story_generators.py", "row_generators.py")),
    )
    dump_file_path = Path("dst.dump")
    config_file_path = Path("example_config.yaml")
    stats_file_path = Path("example_stats.yaml")

    start_dir = os.getcwd()

    env = os.environ.copy()
    env = {
        **env,
        "src_dsn": "postgresql://postgres:password@localhost/src",
        "dst_dsn": "postgresql://postgres:password@localhost/dst",
        "dst_schema": "dstschema",
    }

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()

        # Copy some of the example files over to the workspace.
        for file in self.generator_file_paths + (self.config_file_path,):
            src = self.examples_dir / file
            dst = self.test_dir / file
            dst.unlink(missing_ok=True)
            shutil.copy(src, dst)

        with (self.examples_dir / "example_orm.py").open() as f:
            self.expected_orm = f.readlines()

        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        os.chdir(self.start_dir)
        super().tearDown()

    def test_workflow_minimal_args(self) -> None:
        """Test the recommended CLI workflow runs without errors."""
        completed_process = run(
            ["sqlsynthgen", "make-tables", "--force"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual(
            "Table without PK detected. sqlsynthgen may not be able to continue.\n",
            completed_process.stderr.decode("utf-8"),
        )
        self.assertSuccess(completed_process)
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

        completed_process = run(
            ["sqlsynthgen", "create-generators", "--force"],
            capture_output=True,
            env=self.env,
        )
        # The database has some multi-column unique constraints, but the minimal
        # configuration here generates values for each column individually. In principle
        # this could mean that we might accidentally violate the constraints. In
        # practice this won't happen because we only write one row to an empty table.
        self.assertEqual(
            "Unsupported SQLAlchemy type "
            "<class 'sqlalchemy.dialects.postgresql.types.CIDR'> "
            "for column column_with_unusual_type. "
            "Setting this column to NULL always, "
            "you may want to configure a row generator for it instead.\n"
            "Unsupported SQLAlchemy type "
            "<class 'sqlalchemy.dialects.postgresql.types.BIT'> "
            "for column column_with_unusual_type_and_length. "
            "Setting this column to NULL always, "
            "you may want to configure a row generator for it instead.\n"
            "A unique constraint (ab_uniq) isn't fully covered by one "
            "row generator (['a']). Enforcement of the constraint may not work.\n"
            "A unique constraint (ab_uniq) isn't fully covered by one "
            "row generator (['b']). Enforcement of the constraint may not work.\n"
            "A unique constraint (abc_uniq2) isn't fully covered by one "
            "row generator (['a']). Enforcement of the constraint may not work.\n"
            "A unique constraint (abc_uniq2) isn't fully covered by one "
            "row generator (['b']). Enforcement of the constraint may not work.\n"
            "A unique constraint (abc_uniq2) isn't fully covered by one "
            "row generator (['c']). Enforcement of the constraint may not work.\n",
            completed_process.stderr.decode("utf-8"),
        )
        self.assertSuccess(completed_process)
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

        completed_process = run(
            ["sqlsynthgen", "create-tables"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

        completed_process = run(
            ["sqlsynthgen", "create-vocab"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

        completed_process = run(
            ["sqlsynthgen", "create-data"],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

        completed_process = run(
            ["sqlsynthgen", "remove-data"],
            capture_output=True,
            env=self.env,
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Are you sure? [y/N]: "
            "Would truncate non-vocabulary tables if called with --yes.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            ["sqlsynthgen", "remove-vocab"],
            capture_output=True,
            env=self.env,
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Are you sure? [y/N]: "
            "Would truncate vocabulary tables if called with --yes.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            ["sqlsynthgen", "remove-tables"],
            capture_output=True,
            env=self.env,
            input=b"\n",  # To select the default prompt option
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Are you sure? [y/N]: Would remove tables if called with --yes.\n",
            completed_process.stdout.decode("utf-8"),
        )

    def test_workflow_maximal_args(self) -> None:
        """Test the CLI workflow runs with optional arguments."""
        completed_process = run(
            [
                "sqlsynthgen",
                "make-tables",
                f"--config-file={self.config_file_path}",
                f"--orm-file={self.alt_orm_file_path}",
                "--force",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual(
            "Table unignorable_table is supposed to be ignored but "
            "there is a foreign key reference to it. "
            "You may need to create this table manually at the dst schema before "
            "running create-tables.\n"
            "Table without PK detected. sqlsynthgen may not be able to continue.\n",
            completed_process.stderr.decode("utf-8"),
        )
        self.assertSuccess(completed_process)
        self.assertEqual(
            f"Creating {self.alt_orm_file_path}.\n{self.alt_orm_file_path} created.\n",
            completed_process.stdout.decode("utf-8"),
        )

        with self.alt_orm_file_path.open("r", encoding="UTF-8") as f:
            written_orm = f.readlines()
        self.assertEqual(written_orm, self.expected_orm)

        completed_process = run(
            [
                "sqlsynthgen",
                "make-stats",
                f"--stats-file={self.stats_file_path}",
                f"--config-file={self.config_file_path}",
                "--force",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            f"Creating {self.stats_file_path}.\n"
            "Executing query count_names\n"
            "Executing query avg_person_id\n"
            "Executing query count_opt_outs\n"
            "Executing dp-query for count_opt_outs\n"
            f"{self.stats_file_path} created.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "create-generators",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                f"--stats-file={self.stats_file_path}",
                "--force",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual(
            "Unsupported SQLAlchemy type "
            "<class 'sqlalchemy.dialects.postgresql.types.CIDR'> "
            "for column column_with_unusual_type. "
            "Setting this column to NULL always, "
            "you may want to configure a row generator for it instead.\n"
            "Unsupported SQLAlchemy type "
            "<class 'sqlalchemy.dialects.postgresql.types.BIT'> "
            "for column column_with_unusual_type_and_length. "
            "Setting this column to NULL always, "
            "you may want to configure a row generator for it instead.\n",
            completed_process.stderr.decode("utf-8"),
        )
        self.assertSuccess(completed_process)
        self.assertEqual(
            f"Making {self.alt_ssg_file_path}.\n"
            "Downloading vocabulary table empty_vocabulary\n"
            "Done downloading empty_vocabulary\n"
            "Downloading vocabulary table mitigation_type\n"
            "Done downloading mitigation_type\n"
            "Downloading vocabulary table ref_to_unignorable_table\n"
            "Done downloading ref_to_unignorable_table\n"
            "Downloading vocabulary table concept_type\n"
            "Done downloading concept_type\n"
            "Downloading vocabulary table concept\n"
            "Done downloading concept\n"
            f"{self.alt_ssg_file_path} created.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "create-tables",
                f"--orm-file={self.alt_orm_file_path}",
                f"--config-file={self.config_file_path}",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Creating tables.\nTables created.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "create-vocab",
                f"--ssg-file={self.alt_ssg_file_path}",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual(
            "No rows in empty_vocabulary.yaml. Skipping...\n",
            completed_process.stderr.decode("utf-8"),
        )
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Loading vocab.\n"
            "Loading vocabulary table empty_vocabulary\n"
            "Loading vocabulary table mitigation_type\n"
            "Loading vocabulary table ref_to_unignorable_table\n"
            "Loading vocabulary table concept_type\n"
            "Loading vocabulary table concept\n"
            "5 tables loaded.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "create-data",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                "--num-passes=2",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Creating data.\n"
            'Generating data for story "story_generators.short_story".\n'
            'Generating data for story "story_generators.short_story".\n'
            'Generating data for story "story_generators.short_story".\n'
            'Generating data for story "story_generators.full_row_story".\n'
            'Generating data for story "story_generators.long_story".\n'
            'Generating data for story "story_generators.long_story".\n'
            'Generating data for table "data_type_test".\n'
            'Generating data for table "no_pk_test".\n'
            'Generating data for table "person".\n'
            'Generating data for table "strange_type_table".\n'
            'Generating data for table "unique_constraint_test".\n'
            'Generating data for table "unique_constraint_test2".\n'
            'Generating data for table "test_entity".\n'
            'Generating data for table "hospital_visit".\n'
            'Generating data for story "story_generators.short_story".\n'
            'Generating data for story "story_generators.short_story".\n'
            'Generating data for story "story_generators.short_story".\n'
            'Generating data for story "story_generators.full_row_story".\n'
            'Generating data for story "story_generators.long_story".\n'
            'Generating data for story "story_generators.long_story".\n'
            'Generating data for table "data_type_test".\n'
            'Generating data for table "no_pk_test".\n'
            'Generating data for table "person".\n'
            'Generating data for table "strange_type_table".\n'
            'Generating data for table "unique_constraint_test".\n'
            'Generating data for table "unique_constraint_test2".\n'
            'Generating data for table "test_entity".\n'
            'Generating data for table "hospital_visit".\n'
            "Data created in 2 passes.\n"
            f"person: {2*(3+1+2+2)} rows created.\n"
            f"hospital_visit: {2*(2*2+3)} rows created.\n"
            "data_type_test: 2 rows created.\n"
            "no_pk_test: 2 rows created.\n"
            "strange_type_table: 2 rows created.\n"
            "unique_constraint_test: 2 rows created.\n"
            "unique_constraint_test2: 2 rows created.\n"
            "test_entity: 2 rows created.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "remove-data",
                "--yes",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Truncating non-vocabulary tables.\n"
            'Truncating table "hospital_visit".\n'
            'Truncating table "test_entity".\n'
            'Truncating table "unique_constraint_test2".\n'
            'Truncating table "unique_constraint_test".\n'
            'Truncating table "strange_type_table".\n'
            'Truncating table "person".\n'
            'Truncating table "no_pk_test".\n'
            'Truncating table "data_type_test".\n'
            "Non-vocabulary tables truncated.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "remove-vocab",
                "--yes",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Truncating vocabulary tables.\n"
            'Truncating vocabulary table "concept".\n'
            'Truncating vocabulary table "concept_type".\n'
            'Truncating vocabulary table "ref_to_unignorable_table".\n'
            'Truncating vocabulary table "mitigation_type".\n'
            'Truncating vocabulary table "empty_vocabulary".\n'
            "Vocabulary tables truncated.\n",
            completed_process.stdout.decode("utf-8"),
        )

        completed_process = run(
            [
                "sqlsynthgen",
                "remove-tables",
                "--yes",
                f"--orm-file={self.alt_orm_file_path}",
                f"--config-file={self.config_file_path}",
                "--verbose",
            ],
            capture_output=True,
            env=self.env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)
        self.assertEqual(
            "Dropping tables.\nTables dropped.\n",
            completed_process.stdout.decode("utf-8"),
        )

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
        run(
            [
                "sqlsynthgen",
                "make-tables",
                f"--orm-file={self.alt_orm_file_path}",
                "--force",
            ],
            capture_output=True,
            env=self.env,
        )
        run(
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
        run(
            [
                "sqlsynthgen",
                "create-generators",
                f"--orm-file={self.alt_orm_file_path}",
                f"--ssg-file={self.alt_ssg_file_path}",
                f"--config-file={self.config_file_path}",
                f"--stats-file={self.stats_file_path}",
                "--force",
            ],
            capture_output=True,
            env=self.env,
        )
        run(
            [
                "sqlsynthgen",
                "create-tables",
                f"--orm-file={self.alt_orm_file_path}",
            ],
            capture_output=True,
            env=self.env,
        )
        run(
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
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

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
        self.assertEqual("", completed_process.stdout.decode("utf-8"))

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

    def test_create_schema(self) -> None:
        """Check that we create a destination schema if it doesn't exist."""
        env = self.env.copy()
        env["dst_schema"] = "doesntexistyetschema"

        engine = create_engine(env["dst_dsn"])
        inspector = inspect(engine)
        self.assertFalse(inspector.has_schema(env["dst_schema"]))

        completed_process = run(
            [
                "sqlsynthgen",
                "make-tables",
                "--force",
            ],
            capture_output=True,
            env=env,
        )
        self.assertSuccess(completed_process)

        completed_process = run(
            [
                "sqlsynthgen",
                "create-tables",
            ],
            capture_output=True,
            env=env,
        )
        self.assertEqual("", completed_process.stderr.decode("utf-8"))
        self.assertSuccess(completed_process)

        engine = create_engine(env["dst_dsn"])
        inspector = inspect(engine)
        self.assertTrue(inspector.has_schema(env["dst_schema"]))
