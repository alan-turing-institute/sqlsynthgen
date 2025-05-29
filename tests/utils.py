"""Utilities for testing."""
import asyncio
from functools import lru_cache
import os
from pathlib import Path
import shutil
from sqlalchemy.schema import MetaData
from subprocess import run
import testing.postgresql
from typing import Any
from unittest import TestCase, skipUnless
import yaml

from sqlalchemy import MetaData
from tempfile import mkstemp

from datafaker import settings
from datafaker.create import create_db_data_into
from datafaker.make import make_tables_file, make_src_stats, make_table_generators
from datafaker.remove import remove_db_data_from
from datafaker.utils import import_file, sorted_non_vocabulary_tables, create_db_engine

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
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)

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


class GeneratesDBTestCase(RequiresDBTestCase):
    def setUp(self) -> None:
        super().setUp()
        # Generate the `orm.yaml` from the database
        (self.orm_fd, self.orm_file_path) = mkstemp(".yaml", "orm_", text=True)
        with os.fdopen(self.orm_fd, "w", encoding="utf-8") as orm_fh:
            orm_fh.write(make_tables_file(self.dsn, self.schema_name, {}))

    def set_configuration(self, config) -> None:
        """
        Accepts a configuration file, writes it out.
        """
        (self.config_fd, self.config_file_path) = mkstemp(".yaml", "config_", text=True)
        with os.fdopen(self.config_fd, "w", encoding="utf-8") as config_fh:
            config_fh.write(yaml.dump(config))

    def get_src_stats(self, config) -> dict[str, any]:
        """
        Runs `make-stats` producing `src-stats.yaml`
        :return: Python dictionary representation of the contents of the src-stats file
        """
        loop = asyncio.new_event_loop()
        src_stats = loop.run_until_complete(
            make_src_stats(self.dsn, config, self.metadata, self.schema_name)
        )
        loop.close()
        (self.stats_fd, self.stats_file_path) = mkstemp(".yaml", "src_stats_", text=True)
        with os.fdopen(self.stats_fd, "w", encoding="utf-8") as stats_fh:
            stats_fh.write(yaml.dump(src_stats))

    def create_generators(self, config) -> None:
        """ ``create-generators`` with ``src-stats.yaml`` and the rest, producing ``datafaker.py`` """
        datafaker_content = make_table_generators(
            self.metadata,
            config,
            self.orm_file_path,
            self.config_file_path,
            self.stats_file_path,
        )
        (generators_fd, self.generators_file_path) = mkstemp(".py", "dfgen_", text=True)
        with os.fdopen(generators_fd, "w", encoding="utf-8") as datafaker_fh:
            datafaker_fh.write(datafaker_content)

    def create_data(self, config, num_passes=1):
        """ Remove source data from the DB and create fake data in its place. """
        # `remove-data` so we don't have to use a separate database for the destination
        remove_db_data_from(self.metadata, config, self.dsn, self.schema_name)
        # `create-data` with all this stuff
        datafaker_module = import_file(self.generators_file_path)
        table_generator_dict = datafaker_module.table_generator_dict
        story_generator_list = datafaker_module.story_generator_list
        create_db_data_into(
            sorted_non_vocabulary_tables(self.metadata, config),
            table_generator_dict,
            story_generator_list,
            num_passes,
            self.dsn,
            self.schema_name,
        )

    def generate_data(self, config, num_passes=1):
        """
        Replaces the DB's source data with generated data.
        :return: A Python dictionary representation of the src-stats.yaml file, for what it's worth.
        """
        self.set_configuration(config)
        src_stats = self.get_src_stats(config)
        self.create_generators(config)
        self.create_data(config, num_passes)
        return src_stats
