"""Utilities for testing."""
import asyncio
import os
import random
import re
import shutil
import string
import time
import traceback
from abc import ABC, abstractmethod
from collections.abc import MutableSequence, Sequence
from functools import lru_cache
from pathlib import Path
from subprocess import run
from tempfile import mkdtemp, mkstemp
from typing import Any, Mapping
from unittest import SkipTest, TestCase

import duckdb
import testing.postgresql
import yaml
from sqlalchemy import Engine, MetaData

from datafaker import settings
from datafaker.create import create_db_data_into, create_db_tables_into
from datafaker.interactive.base import DbCmd
from datafaker.make import make_src_stats, make_table_generators, make_tables_file
from datafaker.utils import (
    MaybeAsyncEngine,
    T,
    create_db_engine,
    create_db_engine_dst,
    get_sync_engine,
    import_file,
    sorted_non_vocabulary_tables,
)


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


class TestDatabaseBase(ABC):
    """Abstract base class for test databases."""

    @classmethod
    @abstractmethod
    def skip(cls) -> str | None:
        """Returns an error message if this database type is not available."""

    @classmethod
    def setup(cls) -> None:
        """Set up the class."""

    @classmethod
    def final(cls) -> None:
        """Clean up the class."""

    @abstractmethod
    def close(self) -> None:
        """Tear down the test database."""

    @abstractmethod
    def open(self) -> None:
        """Open a fresh test database (closing any previous)."""

    @abstractmethod
    def get_dsn(self, database_name: str | None) -> str:
        """Get the DSN for the test database."""

    @abstractmethod
    def run_sql(self, sql_file: Path) -> None:
        """Run the provided SQL file on the test database."""


class TestPostgres(TestDatabaseBase):
    """Postgres test database."""

    Postgresql = None

    @classmethod
    def skip(cls) -> str | None:
        if shutil.which("psql"):
            return None
        return "need to find 'psql': install PostgreSQL to enable"

    @classmethod
    def setup(cls) -> None:
        """Set up the test database."""
        cls.Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)

    def __init__(self) -> None:
        """Initialize the test database."""
        if self.Postgresql is None:
            self.setup()
        super().__init__()
        self.postgresql: Any = None
        self.open()

    def open(self) -> None:
        """Start the test database"""
        assert self.Postgresql is not None
        self.postgresql = self.Postgresql()  # pylint: disable=not-callable

    def close(self) -> None:
        """Tear down the test database."""
        if self.postgresql is not None:
            self.postgresql.terminate()

    @classmethod
    def final(cls) -> None:
        """Clean up after all testing"""
        if cls.Postgresql is not None:
            cls.Postgresql.clear_cache()

    def get_dsn(self, database_name: str | None) -> str:
        """Get the DSN for the test database."""
        if database_name:
            url = self.postgresql.url(database=database_name)
        else:
            url = self.postgresql.url()
        assert isinstance(url, str)
        return url

    def run_sql(self, sql_file: Path) -> None:
        """Run psql and pass a sql file as the --file option."""

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
            ["psql", "-d", self.postgresql.url(), "-f", sql_file],
            capture_output=True,
            check=True,
        )
        # psql doesn't always return != 0 if it fails
        assert completed_process.stderr == b"", completed_process.stderr


class TestDuckDb(TestDatabaseBase):
    """Test DuckDB database."""

    SQL_REMOVALS_RE = re.compile(
        r"^(CREATE DATABASE .*;)|(\\.*)|(ALTER [^;]*;)", re.MULTILINE
    )
    # Postgres' default stupid time format, ingoring time zone for now
    TIME_FORMAT_RE = re.compile(
        r"'([A-Z][a-z]+ \d+ \d\d:\d\d:\d\d \d\d\d\d) ([A-Z+\-0-9:]+)'"
    )

    @classmethod
    def skip(cls) -> str | None:
        """Return None because DuckDB always works."""
        return None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize TestDuckDb"""
        super().__init__(*args, **kwargs)
        self._duckdb_con: Any = None
        self._db_dir = Path(mkdtemp("duck"))
        self._make_con_string()

    def _make_con_string(self) -> None:
        """Make a fresh connection string."""
        self._db_path = self._db_dir / (
            "".join(random.choice(string.ascii_letters) for _ in range(8)) + ".db"
        )

    def open(self) -> None:
        """Start the test database"""
        self.close()
        self._make_con_string()
        # create the database (must be non-read-only)
        # duckdb_con = duckdb.connect(self._db_path)
        # duckdb_con.close()

    def close(self) -> None:
        """Tear down the test database."""
        if self._duckdb_con is not None:
            self._duckdb_con.close()
            self._duckdb_con = None

    def get_dsn(self, _database_name: str | None) -> str:
        """Get the DSN for the test database."""
        return f"duckdb:///{self._db_path}"

    def run_sql(self, sql_file: Path) -> None:
        """Run all the SQL commands in ``sql_file`` on the database."""
        with sql_file.open() as sql_fh:
            sql = sql_fh.read()
        # Remove Postgresisms that DuckDB doesn't understand
        sanitized1 = re.sub(self.SQL_REMOVALS_RE, "", sql)
        # Convert time formats
        sanitized = re.sub(
            self.TIME_FORMAT_RE,
            lambda tf: time.strftime(
                f"(TIMESTAMPTZ '%Y-%m-%d %H:%M:%S {tf.group(2)}')",
                time.strptime(tf.group(1), "%B %d %H:%M:%S %Y"),
            ),
            sanitized1,
        )
        # Postgres has default schema "public", so we must have the same
        duckdb_con = duckdb.connect(self._db_path)
        duckdb_con.execute("CREATE SCHEMA public;")
        duckdb_con.execute(sanitized)
        duckdb_con.close()


class DatafakerTestCase(TestCase):
    """Parent class for all TestCases in datafaker."""

    schema_name: str | None = None
    use_asyncio = False
    examples_dir = Path("tests/examples")
    dump_file_path: str | None = None
    database_name: str | None = None

    def setUp(self) -> None:
        settings.get_settings.cache_clear()

    def assertReturnCode(  # pylint: disable=invalid-name
        self, result: Any, expected_code: int
    ) -> None:
        """Give details for a subprocess result and raise if it's not as expected."""
        code = result.exit_code if hasattr(result, "exit_code") else result.returncode
        if code != expected_code:
            print(result.exception)
            print(result.stdout)
            print(result.stderr)
            self.assertEqual(expected_code, code)

    def assertSuccess(self, result: Any) -> None:  # pylint: disable=invalid-name
        """Give details for a subprocess result and raise if the result isn't good."""
        self.assertReturnCode(result, 0)

    def assert_successful_and_no_output(self, result: Any) -> None:
        """Assert that a process was successful and no output was produced."""
        code = result.exit_code if hasattr(result, "exit_code") else result.returncode
        errors = []
        if code != 0:
            errors.append(f"Result code was {code} not 0.")
        if result.stdout:
            errors.append(f"Process unexpectedly produced output:\n{result.stdout}")
        if result.stderr:
            errors.append(f"Process unexpectedly produced error text:\n{result.stderr}")
        if errors:
            self.fail("\n".join(errors))

    def assertFailure(self, result: Any) -> None:  # pylint: disable=invalid-name
        """Give details for a subprocess result and raise if the result isn't bad."""
        self.assertReturnCode(result, 1)

    # pylint: disable=invalid-name
    def assertNoException(self, result: Any) -> None:
        """Assert that the result has no exception."""
        assert hasattr(result, "exception")
        if result.exception is None:
            return
        self.fail("".join(traceback.format_exception(result.exception)))

    def assert_greater_and_not_none(self, left: float | None, right: float) -> None:
        """
        Assert left is not None and greater than right
        """
        if left is None:
            self.fail("first argument is None")
        else:
            self.assertGreater(left, right)

    def assert_subset(self, set1: set[T], set2: set[T], msg: str | None = None) -> None:
        """Assert a set is a (non-strict) subset.

        :param set1: The asserted subset.
        :param set2: The asserted superset.
        :param msg: Optional message to use on failure instead of a list of
        differences.
        """
        try:
            difference = set1.difference(set2)
        except TypeError as e:
            self.fail(f"invalid type when attempting set difference: {e}")
        except AttributeError as e:
            self.fail(f"first argument does not support set difference: {e}")

        if not difference:
            return

        lines = []
        if difference:
            lines.append("Items in the first set but not the second:")
            for item in difference:
                lines.append(repr(item))

        standard_msg = "\n".join(lines)
        self.fail(self._formatMessage(msg, standard_msg))


class RequiresDBTestCase(DatafakerTestCase):
    """
    A test case that only runs if a database (PostgreSQL or DuckDB) is installed.

    ``dump_file_path`` can be set to run in this postgres database.
    ``database_name`` is the name of the database referred to in dump_file_path.
    You can use ``self.dsn`` to retrieve the DSN of this database, ``self.engine``
    to get an engine to access the database and ``self.metadata`` to get metadata
    reflected from that engine.
    """

    database_type: type[TestDatabaseBase] = TestPostgres

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        skip_msg = cls.database_type.skip()
        if skip_msg:
            raise SkipTest(skip_msg)
        cls.database_type.setup()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.database_type.final()
        super().tearDownClass()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise RequiresDBTestCase."""
        super().__init__(*args, **kwargs)
        self.database: TestDatabaseBase | None = None
        self.metadata = MetaData()
        self.engine: MaybeAsyncEngine
        self.sync_engine: Engine

    def setUp(self) -> None:
        super().setUp()
        if self.database is None:
            self.database = self.database_type()
        else:
            self.database.open()
        if self.dump_file_path is not None:
            self.database.run_sql(Path(self.examples_dir) / Path(self.dump_file_path))
        self.engine = create_db_engine(
            self.database.get_dsn(self.database_name),
            schema_name=self.schema_name,
            use_asyncio=self.use_asyncio,
        )
        self.sync_engine = get_sync_engine(self.engine)
        self.metadata.reflect(self.sync_engine)

    def tearDown(self) -> None:
        assert self.database is not None
        self.database.close()
        super().tearDown()

    @property
    def dsn(self) -> str:
        """Get the database connection string."""
        assert self.database is not None
        return self.database.get_dsn(self.database_name)


# pylint: disable=too-many-instance-attributes
class GeneratesDBTestCase(RequiresDBTestCase):
    """A test case for which a database is generated."""

    dst_schema_name: str | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise a GeneratedDB test case."""
        super().__init__(*args, **kwargs)
        self.generators_file_path = ""
        self.stats_fd = 0
        self.stats_file_path = ""
        self.config_file_path = ""
        self.config_fd = 0
        self.dst_database: TestDatabaseBase | None = None
        self.dst_engine: MaybeAsyncEngine
        self.dst_sync_engine: Engine

    @property
    def dst_dsn(self) -> str:
        """Get the database connection string."""
        assert self.dst_database is not None
        return self.dst_database.get_dsn(None)

    def setUp(self) -> None:
        """Set up the test case with an actual orm.yaml file."""
        super().setUp()
        if self.dst_database is None:
            self.dst_database = self.database_type()
        else:
            self.dst_database.open()
        self.dst_engine = create_db_engine_dst(
            self.dst_dsn,
            schema_name=self.dst_schema_name,
            use_asyncio=self.use_asyncio,
        )
        self.dst_sync_engine = get_sync_engine(self.dst_engine)
        # Generate the `orm.yaml` from the database
        (self.orm_fd, self.orm_file_path) = mkstemp(".yaml", "orm_", text=True)
        with os.fdopen(self.orm_fd, "w", encoding="utf-8") as orm_fh:
            orm_fh.write(make_tables_file(self.dsn, self.schema_name))

    def set_configuration(self, config: Mapping[str, Any]) -> None:
        """Accepts a configuration file, writes it out."""
        (self.config_fd, self.config_file_path) = mkstemp(".yaml", "config_", text=True)
        with os.fdopen(self.config_fd, "w", encoding="utf-8") as config_fh:
            config_fh.write(yaml.dump(config))

    def get_src_stats(self, config: Mapping[str, Any]) -> dict[str, Any]:
        """
        Runs `make-stats` producing `src-stats.yaml`.

        :return: Python dictionary representation of the contents of the src-stats file
        """
        loop = asyncio.new_event_loop()
        src_stats = loop.run_until_complete(
            make_src_stats(self.dsn, config, self.schema_name)
        )
        loop.close()
        (self.stats_fd, self.stats_file_path) = mkstemp(
            ".yaml", "src_stats_", text=True
        )
        with os.fdopen(self.stats_fd, "w", encoding="utf-8") as stats_fh:
            stats_fh.write(yaml.dump(src_stats))
        return src_stats

    def create_generators(self, config: Mapping[str, Any]) -> None:
        """``create-generators`` with ``src-stats.yaml`` and the rest, producing ``df.py``"""
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

    def create_tables(self) -> None:
        """Create tables in the output DB."""
        create_db_tables_into(self.metadata, self.dst_dsn, self.dst_schema_name)

    def create_data(self, config: Mapping[str, Any], num_passes: int = 1) -> None:
        """Create fake data in the DB."""
        # `create-data` with all this stuff
        datafaker_module = import_file(self.generators_file_path)
        create_db_data_into(
            sorted_non_vocabulary_tables(self.metadata, config),
            datafaker_module,
            num_passes,
            self.dst_dsn,
            self.dst_schema_name,
            self.metadata,
        )

    def generate_data(
        self, config: Mapping[str, Any], num_passes: int = 1
    ) -> Mapping[str, Any]:
        """
        Replaces the DB's source data with generated data.
        :return: A Python dictionary representation of the src-stats.yaml file, for what it's worth.
        """
        self.set_configuration(config)
        src_stats = self.get_src_stats(config)
        self.create_generators(config)
        self.create_tables()
        self.create_data(config, num_passes)
        return src_stats


class TestDbCmdMixin(DbCmd):
    """A mixin for capturing output from interactive commands."""

    def __init__(self, *args: Any, print_tables: bool = False, **kwargs: Any) -> None:
        """Initialize a TestDbCmdMixin"""
        super().__init__(*args, **kwargs)
        self._print_tables = print_tables
        self.reset()

    def reset(self) -> None:
        """Reset all the debug messages collected so far."""
        self.messages: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.headings: Sequence[str] = []
        self.rows: Sequence[Sequence[str]] = []
        self.column_items: MutableSequence[Sequence[str]] = []
        self.columns: Mapping[str, Sequence[Any]] = {}

    def print(self, text: str, *args: Any, **kwargs: Any) -> None:
        """Capture the printed message."""
        self.messages.append((text, args, kwargs))

    def print_table(
        self, headings: Sequence[str], rows: Sequence[Sequence[str]]
    ) -> None:
        """Capture the printed table."""
        self.headings = headings
        self.rows = rows
        if self._print_tables:
            super().print_table(headings, rows)

    def print_table_by_columns(self, columns: Mapping[str, Sequence[str]]) -> None:
        """Capture the printed table."""
        self.columns = columns

    # pylint: disable=arguments-renamed
    def columnize(self, items: Sequence[str] | None, _displaywidth: int = 80) -> None:
        """Capture the printed table."""
        if items is not None:
            self.column_items.append(items)

    def ask_save(self) -> str:
        """Quitting always works without needing to ask the user."""
        return "yes"
