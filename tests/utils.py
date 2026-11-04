"""Utilities for testing."""
import asyncio
import os
import random
import re
import shutil
import string
import sys
import time
import traceback
from abc import ABC, abstractmethod
from collections.abc import MutableSequence, Sequence, Set
from functools import lru_cache
from importlib import resources
from pathlib import Path
from subprocess import run
from tempfile import mkdtemp, mkstemp
from typing import Any, Mapping
from unittest import SkipTest, TestCase

import duckdb
import sqlalchemy
import testing.postgresql
import yaml
from prettytable import PrettyTable
from sqlalchemy import Engine, MetaData, create_engine
from sqlalchemy.engine import make_url
from sqlalchemy_utils import create_database

from datafaker import settings
from datafaker.create import create_db_data_into, create_db_tables_into
from datafaker.db_utils import (
    MaybeAsyncEngine,
    create_db_engine,
    create_db_engine_dst,
    get_sync_engine,
    sorted_non_vocabulary_tables,
)
from datafaker.interactive.base import DbCmd
from datafaker.make import make_src_stats, make_tables_file
from datafaker.serialize_metadata import dict_to_metadata, metadata_to_dict
from datafaker.theme import ThemeEntry, set_active_theme
from datafaker.utils import T


@lru_cache(1)
def get_test_settings(
    src_dsn="postgresql://suser:spassword@shost:5432/sdbname",
    dst_dsn="postgresql://duser:dpassword@dhost:5432/ddbname",
    src_schema=None,
    dst_schema=None,
) -> settings.Settings:
    """Get a Settings object that ignores .env files and environment variables."""

    return settings.Settings(
        src_dsn=src_dsn,
        dst_dsn=dst_dsn,
        src_schema=src_schema,
        dst_schema=dst_schema,
        # To stop any local .env files influencing the test
        # The mypy ignore can be removed once we upgrade to pydantic 2.
        _env_file=None,  # ty: ignore[unknown-argument]
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

    def create_empty(  # pylint: disable=unused-argument
        self,
        name: str,
        schema_name: str | None,
    ) -> None:
        """Create an empty database and return the DSN string."""
        dsn = self.get_dsn(name)
        create_database(dsn)


class PostgresTestDb(TestDatabaseBase):
    """Postgres test database."""

    Postgresql = None

    @classmethod
    def skip(cls) -> str | None:
        try:
            import psycopg2 as _psycopg2  # noqa: F401 pylint: disable=import-outside-toplevel
        except ImportError:
            return "psycopg2 not installed; run: poetry install --all-extras"
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


class DuckTestDb(TestDatabaseBase):
    """Test DuckDB database."""

    SQL_REMOVALS_RE = re.compile(
        r"^(CREATE DATABASE .*;)|(\\.*)|(ALTER [^;]*;)|(\bUSING\s+btree)", re.MULTILINE
    )
    # Postgres' default stupid time format, ignoring time zone for now
    TIME_FORMAT_RE = re.compile(
        r"'([A-Z][a-z]+ \d+ \d\d:\d\d:\d\d \d\d\d\d) ([A-Z+\-0-9:]+)'"
    )

    @classmethod
    def skip(cls) -> str | None:
        """Return None because DuckDB always works."""
        return None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize DuckTestDb"""
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

    def close(self) -> None:
        """Tear down the test database."""
        if self._duckdb_con is not None:
            self._duckdb_con.close()
            self._duckdb_con = None

    def get_dsn(self, database_name: str | None) -> str:
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

    def create_empty(  # pylint: disable=unused-argument
        self,
        name: str,
        schema_name: str | None,
    ) -> None:
        """
        The standard SQLAlchemy database creation tool doesn't work for DuckDB.

        Thankfully, it is not necessary either.
        """
        return


class MsSqlTestDb(TestDatabaseBase):
    """MS-SQL Server test database.

    Requires the ``MSSQL_TEST_DSN`` environment variable to be set to a
    ``mssql+pyodbc://`` connection string pointing at a running SQL Server
    instance (e.g. the docker-compose ``mssql`` service).

    Tests that use this class are skipped automatically when the variable is
    absent or when a connection cannot be established.
    """

    _ENV_VAR = "MSSQL_TEST_DSN"
    _base_dsn: str = ""
    _CREATE_DATABASE_RE = re.compile(r"CREATE\s+DATABASE\s+([A-Za-z0-9_]+)\s+WITH\s.*;")
    _ALTER_DATABASE_RE = re.compile(
        r"ALTER\s+DATABASE\s+([A-Za-z0-9_]+)\s+OWNER\s+TO\s.*;"
    )
    _CONNECT_RE = re.compile(r"^\\connect\s+([A-Za-z0-9_]+)\s*$", re.MULTILINE)
    _PUBLIC_SCHEMA = re.compile(r"(\s)public\.")
    _GO_RE = re.compile(r"^\s*GO\s*$", re.MULTILINE)
    _EOL_RE = re.compile(r";$", re.MULTILINE)
    _TIMESTAMP_RE = re.compile(r"(TIMESTAMP)\s+WITH\s+TIME\s+ZONE", re.IGNORECASE)
    _ALTER_OWNER_RE = re.compile(
        r'ALTER\s+(TABLE|DATABASE|VIEW)\s+["A-Za-z0-9_.]+\s+OWNER\s+TO\s+[A-Za-z0-9_]+\s*;'
    )
    _ALTER_ONLY_RE = re.compile(r"ALTER\s+TABLE\s+ONLY\s+")
    _CREATE_INDEX_RE = re.compile(
        r"(CREATE\s+INDEX\s+[A-Za-z0-9_.]+\s+ON\s+[A-Za-z0-9_.]+\s+)"
        r"USING\s+[A-Za-z0-9_.]+\s+(\([A-Za-z0-9_., ]+\))\s*;"
    )
    _BOOLEAN_RE = re.compile(r"\bboolean\b", re.IGNORECASE)
    _TRUE_RE = re.compile(r"\btrue\b", re.IGNORECASE)
    _FALSE_RE = re.compile(r"\bfalse\b", re.IGNORECASE)
    _UUID_RE = re.compile(r"\buuid\b", re.IGNORECASE)
    _TEXT_RE = re.compile(r"\btext\b", re.IGNORECASE)

    @classmethod
    def get_test_db_dsn(cls) -> str:
        """Get the DSN for the master database to connect to."""
        return os.environ.get(
            cls._ENV_VAR,
            "mssql+pyodbc://sa:Datafaker!Test123"
            "@127.0.0.1:21433/master"
            "?driver=ODBC+Driver+18+for+SQL+Server"
            "&TrustServerCertificate=yes",
        )

    @classmethod
    def skip(cls) -> str | None:
        """Return a skip message if SQL Server is not reachable."""
        dsn = cls.get_test_db_dsn()
        try:
            import pyodbc as _pyodbc  # noqa: F401 pylint: disable=import-outside-toplevel
        except ImportError:
            return "pyodbc not installed; run: poetry install --all-extras"
        try:
            with create_engine(dsn).connect():
                pass
        except Exception as exc:  # pylint: disable=broad-except
            return f"cannot connect to MS-SQL ({exc})"
        return None

    @classmethod
    def setup(cls) -> None:
        """Store the base DSN for use by all instances."""
        cls._base_dsn = cls.get_test_db_dsn()

    def _generate_prefix(self) -> None:
        self.prefix = f"tmp{''.join(random.choices(string.digits, k=3))}_"

    def __init__(self):
        super().__init__()
        self.db_names = []
        self._generate_prefix()

    def open(self) -> None:
        """Nothing to open — SQL Server runs externally."""
        self._generate_prefix()

    def close(self) -> None:
        self._drop_databases_then(self.db_names, [])
        self.db_names = []

    def get_dsn(self, database_name: str | None) -> str:
        """Return a DSN pointing at ``database_name`` within the SQL Server instance."""
        if not database_name:
            return self._base_dsn

        url = make_url(self._base_dsn)
        return url.set(database=self.prefix + database_name).render_as_string(
            hide_password=False
        )

    def _get_connection_string(self, db_name: str) -> str:
        """Get a connection string for the named database."""
        url = make_url(self._base_dsn)
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={url.host},{url.port or 21433};"
            f"DATABASE={db_name};"
            f"UID={url.username};PWD={url.password};"
            "TrustServerCertificate=yes;"
        )

    def _drop_databases_then(self, db_names: list[str], execs: list[str]) -> None:
        """Drop the database ``db_name`` and maybe execute a command."""
        import pyodbc  # noqa: F401 pylint: disable=import-outside-toplevel

        conn_str = self._get_connection_string("master")
        with pyodbc.connect(  # pylint: disable=c-extension-no-member
            conn_str,
            autocommit=True,
        ) as conn:
            conn.execute("USE master")
            for db_name in db_names:
                conn.execute(
                    f"""IF DB_ID('{db_name}') IS NOT NULL
BEGIN
    ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{db_name}];
END"""
                )
            for ex in execs:
                sql = ex.strip()
                if sql:
                    conn.execute(sql)

    def create_empty(self, name: str, schema_name: str | None) -> None:
        """Drop (if exists) and create a fresh SQL Server database named ``name``."""
        import pyodbc  # noqa: F401 pylint: disable=import-outside-toplevel

        qname = self.prefix + name
        instructions = [
            f"CREATE DATABASE [{qname}]",
            f"USE [{qname}]",
        ]
        if schema_name:
            instructions.append(f"CREATE SCHEMA [{schema_name}]")
        self._drop_databases_then([qname], instructions)
        self.db_names.append(qname)

        # SQL Server can take a moment to bring the new database fully online.
        # Poll until a connection succeeds rather than returning a DSN that
        # immediately produces TCP resets.
        db_conn_str = self._get_connection_string(qname)
        deadline = time.monotonic() + 30

        while True:
            try:
                with pyodbc.connect(  # pylint: disable=c-extension-no-member
                    db_conn_str,
                    autocommit=True,
                    timeout=5,
                ) as probe:
                    probe.execute("SELECT 1")
                break
            except pyodbc.Error:  # pylint: disable=c-extension-no-member
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)

    def run_sql(self, sql_file: Path) -> None:
        """Execute a T-SQL file via pyodbc, splitting batches on GO."""
        sql = sql_file.read_text(encoding="utf-8")
        # Which databases are we creating?
        db_names = self._CREATE_DATABASE_RE.findall(sql)
        self.db_names = [self.prefix + name for name in db_names]
        # Get rid of parameters MSSQL does not understand
        sql = self._CREATE_DATABASE_RE.sub(
            f"CREATE DATABASE {self.prefix}\\1;\nGO\n", sql
        )
        # Get rid of ownership change
        sql = self._ALTER_DATABASE_RE.sub("", sql)
        # Turn \connect into USE
        sql = self._CONNECT_RE.sub(f"USE {self.prefix}\\1;", sql)
        # Get rid of public. (preceded by space)
        sql = self._PUBLIC_SCHEMA.sub("\\1", sql)
        # Get rid of WITH TIME ZONE
        sql = self._TIMESTAMP_RE.sub("DATETIME2", sql)
        # Get rid of changing owner
        sql = self._ALTER_OWNER_RE.sub("", sql)
        # Get rid of ONLY
        sql = self._ALTER_ONLY_RE.sub("ALTER TABLE ", sql)
        # Get USING out of CREATE INDEX
        sql = self._CREATE_INDEX_RE.sub("\\1\\2;", sql)
        # BOOLEAN -> BIT
        sql = self._BOOLEAN_RE.sub("BIT", sql)
        sql = self._TRUE_RE.sub("1", sql)
        sql = self._FALSE_RE.sub("0", sql)
        # UUID -> UNIQUEIDENTIFIER
        sql = self._UUID_RE.sub("UNIQUEIDENTIFIER", sql)
        # TEXT -> NVARCHAR(450), the largest unicode text that have a unique constaint
        sql = self._TEXT_RE.sub("NVARCHAR(450)", sql)
        sql = self._EOL_RE.sub(";\nGO", sql)
        self._drop_databases_then(
            self.db_names,
            self._GO_RE.split(sql),
        )


class DatafakerTestCase(TestCase):
    """Parent class for all TestCases in datafaker."""

    schema_name: str | None = None
    use_asyncio = False
    examples_dir = Path("examples")
    dump_file_path: str | None = None
    database_name: str | None = None
    use_temporary_cwd = False
    copy_files: list[str] = []
    copy_from_directory: Path = Path(".")

    def get_abs_example_dir(self) -> Path:
        """Get an absolute path to the examples directory."""
        test_module = resources.files(sys.modules["tests"])
        trav = test_module.joinpath(str(self.examples_dir))
        assert isinstance(trav, Path)
        return trav

    def setUp(self) -> None:
        """Set up the test case with an actual orm.yaml file."""
        super().setUp()
        settings.get_settings.cache_clear()
        if self.use_temporary_cwd:
            self.start_dir = os.getcwd()
            self.working_dir = mkdtemp("test")
            from_dir = resources.files(sys.modules["tests"]) / str(
                self.copy_from_directory
            )
            for cf in self.copy_files:
                with resources.as_file(from_dir / cf) as cff:
                    shutil.copy(cff, self.working_dir)
            os.chdir(self.working_dir)

    def tearDown(self) -> None:
        if self.use_temporary_cwd:
            os.chdir(self.start_dir)
            shutil.rmtree(self.working_dir)
        super().tearDown()

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

    def assert_subset(self, set1: Set[T], set2: Set[T], msg: str | None = None) -> None:
        """Assert a set is a (non-strict) subset.

        :param set1: The asserted subset.
        :param set2: The asserted superset.
        :param msg: Optional message to use on failure instead of a list of
        differences.
        """
        try:
            difference = set1 - set2
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

    def assert_str_in(self, needle: str | None, haystack: str | None) -> None:
        """Assert that the string ``needle`` is in the string ``haystack``."""
        if needle is None or haystack is None or needle not in haystack:
            self.fail(f"Expected {repr(needle)} to be found in {repr(haystack)}")

    def assert_str_not_in(self, needle: str, haystack: str) -> None:
        """Assert that the string ``needle`` is not in the string ``haystack``."""
        if needle in haystack:
            pos = haystack.index(needle)
            length = len(needle)
            trimmed = haystack
            if 120 < len(haystack):
                if 50 < pos:
                    trimmed = "... " + trimmed[pos - 46 :]
                    pos = 50
                if 120 < len(trimmed):
                    trimmed = trimmed[:116] + " ..."
                    if 117 < pos + length:
                        length = 117 - pos
            self.fail(
                f'Expected "{needle}" not to be found but:\n'
                + f"{trimmed}\n{' ' * pos}{'^' * length}"
            )


def print_sql_results(engine: Engine, sql: str) -> None:
    """
    Pretty print the result of some sql.
    """
    with engine.connect() as conn:
        results = conn.execute(sqlalchemy.text(sql)).fetchall()
        if len(results) == 0:
            print("No results returned")
            return
        pt = PrettyTable(results[0]._fields)
        to_print = min(len(results), 20)
        pt.add_rows(results[:to_print])  # type: ignore
        print(pt)
        skipped = len(results) - to_print
        if skipped:
            print(f"... and {skipped} more rows")


class RequiresDBTestCase(
    DatafakerTestCase
):  # pylint: disable=too-many-instance-attributes
    """
    A test case that only runs if a database (PostgreSQL or DuckDB) is installed.

    ``dump_file_path`` can be set to run in this postgres database.
    ``database_name`` is the name of the database referred to in dump_file_path.
    You can use ``self.dsn`` to retrieve the DSN of this database, ``self.engine``
    to get an engine to access the database and ``self.metadata`` to get metadata
    reflected from that engine.
    """

    database_type: type[TestDatabaseBase] = PostgresTestDb
    dst_schema_name: str | None = None

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
        self.dst_name: str | None = None
        self.dst_database: TestDatabaseBase | None = None
        self.dst_metadata = MetaData()
        self.dst_engine: Engine | None = None

    @property
    def dst_dsn(self) -> str:
        """Get the database connection string."""
        assert self.dst_database is not None
        return self.dst_database.get_dsn(self.dst_name)

    def _create_engine(self) -> None:
        assert self.database is not None
        self.engine = create_db_engine(
            self.database.get_dsn(self.database_name),
            schema_name=self.schema_name,
            use_asyncio=self.use_asyncio,
        )
        self.sync_engine = get_sync_engine(self.engine)

    def setUp(self) -> None:
        super().setUp()
        set_active_theme(ThemeEntry.NONE)
        if self.database is None:
            self.database = self.database_type()
        else:
            self.database.open()
        if self.dump_file_path is not None:
            self.database.run_sql(self.get_abs_example_dir() / self.dump_file_path)
        self._create_engine()
        self.metadata.reflect(self.sync_engine)

    def sql(self, sql: str) -> None:
        """
        Execute some SQL against the source database.

        This method is useful during debugging.
        """
        print_sql_results(self.sync_engine, sql)

    def dst_sql(self, sql: str) -> None:
        """
        Execute some SQL against the destination database.

        This method is useful during debugging.
        """
        if self.dst_engine is not None:
            print_sql_results(self.dst_engine, sql)
        else:
            print("No destination engine")

    def _create_engine_dst(self):
        self.dst_engine = get_sync_engine(
            create_db_engine_dst(
                self.dst_dsn,
                schema_name=self.dst_schema_name,
                use_asyncio=self.use_asyncio,
            )
        )

    def make_destination_database(self, name: str) -> None:
        """Make an empty destination database."""
        self.dst_name = name
        if self.dst_database is None:
            self.dst_database = self.database_type()
        else:
            self.dst_database.open()
        self.dst_database.create_empty(name, self.dst_schema_name)
        self._create_engine_dst()

    def tearDown(self) -> None:
        assert self.database is not None
        if hasattr(self, "sync_engine"):
            self.sync_engine.dispose()
        if self.dst_engine is not None:
            self.dst_engine.dispose()
        if self.dst_database is not None:
            self.dst_database.close()
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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialise a GeneratedDB test case."""
        super().__init__(*args, **kwargs)
        self.generators_file_path = ""
        self.stats_fd = 0
        self.stats_file_path = ""
        self.config_file_path = ""
        self.config_fd = 0
        self.dst_database: TestDatabaseBase | None = None

    def setUp(self) -> None:
        """Set up the test case with an actual orm.yaml file."""
        super().setUp()
        # Generate the `orm.yaml` from the database
        (self.orm_fd, self.orm_file_path) = mkstemp(".yaml", "orm_", text=True)
        with os.fdopen(self.orm_fd, "w", encoding="utf-8") as orm_fh:
            orm_fh.write(make_tables_file(self.dsn, self.schema_name, False))
        # Create a separate empty destination database
        self.make_destination_database("dst")

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

    def create_tables(self, config: Mapping[str, Any]) -> None:
        """Create tables in the output DB."""
        assert self.dst_engine is not None
        dm = metadata_to_dict(self.metadata, None, self.dst_engine, None)
        self.dst_metadata = dict_to_metadata(dm, config)
        create_db_tables_into(self.dst_metadata, self.dst_dsn, self.dst_schema_name)

    def create_data(self, config: Mapping[str, Any], num_passes: int = 1) -> None:
        """Create fake data in the DB."""
        # `create-data` with all this stuff
        if self.stats_file_path is None:
            src_stats = None
        else:
            with Path(self.stats_file_path).open(encoding="utf-8") as fh:
                src_stats = yaml.load(fh, yaml.SafeLoader)
        metadata = self.metadata if self.dst_engine is None else self.dst_metadata
        create_db_data_into(
            sorted_non_vocabulary_tables(metadata, config),
            config,
            src_stats,
            num_passes,
            self.dst_dsn,
            self.dst_schema_name,
            metadata,
        )

    def generate_data(
        self, config: Mapping[str, Any], num_passes: int = 1
    ) -> Mapping[str, Any]:
        """
        Replaces the destination DB's data with fresh generated data.
        :return: A Python dictionary representation of the src-stats.yaml file, for what it's worth.
        """
        self.set_configuration(config)
        src_stats = self.get_src_stats(config)
        self.create_tables(config)
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
    def columnize(
        self,
        list: list[str] | None,  # pylint: disable=redefined-builtin
        displaywidth: int = 80,
    ) -> None:
        """Capture the printed table."""
        if list is not None:
            self.column_items.append(list)

    def ask_save(self) -> str:
        """Quitting always works without needing to ask the user."""
        return "yes"

    def get_roles_from_columns_output(self, column: str) -> set[str]:
        """Get the roles in the named column from the ``columns`` command output."""
        name_index = self.headings.index("name")
        our_rows = [r for r in self.rows if r[name_index] == column]
        assert len(our_rows) == 1
        role_index = self.headings.index("roles")
        roles = our_rows[0][role_index].split(", ")
        if roles == [""]:
            return set()
        return set(roles)
