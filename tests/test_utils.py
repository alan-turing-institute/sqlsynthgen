"""Tests for the utils module."""
import os
import sys
from pathlib import Path

from pydantic import PostgresDsn
from pydantic.tools import parse_obj_as
from sqlalchemy import Column, Integer, create_engine, insert
from sqlalchemy.orm import declarative_base

from sqlsynthgen.utils import (
    create_db_engine,
    download_table,
    import_file,
    read_config_file,
)
from tests.utils import RequiresDBTestCase, SSGTestCase, run_psql

# pylint: disable=invalid-name
Base = declarative_base()
# pylint: enable=invalid-name
metadata = Base.metadata


class MyTable(Base):  # type: ignore
    """A SQLAlchemy model."""

    __tablename__ = "mytable"
    id = Column(
        Integer,
        primary_key=True,
    )


class TestImport(SSGTestCase):
    """Tests for the import_file function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    def test_import_file(self) -> None:
        """Test that we can import an example module."""
        old_path = sys.path.copy()
        module = import_file("import_test.py")
        self.assertEqual(10, module.x)

        self.assertEqual(old_path, sys.path)


class TestDownload(RequiresDBTestCase):
    """Tests for the download_table function."""

    mytable_file_path = Path("mytable.yaml")

    test_dir = Path("tests/workspace")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql(Path("tests/examples/providers.dump"))

        self.engine = create_engine(
            "postgresql://postgres:password@localhost:5432/providers",
            connect_args={"connect_timeout": 10},
        )
        metadata.create_all(self.engine)

        os.chdir(self.test_dir)
        self.mytable_file_path.unlink(missing_ok=True)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    def test_download_table(self) -> None:
        """Test the download_table function."""
        # pylint: disable=protected-access

        with self.engine.connect() as conn:
            conn.execute(insert(MyTable).values({"id": 1}))

        download_table(MyTable.__table__, self.engine, "mytable.yaml")

        # The .strip() gets rid of any possible empty lines at the end of the file.
        with Path("../examples/expected.yaml").open(encoding="utf-8") as yamlfile:
            expected = yamlfile.read().strip()

        with self.mytable_file_path.open(encoding="utf-8") as yamlfile:
            actual = yamlfile.read().strip()

        self.assertEqual(expected, actual)


class TestCreateDBEngine(RequiresDBTestCase):
    """Tests for the create_db_engine function."""

    dsn = parse_obj_as(PostgresDsn, "postgresql://postgres:password@localhost")

    def test_connect_sync(self) -> None:
        """Check that we can create a synchronous engine."""
        # All default params
        create_db_engine(self.dsn)

        # With schema
        create_db_engine(self.dsn, schema_name="public")

    def test_connect_async(self) -> None:
        """Check that we can create an asynchronous engine."""
        # All default params
        create_db_engine(self.dsn, use_asyncio=True)

        # With schema
        create_db_engine(self.dsn, schema_name="public", use_asyncio=True)


class TestReadConfig(SSGTestCase):
    """Tests for the read_config_file function."""

    def test_warns_of_invalid_config(self) -> None:
        """Test that we get a warning if the config is invalid."""
        with self.assertLogs(level="WARNING") as log:
            read_config_file("tests/examples/invalid_config.yaml")

        self.assertIn("The config file is invalid:", log.output[0])
