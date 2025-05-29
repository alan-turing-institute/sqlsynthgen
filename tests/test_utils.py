"""Tests for the utils module."""
import os
import sys
from pathlib import Path
from unittest.mock import patch

from pydantic import PostgresDsn
from pydantic.tools import parse_obj_as
from sqlalchemy import Column, Integer, create_engine, insert
from sqlalchemy.orm import declarative_base

from datafaker.utils import (
    create_db_engine,
    download_table,
    import_file,
    read_config_file,
)
from tests.utils import RequiresDBTestCase, DatafakerTestCase

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


class TestImport(DatafakerTestCase):
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

    dump_file_path = "providers.dump"
    mytable_file_path = Path("mytable.yaml")

    test_dir = Path("tests/workspace")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()

        metadata.create_all(self.engine)

        os.chdir(self.test_dir)
        self.mytable_file_path.unlink(missing_ok=True)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)
        super().tearDown()

    def test_download_table(self) -> None:
        """Test the download_table function."""
        # pylint: disable=protected-access

        with self.engine.connect() as conn:
            conn.execute(insert(MyTable).values({"id": 1}))
            conn.commit()

        download_table(MyTable.__table__, self.engine, self.mytable_file_path, compress=False)

        # The .strip() gets rid of any possible empty lines at the end of the file.
        with Path("../examples/expected.yaml").open(encoding="utf-8") as yamlfile:
            expected = yamlfile.read().strip()

        with self.mytable_file_path.open(encoding="utf-8") as yamlfile:
            actual = yamlfile.read().strip()

        self.assertEqual(expected, actual)


class TestReadConfig(DatafakerTestCase):
    """Tests for the read_config_file function."""

    def test_warns_of_invalid_config(self) -> None:
        """Test that we get a warning if the config is invalid."""
        with patch("datafaker.utils.logger") as mock_logger:
            read_config_file("tests/examples/invalid_config.yaml")
            mock_logger.error.assert_called_with(
                "The config file is invalid: %s", "'a' is not of type 'integer'"
            )
