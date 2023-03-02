"""Tests for the utils module."""
import os
import sys
from io import StringIO
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from sqlalchemy import Column, Integer, create_engine, insert
from sqlalchemy.orm import declarative_base

from sqlsynthgen.utils import download_table, import_file
from tests.utils import RequiresDBTestCase, SysExit, run_psql

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


class TestImport(TestCase):
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

    mytable_file_path = Path("mytable.csv")

    test_dir = Path("tests/workspace")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""

        run_psql("providers.dump")

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

        download_table(MyTable.__table__, self.engine)

        with Path("../examples/expected.csv").open(encoding="utf-8") as csvfile:
            expected = csvfile.read()

        with self.mytable_file_path.open(encoding="utf-8") as csvfile:
            actual = csvfile.read()

        self.assertEqual(expected, actual)

    @patch("sys.exit")
    @patch("sqlsynthgen.utils.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.utils.Path")
    def test_download_table_does_not_overwrite(
        self, mock_path: MagicMock, mock_stderr: MagicMock, mock_exit: MagicMock
    ) -> None:
        """Test the download_table function."""
        # pylint: disable=protected-access

        mock_exit.side_effect = SysExit
        mock_path.return_value.exists.return_value = True

        try:
            download_table(MyTable.__table__, None)
        except SysExit:
            pass

        self.assertEqual(
            "mytable.csv already exists. Exiting...\n", mock_stderr.getvalue()
        )
        mock_exit.assert_called_once_with(1)
