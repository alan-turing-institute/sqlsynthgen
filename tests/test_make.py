"""Tests for the main module."""
import os
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from sqlsynthgen.make import (
    make_generators_from_tables,
    make_src_stats,
    make_tables_file,
)
from tests.examples import example_orm
from tests.utils import RequiresDBTestCase, SSGTestCase


class TestMakeGenerators(SSGTestCase):
    """Test the make_generators_from_tables function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    @patch("sqlsynthgen.make.get_settings")
    @patch("sqlsynthgen.make.create_engine")
    @patch("sqlsynthgen.make.download_table")
    def test_make_generators_from_tables(
        self, mock_download: MagicMock, mock_create: MagicMock, _: MagicMock
    ) -> None:
        """Check that we can make a generators file from a tables module."""
        with open("expected_ssg.py", encoding="utf-8") as expected_output:
            expected = expected_output.read()
        conf_path = "example_config.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)
        stats_path = "example_stats.yaml"

        actual = make_generators_from_tables(example_orm, config, stats_path)
        mock_download.assert_called_once()
        mock_create.assert_called_once()

        self.assertEqual(expected, actual)


class TestMakeTables(SSGTestCase):
    """Test the make_tables function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    @patch("sqlsynthgen.make.MetaData")
    @patch("sqlsynthgen.make.entry_points")
    def test_make_tables_file(self, mock_entry: MagicMock, _: MagicMock) -> None:
        """Test the make_tables_file function."""
        mock_ep = MagicMock()
        mock_ep.name = "declarative"
        mock_ep.load.return_value.return_value.generate.return_value = (
            "some generated code"
        )
        mock_entry.return_value = [mock_ep]

        self.assertEqual(
            "some generated code",
            make_tables_file("postgresql://postgres@1.2.3.4/db", None),
        )

    @patch("sqlsynthgen.make.MetaData")
    @patch("sqlsynthgen.make.entry_points")
    def test_make_tables_file_with_schema(
        self, mock_entry: MagicMock, _: MagicMock
    ) -> None:
        """Check that the function handles the schema setting."""

        mock_ep = MagicMock()
        mock_ep.name = "declarative"
        mock_ep.load.return_value.return_value.generate.return_value = (
            "some generated code"
        )
        mock_entry.return_value = [mock_ep]

        self.assertEqual(
            "some generated code",
            make_tables_file("postgresql://postgres@1.2.3.4/db", "myschema"),
        )

    @patch("sqlsynthgen.make.MetaData")
    @patch("sqlsynthgen.make.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.make.entry_points")
    def test_make_tables_warns_no_pk(
        self, mock_entry: MagicMock, mock_stderr: MagicMock, _: MagicMock
    ) -> None:
        """Test the make-tables sub-command warns about Tables()."""

        mock_ep = MagicMock()
        mock_ep.name = "declarative"
        mock_entry.return_value = [mock_ep]

        mock_ep.load.return_value.return_value.generate.return_value = (
            "t_nopk_table = Table("
        )

        make_tables_file("postgresql://postgres@127.0.0.1:5432", None)

        self.assertEqual(
            "WARNING: Table without PK detected. sqlsynthgen may not be able to continue.\n",
            mock_stderr.getvalue(),
        )


class TestMakeStats(RequiresDBTestCase):
    """Test the make_src_stats function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    def test_make_stats(self) -> None:
        """Test the make_src_stats function."""
        connection_string = "postgresql://postgres:password@localhost:5432/src"
        conf_path = Path("example_config.yaml")
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)

        # Check that make_src_stats works with, or without, a schema
        for args in (
            (connection_string, config),
            (connection_string, config, "public"),
        ):

            src_stats = make_src_stats(*args)

            self.assertSetEqual({"count_opt_outs"}, set(src_stats.keys()))
            count_opt_outs = src_stats["count_opt_outs"]
            self.assertEqual(len(count_opt_outs), 2)
            self.assertIsInstance(count_opt_outs[0][0], int)
            self.assertIs(count_opt_outs[0][1], False)
            self.assertIsInstance(count_opt_outs[1][0], int)
            self.assertIs(count_opt_outs[1][1], True)
