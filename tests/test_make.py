"""Tests for the main module."""
import os
from io import StringIO
from subprocess import CalledProcessError
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import yaml

from sqlsynthgen import make
from sqlsynthgen.make import make_tables_file
from tests.examples import example_orm
from tests.utils import SysExit


class TestMake(TestCase):
    """Tests that don't require a database."""

    def setUp(self) -> None:
        """Pre-test setup."""

        os.chdir("tests/examples")

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir("../..")

    @patch("sqlsynthgen.make.get_settings")
    @patch("sqlsynthgen.make.create_engine")
    @patch("sqlsynthgen.make.download_table")
    def test_make_generators_from_tables(
        self, mock_download: MagicMock, mock_create: MagicMock, _: MagicMock
    ) -> None:
        """Check that we can make a generators file from a tables module."""
        self.maxDiff = None  # pylint: disable=invalid-name
        with open("expected_ssg.py", encoding="utf-8") as expected_output:
            expected = expected_output.read()
        conf_path = "generator_conf.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)

            actual = make.make_generators_from_tables(example_orm, config)
            mock_download.assert_called_once()
            mock_create.assert_called_once()

        self.assertEqual(expected, actual)

    @patch("sqlsynthgen.make.run")
    def test_make_tables_file(self, mock_run: MagicMock) -> None:
        """Test the make_tables_file function."""

        mock_run.return_value.stdout = "some output"

        make_tables_file("my:postgres/db", None)

        self.assertEqual(
            call(
                [
                    "sqlacodegen",
                    "my:postgres/db",
                ],
                capture_output=True,
                encoding="utf-8",
                check=True,
            ),
            mock_run.call_args_list[0],
        )

    @patch("sqlsynthgen.make.run")
    def test_make_tables_file_with_schema(self, mock_run: MagicMock) -> None:
        """Check that the function handles the schema setting."""

        make_tables_file("my:postgres/db", "my_schema")

        self.assertEqual(
            call(
                [
                    "sqlacodegen",
                    "--schema=my_schema",
                    "my:postgres/db",
                ],
                capture_output=True,
                encoding="utf-8",
                check=True,
            ),
            mock_run.call_args_list[0],
        )

    @patch("sys.exit")
    @patch("sqlsynthgen.make.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.make.run")
    def test_make_tables_handles_errors(
        self, mock_run: MagicMock, mock_stderr: MagicMock, mock_exit: MagicMock
    ) -> None:
        """Test the make-tables sub-command handles sqlacodegen errors."""

        mock_run.side_effect = CalledProcessError(
            returncode=99, cmd="some-cmd", stderr="some-error-output"
        )
        mock_exit.side_effect = SysExit

        try:
            make_tables_file("my:postgres/db", None)
        except SysExit:
            pass

        mock_exit.assert_called_once_with(99)
        self.assertEqual("some-error-output\n", mock_stderr.getvalue())

    @patch("sqlsynthgen.make.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.make.run")
    def test_make_tables_warns_no_pk(
        self, mock_run: MagicMock, mock_stderr: MagicMock
    ) -> None:
        """Test the make-tables sub-command warns about Tables()."""

        mock_run.return_value.stdout = "t_nopk_table = Table("
        make_tables_file("my:postgres/db", None)

        self.assertEqual(
            "WARNING: Table without PK detected. sqlsynthgen may not be able to continue.\n",
            mock_stderr.getvalue(),
        )
