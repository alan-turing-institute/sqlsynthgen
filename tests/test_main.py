"""Tests for the main module."""
from io import StringIO
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from click.testing import Result
from typer.testing import CliRunner

from sqlsynthgen.main import app
from tests.utils import get_test_settings

runner = CliRunner(mix_stderr=False)


class TestCLI(TestCase):
    """Tests for the command-line interface."""

    def assertSuccess(self, result: Result) -> None:
        """Give details and raise if the result isn't good."""
        # pylint: disable=invalid-name
        if result.exit_code != 0:
            print(result.stdout)
            self.assertEqual(0, result.exit_code)

    @patch("sqlsynthgen.main.import_file")
    @patch("sqlsynthgen.main.create_db_vocab")
    def test_create_vocab(self, mock_create: MagicMock, mock_import: MagicMock) -> None:
        """Test the create-vocab sub-command."""
        result = runner.invoke(
            app,
            [
                "create-vocab",
            ],
            catch_exceptions=False,
        )

        mock_create.assert_called_once_with(mock_import.return_value.sorted_vocab)
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.import_file")
    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.make_generators_from_tables")
    def test_make_generators(
        self, mock_make: MagicMock, mock_path: MagicMock, mock_import: MagicMock
    ) -> None:
        """Test the make-generators sub-command."""
        mock_path.return_value.exists.return_value = False
        mock_make.return_value = "some text"

        result = runner.invoke(
            app,
            [
                "make-generators",
            ],
            catch_exceptions=False,
        )

        mock_make.assert_called_once_with(mock_import.return_value, {})
        mock_path.return_value.write_text.assert_called_once_with(
            "some text", encoding="utf-8"
        )
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.stderr", new_callable=StringIO)
    def test_make_generators_errors_if_file_exists(
        self, mock_stderr: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test the make-tables sub-command doesn't overwrite."""

        mock_path.return_value.exists.return_value = True

        result = runner.invoke(
            app,
            [
                "make-generators",
            ],
            catch_exceptions=False,
        )
        self.assertEqual(
            "ssg.py should not already exist. Exiting...\n", mock_stderr.getvalue()
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.create_db_tables")
    @patch("sqlsynthgen.main.import_file")
    def test_create_tables(
        self, mock_import: MagicMock, mock_create: MagicMock
    ) -> None:
        """Test the create-tables sub-command."""

        result = runner.invoke(
            app,
            [
                "create-tables",
            ],
            catch_exceptions=False,
        )

        mock_create.assert_called_once_with(mock_import.return_value.Base.metadata)
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.import_file")
    @patch("sqlsynthgen.main.create_db_data")
    def test_create_data(self, mock_create: MagicMock, mock_import: MagicMock) -> None:
        """Test the create-data sub-command."""

        result = runner.invoke(
            app,
            [
                "create-data",
            ],
            catch_exceptions=False,
        )
        self.assertListEqual(
            [
                call("orm.py"),
                call("ssg.py"),
            ],
            mock_import.call_args_list,
        )

        mock_create.assert_called_once_with(
            mock_import.return_value.Base.metadata.sorted_tables,
            mock_import.return_value.sorted_generators,
            1,
        )
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.make_tables_file")
    @patch("sqlsynthgen.main.get_settings")
    def test_make_tables(
        self,
        mock_get_settings: MagicMock,
        mock_make_tables_file: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Test the make-tables sub-command."""

        mock_path.return_value.exists.return_value = False
        mock_get_settings.return_value = get_test_settings()
        mock_make_tables_file.return_value = "some text"

        result = runner.invoke(
            app,
            [
                "make-tables",
            ],
            catch_exceptions=False,
        )

        mock_make_tables_file.assert_called_once_with(
            "postgresql://suser:spassword@shost:5432/sdbname", None
        )
        mock_path.return_value.write_text.assert_called_once_with(
            "some text", encoding="utf-8"
        )
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.main.Path")
    def test_make_tables_errors_if_file_exists(
        self, mock_path: MagicMock, mock_stderr: MagicMock
    ) -> None:
        """Test the make-tables sub-command doesn't overwrite."""

        mock_path.return_value.exists.return_value = True

        result = runner.invoke(
            app,
            [
                "make-tables",
            ],
            catch_exceptions=False,
        )
        self.assertEqual(
            "orm.py should not already exist. Exiting...\n", mock_stderr.getvalue()
        )
        self.assertEqual(1, result.exit_code)
