"""Tests for the main module."""
from subprocess import CalledProcessError
from unittest import TestCase
from unittest.mock import call, patch

from click.testing import Result
from typer.testing import CliRunner

from sqlsynthgen.main import app
from tests.examples import example_tables, expected_output
from tests.utils import get_test_settings

runner = CliRunner()


class TestCLI(TestCase):
    """Tests for the command-line interface."""

    def assertSuccess(self, result: Result) -> None:
        """Give details and raise if the result isn't good."""
        # pylint: disable=invalid-name
        if result.exit_code != 0:
            print(result.stdout)
            self.assertEqual(0, result.exit_code)

    def test_make_tables(self) -> None:
        """Test the make-tables sub-command."""

        with patch("sqlsynthgen.main.run") as mock_run, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value = get_test_settings()
            mock_run.return_value.returncode = 0

            result = runner.invoke(
                app,
                [
                    "make-tables",
                ],
                catch_exceptions=False,
            )

        self.assertSuccess(result)

        mock_run.assert_has_calls(
            [
                call(
                    [
                        "sqlacodegen",
                        get_test_settings().src_postgres_dsn,
                    ],
                    capture_output=True,
                    encoding="utf-8",
                    check=True,
                ),
            ]
        )
        self.assertNotEqual("", result.stdout)

    def test_make_tables_with_schema(self) -> None:
        """Test the make-tables sub-command handles the schema setting."""

        with patch("sqlsynthgen.main.run") as mock_run, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value = get_test_settings()
            mock_get_settings.return_value.src_schema = "sschema"

            result = runner.invoke(
                app,
                [
                    "make-tables",
                ],
                catch_exceptions=False,
            )

        self.assertSuccess(result)

        mock_run.assert_has_calls(
            [
                call(
                    [
                        "sqlacodegen",
                        "--schema=sschema",
                        get_test_settings().src_postgres_dsn,
                    ],
                    capture_output=True,
                    encoding="utf-8",
                    check=True,
                ),
            ]
        )
        self.assertNotEqual("", result.stdout)

    def test_make_tables_handles_errors(self) -> None:
        """Test the make-tables sub-command handles sqlacodegen errors."""

        with patch("sqlsynthgen.main.run") as mock_run, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings, patch("sqlsynthgen.main.stderr") as mock_stderr:
            mock_run.side_effect = CalledProcessError(
                returncode=99, cmd="some-cmd", stderr="some-error-output"
            )
            mock_get_settings.return_value = get_test_settings()

            result = runner.invoke(
                app,
                [
                    "make-tables",
                ],
                catch_exceptions=False,
            )

        self.assertEqual(99, result.exit_code)
        mock_stderr.assert_has_calls(
            [call.write("some-error-output"), call.write("\n")]
        )

    def test_make_generators(self) -> None:
        """Test the make-generators sub-command."""
        with patch("sqlsynthgen.main.make_generators_from_tables") as mock_make:
            result = runner.invoke(
                app,
                ["make-generators", "tests/examples/example_tables.py"],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_make.assert_called_once_with(example_tables)

    def test_create_tables(self) -> None:
        """Test the create-tables sub-command."""

        with patch("sqlsynthgen.main.create_db_tables") as mock_create:
            result = runner.invoke(
                app,
                ["create-tables", "tests/examples/example_tables.py"],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_create.assert_called_once_with(example_tables.metadata)

    def test_create_data(self) -> None:
        """Test the create-data sub-command."""

        with patch("sqlsynthgen.main.create_db_data") as mock_create_db_data:
            result = runner.invoke(
                app,
                [
                    "create-data",
                    "tests/examples/example_tables.py",
                    "tests/examples/expected_output.py",
                ],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_create_db_data.assert_called_once_with(
            example_tables.metadata.sorted_tables, expected_output.sorted_generators
        )
