"""Tests for the main module."""
from unittest import TestCase
from unittest.mock import call, patch

from click.testing import Result
from typer.testing import CliRunner

from sqlsynthgen.main import app
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

    def test_make_generators(self) -> None:
        """Test the make-generators sub-command."""
        with patch("sqlsynthgen.main.make_generators_from_tables") as mock_make:
            result = runner.invoke(
                app,
                ["make-generators", "directory/orm_file.py"],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_make.assert_called_once_with("directory.orm_file")

    def test_create_tables(self) -> None:
        """Test the create-tables sub-command."""
        # pylint: disable=import-outside-toplevel
        from tests.examples.example_tables import metadata

        with patch("sqlsynthgen.main.create_db_tables") as mock_create:
            result = runner.invoke(
                app,
                ["create-tables", "tests/examples/example_tables.py"],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_create.assert_called_once_with(metadata)

    def test_create_data(self) -> None:
        """Test the create-data sub-command."""
        result = runner.invoke(
            app,
            [
                "create-data",
            ],
            catch_exceptions=False,
        )

        self.assertSuccess(result)
