"""Tests for the main module."""
from unittest import TestCase

from click.testing import Result
from typer.testing import CliRunner

from sqlsynthgen.main import app

runner = CliRunner()


class TestCLI(TestCase):
    """Tests for the command-line interface."""

    def assertSuccess(self, result: Result) -> None:
        """Give details and raise if the result isn't good."""
        # pylint: disable=invalid-name
        if result.exit_code != 0:
            print(result.stdout)
            self.assertEqual(0, result.exit_code)

    def test_make_table_file(self) -> None:
        """Test the make-tables-file sub-command."""
        result = runner.invoke(
            app,
            [
                "make-tables-file",
            ],
        )
        self.assertSuccess(result)

    def test_make_generators_file(self) -> None:
        """Test the make-generators-file sub-command."""
        result = runner.invoke(
            app,
            [
                "make-generators-file",
            ],
        )

        self.assertSuccess(result)

    def test_create_tables(self) -> None:
        """Test the create-tables sub-command."""
        result = runner.invoke(
            app,
            [
                "create-tables",
            ],
        )

        self.assertSuccess(result)

    def test_create_data(self) -> None:
        """Test the create-data sub-command."""
        result = runner.invoke(
            app,
            [
                "create-data",
            ],
        )

        self.assertSuccess(result)
