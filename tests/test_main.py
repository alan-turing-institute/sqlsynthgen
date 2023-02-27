"""Tests for the main module."""
from unittest import TestCase
from unittest.mock import patch

import yaml
from click.testing import Result
from typer.testing import CliRunner

from sqlsynthgen.main import app
from tests.examples import example_orm, expected_ssg
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

    def test_make_generators(self) -> None:
        """Test the make-generators sub-command."""
        with patch("sqlsynthgen.main.make_generators_from_tables") as mock_make:
            conf_path = "tests/examples/generator_conf.yaml"
            with open(conf_path, "r", encoding="utf8") as f:
                config = yaml.safe_load(f)
            result = runner.invoke(
                app,
                [
                    "make-generators",
                    "tests/examples/example_orm.py",
                    conf_path,
                ],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_make.assert_called_once_with(example_orm, config)

    def test_create_tables(self) -> None:
        """Test the create-tables sub-command."""

        with patch("sqlsynthgen.main.create_db_tables") as mock_create:
            result = runner.invoke(
                app,
                ["create-tables", "tests/examples/example_orm.py"],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_create.assert_called_once_with(example_orm.metadata)

    def test_create_data(self) -> None:
        """Test the create-data sub-command."""

        with patch("sqlsynthgen.main.create_db_data") as mock_create_db_data:
            result = runner.invoke(
                app,
                [
                    "create-data",
                    "tests/examples/example_orm.py",
                    "tests/examples/expected_ssg.py",
                    "10",
                ],
                catch_exceptions=False,
            )

        self.assertSuccess(result)
        mock_create_db_data.assert_called_once_with(
            example_orm.metadata.sorted_tables, expected_ssg.sorted_generators, 10
        )

    def test_make_tables(self) -> None:
        """Test the make-tables sub-command."""

        with patch("sqlsynthgen.main.make_tables_file") as mock_make_tables_file, patch(
            "sqlsynthgen.main.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value = get_test_settings()

            runner.invoke(
                app,
                [
                    "make-tables",
                ],
                catch_exceptions=False,
            )

            mock_make_tables_file.assert_called_once_with(
                "postgresql://suser:spassword@shost:5432/sdbname", None
            )
