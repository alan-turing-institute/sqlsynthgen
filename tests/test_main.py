"""Tests for the main module."""
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, call, patch

import yaml
from click.testing import Result
from typer.testing import CliRunner

from sqlsynthgen.main import app
from sqlsynthgen.settings import Settings
from tests.utils import SSGTestCase, get_test_settings

runner = CliRunner(mix_stderr=False)


class TestCLI(SSGTestCase):
    """Tests for the command-line interface."""

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

        mock_create.assert_called_once_with(mock_import.return_value.vocab_dict)
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.get_settings")
    @patch("sqlsynthgen.main.import_file")
    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.make_table_generators")
    def test_make_generators(
        self,
        mock_make: MagicMock,
        mock_path: MagicMock,
        mock_import: MagicMock,
        mock_settings: MagicMock,
    ) -> None:
        """Test the make-generators sub-command."""
        mock_path.return_value.exists.return_value = False
        mock_make.return_value = "some text"
        mock_settings.return_value.src_postges_dsn = ""

        result = runner.invoke(
            app,
            [
                "make-generators",
            ],
            catch_exceptions=False,
        )

        mock_make.assert_called_once_with(
            mock_import.return_value, {}, None, overwrite_files=False
        )
        mock_path.return_value.write_text.assert_called_once_with(
            "some text", encoding="utf-8"
        )
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.echo")
    def test_make_generators_errors_if_file_exists(
        self, mock_echo: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test the make-generators sub-command doesn't overwrite."""

        mock_path.return_value.exists.return_value = True
        mock_path.return_value.__str__.return_value = "ssg.py"

        result = runner.invoke(
            app,
            [
                "make-generators",
            ],
            catch_exceptions=False,
        )
        mock_echo.assert_called_once_with(
            "ssg.py should not already exist. Exiting...", err=True
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.echo")
    def test_make_generators_errors_if_src_dsn_missing(
        self, mock_echo: MagicMock
    ) -> None:
        """Test the make-generators sub-command with missing db params."""
        result = runner.invoke(
            app,
            [
                "make-generators",
            ],
            catch_exceptions=False,
        )
        mock_echo.assert_called_once_with(
            "Missing source database connection details.", err=True
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.get_settings")
    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.import_file")
    @patch("sqlsynthgen.main.make_table_generators")
    def test_make_generators_with_force_enabled(
        self,
        mock_make: MagicMock,
        mock_import: MagicMock,
        mock_path: MagicMock,
        mock_settings: MagicMock,
    ) -> None:
        """Tests the make-generators sub-commands overwrite files when instructed."""

        mock_path.return_value.exists.return_value = True
        mock_make.return_value = "make result"
        mock_settings.return_value.src_postges_dsn = ""

        for force_option in ["--force", "-f"]:
            with self.subTest(f"Using option {force_option}"):
                result: Result = runner.invoke(app, ["make-generators", force_option])

                mock_make.assert_called_once_with(
                    mock_import.return_value, {}, None, overwrite_files=True
                )
                mock_path.return_value.write_text.assert_called_once_with(
                    "make result", encoding="utf-8"
                )
                self.assertSuccess(result)

                mock_make.reset_mock()
                mock_path.reset_mock()

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
            mock_import.return_value.table_generator_dict,
            mock_import.return_value.story_generator_list,
            1,
        )
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.make_tables_file")
    @patch("sqlsynthgen.main.get_settings")
    @patch("sqlsynthgen.main.read_yaml_file")
    def test_make_tables(
        self,
        mock_read_yaml_file: MagicMock,
        mock_get_settings: MagicMock,
        mock_make_tables_file: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Test the make-tables sub-command."""

        mock_config = MagicMock()
        mock_path.return_value.exists.return_value = False
        mock_get_settings.return_value = get_test_settings()
        mock_make_tables_file.return_value = "some text"
        mock_read_yaml_file.return_value = mock_config

        result = runner.invoke(
            app,
            [
                "make-tables",
                "--config-file=config.yaml",
            ],
            catch_exceptions=False,
        )

        mock_make_tables_file.assert_called_once_with(
            "postgresql://suser:spassword@shost:5432/sdbname", None, mock_config
        )
        mock_path.return_value.write_text.assert_called_once_with(
            "some text", encoding="utf-8"
        )
        self.assertSuccess(result)

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.echo")
    def test_make_tables_errors_if_file_exists(
        self, mock_echo: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test the make-tables sub-command doesn't overwrite."""

        mock_path.return_value.exists.return_value = True
        mock_path.return_value.__str__.return_value = "orm.py"

        result = runner.invoke(
            app,
            [
                "make-tables",
            ],
            catch_exceptions=False,
        )
        mock_echo.assert_called_once_with(
            "orm.py should not already exist. Exiting...", err=True
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.echo")
    def test_make_tables_errors_if_src_dsn_missing(self, mock_echo: MagicMock) -> None:
        """Test the make-tables sub-command doesn't overwrite."""

        result = runner.invoke(
            app,
            [
                "make-tables",
            ],
            catch_exceptions=False,
        )
        mock_echo.assert_called_once_with(
            "Missing source database connection details.", err=True
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.make_tables_file")
    @patch("sqlsynthgen.main.get_settings")
    @patch("sqlsynthgen.main.Path")
    def test_make_tables_with_force_enabled(
        self,
        mock_path: MagicMock,
        mock_get_settings: MagicMock,
        mock_make_tables: MagicMock,
    ) -> None:
        """Test the make-table sub-command, when the force option is activated."""

        mock_path.return_value.exists.return_value = True

        test_settings: Settings = get_test_settings()
        mock_tables_output: str = "make_tables_file output"

        mock_get_settings.return_value = test_settings
        mock_make_tables.return_value = mock_tables_output

        for force_option in ["--force", "-f"]:
            with self.subTest(f"Using option {force_option}"):
                result: Result = runner.invoke(app, ["make-tables", force_option])

                mock_make_tables.assert_called_once_with(
                    test_settings.src_dsn, test_settings.src_schema, {}
                )
                mock_path.return_value.write_text.assert_called_once_with(
                    mock_tables_output, encoding="utf-8"
                )
                self.assertSuccess(result)

                mock_make_tables.reset_mock()
                mock_path.reset_mock()

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.make_src_stats")
    @patch("sqlsynthgen.main.get_settings")
    def test_make_stats(
        self, mock_get_settings: MagicMock, mock_make: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test the make-stats sub-command."""
        example_conf_path = "tests/examples/example_config.yaml"
        output_path = Path("make_stats_output.yaml")
        mock_path.return_value.exists.return_value = False
        mock_make.return_value = {"a": 1}
        mock_get_settings.return_value = get_test_settings()
        result = runner.invoke(
            app,
            [
                "make-stats",
                f"--stats-file={output_path}",
                f"--config-file={example_conf_path}",
            ],
            catch_exceptions=False,
        )
        self.assertSuccess(result)
        with open(example_conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)
        mock_make.assert_called_once_with(get_test_settings().src_dsn, config, None)
        mock_path.return_value.write_text.assert_called_once_with(
            "a: 1\n", encoding="utf-8"
        )

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.echo")
    def test_make_stats_errors_if_file_exists(
        self, mock_echo: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test the make-stats sub-command when the stats file already exists."""
        mock_path.return_value.exists.return_value = True
        example_conf_path = "tests/examples/example_config.yaml"
        output_path = "make_stats_output.yaml"
        mock_path.return_value.__str__.return_value = output_path

        result = runner.invoke(
            app,
            [
                "make-stats",
                f"--stats-file={output_path}",
                f"--config-file={example_conf_path}",
            ],
            catch_exceptions=False,
        )
        mock_echo.assert_called_once_with(
            f"{output_path} should not already exist. Exiting...", err=True
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.echo")
    def test_make_stats_errors_if_no_src_dsn(
        self,
        mock_echo: MagicMock,
    ) -> None:
        """Test the make-stats sub-command with missing settings."""
        example_conf_path = "tests/examples/example_config.yaml"

        result = runner.invoke(
            app,
            [
                "make-stats",
                f"--config-file={example_conf_path}",
            ],
            catch_exceptions=False,
        )
        mock_echo.assert_called_once_with(
            "Missing source database connection details.", err=True
        )
        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.Path")
    @patch("sqlsynthgen.main.make_src_stats")
    @patch("sqlsynthgen.main.get_settings")
    def test_make_stats_with_force_enabled(
        self, mock_get_settings: MagicMock, mock_make: MagicMock, mock_path: MagicMock
    ) -> None:
        """Tests that the make-stats command overwrite files when instructed."""
        test_config_file: str = "tests/examples/example_config.yaml"
        with open(test_config_file, "r", encoding="utf8") as f:
            config_file_content: Dict = yaml.safe_load(f)

        mock_path.return_value.exists.return_value = True
        test_settings: Settings = get_test_settings()
        mock_get_settings.return_value = test_settings
        make_test_output: Dict = {"some_stat": 0}
        mock_make.return_value = make_test_output

        for force_option in ["--force", "-f"]:
            with self.subTest(f"Using option {force_option}"):
                result: Result = runner.invoke(
                    app,
                    [
                        "make-stats",
                        "--stats-file=stats_file.yaml",
                        f"--config-file={test_config_file}",
                        force_option,
                    ],
                )

                mock_make.assert_called_once_with(
                    test_settings.src_dsn, config_file_content, None
                )
                mock_path.return_value.write_text.assert_called_once_with(
                    "some_stat: 0\n", encoding="utf-8"
                )
                self.assertSuccess(result)

                mock_make.reset_mock()
                mock_path.reset_mock()

    def test_validate_config(self) -> None:
        """Test the validate-config sub-command."""
        result = runner.invoke(
            app,
            ["validate-config", "tests/examples/example_config.yaml"],
            catch_exceptions=False,
        )

        self.assertSuccess(result)

    def test_validate_config_invalid(self) -> None:
        """Test the validate-config sub-command."""
        result = runner.invoke(
            app,
            ["validate-config", "tests/examples/invalid_config.yaml"],
            catch_exceptions=False,
        )

        self.assertEqual(1, result.exit_code)

    @patch("sqlsynthgen.main.remove_db_data")
    @patch("sqlsynthgen.main.import_file", side_effect=(1, 2))
    def test_remove_data(self, _: MagicMock, mock_remove: MagicMock) -> None:
        """Test the remove-data command."""
        result = runner.invoke(
            app,
            ["remove-data", "--yes"],
            catch_exceptions=False,
        )
        self.assertEqual(0, result.exit_code)
        mock_remove.assert_called_once_with(1, 2)

    @patch("sqlsynthgen.main.remove_db_vocab")
    @patch("sqlsynthgen.main.import_file", side_effect=(1, 2))
    def test_remove_vocab(self, _: MagicMock, mock_remove: MagicMock) -> None:
        """Test the remove-vocab command."""
        result = runner.invoke(
            app,
            ["remove-vocab", "--yes"],
            catch_exceptions=False,
        )
        self.assertEqual(0, result.exit_code)
        mock_remove.assert_called_once_with(1, 2)

    @patch("sqlsynthgen.main.remove_db_tables")
    @patch("sqlsynthgen.main.import_file", side_effect=(1,))
    def test_remove_tables(self, _: MagicMock, mock_remove: MagicMock) -> None:
        """Test the remove-tables command."""
        result = runner.invoke(
            app,
            ["remove-tables", "--yes"],
            catch_exceptions=False,
        )
        self.assertEqual(0, result.exit_code)
        mock_remove.assert_called_once_with(1)
