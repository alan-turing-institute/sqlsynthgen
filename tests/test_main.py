"""Tests for the main module."""
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import yaml
from click.testing import Result
from typer.testing import CliRunner

from datafaker.main import app
from datafaker.settings import Settings, SettingsError
from tests.utils import (
    DatafakerTestCase,
    DuckTestDb,
    GeneratesDBTestCase,
    MsSqlTestDb,
    get_test_settings,
)

runner = CliRunner(mix_stderr=False)


class TestCLI(DatafakerTestCase):
    """Tests for the command-line interface."""

    @patch("datafaker.main.create_db_tables")
    @patch("datafaker.main.read_config_file")
    @patch("datafaker.main.load_metadata_for_output")
    def test_create_tables(
        self, mock_load_meta: MagicMock, _mock_config: MagicMock, mock_create: MagicMock
    ) -> None:
        """Test the create-tables sub-command."""

        result = runner.invoke(
            app,
            [
                "create-tables",
            ],
            catch_exceptions=False,
        )

        mock_create.assert_called_once_with(mock_load_meta.return_value)
        self.assertSuccess(result)

    @patch("datafaker.main.sorted_non_vocabulary_tables")
    @patch("datafaker.main.logger")
    @patch("datafaker.main.create_db_data")
    @patch("datafaker.main.load_metadata_for_output")
    @patch("datafaker.main.read_config_file")
    # pylint: disable=too-many-arguments too-many-positional-arguments
    def test_create_data(
        self,
        mock_read_config: MagicMock,
        mock_load_metadata: MagicMock,
        mock_create: MagicMock,
        mock_logger: MagicMock,
        mock_tables: MagicMock,
    ) -> None:
        """Test the create-data sub-command."""

        mock_create.return_value = {"a": 1}
        result = runner.invoke(
            app,
            [
                "create-data",
                "--config-file=tests/examples/example_config.yaml",
                "--orm-file=tests/examples/example_orm.yaml",
            ],
            catch_exceptions=False,
        )

        mock_create.assert_called_once_with(
            mock_tables.return_value,
            mock_read_config.return_value,
            None,
            1,
            mock_load_metadata.return_value,
        )
        self.assertSuccess(result)

        mock_logger.debug.assert_has_calls(
            [
                call("Creating data."),
                call("Data created in %s %s.", 1, "pass"),
                call("%s: %s %s created.", "a", 1, "row"),
            ]
        )

    @patch("datafaker.main.Path")
    @patch("datafaker.main.make_tables_file")
    @patch("datafaker.settings.get_settings")
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
            "postgresql://suser:spassword@shost:5432/sdbname",
            None,
            views_to_tables=False,
            parquet_dir=None,
        )
        mock_path.return_value.write_text.assert_called_once_with(
            "some text", encoding="utf-8"
        )
        self.assertSuccess(result)

    @patch("datafaker.main.Path")
    @patch("datafaker.main.logger")
    def test_make_tables_errors_if_file_exists(
        self, mock_logger: MagicMock, mock_path: MagicMock
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
        mock_logger.error.assert_called_once_with(
            "%s should not already exist. Exiting...", mock_path.return_value
        )
        self.assertEqual(1, result.exit_code)

    @patch.dict(os.environ, {"SRC_SCHEMA": "myschema"}, clear=True)
    def test_make_tables_errors_if_src_dsn_missing(self) -> None:
        """Test the make-tables sub-command refuses to work if SRC_DSN is not set."""
        self.assertFalse(
            (self.get_abs_example_dir() / "does-not-exist.yaml").exists(),
            "Precondition failed: tests/examples/does-not-exist.yaml must not exist",
        )
        self.assertRaises(
            SettingsError,
            runner.invoke,
            app,
            [
                "make-tables",
                "--orm-file=tests/examples/does-not-exist.yaml",
            ],
            catch_exceptions=False,
        )

    @patch("datafaker.main.make_tables_file")
    @patch("datafaker.main.Path")
    @patch("datafaker.settings.get_settings")
    def test_make_tables_with_force_enabled(
        self,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
        mock_make_tables: MagicMock,
    ) -> None:
        """Test the make-tables sub-command, when the force option is activated."""
        mock_get_settings.return_value = get_test_settings()
        mock_path.return_value.exists.return_value = True
        mock_tables_output: str = "make_tables_file output"

        mock_make_tables.return_value = mock_tables_output

        for force_option in ["--force", "-f"]:
            with self.subTest(f"Using option {force_option}"):
                mock_make_tables.reset_mock()
                mock_path.reset_mock()
                result: Result = runner.invoke(
                    app,
                    [
                        "make-tables",
                        force_option,
                        "--orm-file=tests/examples/example_orm.yaml",
                    ],
                )

                mock_make_tables.assert_called_once_with(
                    mock_get_settings.return_value.src_dsn,
                    mock_get_settings.return_value.src_schema,
                    views_to_tables=False,
                    parquet_dir=None,
                )
                mock_path.return_value.write_text.assert_called_once_with(
                    mock_tables_output, encoding="utf-8"
                )
                self.assertSuccess(result)

    @patch("datafaker.main.Path")
    @patch("datafaker.settings.get_settings")
    def test_incorrect_dialect_causes_nice_error_message(
        self,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Test the make-tables sub-command, when the force option is activated."""
        mock_get_settings.return_value = get_test_settings(
            # postgres: not postgresql: will cause sqlalchemy to fail to connect
            src_dsn="postgres://suser:spassword@shost:5432/sdbname",
            dst_dsn="postgresql://duser:dpassword@dhost:5432/ddbname",
        )
        mock_path.return_value.exists.return_value = True

        result = runner.invoke(
            app,
            [
                "make-tables",
                "--force",
                "--orm-file=tests/examples/example_orm.yaml",
            ],
        )
        self.assertIs(type(result.exception), SystemExit)

    @patch("datafaker.main.Path")
    @patch("datafaker.settings.get_settings")
    def test_invalid_host_causes_nice_error_message(
        self,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Test the make-tables sub-command, when the force option is activated."""
        mock_get_settings.return_value = get_test_settings(
            # postgres: not postgresql: will cause sqlalchemy to fail to connect
            src_dsn="postgresql://suser:spassword@invalid_host:5432/sdbname",
            dst_dsn="postgresql://duser:dpassword@dhost:5432/ddbname",
        )
        mock_path.return_value.exists.return_value = True

        result = runner.invoke(
            app,
            [
                "make-tables",
                "--force",
                "--orm-file=tests/examples/example_orm.yaml",
            ],
        )
        self.assertIs(type(result.exception), SystemExit)

    @patch("datafaker.main.Path")
    @patch("datafaker.settings.get_settings")
    def test_incorrect_dsn_causes_nice_error_message(
        self,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Test the make-tables sub-command, when the force option is activated."""
        mock_get_settings.return_value = get_test_settings(
            # postgres: not postgresql: will cause sqlalchemy to fail to connect
            src_dsn="postgresql://suser:spassword:localhost:5432/sdbname",
            dst_dsn="postgresql://duser:dpassword@dhost:5432/ddbname",
        )
        mock_path.return_value.exists.return_value = True

        result = runner.invoke(
            app,
            [
                "make-tables",
                "--force",
                "--orm-file=tests/examples/example_orm.yaml",
            ],
        )
        self.assertIs(type(result.exception), SystemExit)

    def test_validate_config(self) -> None:
        """Test the validate-config sub-command."""
        result = runner.invoke(
            app,
            [
                "validate-config",
                str(self.get_abs_example_dir() / "example_config.yaml"),
            ],
            catch_exceptions=False,
        )

        self.assertSuccess(result)

    def test_validate_config_invalid(self) -> None:
        """Test the validate-config sub-command."""
        result = runner.invoke(
            app,
            [
                "validate-config",
                str(self.get_abs_example_dir() / "invalid_config.yaml"),
            ],
            catch_exceptions=False,
        )

        self.assertEqual(1, result.exit_code)

    @patch("datafaker.main.remove_db_data")
    @patch("datafaker.main.read_config_file")
    @patch("datafaker.main.load_metadata_for_output")
    def test_remove_data(
        self,
        mock_meta: MagicMock,
        mock_config: MagicMock,
        mock_remove: MagicMock,
    ) -> None:
        """Test the remove-data command."""
        result = runner.invoke(
            app,
            ["remove-data", "--yes"],
            catch_exceptions=False,
        )
        self.assertEqual(0, result.exit_code)
        mock_remove.assert_called_once_with(
            mock_meta.return_value, mock_config.return_value
        )

    @patch("datafaker.main.read_config_file")
    @patch("datafaker.main.remove_db_vocab")
    @patch("datafaker.main.load_metadata_config")
    @patch("datafaker.main.dict_to_metadata")
    def test_remove_vocab(
        self,
        mock_d2m: MagicMock,
        mock_load_metadata: MagicMock,
        mock_remove: MagicMock,
        mock_read_config: MagicMock,
    ) -> None:
        """Test the remove-vocab command."""
        result = runner.invoke(
            app,
            ["remove-vocab", "--yes"],
            catch_exceptions=False,
        )
        self.assertEqual(0, result.exit_code)
        mock_read_config.assert_called_once_with(Path("config.yaml"))
        mock_remove.assert_called_once_with(
            mock_d2m.return_value,
            mock_load_metadata.return_value,
            mock_read_config.return_value,
        )

    @patch("datafaker.main.remove_db_tables")
    @patch("datafaker.main.load_metadata_for_output")
    @patch("datafaker.main.read_config_file")
    def test_remove_tables(
        self, _: MagicMock, mock_meta: MagicMock, mock_remove: MagicMock
    ) -> None:
        """Test the remove-tables command."""
        result = runner.invoke(
            app,
            ["remove-tables", "--yes"],
            catch_exceptions=False,
        )
        self.assertEqual(0, result.exit_code)
        mock_remove.assert_called_once_with(mock_meta.return_value)


class TestCliOutput(DatafakerTestCase):
    """Test CLI commands that output files."""

    use_temporary_cwd = True
    example_conf = "example_config.yaml"
    copy_files = [example_conf, "orm.yaml"]
    copy_from_directory = Path("examples")

    def load_yaml(self, file_name: str | Path) -> Any:
        """Load the YAML and return it as a dict."""
        with open(file_name, "r", encoding="utf-8") as fh:
            return yaml.load(fh, yaml.SafeLoader)

    @patch("datafaker.main.read_config_file")
    @patch("datafaker.main.dict_to_metadata")
    @patch("datafaker.main.load_metadata_config")
    @patch("datafaker.main.create_db_vocab")
    def test_create_vocab(
        self,
        mock_create: MagicMock,
        mock_mdict: MagicMock,
        mock_meta: MagicMock,
        mock_config: MagicMock,
    ) -> None:
        """Test the create-vocab sub-command."""
        result = runner.invoke(
            app,
            [
                "create-vocab",
            ],
            catch_exceptions=False,
        )

        mock_create.assert_called_once_with(
            mock_meta.return_value, mock_mdict.return_value, mock_config.return_value
        )
        self.assertSuccess(result)

    @patch("datafaker.main.make_src_stats")
    @patch("datafaker.settings.get_settings")
    def test_make_stats(
        self,
        mock_get_settings: MagicMock,
        mock_make: MagicMock,
    ) -> None:
        """Test the make-stats sub-command."""
        output_path = Path("make_stats_output.yaml")
        mock_make.return_value = {"a": 1}
        mock_get_settings.return_value = get_test_settings()
        result = runner.invoke(
            app,
            [
                "make-stats",
                f"--stats-file={output_path}",
                f"--config-file={self.example_conf}",
            ],
            catch_exceptions=False,
        )
        self.assertSuccess(result)
        config = self.load_yaml(self.example_conf)
        mock_make.assert_called_once_with(
            get_test_settings().src_dsn, config, None, parquet_dir=None
        )
        output = self.load_yaml(output_path)
        self.assertDictEqual(output, {"a": 1})

    @patch("datafaker.main.logger")
    def test_make_stats_errors_if_file_exists(
        self,
        mock_logger: MagicMock,
    ) -> None:
        """Test the make-stats sub-command when the stats file already exists."""
        output_path = "make_stats_output.yaml"
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("some content\n")

        result = runner.invoke(
            app,
            [
                "make-stats",
                f"--stats-file={output_path}",
                f"--config-file={self.example_conf}",
            ],
            catch_exceptions=False,
        )
        mock_logger.error.assert_called_once_with(
            "%s should not already exist. Exiting...", Path(output_path)
        )
        self.assertEqual(1, result.exit_code)

    @patch.dict(os.environ, {"SRC_SCHEMA": "myschema"}, clear=True)
    def test_make_stats_errors_if_no_src_dsn(self) -> None:
        """Test the make-stats sub-command with missing settings."""

        self.assertRaises(
            SettingsError,
            runner.invoke,
            app,
            [
                "make-stats",
                f"--config-file={self.example_conf}",
                "--stats-file=does-not-exist.yaml",
            ],
            catch_exceptions=False,
        )

    @patch("datafaker.main.make_src_stats")
    @patch("datafaker.settings.get_settings")
    def test_make_stats_with_force_enabled(
        self,
        mock_get_settings: MagicMock,
        mock_make_src_stats: MagicMock,
    ) -> None:
        """Tests that the make-stats command overwrite files when instructed."""
        with open(self.example_conf, "r", encoding="utf8") as f:
            config_file_content: dict = yaml.safe_load(f)

        test_settings: Settings = get_test_settings()
        mock_get_settings.return_value = test_settings
        make_test_output: dict = {"some_stat": 0}
        mock_make_src_stats.return_value = make_test_output

        for force_option in ["--force", "-f"]:
            with self.subTest(f"Using option {force_option}"):
                result: Result = runner.invoke(
                    app,
                    [
                        "make-stats",
                        "--stats-file=stats_file.yaml",
                        f"--config-file={self.example_conf}",
                        force_option,
                    ],
                )

                mock_make_src_stats.assert_called_once_with(
                    test_settings.src_dsn,
                    config_file_content,
                    test_settings.src_schema,
                    parquet_dir=None,
                )
                mock_make_src_stats.reset_mock()
                self.assertDictEqual(
                    self.load_yaml("stats_file.yaml"), {"some_stat": 0}
                )
                self.assertSuccess(result)


class TestsCliCreate(GeneratesDBTestCase):
    """Tests that use the CLI to generate output in the destination database."""

    use_temporary_cwd = True
    dst_schema_name = "fake.dstschema"

    def setUp(self) -> None:
        """Set the runner with the environment variables we need."""
        super().setUp()
        self.runner = CliRunner(
            mix_stderr=False,
            env={
                "SRC_DSN": self.dsn,
                "SRC_SCHEMA": None,
                "DST_DSN": self.dst_dsn,
                "DST_SCHEMA": None,
            },
        )

    def test_create_primary_key(self) -> None:
        """Test the creation of a simple database with one primary key column."""
        orm = {
            "tables": {
                "tab": {
                    "columns": {
                        "col": {
                            "primary": True,
                            # For some reason, nullable primary keys triggers a
                            # weird corner case on DuckDB
                            "nullable": True,
                            "type": "INTEGER",
                        }
                    }
                }
            }
        }
        config: dict[str, Any] = {}
        with Path("orm.yaml").open("w", encoding="utf-8") as fh:
            fh.write(yaml.dump(orm))
        with Path("config.yaml").open("w", encoding="utf-8") as fh:
            fh.write(yaml.dump(config))
        result = self.runner.invoke(
            app,
            [
                "create-tables",
            ],
            catch_exceptions=False,
        )
        self.assertSuccess(result)


class TestCliCreateDuckDb(TestsCliCreate):
    """Tests that use the CLI to generate output in a DuckDB database."""

    database_type = DuckTestDb


class TestCliCreateMsSql(TestsCliCreate):
    """Tests that use the CLI to generate output in an MS Sql database."""

    database_type = MsSqlTestDb
