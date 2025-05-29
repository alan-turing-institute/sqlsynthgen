"""Tests for the main module."""
import asyncio
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from pydantic import PostgresDsn
from pydantic.tools import parse_obj_as
from sqlalchemy import BigInteger, Column, String
from sqlalchemy.dialects.mysql.types import INTEGER
from sqlalchemy.dialects.postgresql import UUID

from datafaker.make import (
    _get_provider_for_column,
    make_src_stats,
    make_table_generators,
    make_tables_file,
)
from tests.examples import example_orm
from tests.utils import RequiresDBTestCase, DatafakerTestCase, get_test_settings


class TestMakeGenerators(DatafakerTestCase):
    """Test the make_table_generators function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    @patch("datafaker.make.Path")
    @patch("datafaker.make.get_settings")
    @patch("datafaker.utils.create_engine")
    @patch("datafaker.make.download_table")
    def test_make_table_generators(
        self,
        mock_download: MagicMock,
        mock_create: MagicMock,
        #... and orm_file_name and config_file
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Check that we can make a generators file from a tables module."""

        mock_path.return_value.exists.return_value = False

        mock_get_settings.return_value = get_test_settings()
        with open("expected_datafaker.py", encoding="utf-8") as expected_output:
            expected = expected_output.read()
        conf_path = "example_config.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)
        stats_path = "example_stats.yaml"

        actual = make_table_generators(example_orm, config, stats_path) #... and orm_file_name and config_file_name
        # 5 because there are 5 vocabulary tables in the example orm.
        self.assertEqual(mock_path.call_count, 5)
        self.assertEqual(mock_download.call_count, 5)
        mock_create.assert_called_once()
        self.assertEqual(expected, actual)

    @patch("datafaker.make.logger")
    @patch("datafaker.make.Path")
    @patch("datafaker.make.get_settings")
    @patch("datafaker.utils.create_engine")
    def test_create_generators_do_not_overwrite(
        self,
        mock_create: MagicMock,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Tests that the making generators do not overwrite files."""
        mock_path.return_value.exists.return_value = True
        mock_get_settings.return_value = get_test_settings()
        configuration_file: str = "example_config.yaml"
        with open(configuration_file, "r", encoding="utf8") as f:
            configuration: dict = yaml.safe_load(f)
        stats_path = "example_stats.yaml"

        try:
            make_table_generators(example_orm, configuration, stats_path) #... and orm_file_name and config_file_name
        except SystemExit:
            pass

        mock_create.assert_called_once()
        mock_logger.error.assert_called_with(
            "%s already exists. Exiting...", "empty_vocabulary.yaml"
        )

    @patch("datafaker.make.download_table")
    @patch("datafaker.utils.create_engine")
    @patch("datafaker.make.get_settings")
    @patch("datafaker.make.Path")
    def test_create_generators_force_overwrite(
        self,
        mock_path: MagicMock,
        mock_get_settings: MagicMock,
        mock_create: MagicMock,
        mock_download: MagicMock,
    ) -> None:
        """Tests that making generators overwrite files, when instructed."""
        mock_path.return_value.exists.return_value = True

        mock_get_settings.return_value = get_test_settings()
        with open("expected_datafaker.py", encoding="utf-8") as expected_output:
            expected: str = expected_output.read()
        conf_path = "example_config.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config: dict = yaml.safe_load(f)
        stats_path: str = "example_stats.yaml"

        actual: str = make_table_generators(
            example_orm, config, stats_path, overwrite_files=True #... and orm_file_name and config_file_name
        )

        mock_create.assert_called_once()
        self.assertEqual(mock_download.call_count, 5)

        self.assertEqual(expected, actual)

    def test_get_provider_for_column(self) -> None:
        """Test the _get_provider_for_column function."""

        # Simple case
        (
            variable_name,
            generator_function,
            generator_arguments,
        ) = _get_provider_for_column(Column("myint", BigInteger))
        self.assertListEqual(
            variable_name,
            ["myint"],
        )
        self.assertEqual(
            generator_function,
            "generic.numeric.integer_number",
        )
        self.assertEqual(
            generator_arguments,
            [],
        )

        # Column type from another dialect
        _, generator_function, __ = _get_provider_for_column(Column("myint", INTEGER))
        self.assertEqual(
            generator_function,
            "generic.numeric.integer_number",
        )

        # Text value with length
        (
            variable_name,
            generator_function,
            generator_arguments,
        ) = _get_provider_for_column(Column("mystring", String(100)))
        self.assertEqual(
            variable_name,
            ["mystring"],
        )
        self.assertEqual(
            generator_function,
            "generic.person.password",
        )
        self.assertEqual(
            generator_arguments,
            ["100"],
        )

        # UUID
        (
            _,
            generator_function,
            __,
        ) = _get_provider_for_column(Column("myuuid", UUID))
        self.assertEqual(
            generator_function,
            "generic.cryptographic.uuid",
        )


class TestMakeTables(DatafakerTestCase):
    """Test the make_tables function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    @patch("datafaker.make.MetaData")
    @patch("datafaker.make.DeclarativeGenerator")
    def test_make_tables_file(self, mock_declarative: MagicMock, _: MagicMock) -> None:
        """Test the make_tables_file function."""
        mock_declarative.return_value.generate.return_value = "pass\n"

        self.assertEqual(
            "pass\n",
            make_tables_file(
                parse_obj_as(PostgresDsn, "postgresql://postgres@1.2.3.4/db"), None, {}
            ),
        )

    @patch("datafaker.make.MetaData")
    @patch("datafaker.make.DeclarativeGenerator")
    def test_make_tables_file_with_schema(
        self, mock_declarative: MagicMock, _: MagicMock
    ) -> None:
        """Check that the function handles the schema setting."""
        mock_declarative.return_value.generate.return_value = "pass\n"

        self.assertEqual(
            "pass\n",
            make_tables_file(
                parse_obj_as(PostgresDsn, "postgresql://postgres@1.2.3.4/db"),
                "myschema",
                {},
            ),
        )

    @patch("datafaker.make.MetaData")
    @patch("datafaker.make.logger")
    @patch("datafaker.make.DeclarativeGenerator")
    def test_make_tables_warns_no_pk(
        self, mock_declarative: MagicMock, mock_logger: MagicMock, _: MagicMock
    ) -> None:
        """Test the make-tables sub-command warns about Tables()."""
        mock_declarative.return_value.generate.return_value = "t_nopk_table = Table()"

        make_tables_file(
            parse_obj_as(PostgresDsn, "postgresql://postgres@127.0.0.1:5432"), None, {}
        )

        mock_logger.warning.assert_called_with(
            "Table without PK detected. datafaker may not be able to continue.",
        )


class TestMakeStats(RequiresDBTestCase):
    """Test the make_src_stats function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        super().setUp()
        os.chdir(self.test_dir)
        conf_path = Path("example_config.yaml")
        with open(conf_path, "r", encoding="utf8") as f:
            self.config = yaml.safe_load(f)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)
        super().tearDown()

    def check_make_stats_output(self, src_stats: dict) -> None:
        """Check that the output of make_src_stats is as expected."""
        self.assertSetEqual(
            {"count_opt_outs", "avg_person_id", "count_names"},
            set(src_stats.keys()),
        )
        count_opt_outs = src_stats["count_opt_outs"]
        self.assertEqual(len(count_opt_outs), 2)
        self.assertIsInstance(count_opt_outs[0]["num"], int)
        self.assertIs(count_opt_outs[0]["research_opt_out"], False)
        self.assertIsInstance(count_opt_outs[1]["num"], int)
        self.assertIs(count_opt_outs[1]["research_opt_out"], True)

        count_names = src_stats["count_names"]
        self.assertEqual(len(count_names), 1)
        self.assertEqual(count_names[0]["num"], 1000)
        self.assertEqual(count_names[0]["name"], "Randy Random")

        avg_person_id = src_stats["avg_person_id"]
        self.assertEqual(len(avg_person_id), 1)
        self.assertEqual(avg_person_id[0]["avg_id"], 500.5)

        # Check that dumping into YAML goes fine.
        yaml.dump(src_stats)

    def test_make_stats_no_asyncio_schema(self) -> None:
        """Test that make_src_stats works when explicitly naming a schema."""
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, self.config, "public")
        )
        self.check_make_stats_output(src_stats)

    def test_make_stats_no_asyncio(self) -> None:
        """Test that make_src_stats works using the example configuration."""
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, self.config)
        )
        self.check_make_stats_output(src_stats)

    def test_make_stats_asyncio(self) -> None:
        """Test that make_src_stats errors if we use asyncio when some of the queries
        also use snsql.
        """
        config_asyncio = {**self.config, "use-asyncio": True}
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, config_asyncio)
        )
        self.check_make_stats_output(src_stats)

    @patch("datafaker.make.logger")
    def test_make_stats_empty_result(self, mock_logger: MagicMock) -> None:
        """Test that make_src_stats logs a warning if a query returns nothing."""
        query_name1 = "non-existent-person"
        query_name2 = "extreme-dp-parameters"
        config = {
            "src-stats": [
                {
                    "name": query_name1,
                    "query": "SELECT * FROM person WHERE name='Nat Nonexistent'",
                },
                {
                    "name": query_name2,
                    "query": "SELECT * FROM person",
                    "dp-query": (
                        "SELECT COUNT(*) FROM query_result GROUP BY research_opt_out"
                    ),
                    "epsilon": 1e-4,  # This makes the query result be empty.
                    "delta": 1e-15,
                    "snsql-metadata": {
                        "person_id": {"type": "int", "private_id": True},
                        "research_opt_out": {"type": "boolean"},
                    },
                },
            ]
        }
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, config, "public")
        )
        self.assertEqual(src_stats[query_name1], [])
        self.assertEqual(src_stats[query_name2], [])
        warning_template = "src-stats query %s returned no results"
        mock_logger.warning.assert_any_call(warning_template, query_name1)
        mock_logger.warning.assert_any_call(warning_template, query_name2)
        self.assertEqual(len(mock_logger.warning.call_args_list), 2)
