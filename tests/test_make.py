"""Tests for the main module."""
import asyncio
import os
from io import StringIO
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import yaml
from pydantic import PostgresDsn
from pydantic.tools import parse_obj_as
from sqlalchemy import BigInteger, Column, String
from sqlalchemy.dialects.mysql.types import INTEGER
from sqlalchemy.dialects.postgresql import UUID

from sqlsynthgen.make import (
    _get_provider_for_column,
    make_src_stats,
    make_table_generators,
    make_tables_file,
)
from tests.examples import example_orm
from tests.utils import RequiresDBTestCase, SSGTestCase, get_test_settings


class TestMakeGenerators(SSGTestCase):
    """Test the make_table_generators function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    @patch("sqlsynthgen.make.Path")
    @patch("sqlsynthgen.make.get_settings")
    @patch("sqlsynthgen.utils.create_engine")
    @patch("sqlsynthgen.make.download_table")
    def test_make_table_generators(
        self,
        mock_download: MagicMock,
        mock_create: MagicMock,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
    ) -> None:
        """Check that we can make a generators file from a tables module."""

        mock_path.return_value.exists.return_value = False

        mock_get_settings.return_value = get_test_settings()
        with open("expected_ssg.py", encoding="utf-8") as expected_output:
            expected = expected_output.read()
        conf_path = "example_config.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)
        stats_path = "example_stats.yaml"

        actual = make_table_generators(example_orm, config, stats_path)
        mock_download.assert_called_once()
        mock_create.assert_called_once()
        mock_path.assert_called_once()

        self.assertEqual(expected, actual)

    @patch("sqlsynthgen.make.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.make.Path")
    @patch("sqlsynthgen.make.get_settings")
    @patch("sqlsynthgen.utils.create_engine")
    def test_make_generators_do_not_overwrite(
        self,
        mock_create: MagicMock,
        mock_get_settings: MagicMock,
        mock_path: MagicMock,
        mock_stderr: MagicMock,
    ) -> None:
        """Tests that the making generators do not overwrite files."""
        mock_path.return_value.exists.return_value = True
        mock_get_settings.return_value = get_test_settings()
        configuration_file: str = "example_config.yaml"
        with open(configuration_file, "r", encoding="utf8") as f:
            configuration: Dict = yaml.safe_load(f)
        stats_path = "example_stats.yaml"

        try:
            make_table_generators(example_orm, configuration, stats_path)
        except SystemExit:
            pass

        mock_create.assert_called_once()
        self.assertEqual(
            "concept.yaml already exists. Exiting...\n", mock_stderr.getvalue()
        )

    @patch("sqlsynthgen.make.download_table")
    @patch("sqlsynthgen.utils.create_engine")
    @patch("sqlsynthgen.make.get_settings")
    @patch("sqlsynthgen.make.Path")
    def test_make_generators_force_overwrite(
        self,
        mock_path: MagicMock,
        mock_get_settings: MagicMock,
        mock_create: MagicMock,
        mock_download: MagicMock,
    ) -> None:
        """Tests that making generators overwrite files, when instructed."""
        mock_path.return_value.exists.return_value = True

        mock_get_settings.return_value = get_test_settings()
        with open("expected_ssg.py", encoding="utf-8") as expected_output:
            expected: str = expected_output.read()
        conf_path = "example_config.yaml"
        with open(conf_path, "r", encoding="utf8") as f:
            config: Dict = yaml.safe_load(f)
        stats_path: str = "example_stats.yaml"

        actual: str = make_table_generators(
            example_orm, config, stats_path, overwrite_files=True
        )

        mock_create.assert_called_once()
        mock_download.assert_called_once()

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
    @patch("sqlsynthgen.make.DeclarativeGenerator")
    def test_make_tables_file(self, mock_declarative: MagicMock, _: MagicMock) -> None:
        """Test the make_tables_file function."""
        mock_declarative.return_value.generate.return_value = "some generated code"

        self.assertEqual(
            "some generated code",
            make_tables_file(
                parse_obj_as(PostgresDsn, "postgresql://postgres@1.2.3.4/db"), None
            ),
        )

    @patch("sqlsynthgen.make.MetaData")
    @patch("sqlsynthgen.make.DeclarativeGenerator")
    def test_make_tables_file_with_schema(
        self, mock_declarative: MagicMock, _: MagicMock
    ) -> None:
        """Check that the function handles the schema setting."""
        mock_declarative.return_value.generate.return_value = "some generated code"

        self.assertEqual(
            "some generated code",
            make_tables_file(
                parse_obj_as(PostgresDsn, "postgresql://postgres@1.2.3.4/db"),
                "myschema",
            ),
        )

    @patch("sqlsynthgen.make.MetaData")
    @patch("sqlsynthgen.make.stderr", new_callable=StringIO)
    @patch("sqlsynthgen.make.DeclarativeGenerator")
    def test_make_tables_warns_no_pk(
        self, mock_declarative: MagicMock, mock_stderr: MagicMock, _: MagicMock
    ) -> None:
        """Test the make-tables sub-command warns about Tables()."""
        mock_declarative.return_value.generate.return_value = "t_nopk_table = Table("

        make_tables_file(
            parse_obj_as(PostgresDsn, "postgresql://postgres@127.0.0.1:5432"), None
        )

        self.assertEqual(
            "WARNING: Table without PK detected. sqlsynthgen may not be able to "
            "continue.\n",
            mock_stderr.getvalue(),
        )


class TestMakeStats(RequiresDBTestCase):
    """Test the make_src_stats function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)
        self.connection_string = "postgresql://postgres:password@localhost:5432/src"
        conf_path = Path("example_config.yaml")
        with open(conf_path, "r", encoding="utf8") as f:
            self.config = yaml.safe_load(f)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    def check_make_stats_output(self, src_stats: dict) -> None:
        """Check that the output of make_src_stats is as expected."""
        self.assertSetEqual(
            {"count_opt_outs", "avg_person_id", "count_names"},
            set(src_stats.keys()),
        )
        count_opt_outs = src_stats["count_opt_outs"]
        self.assertEqual(len(count_opt_outs), 2)
        self.assertIsInstance(count_opt_outs[0][0], int)
        self.assertIs(count_opt_outs[0][1], False)
        self.assertIsInstance(count_opt_outs[1][0], int)
        self.assertIs(count_opt_outs[1][1], True)

        count_names = src_stats["count_names"]
        self.assertEqual(len(count_names), 1)
        self.assertEqual(count_names[0][0], 1000)
        self.assertEqual(count_names[0][1], "Randy Random")

    def test_make_stats_no_asyncio_schema(self) -> None:
        """Test that make_src_stats works when explicitly naming a schema."""
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.connection_string, self.config, "public")
        )
        self.check_make_stats_output(src_stats)

    def test_make_stats_no_asyncio(self) -> None:
        """Test that make_src_stats works using the example configuration."""
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.connection_string, self.config)
        )
        self.check_make_stats_output(src_stats)

    def test_make_stats_asyncio(self) -> None:
        """Test that make_src_stats errors if we use asyncio when some of the queries
        also use snsql.
        """
        config_asyncio = {**self.config, "use-asyncio": True}
        with self.assertRaises(ValueError):
            _ = asyncio.get_event_loop().run_until_complete(
                make_src_stats(self.connection_string, config_asyncio)
            )

    def test_make_stats_asyncio_no_snsql(self) -> None:
        """Test that make_src_stats works if we use asyncio as long as we disable snsql
        on all queries.
        """
        config_asyncio_no_snsql: dict[str, Any] = {**self.config, "use-asyncio": True}
        for query_block in config_asyncio_no_snsql["src-stats"]:
            query_block["use-smartnoise-sql"] = False
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.connection_string, config_asyncio_no_snsql)
        )
        self.check_make_stats_output(src_stats)
