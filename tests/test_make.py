"""Tests for the main module."""
import asyncio
import os
from io import StringIO
from pathlib import Path
from typing import Dict
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
            "WARNING: Table without PK detected. sqlsynthgen may not be able to continue.\n",
            mock_stderr.getvalue(),
        )


class TestMakeStats(RequiresDBTestCase):
    """Test the make_src_stats function."""

    test_dir = Path("tests/examples")
    start_dir = os.getcwd()

    def setUp(self) -> None:
        """Pre-test setup."""
        os.chdir(self.test_dir)

    def tearDown(self) -> None:
        """Post-test cleanup."""
        os.chdir(self.start_dir)

    def test_make_stats(self) -> None:
        """Test the make_src_stats function."""
        connection_string = "postgresql://postgres:password@localhost:5432/src"
        conf_path = Path("example_config.yaml")
        with open(conf_path, "r", encoding="utf8") as f:
            config = yaml.safe_load(f)
        config_no_snsql = {**config, "use-smartnoise-sql": False}

        # Check that make_src_stats works with, or without, a schema
        for args in (
            (connection_string, config),
            (connection_string, config, "public"),
            (connection_string, config_no_snsql),
        ):
            src_stats = asyncio.get_event_loop().run_until_complete(
                make_src_stats(*args)
            )

            self.assertSetEqual({"count_opt_outs"}, set(src_stats.keys()))
            count_opt_outs = src_stats["count_opt_outs"]
            self.assertEqual(len(count_opt_outs), 2)
            self.assertIsInstance(count_opt_outs[0][0], int)
            self.assertIs(count_opt_outs[0][1], False)
            self.assertIsInstance(count_opt_outs[1][0], int)
            self.assertIs(count_opt_outs[1][1], True)
