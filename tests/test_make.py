"""Tests for the main module."""
import asyncio
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from sqlalchemy import BigInteger, Column, String, select
from sqlalchemy.dialects.mysql.types import INTEGER
from sqlalchemy.dialects.postgresql import UUID

from datafaker.make import _get_provider_for_column, make_src_stats
from tests.utils import GeneratesDBTestCase, RequiresDBTestCase


class TestMakeGenerators(GeneratesDBTestCase):
    """Test the make_table_generators function."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def test_make_table_generators(self) -> None:
        """Check that we can make a generators file."""
        config = {
            "tables": {
                "player": {
                    "row_generators": [
                        {
                            "name": "dist_gen.constant",
                            "kwargs": {
                                "value": '"Cave"',
                            },
                            "columns_assigned": "given_name",
                        },
                        {
                            "name": "dist_gen.constant",
                            "kwargs": {
                                "value": '"Johnson"',
                            },
                            "columns_assigned": "family_name",
                        },
                    ],
                },
            },
        }
        self.generate_data(config, num_passes=3)
        with self.dst_sync_engine.connect() as conn:
            stmt = select(self.metadata.tables["player"])
            rows = conn.execute(stmt).mappings().fetchall()
            for row in rows:
                self.assertEqual(row.given_name, "Cave")
                self.assertEqual(row.family_name, "Johnson")

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
            {},
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
            {"length": "100"},
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


class TestMakeStats(RequiresDBTestCase):
    """Test the make_src_stats function."""

    dump_file_path = "src.dump"
    database_name = "src"
    schema_name = "public"

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
        count_opt_outs = src_stats["count_opt_outs"]["results"]
        self.assertEqual(len(count_opt_outs), 2)
        self.assertIsInstance(count_opt_outs[0]["num"], int)
        self.assertIs(count_opt_outs[0]["research_opt_out"], False)
        self.assertIsInstance(count_opt_outs[1]["num"], int)
        self.assertIs(count_opt_outs[1]["research_opt_out"], True)

        count_names = src_stats["count_names"]["results"]
        count_names.sort(key=lambda c: c["name"])
        self.assertListEqual(
            count_names,
            [
                {"num": 1, "name": "Miranda Rando-Generata"},
                {"num": 997, "name": "Randy Random"},
                {"num": 1, "name": "Testfried Testermann"},
                {"num": 1, "name": "Veronica Fyre"},
            ],
        )

        avg_person_id = src_stats["avg_person_id"]["results"]
        self.assertEqual(len(avg_person_id), 1)
        self.assertEqual(avg_person_id[0]["avg_id"], 500.5)

        # Check that dumping into YAML goes fine.
        yaml.dump(src_stats)

    def test_make_stats_no_asyncio_schema(self) -> None:
        """Test that make_src_stats works when explicitly naming a schema."""
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, self.config, self.schema_name)
        )
        self.check_make_stats_output(src_stats)

    def test_make_stats_no_asyncio(self) -> None:
        """Test that make_src_stats works using the example configuration."""
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, self.config, self.schema_name)
        )
        self.check_make_stats_output(src_stats)

    def test_make_stats_asyncio(self) -> None:
        """Test that make_src_stats errors if we use asyncio when some of the queries
        also use snsql.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        config_asyncio = {**self.config, "use-asyncio": True}
        src_stats = asyncio.get_event_loop().run_until_complete(
            make_src_stats(self.dsn, config_asyncio, self.schema_name)
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
            make_src_stats(self.dsn, config, self.schema_name)
        )
        self.assertEqual(src_stats[query_name1]["results"], [])
        self.assertEqual(src_stats[query_name2]["results"], [])
        debug_template = "src-stats query %s returned no results"
        mock_logger.debug.assert_any_call(debug_template, query_name1)
        mock_logger.debug.assert_any_call(debug_template, query_name2)
