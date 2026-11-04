""" Tests for the configure-missingness command. """
import random
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select

from datafaker.interactive import MissingnessCmd
from datafaker.interactive.base import DbCmd
from tests.utils import GeneratesDBTestCase, RequiresDBTestCase, TestDbCmdMixin


def count_nonnull(it: Iterable):
    """Count the number of non-null values in an iterable."""
    n = 0
    for v in it:
        if v is not None:
            n += 1
    return n


class MockMissingnessCmd(MissingnessCmd, TestDbCmdMixin):
    """MissingnessCmd but mocked"""


class ConfigureMissingnessTests(RequiresDBTestCase):
    """Testing configure-missing."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockMissingnessCmd:
        """We are using configure-missingness."""
        return MockMissingnessCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def test_set_missingness_to_sampled(self) -> None:
        """Test that we can set one table to sampled missingness."""
        with self._get_cmd({}) as mc:
            table = "signature_model"
            mc.do_next(table)
            mc.do_counts("")
            self.assertSequenceEqual(
                mc.messages, [(MissingnessCmd.ROW_COUNT_MSG, (10,), {})]
            )
            # Check the counts of NULLs in each column
            self.assertSequenceEqual(mc.rows, [["player_id", 4], ["based_on", 3]])
            mc.do_sampled("")
            mc.do_quit("")
            self.assertListEqual(
                mc.config["tables"][table]["missingness_generators"],
                [
                    {
                        "columns": ["player_id", "based_on"],
                        "kwargs": {
                            "patterns": 'SRC_STATS["missing_auto__signature_model__0"]["results"]'
                        },
                        "name": "column_presence.sampled",
                    }
                ],
            )
            self.assertEqual(
                mc.config["src-stats"][0]["name"],
                "missing_auto__signature_model__0",
            )
            q: str = mc.config["src-stats"][0]["query"]
            q = q.replace("\n", " ").replace("  ", " ").replace("  ", " ")
            self.assertEqual(
                q,
                (
                    "SELECT count(*) AS row_count,"
                    " __t.player_id__is_null AS player_id__is_null, "
                    "__t.based_on__is_null AS based_on__is_null FROM"
                    " (SELECT signature_model.player_id IS NULL AS player_id__is_null,"
                    " signature_model.based_on IS NULL AS based_on__is_null FROM"
                    " signature_model ORDER BY RANDOM() LIMIT 1000)"
                    " AS __t GROUP BY __t.player_id__is_null,"
                    " __t.based_on__is_null"
                ),
            )


class ConfigureMissingnessTestsWithGeneration(GeneratesDBTestCase):
    """Testing configure-missing with generation."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockMissingnessCmd:
        return MockMissingnessCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def test_create_with_missingness(self) -> None:
        """Test that we can sample real missingness and reproduce it."""
        random.seed(45)
        # Configure the missingness
        table_name = "signature_model"
        with self._get_cmd({}) as mc:
            mc.do_next(table_name)
            mc.do_sampled("")
            mc.do_quit("")
            config = mc.config
            self.generate_data(config, num_passes=100)
        # Test that each missingness pattern is present in the database
        with self.sync_engine.connect() as conn:
            stmt = select(self.metadata.tables[table_name])
            rows = conn.execute(stmt).mappings().fetchall()
            patterns: set[int] = set()
            for row in rows:
                p = 0 if row["player_id"] is None else 1
                b = 0 if row["based_on"] is None else 2
                patterns.add(p + b)
            # all pattern possibilities should be present
            self.assertSetEqual(patterns, {0, 1, 2, 3})


class ConfigureMissingnessEndToEndTests(ConfigureMissingnessTestsWithGeneration):
    """Test `configure-missing` end-to-end."""

    use_temporary_cwd = True
    copy_files = ["row_generators.py", "story_generators.py"]
    copy_from_directory = Path("examples")

    def test_missingness_end_to_end(self) -> None:
        """Test that if an interval end is applicable it gets proposed and works."""
        table = "signature_model"
        with self._get_cmd({}) as mc:
            mc.do_next(table)
            mc.do_sampled("")
            mc.reset()
            mc.do_quit("")
            self.generate_data(mc.config, num_passes=100)
            self.assertIs(
                type(
                    mc.config["tables"][table]["missingness_generators"][0]["columns"][
                        0
                    ]
                ),
                str,
            )
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            stmt = select(self.metadata.tables[table])
            rows = conn.execute(stmt).mappings().fetchall()
            name_count = count_nonnull(r.name for r in rows)
            player_id_count = count_nonnull(r.player_id for r in rows)
            based_on_count = count_nonnull(r.based_on for r in rows)
            self.assertEqual(name_count, 100)
            self.assertAlmostEqual(player_id_count, 50, delta=20)
            self.assertAlmostEqual(based_on_count, 70, delta=20)
