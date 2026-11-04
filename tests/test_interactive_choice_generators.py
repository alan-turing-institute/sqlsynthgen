""" Tests for the configure-generators choice generators. """
from collections.abc import MutableMapping
from typing import Any

from sqlalchemy import Connection, MetaData, select

from datafaker.interactive.base import DbCmd
from datafaker.proposers.choice import ChoiceProposerFactory
from tests.test_interactive_generators import MockGeneratorCmd
from tests.utils import DuckTestDb, GeneratesDBTestCase, MsSqlTestDb


class ChoiceMeasurementTableStats:
    """Measure the data in the ``choice.sql`` schema."""

    def __init__(self, metadata: MetaData, connection: Connection):
        """Get the data and do the analysis."""
        stmt = select(metadata.tables["number_table"])
        rows = connection.execute(stmt).fetchall()
        self.ones: set[int] = set()
        self.twos: set[int] = set()
        self.threes: set[int] = set()
        for row in rows:
            self.ones.add(row.one)
            self.twos.add(row.two)
            self.threes.add(row.three)


class GeneratorsChoiceTests(GeneratesDBTestCase):
    """Testing choice generation."""

    dump_file_path = "choice.sql"
    database_name = "numbers"
    schema_name = "public"

    def setUp(self) -> None:
        super().setUp()
        ChoiceProposerFactory.SAMPLE_COUNT = 500
        ChoiceProposerFactory.SUPPRESS_COUNT = 5

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def _propose(self, gc: MockGeneratorCmd) -> dict[str, tuple[int, str, list[str]]]:
        gc.reset()
        # These tests check for the full set of choice/zipf_choice/
        # weighted_choice variants (sampled/suppressed combinations), which
        # may not all be Pareto front 1 - 'all' bypasses that cap so every
        # variant is listed.
        gc.do_propose("all")
        return gc.get_proposals()

    def test_create_with_sampled_choice(self) -> None:
        """Test that suppression works for choice and zipf_choice."""
        with self._get_cmd({}) as gc:
            gc.do_next("number_table.one")
            proposals = self._propose(gc)
            self.assertIn("dist_gen.choice", proposals)
            self.assertIn("dist_gen.zipf_choice", proposals)
            self.assertIn("dist_gen.choice [sampled]", proposals)
            self.assertIn("dist_gen.zipf_choice [sampled]", proposals)
            self.assertIn("dist_gen.choice [sampled and suppressed]", proposals)
            self.assertIn("dist_gen.zipf_choice [sampled and suppressed]", proposals)
            gc.do_set(str(proposals["dist_gen.choice [sampled and suppressed]"][0]))
            gc.do_next("number_table.two")
            proposals = self._propose(gc)
            self.assertIn("dist_gen.choice", proposals)
            self.assertIn("dist_gen.zipf_choice", proposals)
            self.assertIn("dist_gen.choice [sampled]", proposals)
            self.assertIn("dist_gen.zipf_choice [sampled]", proposals)
            self.assertIn("dist_gen.choice [sampled and suppressed]", proposals)
            self.assertIn("dist_gen.zipf_choice [sampled and suppressed]", proposals)
            gc.do_set(
                str(proposals["dist_gen.zipf_choice [sampled and suppressed]"][0])
            )
            gc.do_next("number_table.three")
            proposals = self._propose(gc)
            self.assertIn("dist_gen.choice", proposals)
            self.assertIn("dist_gen.zipf_choice", proposals)
            self.assertIn("dist_gen.choice [sampled]", proposals)
            self.assertIn("dist_gen.zipf_choice [sampled]", proposals)
            self.assertNotIn("dist_gen.choice [sampled and suppressed]", proposals)
            self.assertNotIn("dist_gen.zipf_choice [sampled and suppressed]", proposals)
            gc.do_set(str(proposals["dist_gen.choice [sampled]"][0]))
            gc.do_quit("")
            self.generate_data(gc.config, num_passes=200)
            # all generation possibilities should be present
            assert self.dst_engine is not None
            with self.dst_engine.connect() as conn:
                stats = ChoiceMeasurementTableStats(self.metadata, conn)
                self.assertSetEqual(stats.ones, {1, 4})
                self.assertSetEqual(stats.twos, {2, 3})
                self.assertSetEqual(stats.threes, {1, 2, 3, 4, 5})

    def test_create_with_choice(self) -> None:
        """Smoke test normal choice works."""
        table_name = "number_table"
        with self._get_cmd({}) as gc:
            gc.do_next("number_table.one")
            proposals = self._propose(gc)
            gc.do_set(str(proposals["dist_gen.choice"][0]))
            gc.do_next("number_table.two")
            proposals = self._propose(gc)
            gc.do_set(str(proposals["dist_gen.zipf_choice"][0]))
            gc.do_quit("")
            self.generate_data(gc.config, num_passes=200)
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            stmt = select(self.metadata.tables[table_name])
            rows = conn.execute(stmt).fetchall()
            ones = set()
            twos = set()
            for row in rows:
                ones.add(row.one)
                twos.add(row.two)
            # all generation possibilities should be present
            self.assertSetEqual(ones, {1, 2, 3, 4, 5})
            self.assertSetEqual(twos, {1, 2, 3, 4, 5})

    def test_create_with_weighted_choice(self) -> None:
        """Smoke test weighted choice."""
        with self._get_cmd({}) as gc:
            gc.do_next("number_table.one")
            proposals = self._propose(gc)
            self.assert_subset(
                {
                    "dist_gen.weighted_choice",
                    "dist_gen.weighted_choice [sampled]",
                    "dist_gen.weighted_choice [suppressed]",
                    "dist_gen.weighted_choice [sampled and suppressed]",
                },
                set(proposals),
            )
            prop = proposals["dist_gen.weighted_choice [sampled and suppressed]"]
            self.assert_subset(set(prop[2]), {"1", "4"})
            gc.reset()
            gc.do_compare(str(prop[0]))
            col_heading = (
                f"{prop[0]}. dist_gen.weighted_choice [sampled and suppressed]"
            )
            self.assertIn(col_heading, set(gc.columns.keys()))
            col_set: set[int] = set(gc.columns[col_heading])
            self.assert_subset(col_set, {1, 4})
            gc.do_set(str(prop[0]))
            gc.do_next("number_table.two")
            proposals = self._propose(gc)
            self.assert_subset(
                {
                    "dist_gen.weighted_choice",
                    "dist_gen.weighted_choice [sampled]",
                    "dist_gen.weighted_choice [suppressed]",
                    "dist_gen.weighted_choice [sampled and suppressed]",
                },
                set(proposals),
            )
            prop = proposals["dist_gen.weighted_choice"]
            self.assert_subset(set(prop[2]), {"1", "2", "3", "4", "5"})
            gc.reset()
            gc.do_compare(str(prop[0]))
            col_heading = f"{prop[0]}. dist_gen.weighted_choice"
            self.assertIn(col_heading, set(gc.columns.keys()))
            col_set2: set[int] = set(gc.columns[col_heading])
            self.assert_subset(col_set2, {1, 2, 3, 4, 5})
            gc.do_set(str(prop[0]))
            gc.do_next("number_table.three")
            proposals = self._propose(gc)
            self.assert_subset(
                {
                    "dist_gen.weighted_choice",
                    "dist_gen.weighted_choice [sampled]",
                },
                set(proposals),
            )
            self.assertNotIn(
                "dist_gen.weighted_choice [sampled and suppressed]", proposals
            )
            prop = proposals["dist_gen.weighted_choice [sampled]"]
            self.assert_subset(set(prop[2]), {"1", "2", "3", "4", "5"})
            gc.do_compare(str(prop[0]))
            col_heading = f"{prop[0]}. dist_gen.weighted_choice [sampled]"
            self.assertIn(col_heading, set(gc.columns.keys()))
            col_set3: set[int] = set(gc.columns[col_heading])
            self.assert_subset(col_set3, {1, 2, 3, 4, 5})
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.generate_data(gc.config, num_passes=200)
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            stats = ChoiceMeasurementTableStats(self.metadata, conn)
            # all generation possibilities should be present
            self.assertSetEqual(stats.ones, {1, 4})
            self.assertSetEqual(stats.twos, {1, 2, 3, 4, 5})
            self.assertSetEqual(stats.threes, {1, 2, 3, 4, 5})


class GeneratorsChoiceTestsDuckDb(GeneratorsChoiceTests):
    """As ``GeneratorsChoiceTests`` but with DuckDB."""

    database_type = DuckTestDb


class GeneratorsChoiceTestsMsSql(GeneratorsChoiceTests):
    """As ``GeneratorsChoiceTests`` but with MS Sql."""

    database_type = MsSqlTestDb
    schema_name = None
