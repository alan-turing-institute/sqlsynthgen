"""Tests for null-partitioned generators."""
from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import Any
from unittest import TestCase

from sqlalchemy import Connection, MetaData, insert, select

from datafaker.generators import NullPartitionedNormalGeneratorFactory
from tests.test_interactive_generators import TestGeneratorCmd
from tests.utils import GeneratesDBTestCase


@dataclass
class Stat:
    """Mean and variance calculator."""

    n: int = 0
    x: float = 0
    x2: float = 0

    def add(self, x: float) -> None:
        """Add one datum."""
        self.n += 1
        self.x += x
        self.x2 += x * x

    def count(self) -> int:
        """Get the number of data added."""
        return self.n

    def x_mean(self) -> float:
        """Get the mean of the added data."""
        return self.x / self.n

    def x_var(self) -> float:
        """Get the variance of the added data."""
        x = self.x
        return (self.x2 - x * x / self.n) / (self.n - 1)


@dataclass
class Correlation(Stat):
    """Mean, variance and covariance."""

    y: float = 0
    y2: float = 0
    xy: float = 0

    def add2(self, x: float, y: float) -> None:
        """Add a 2D data point."""
        self.n += 1
        self.x += x
        self.x2 += x * x
        self.y += y
        self.y2 += y * y
        self.xy += x * y

    def y_mean(self) -> float:
        """Get the mean of the second parts of the added points."""
        return self.y / self.n

    def y_var(self) -> float:
        """Get the variance of the second parts of the added points."""
        y = self.y
        return (self.y2 - y * y / self.n) / (self.n - 1)

    def covar(self) -> float:
        """Get the covariance of the two parts of the added points."""
        return (self.xy - self.x * self.y / self.n) / (self.n - 1)


# pylint: disable=too-many-instance-attributes
class EavMeasurementTableStats:
    """The statistics for the Measurement table of eav.sql."""

    def __init__(self, conn: Connection, metadata: MetaData, test: TestCase) -> None:
        stmt = select(metadata.tables["measurement"])
        rows = conn.execute(stmt).fetchall()
        self.types: set[int] = set()
        self.one_count = 0
        self.one_yes_count = 0
        self.two = Correlation()
        self.three = Correlation()
        self.four = Correlation()
        self.fish = Stat()
        self.fowl = Stat()
        for row in rows:
            self.types.add(row.type)
            if row.type == 1:
                # yes or no
                test.assertIsNone(row.first_value)
                test.assertIsNone(row.second_value)
                test.assertIn(row.third_value, {"yes", "no"})
                self.one_count += 1
                if row.third_value == "yes":
                    self.one_yes_count += 1
            elif row.type == 2:
                # positive correlation around 1.4, 1.8
                test.assertIsNotNone(row.first_value)
                test.assertIsNotNone(row.second_value)
                test.assertIsNone(row.third_value)
                self.two.add2(row.first_value, row.second_value)
            elif row.type == 3:
                # negative correlation around 11.8, 12.1
                test.assertIsNotNone(row.first_value)
                test.assertIsNotNone(row.second_value)
                test.assertIsNone(row.third_value)
                self.three.add2(row.first_value, row.second_value)
            elif row.type == 4:
                # positive correlation around 21.4, 23.4
                test.assertIsNotNone(row.first_value)
                test.assertIsNotNone(row.second_value)
                test.assertIsNone(row.third_value)
                self.four.add2(row.first_value, row.second_value)
            elif row.type == 5:
                test.assertIn(row.third_value, {"fish", "fowl"})
                test.assertIsNotNone(row.first_value)
                test.assertIsNone(row.second_value)
                if row.third_value == "fish":
                    # mean 8.1 and sd 0.755
                    self.fish.add(row.first_value)
                else:
                    # mean 11.2 and sd 1.114
                    self.fowl.add(row.first_value)


class NullPartitionedTests(GeneratesDBTestCase):
    """Testing null-partitioned grouped multivariate generation."""

    dump_file_path = "eav.sql"
    database_name = "eav"
    schema_name = "public"

    def setUp(self) -> None:
        """Set up the test with specific sample and suppress counts."""
        super().setUp()
        NullPartitionedNormalGeneratorFactory.SAMPLE_COUNT = 8
        NullPartitionedNormalGeneratorFactory.SUPPRESS_COUNT = 2

    def _get_cmd(self, config: MutableMapping[str, Any]) -> TestGeneratorCmd:
        """Get the configure-generators object as our command."""
        return TestGeneratorCmd(self.dsn, self.schema_name, self.metadata, config)

    def _propose(self, gc: TestGeneratorCmd) -> dict[str, tuple[int, str, list[str]]]:
        gc.reset()
        gc.do_propose("")
        return gc.get_proposals()

    def test_create_with_null_partitioned_grouped_multivariate(self) -> None:
        """Test EAV for all columns."""
        generate_count = 800
        with self._get_cmd({}) as gc:
            self.merge_columns(
                gc,
                "measurement",
                [
                    "type",
                    "first_value",
                    "second_value",
                    "third_value",
                ],
            )
            proposals = self._propose(gc)
            self.assertIn("null-partitioned grouped_multivariate_lognormal", proposals)
            dist_to_choose = "null-partitioned grouped_multivariate_normal"
            self.assertIn(dist_to_choose, proposals)
            prop = proposals[dist_to_choose]
            gc.reset()
            gc.do_compare(str(prop[0]))
            col_heading = f"{prop[0]}. {dist_to_choose}"
            self.assertIn(col_heading, set(gc.columns.keys()))
            gc.do_set(str(prop[0]))
            gc.reset()
            gc.do_quit("")
            self.set_configuration(gc.config)
            self.get_src_stats(gc.config)
            self.create_generators(gc.config)
            self.create_tables()
            self.populate_measurement_type_vocab()
            self.create_data(gc.config, num_passes=generate_count)
        with self.dst_sync_engine.connect() as conn:
            stats = EavMeasurementTableStats(conn, self.metadata, self)
        # type 1
        self.assertAlmostEqual(
            stats.one_count, generate_count * 5 / 20, delta=generate_count * 0.4
        )
        # about 40% are yes
        self.assertAlmostEqual(
            stats.one_yes_count / stats.one_count, 0.4, delta=generate_count * 0.4
        )
        # type 2
        self.assertAlmostEqual(
            stats.two.count(), generate_count * 3 / 20, delta=generate_count * 0.5
        )
        self.assertAlmostEqual(stats.two.x_mean(), 1.4, delta=0.4)
        self.assertAlmostEqual(stats.two.x_var(), 0.315, delta=0.18)
        self.assertAlmostEqual(stats.two.y_mean(), 1.8, delta=0.8)
        self.assertAlmostEqual(stats.two.y_var(), 0.105, delta=0.08)
        self.assertAlmostEqual(stats.two.covar(), 0.105, delta=0.08)
        # type 3
        self.assertAlmostEqual(
            stats.three.count(), generate_count * 3 / 20, delta=generate_count * 0.2
        )
        self.assertAlmostEqual(stats.three.covar(), -2.085, delta=1.1)
        # type 4
        self.assertAlmostEqual(
            stats.four.count(), generate_count * 3 / 20, delta=generate_count * 0.2
        )
        self.assertAlmostEqual(stats.four.covar(), 3.33, delta=1.5)
        # type 5/fish
        self.assertAlmostEqual(
            stats.fish.count(), generate_count * 3 / 20, delta=generate_count * 0.2
        )
        self.assertAlmostEqual(stats.fish.x_mean(), 8.1, delta=3.0)
        self.assertAlmostEqual(stats.fish.x_var(), 0.855, delta=0.6)
        # type 5/fowl
        self.assertAlmostEqual(
            stats.fowl.count(), generate_count * 3 / 20, delta=generate_count * 0.2
        )
        self.assertAlmostEqual(stats.fowl.x_mean(), 11.2, delta=8.0)
        self.assertAlmostEqual(stats.fowl.x_var(), 1.24, delta=0.6)

    def populate_measurement_type_vocab(self) -> None:
        """Add a vocab table without messing around with files"""
        table = self.metadata.tables["measurement_type"]
        with self.dst_sync_engine.connect() as conn:
            conn.execute(insert(table).values({"id": 1, "name": "agreement"}))
            conn.execute(insert(table).values({"id": 2, "name": "acceleration"}))
            conn.execute(insert(table).values({"id": 3, "name": "velocity"}))
            conn.execute(insert(table).values({"id": 4, "name": "position"}))
            conn.execute(insert(table).values({"id": 5, "name": "matter"}))
            conn.commit()

    def merge_columns(
        self, gc: TestGeneratorCmd, table: str, columns: list[str]
    ) -> None:
        """Merge columns in a table"""
        gc.do_next(f"{table}.{columns[0]}")
        for col in columns[1:]:
            gc.do_merge(col)
        gc.reset()

    def test_create_with_null_partitioned_grouped_sampled_and_suppressed(self) -> None:
        """Test EAV for all columns with sampled and suppressed generation."""
        generate_count = 800
        with self._get_cmd({}) as gc:
            self.merge_columns(
                gc,
                "measurement",
                [
                    "type",
                    "first_value",
                    "second_value",
                    "third_value",
                ],
            )
            proposals = self._propose(gc)
            self.assert_subset(
                {
                    "null-partitioned grouped_multivariate_lognormal",
                    "null-partitioned grouped_multivariate_normal",
                    "null-partitioned grouped_multivariate_lognormal [sampled and suppressed]",
                },
                set(proposals),
            )
            dist_to_choose = (
                "null-partitioned grouped_multivariate_normal [sampled and suppressed]"
            )
            self.assertIn(dist_to_choose, proposals)
            prop = proposals[dist_to_choose]
            gc.reset()
            gc.do_compare(str(prop[0]))
            col_heading = f"{prop[0]}. {dist_to_choose}"
            self.assertIn(col_heading, set(gc.columns.keys()))
            gc.do_set(str(prop[0]))
            self.merge_columns(
                gc,
                "observation",
                [
                    "type",
                    "first_value",
                    "second_value",
                    "third_value",
                ],
            )
            proposals = self._propose(gc)
            prop = proposals[dist_to_choose]
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.set_configuration(gc.config)
            self.get_src_stats(gc.config)
            self.create_generators(gc.config)
            self.create_tables()
            self.populate_measurement_type_vocab()
            self.create_data(gc.config, num_passes=generate_count)
        with self.dst_sync_engine.connect() as conn:
            stats = EavMeasurementTableStats(conn, self.metadata, self)
            stmt = select(self.metadata.tables["observation"])
            rows = conn.execute(stmt).fetchall()
            firsts = Stat()
            for row in rows:
                stats.types.add(row.type)
                self.assertEqual(row.type, 1)
                self.assertIsNotNone(row.first_value)
                self.assertIsNone(row.second_value)
                self.assertIn(row.third_value, {"ham", "eggs"})
                firsts.add(row.first_value)
            self.assertEqual(firsts.count(), 800)
            self.assertAlmostEqual(firsts.x_mean(), 1.3, delta=generate_count * 0.3)
        self.assert_subset(stats.types, {1, 2, 3, 4, 5})
        self.assertEqual(len(stats.types), 4)
        self.assert_subset({1, 5}, stats.types)
        # type 1
        self.assertAlmostEqual(
            stats.one_count, generate_count * 5 / 11, delta=generate_count * 0.4
        )
        # about 40% are yes
        self.assertAlmostEqual(
            stats.one_yes_count / stats.one_count, 0.4, delta=generate_count * 0.4
        )
        # type 5/fish
        self.assertAlmostEqual(
            stats.fish.count(), generate_count * 3 / 11, delta=generate_count * 0.2
        )
        self.assertAlmostEqual(stats.fish.x_mean(), 8.1, delta=3.0)
        self.assertAlmostEqual(stats.fish.x_var(), 0.855, delta=0.5)
        # type 5/fowl
        self.assertAlmostEqual(
            stats.fowl.count(), generate_count * 3 / 11, delta=generate_count * 0.2
        )
        self.assertAlmostEqual(stats.fowl.x_mean(), 11.2, delta=8.0)
        self.assertAlmostEqual(stats.fowl.x_var(), 1.24, delta=0.6)

    def test_null_partitioned_grouped_defines_all_its_constants(self) -> None:
        """Test EAV for all columns with sampled and suppressed generation."""
        with self._get_cmd({}) as gc:
            self.merge_columns(
                gc,
                "measurement",
                [
                    "type",
                    "first_value",
                    "second_value",
                    "third_value",
                ],
            )
            proposals = self._propose(gc)
            dist_to_choose = (
                "null-partitioned grouped_multivariate_normal [sampled and suppressed]"
            )
            self.assertIn(dist_to_choose, proposals)
            prop = proposals[dist_to_choose]
            gc.do_compare(str(prop[0]))
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.set_configuration(gc.config)
            src_stats = self.get_src_stats(gc.config)
            results = [result for ss in src_stats.values() for result in ss["results"]]
            constants = [
                v
                for result in results
                for k, v in result.items()
                if k[0] == "k" and k[1:].isdecimal()
            ]
            self.assertNotIn(None, constants)

    def test_create_with_null_partitioned_grouped_sampled_only(self) -> None:
        """Test EAV for all columns with sampled generation but no suppression."""
        table_name = "measurement"
        table2_name = "observation"
        generate_count = 800
        with self._get_cmd({}) as gc:
            self.merge_columns(
                gc, table_name, ["type", "first_value", "second_value", "third_value"]
            )
            proposals = self._propose(gc)
            self.assertIn("null-partitioned grouped_multivariate_lognormal", proposals)
            self.assertIn("null-partitioned grouped_multivariate_normal", proposals)
            self.assertIn(
                "null-partitioned grouped_multivariate_lognormal [sampled and suppressed]",
                proposals,
            )
            self.assertIn(
                "null-partitioned grouped_multivariate_normal [sampled and suppressed]",
                proposals,
            )
            self.assertIn(
                "null-partitioned grouped_multivariate_lognormal [sampled]", proposals
            )
            dist_to_choose = "null-partitioned grouped_multivariate_normal [sampled]"
            self.assertIn(dist_to_choose, proposals)
            prop = proposals[dist_to_choose]
            gc.reset()
            gc.do_compare(str(prop[0]))
            col_heading = f"{prop[0]}. {dist_to_choose}"
            self.assertIn(col_heading, set(gc.columns.keys()))
            gc.do_set(str(prop[0]))
            self.merge_columns(
                gc, table2_name, ["type", "first_value", "second_value", "third_value"]
            )
            proposals = self._propose(gc)
            prop = proposals[dist_to_choose]
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.set_configuration(gc.config)
            self.get_src_stats(gc.config)
            self.create_generators(gc.config)
            self.create_tables()
            self.populate_measurement_type_vocab()
            self.create_data(gc.config, num_passes=generate_count)
        with self.dst_sync_engine.connect() as conn:
            stmt = select(self.metadata.tables[table_name])
            rows = conn.execute(stmt).fetchall()
            self.assert_subset({row.type for row in rows}, {1, 2, 3, 4, 5})
            stmt = select(self.metadata.tables[table2_name])
            rows = conn.execute(stmt).fetchall()
            self.assertEqual(
                {row.third_value for row in rows}, {"ham", "eggs", "cheese"}
            )

    def test_create_with_null_partitioned_grouped_sampled_tiny(self) -> None:
        """
        Test EAV for all columns with sampled generation that only gets a tiny sample.
        """
        # five will ensure that at least one group will have two elements in it,
        # but all three cannot.
        NullPartitionedNormalGeneratorFactory.SAMPLE_COUNT = 5
        table_name = "observation"
        generate_count = 100
        with self._get_cmd({}) as gc:
            dist_to_choose = "null-partitioned grouped_multivariate_normal [sampled]"
            self.merge_columns(
                gc, table_name, ["type", "first_value", "second_value", "third_value"]
            )
            proposals = self._propose(gc)
            prop = proposals[dist_to_choose]
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.set_configuration(gc.config)
            self.get_src_stats(gc.config)
            self.create_generators(gc.config)
            self.create_tables()
            self.populate_measurement_type_vocab()
            self.create_data(gc.config, num_passes=generate_count)
        with self.dst_sync_engine.connect() as conn:
            stmt = select(self.metadata.tables[table_name])
            rows = conn.execute(stmt).fetchall()
            # we should only have one or two of "ham", "eggs" and "cheese" represented
            foods = {row.third_value for row in rows}
            self.assert_subset(foods, {"ham", "eggs", "cheese"})
            self.assertLess(len(foods), 3)

    def test_named_foreign_keys(self) -> None:
        """Test foreign keys gain names in source stats."""
        with self._get_cmd(
            {
                "tables": {
                    "measurement_type": {
                        "name_column": "name",
                    },
                },
            }
        ) as gc:
            self.merge_columns(
                gc,
                "measurement",
                [
                    "type",
                    "first_value",
                    "second_value",
                    "third_value",
                ],
            )
            proposals = self._propose(gc)
            dist_to_choose = "null-partitioned grouped_multivariate_normal"
            self.assertIn(dist_to_choose, proposals)
            prop = proposals[dist_to_choose]
            gc.do_compare(str(prop[0]))
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.set_configuration(gc.config)
            src_stats = self.get_src_stats(gc.config)
            with self.sync_engine.connect() as conn:
                stmt = select(self.metadata.tables["measurement_type"])
                rows = conn.execute(stmt).fetchall()
                mt = {row.id: row.name for row in rows}
                results = [
                    result for ss in src_stats.values() for result in ss["results"]
                ]
                ids = set()
                for result in results:
                    if "k0" in result:
                        k0 = result["k0"]
                        ids.add(k0)
                        self.assertEqual(result["k0_type__name"], mt[k0])
                self.assertSetEqual(ids, set(mt.keys()))
