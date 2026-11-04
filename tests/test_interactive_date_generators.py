""" Tests for the configure-generators command. """
import sys
from collections.abc import MutableMapping
from importlib import resources
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import func, select

from datafaker.dialects import SecondsDifference, StdDev
from datafaker.interactive.base import DbCmd
from datafaker.interactive.generators import GeneratorCmd
from tests.utils import DuckTestDb, GeneratesDBTestCase, MsSqlTestDb, TestDbCmdMixin


class MockGeneratorCmd(GeneratorCmd, TestDbCmdMixin):
    """GeneratorCmd but mocked"""

    def get_proposals(self) -> dict[str, tuple[int, str, list[str]]]:
        """
        Returns a dict of generator name to a tuple of (index, '', [list,of,samples]).

        The middle element is unused: it used to carry the old per-proposer
        fit score, but 'propose' no longer prints that separate (now
        removed) list, only the ranked table (``RANKED_SAMPLE_TEXT``), which
        has no equivalent per-candidate fit figure.
        """
        return {
            kw["name"]: (kw["index"], "", kw["sample"].split("; "))
            for (s, _, kw) in self.messages
            if s == self.RANKED_SAMPLE_TEXT
        }


class ConfigureGeneratorsWithSrc2Tests(GeneratesDBTestCase):
    """Test `configure-generators` with the `src2.dump` database."""

    dump_file_path = "src2.dump"
    database_name = "src"
    schema_name = "public"
    use_temporary_cwd = True
    copy_files = ["row_generators.py", "story_generators.py"]
    copy_from_directory = Path("examples")

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        """Get the command we are using for this test case."""
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def _get_config(self) -> dict[Any, Any]:
        test_module = resources.files(sys.modules["tests"])
        with test_module.joinpath("examples/example_config2.yaml").open(
            encoding="utf-8",
        ) as config_fh:
            cy = yaml.load(config_fh, yaml.SafeLoader)
            assert isinstance(cy, dict)
            return cy

    def test_intervals_end_to_end(self) -> None:
        """Test that if an interval end is applicable it gets proposed and works."""
        table = "hospital_visit"
        column = "visit_end"
        config = self._get_config()
        # let's not test the uniqueness failures!
        config["tables"]["unique_constraint_test"]["num_rows_per_pass"] = 0
        config["tables"]["unique_constraint_test2"]["num_rows_per_pass"] = 0
        with self._get_cmd(config) as gc:
            # set up our interval proposer
            gc.do_next(f"{table}.{column}")
            gc.do_unmerge("visit_start")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.normal_date [anchored to visit_start]"
            )
            self.assertIn(provider_name, proposals)
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[provider_name][0]))
            gc.do_quit("")
            self.generate_data(config, num_passes=15)
        with self.sync_engine.connect() as conn:
            src_diff = SecondsDifference(
                self.metadata.tables[table].c[column],
                self.metadata.tables[table].c["visit_start"],
            )
            src_result = conn.execute(
                select(
                    func.avg(src_diff).label("mean"), StdDev(src_diff).label("sd")
                ).select_from(self.metadata.tables[table])
            ).one()
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            dst_diff = SecondsDifference(
                self.dst_metadata.tables[table].c[column],
                self.dst_metadata.tables[table].c["visit_start"],
            )
            dst_result = conn.execute(
                select(
                    func.avg(dst_diff).label("mean"), StdDev(dst_diff).label("sd")
                ).select_from(self.dst_metadata.tables[table])
            ).one()
        self.assertAlmostEqual(
            src_result.mean, dst_result.mean, delta=src_result.mean * 0.3
        )
        self.assertAlmostEqual(src_result.sd, dst_result.sd, delta=src_result.sd * 0.5)


class ConfigureGeneratorsWithSrc2DuckDbTests(ConfigureGeneratorsWithSrc2Tests):
    """Test `configure-generators` with `src2.dump` with DuckDB."""

    database_type = DuckTestDb


class ConfigureGeneratorsWithSrc2MsSqlTests(ConfigureGeneratorsWithSrc2Tests):
    """Test `configure-generators` with `src2.dump` with MS SQL."""

    database_type = MsSqlTestDb
    schema_name = None


class ConfigureGeneratorsWithInstrumentsTests(GeneratesDBTestCase):
    """Test `configure-generators` with the `instrument.sql` database."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"
    use_temporary_cwd = True

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        """Get the command we are using for this test case."""
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    # pylint: disable=too-many-locals
    def test_cross_table_interval_end_to_end(self) -> None:
        """Test that the cross-table interval is proposed and works."""
        table = "model"
        column = "introduced"
        atable = "manufacturer"
        anchor = "founded"
        config = {
            "tables": {
                "manufacturer": {
                    "row_generators": [
                        {
                            "name": "generic.datetime.datetime",
                            "kwargs": {
                                "start": 1930,
                                "end": 1980,
                            },
                            "columns_assigned": ["founded"],
                        },
                    ],
                    "columns": {
                        "founded": {
                            "roles": ["start"],
                        },
                    },
                },
            }
        }
        with self._get_cmd(config) as gc:
            # set up our interval proposer
            gc.do_next(f"{table}.{column}")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.normal_date_fk"
                f" [anchored to {anchor} of table {atable}"
                " (via manufacturer_id)]"
            )
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            gc.reset()
            gc.do_compare(str(prop[0]))
            self.assertEqual(gc.messages[0][0], gc.NOT_PRIVATE_TEXT)
            self.assertEqual(gc.messages[1][0], gc.REQUIRES_SOURCE_DATA_TEXT)
            self.assertEqual(gc.messages[2][0], gc.PROVIDING_VALUES_TEXT)
            self.assertAlmostEqual(gc.messages[2][1][1]["sd"], 5.4e7, delta=1.0e6)
            self.assertAlmostEqual(gc.messages[2][1][1]["mean"], 3.3e7, delta=1.0e6)
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.generate_data(config, num_passes=15)
        with self.sync_engine.connect() as conn:
            mt = self.metadata.tables[table]
            mat = self.metadata.tables[atable]
            src_diff = SecondsDifference(mt.c[column], mat.c[anchor])
            src_result = conn.execute(
                select(func.avg(src_diff).label("mean"), StdDev(src_diff).label("sd"))
                .select_from(mt)
                .join(mat)
            ).one()
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            dmt = self.dst_metadata.tables[table]
            dmat = self.dst_metadata.tables[atable]
            dst_diff = SecondsDifference(dmt.c[column], dmat.c[anchor])
            dst_result = conn.execute(
                select(func.avg(dst_diff).label("mean"), StdDev(dst_diff).label("sd"))
                .select_from(dmt)
                .join(dmat)
            ).one()
        self.assertAlmostEqual(
            dst_result.mean, dst_result.mean, delta=dst_result.mean * 0.3
        )
        self.assertAlmostEqual(src_result.sd, dst_result.sd, delta=src_result.sd * 0.5)


# Note that this test won't work with DuckDB because it needs foreign keys to work
class ConfigureGeneratorsWithInstrumentsMsSqlTests(
    ConfigureGeneratorsWithInstrumentsTests
):
    """Test `configure-generators` with `instrument.sql` with MS SQL."""

    database_type = MsSqlTestDb
    schema_name = None


class ConfigureGeneratorsWithDateTests(GeneratesDBTestCase):
    """Test `configure-generators` with the `date.sql` database."""

    dump_file_path = "date.sql"
    database_name = "date_tables"
    schema_name = "public"
    use_temporary_cwd = True

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        """Get the command we are using for this test case."""
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    # pylint: disable=too-many-locals
    def test_cross_table_interval_timestamp_vs_date(self) -> None:
        """
        Test that the cross-table interval is proposed and compared.

        Tests all combinations of date vs timestamp.
        """
        table = "happening"
        column = "at_time"
        column2 = "at_date"
        atable = "person"
        anchor = "date_of_birth"
        anchor2 = "timestamp_of_birth"
        config = {
            "tables": {
                atable: {
                    "row_generators": [
                        {
                            "name": "generic.datetime.datetime",
                            "kwargs": {
                                "start": 1930,
                                "end": 1980,
                            },
                            "columns_assigned": [anchor],
                        },
                    ],
                    "columns": {
                        anchor: {
                            "roles": ["start"],
                        },
                        anchor2: {
                            "roles": ["start"],
                        },
                    },
                },
            }
        }
        with self._get_cmd(config) as gc:
            # set up our interval proposer
            gc.do_next(f"{table}.{column}")
            gc.reset()
            # Both anchored variants must be listed at once - 'all' bypasses
            # the Pareto-front-1 cap that might otherwise hide one of them.
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.normal_date_fk"
                f" [anchored to {anchor} of table {atable}"
                " (via person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            provider_name2 = (
                "generic.anchored_provider.normal_date_fk"
                f" [anchored to {anchor2} of table {atable}"
                " (via person_id)]"
            )
            self.assertIn(provider_name2, proposals.keys())
            prop2 = proposals[provider_name2]
            gc.reset()
            gc.do_compare(f"{prop[0]} {prop2[0]}")
            self.assertEqual(gc.messages[0][0], gc.NOT_PRIVATE_TEXT)
            self.assertEqual(gc.messages[1][0], gc.REQUIRES_SOURCE_DATA_TEXT)
            self.assertEqual(gc.messages[2][0], gc.PROVIDING_VALUES_TEXT)
            gc.do_next(column2)
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.normal_date_fk"
                f" [anchored to {anchor} of table {atable}"
                " (via person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            provider_name2 = (
                "generic.anchored_provider.normal_date_fk"
                f" [anchored to {anchor2} of table {atable}"
                " (via person_id)]"
            )
            self.assertIn(provider_name2, proposals.keys())
            prop2 = proposals[provider_name2]
            gc.reset()
            gc.do_compare(f"{prop[0]} {prop2[0]}")
            self.assertEqual(gc.messages[0][0], gc.NOT_PRIVATE_TEXT)
            self.assertEqual(gc.messages[1][0], gc.REQUIRES_SOURCE_DATA_TEXT)
            self.assertEqual(gc.messages[2][0], gc.PROVIDING_VALUES_TEXT)

    def test_regression_date_start_crash(self) -> None:
        """Test for regression where ``start`` role on a date leads to crash."""
        config = {
            "tables": {
                "person": {
                    "columns": {
                        "date_of_birth": {
                            "roles": ["start"],
                        },
                    },
                },
            }
        }
        with self._get_cmd(config) as gc:
            gc.do_next("happening.at_date")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.normal_date_fk"
                " [anchored to date_of_birth of table person"
                " (via person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())


# Note that this test won't work with DuckDB because it needs foreign keys to work
class ConfigureGeneratorsWithDateMsSqlTests(ConfigureGeneratorsWithDateTests):
    """Test `configure-generators` with `date.sql` with MS SQL."""

    database_type = MsSqlTestDb
    schema_name = None
