"""Tests for date component extraction proposers."""
from collections.abc import MutableMapping
from typing import Any

from sqlalchemy import select

from datafaker.interactive.base import DbCmd
from tests.test_interactive_generators import MockGeneratorCmd
from tests.utils import GeneratesDBTestCase


class ExtractDateComponentTests(GeneratesDBTestCase):
    """Testing date component extraction proposers."""

    dump_file_path = "datetime.sql"
    database_name = "date_time_extract"
    schema_name = "public"

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        """Get the configure-generators object as our command."""
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def _propose(self, gc: MockGeneratorCmd) -> dict[str, tuple[int, str, list[str]]]:
        gc.reset()
        # These tests check for every per-column extract-component variant
        # at once, which may not all be Pareto front 1 - 'all' bypasses
        # that cap so every variant is listed.
        gc.do_propose("all")
        return gc.get_proposals()

    def _choose_proposal(
        self,
        gc: MockGeneratorCmd,
        column: str,
        choice: str,
        nonchoices: list[str],
    ) -> None:
        gc.do_next(column)
        proposals = self._propose(gc)
        self.assertIn(choice, proposals)
        for nc in nonchoices:
            self.assertIn(nc, proposals)
        prop = proposals[choice]
        gc.do_set(str(prop[0]))
        gc.reset()

    def test_create_with_extract_from_components(self) -> None:
        """Test extraction of date components."""
        with self._get_cmd({}) as gc:
            self._choose_proposal(gc, "dates.getfrom", "generic.datetime.datetime", [])
            self._choose_proposal(gc, "orfrom", "generic.datetime.datetime", [])
            self._choose_proposal(
                gc,
                "year",
                "generic.extract_provider.year [from column getfrom]",
                [
                    "generic.extract_provider.year [from column orfrom]",
                    "generic.extract_provider.month [from column getfrom]",
                    "generic.extract_provider.month [from column orfrom]",
                    "generic.extract_provider.day [from column getfrom]",
                    "generic.extract_provider.day [from column orfrom]",
                ],
            )
            self._choose_proposal(
                gc,
                "month",
                "generic.extract_provider.month [from column getfrom]",
                [],
            )
            self._choose_proposal(
                gc,
                "day",
                "generic.extract_provider.day [from column getfrom]",
                [],
            )
            self._choose_proposal(
                gc,
                "date",
                "generic.extract_provider.date [from column getfrom]",
                ["generic.extract_provider.date [from column orfrom]"],
            )
            gc.do_quit("")
            self.set_configuration(gc.config)
            self.get_src_stats(gc.config)
            self.create_tables(gc.config)
            self.create_data(gc.config, num_passes=50)
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            results = conn.execute(select(self.metadata.tables["dates"])).fetchall()
            for result in results:
                self.assertEqual(result.getfrom.year, result.year)
