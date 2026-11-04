""" Tests for the configure-generators command. """
import re
from collections.abc import MutableMapping
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import aliased

from datafaker.interactive.base import DbCmd
from tests.test_interactive_generators import MockGeneratorCmd
from tests.utils import GeneratesDBTestCase, MsSqlTestDb


class ConfigureCopyGeneratorsWithDateTests(GeneratesDBTestCase):
    """Test `configure-generators` with the `instrument.sql` database."""

    dump_file_path = "date.sql"
    database_name = "date_tables"
    schema_name = "public"
    use_temporary_cwd = True

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockGeneratorCmd:
        """Get the command we are using for this test case."""
        return MockGeneratorCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        )

    def test_same_table_copy(self) -> None:
        """Test that the same-table copy is proposed and compared if appropriate."""
        config = {
            "tables": {
                "happening": {
                    "columns": {
                        "name": {
                            "roles": ["source"],
                        }
                    }
                }
            }
        }
        with self._get_cmd(config) as gc:
            # set up our copy proposer
            gc.do_next("happening.comment")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = "generic.anchored_provider.copy [from name]"
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            gc.reset()
            gc.do_compare(str(prop[0]))
            self.assertEqual(gc.messages[0][0], gc.NOT_PRIVATE_TEXT)
            self.assertEqual(gc.messages[1][0], gc.REQUIRES_NO_SOURCE_DATA_TEXT)
            # and test that copies between columns that have incompatible
            # types are not proposed
            gc.do_next("happening.at_time")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            self.assertNotIn(provider_name, proposals.keys())

    def test_cross_table_copy(self) -> None:
        """Test that the cross-table copy is proposed and compared."""
        config = {
            "tables": {
                "person": {
                    "columns": {
                        "name": {
                            "roles": ["source"],
                        }
                    }
                }
            }
        }
        with self._get_cmd(config) as gc:
            # set up our copy proposer
            gc.do_next("happening.comment")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.copy_fk"
                " [from name of table person (via person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            provider_name2 = (
                "generic.anchored_provider.copy_fk"
                " [from name of table person (via other_person_id)]"
            )
            self.assertIn(provider_name2, proposals.keys())
            prop2 = proposals[provider_name2]
            gc.reset()
            gc.do_compare(f"{prop[0]} {prop2[0]}")
            self.assertEqual(gc.messages[0][0], gc.NOT_PRIVATE_TEXT)
            self.assertEqual(gc.messages[1][0], gc.REQUIRES_NO_SOURCE_DATA_TEXT)

    def test_cross_table_copy_actually_copies(self) -> None:
        """Test that the cross-table copy is copies."""
        config = {
            "tables": {
                "person": {
                    "columns": {
                        "name": {
                            "roles": ["source"],
                        }
                    }
                }
            }
        }
        with self._get_cmd(config) as gc:
            # set up our copy proposer
            gc.do_next("happening.name")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.copy_fk"
                " [from name of table person (via person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            gc.do_set(str(prop[0]))
            gc.do_next("happening.comment")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.copy_fk"
                " [from name of table person (via other_person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())
            prop = proposals[provider_name]
            gc.do_set(str(prop[0]))
            gc.do_quit("")
            self.generate_data(gc.config, num_passes=50)
        assert self.dst_engine is not None
        with self.dst_engine.connect() as conn:
            pert = self.dst_metadata.tables["person"]
            other_pert = aliased(pert)
            hapt = self.dst_metadata.tables["happening"]
            query = (
                select(
                    hapt.columns["name"].label("hn"),
                    hapt.columns["comment"].label("hc"),
                    pert.columns["name"].label("pn"),
                    other_pert.columns["name"].label("opn"),
                )
                .select_from(hapt)
                .join(
                    pert,
                    onclause=pert.columns["id"] == hapt.columns["person_id"],
                )
                .join(
                    other_pert,
                    onclause=other_pert.columns["id"]
                    == hapt.columns["other_person_id"],
                    isouter=True,
                )
            )
            dst_result = conn.execute(query).fetchall()
        saw_other = False
        for row in dst_result:
            if row.opn:
                saw_other = True
                self.assertEqual(row.hc, row.opn)
            self.assertEqual(row.hn, row.pn)
        self.assertTrue(saw_other)

    def test_cross_table_fk_copy(self) -> None:
        """Test that the cross-table foreign key copy is proposed."""
        config = {
            "tables": {
                "person": {
                    "columns": {
                        "parent_of": {
                            "roles": ["source"],
                        }
                    }
                }
            }
        }
        with self._get_cmd(config) as gc:
            gc.do_next("happening.other_person_id")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = (
                "generic.anchored_provider.copy_fk"
                " [from parent_of of table person (via person_id)]"
            )
            self.assertIn(provider_name, proposals.keys())

    COPY_FROM_RE = re.compile(r"generic.anchored_provider.copy \[from ([^\]]+)\]")

    def get_same_table_copy_proposals(self, proposals: Iterable[str]) -> set[str]:
        """Return a set of columns proposed to copy from."""
        return {
            p.group(1)
            for p in (self.COPY_FROM_RE.match(prop) for prop in proposals)
            if p is not None
        }

    def test_different_target_fk_copy_is_not_proposed(self) -> None:
        """Test a foreign key copy with a different target table is not proposed."""
        config = {
            "tables": {
                "happening": {
                    "columns": {
                        "previous_happening_id": {
                            "roles": ["source"],
                        },
                        "person_id": {
                            "roles": ["source"],
                        },
                    }
                }
            }
        }
        with self._get_cmd(config) as gc:
            gc.do_next("happening.other_person_id")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            provider_name = "generic.anchored_provider.copy [from person_id]"
            self.assertIn(provider_name, proposals.keys())
            all_copies = self.get_same_table_copy_proposals(proposals.keys())
            self.assertIn("person_id", all_copies)
            self.assertNotIn("previous_happening_id", all_copies)

    def test_nullable_copy_nonnullable_is_not_proposed(self) -> None:
        """Test a copy from a nullable column to a nonnullable column is not proposed."""
        config = {
            "tables": {
                "happening": {
                    "columns": {
                        "other_person_id": {
                            "roles": ["source"],
                        },
                        "person_id": {
                            "roles": ["source"],
                        },
                    }
                }
            }
        }
        with self._get_cmd(config) as gc:
            # person -> other_person is allowed
            gc.do_next("happening.other_person_id")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            all_copies = self.get_same_table_copy_proposals(proposals.keys())
            self.assertIn("person_id", all_copies)
            # other_person -> person is allowed
            gc.do_next("happening.person_id")
            gc.reset()
            gc.do_propose("all")
            proposals = gc.get_proposals()
            all_copies = self.get_same_table_copy_proposals(proposals.keys())
            self.assertNotIn("other_person_id", all_copies)


# Note that this test won't work with DuckDB because it needs foreign keys to work
class ConfigureCopyGeneratorsWithDateMsSqlTests(ConfigureCopyGeneratorsWithDateTests):
    """Test `configure-generators` with `instrument.sql` with MS SQL."""

    database_type = MsSqlTestDb
    schema_name = None
