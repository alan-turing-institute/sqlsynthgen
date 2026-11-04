""" Tests for the configure-tables command. """
import re
from collections.abc import MutableMapping, Sized
from typing import Any

from sqlalchemy import select

from datafaker.interactive import TableCmd
from datafaker.interactive.base import DbCmd
from datafaker.serialize_metadata import dict_to_metadata
from tests.utils import RequiresDBTestCase, TestDbCmdMixin


class MockTableCmd(TableCmd, TestDbCmdMixin):
    """TableCmd but mocked"""

    def get_roles_from_columns(self, column: str) -> set[str]:
        """Get the roles in the named column from the ``columns`` command output."""
        self.reset()
        self.do_columns("")
        return self.get_roles_from_columns_output(column)


class ConfigureTablesTests(RequiresDBTestCase):
    """Testing configure-tables."""

    def _get_cmd(self, config: MutableMapping[str, Any]) -> MockTableCmd:
        return MockTableCmd(
            DbCmd.Settings(
                self.dsn,
                self.schema_name,
                config,
                self.metadata,
                None,
            )
        )


class ConfigureTablesSrcTests(ConfigureTablesTests):
    """Testing configure-tables with src.dump."""

    dump_file_path = "src.dump"
    database_name = "src"
    schema_name = "public"

    def test_table_name_prompts(self) -> None:
        """Test that the prompts follow the names of the tables."""
        config: MutableMapping[str, Any] = {}
        with self._get_cmd(config) as tc:
            table_names = list(self.metadata.tables.keys())
            for t in table_names:
                self.assertIn(t, tc.prompt)
                tc.do_next("")
            self.assertListEqual(tc.messages, [(TableCmd.INFO_NO_MORE_TABLES, (), {})])
            tc.reset()
            for t in reversed(table_names):
                self.assertIn(t, tc.prompt)
                tc.do_previous("")
            self.assertListEqual(
                tc.messages, [(TableCmd.ERROR_ALREADY_AT_START, (), {})]
            )
            tc.reset()
            bad_table_name = "notarealtable"
            tc.do_next(bad_table_name)
            self.assertListEqual(
                tc.messages, [(TableCmd.ERROR_NO_SUCH_TABLE, (bad_table_name,), {})]
            )
            tc.reset()
            good_table_name = table_names[2]
            tc.do_next(good_table_name)
            self.assertSequenceEqual(tc.messages, [])
            self.assertIn(good_table_name, tc.prompt)

    def test_column_display(self) -> None:
        """Test that we can see the names of the columns."""
        config: MutableMapping[str, Any] = {}
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_columns("")
            self.assertSequenceEqual(
                tc.rows,
                [
                    ["id", "INTEGER", True, False, "", ""],
                    ["a", "BOOLEAN", False, False, "", ""],
                    ["b", "BOOLEAN", False, False, "", ""],
                    ["c", "TEXT", False, False, "", ""],
                ],
            )

    def test_null_configuration(self) -> None:
        """A table still works if its configuration is None."""
        config = {
            "tables": None,
        }
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_private("")
            tc.do_quit("")
            tables = tc.config["tables"]
            self.assertFalse(
                tables["unique_constraint_test"].get("vocabulary_table", False)
            )
            self.assertFalse(tables["unique_constraint_test"].get("ignore", False))
            self.assertTrue(
                tables["unique_constraint_test"].get("primary_private", False)
            )

    def test_null_table_configuration(self) -> None:
        """A table still works if its configuration is None."""
        config = {
            "tables": {
                "unique_constraint_test": None,
            },
        }
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_private("")
            tc.do_quit("")
            tables = tc.config["tables"]
            self.assertFalse(
                tables["unique_constraint_test"].get("vocabulary_table", False)
            )
            self.assertFalse(tables["unique_constraint_test"].get("ignore", False))
            self.assertTrue(
                tables["unique_constraint_test"].get("primary_private", False)
            )

    def test_configure_tables(self) -> None:
        """Test that we can change tables to ignore, vocab or generate."""
        config = {
            "tables": {
                "unique_constraint_test": {
                    "vocabulary_table": True,
                },
                "no_pk_test": {
                    "ignore": True,
                },
                "hospital_visit": {
                    "num_passes": 0,
                },
                "empty_vocabulary": {
                    "private": True,
                },
            },
        }
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_generate("")
            tc.do_next("person")
            tc.do_vocabulary("")
            tc.do_next("mitigation_type")
            tc.do_ignore("")
            tc.do_next("hospital_visit")
            tc.do_private("")
            tc.do_quit("")
            tc.do_next("empty_vocabulary")
            tc.do_empty("")
            tc.do_quit("")
            tables = tc.config["tables"]
            self.assertFalse(
                tables["unique_constraint_test"].get("vocabulary_table", False)
            )
            self.assertFalse(tables["unique_constraint_test"].get("ignore", False))
            self.assertFalse(
                tables["unique_constraint_test"].get("primary_private", False)
            )
            self.assertEqual(tables["unique_constraint_test"].get("num_passes", 1), 1)
            self.assertFalse(tables["no_pk_test"].get("vocabulary_table", False))
            self.assertTrue(tables["no_pk_test"].get("ignore", False))
            self.assertFalse(tables["no_pk_test"].get("primary_private", False))
            self.assertEqual(tables["no_pk_test"].get("num_rows_per_pass", 1), 1)
            self.assertTrue(tables["person"].get("vocabulary_table", False))
            self.assertFalse(tables["person"].get("ignore", False))
            self.assertFalse(tables["person"].get("primary_private", False))
            self.assertEqual(tables["person"].get("num_rows_per_pass", 1), 1)
            self.assertFalse(tables["mitigation_type"].get("vocabulary_table", False))
            self.assertTrue(tables["mitigation_type"].get("ignore", False))
            self.assertFalse(tables["mitigation_type"].get("primary_private", False))
            self.assertEqual(tables["mitigation_type"].get("num_rows_per_pass", 1), 1)
            self.assertFalse(tables["hospital_visit"].get("vocabulary_table", False))
            self.assertFalse(tables["hospital_visit"].get("ignore", False))
            self.assertTrue(tables["hospital_visit"].get("primary_private", False))
            self.assertEqual(tables["hospital_visit"].get("num_rows_per_pass", 1), 1)
            self.assertFalse(tables["empty_vocabulary"].get("vocabulary_table", False))
            self.assertFalse(tables["empty_vocabulary"].get("ignore", False))
            self.assertFalse(tables["empty_vocabulary"].get("primary_private", False))
            self.assertEqual(tables["empty_vocabulary"].get("num_rows_per_pass", 1), 0)

    def test_configure_naming_columns(self) -> None:
        """Test that we can change tables' naming columns."""
        config = {
            "tables": {
                "unique_constraint_test": {
                    "vocabulary_table": True,
                    "name_column": None,
                },
                "no_pk_test": {
                    "ignore": True,
                    "name_column": "not_an_id",
                },
                "hospital_visit": {
                    "num_rows_per_pass": 0,
                },
                "empty_vocabulary": {
                    "num_rows_per_pass": 0,
                    "name_column": "entry_id",
                },
            },
        }
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_generate("name c")
            tc.do_next("person")
            tc.do_vocabulary("")
            tc.do_next("mitigation_type")
            tc.do_ignore("")
            tc.do_next("hospital_visit")
            tc.do_private("")
            tc.do_quit("")
            # changing name column but not type
            tc.do_next("empty_vocabulary")
            tc.do_empty("name entry_name")
            tc.do_quit("")
            tables = tc.config["tables"]
            self.assertEqual(
                tables["unique_constraint_test"].get("name_column", None),
                "c",
            )
            self.assertEqual(
                tables["no_pk_test"].get("name_column", None),
                "not_an_id",
            )
            self.assertIsNone(tables["person"].get("name_column", None))
            self.assertIsNone(tables["mitigation_type"].get("name_column", None))
            self.assertIsNone(tables["hospital_visit"].get("name_column", None))
            self.assertEqual(
                tables["empty_vocabulary"].get("name_column", None),
                "entry_name",
            )

    def test_print_data(self) -> None:
        """Test that we can print random rows from the table and random data from columns."""
        person_table = self.metadata.tables["person"]
        with self.sync_engine.connect() as conn:
            person_rows = conn.execute(select(person_table)).mappings().fetchall()
            person_data = {row["person_id"]: row for row in person_rows}
            name_set = {row["name"] for row in person_rows}
        person_headings = ["person_id", "name", "research_opt_out", "stored_from"]
        with self._get_cmd({}) as tc:
            tc.do_next("person")
            tc.do_data("")
            self.assertSequenceEqual(tc.headings, person_headings)
            self.assertEqual(len(tc.rows), 10)  # default number of rows is 10
            for row in tc.rows:
                expected = person_data[row[0]]
                self.assertSequenceEqual(row, [expected[h] for h in person_headings])
            tc.reset()
            rows_to_get_count = 6
            tc.do_data(str(rows_to_get_count))
            self.assertSequenceEqual(tc.headings, person_headings)
            self.assertEqual(len(tc.rows), rows_to_get_count)
            for row in tc.rows:
                expected = person_data[row[0]]
                self.assertSequenceEqual(row, [expected[h] for h in person_headings])
            tc.reset()
            to_get_count = 12
            tc.do_data(f"{to_get_count} name")
            self.assertEqual(len(tc.column_items), 1)
            self.assertEqual(len(tc.column_items[0]), to_get_count)
            self.assertLessEqual(set(tc.column_items[0]), name_set)
            tc.reset()
            tc.do_data(f"{to_get_count} name 12")
            self.assertEqual(len(tc.column_items), 1)
            self.assertEqual(len(tc.column_items[0]), to_get_count)
            tc.reset()
            tc.do_data("1000 name 13")
            self.assertEqual(len(tc.column_items), 1)
            self.assertEqual(
                set(tc.column_items[0]), set(filter(lambda n: 13 <= len(n), name_set))
            )
            tc.reset()
            tc.do_data("1000 name 16")
            self.assertEqual(len(tc.column_items), 1)
            self.assertEqual(
                set(tc.column_items[0]), set(filter(lambda n: 16 <= len(n), name_set))
            )

    def test_list_tables(self) -> None:
        """Test that we can list the tables"""
        config = {
            "tables": {
                "unique_constraint_test": {
                    "vocabulary_table": True,
                },
                "no_pk_test": {
                    "ignore": True,
                },
            },
        }
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_ignore("")
            tc.do_next("person")
            tc.do_vocabulary("")
            tc.reset()
            tc.do_tables("")
            person_listed = False
            unique_constraint_test_listed = False
            no_pk_test_listed = False
            for _text, args, _kwargs in tc.messages:
                if args[2] == "person":
                    self.assertFalse(person_listed)
                    person_listed = True
                    self.assertEqual(args[0], "G")
                    self.assertEqual(args[1], "->V")
                elif args[2] == "unique_constraint_test":
                    self.assertFalse(unique_constraint_test_listed)
                    unique_constraint_test_listed = True
                    self.assertEqual(args[0], "V")
                    self.assertEqual(args[1], "->I")
                elif args[2] == "no_pk_test":
                    self.assertFalse(no_pk_test_listed)
                    no_pk_test_listed = True
                    self.assertEqual(args[0], "I")
                    self.assertEqual(args[1], "   ")
                else:
                    self.assertEqual(args[0], "G")
                    self.assertEqual(args[1], "   ")
            self.assertTrue(person_listed)
            self.assertTrue(unique_constraint_test_listed)
            self.assertTrue(no_pk_test_listed)


class ConfigureTablesInstrumentsTests(ConfigureTablesTests):
    """Testing configure-tables with the instrument.sql database."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def test_sanity_checks_both(self) -> None:
        """
        Test ``configure-tables`` sanity checks.
        """
        config = {
            "tables": {
                "model": {
                    "vocabulary_table": True,
                },
                "manufacturer": {
                    "ignore": True,
                },
                "player": {
                    "num_rows_per_pass": 0,
                },
            },
        }
        with self._get_cmd(config) as tc:
            tc.reset()
            tc.do_quit("")
            self.assertEqual(tc.messages[0], (TableCmd.NOTE_TEXT_NO_CHANGES, (), {}))
            self.assertEqual(
                tc.messages[1], (TableCmd.WARNING_TEXT_PROBLEMS_EXIST, (), {})
            )
            self.assertEqual(
                tc.messages[2],
                (
                    TableCmd.WARNING_TEXT_VOCAB_TO_NON_VOCAB,
                    ("model", "manufacturer"),
                    {},
                ),
            )
            self.assertEqual(
                tc.messages[3], (TableCmd.WARNING_TEXT_POTENTIAL_PROBLEMS, (), {})
            )
            self.assertEqual(
                tc.messages[4],
                (
                    TableCmd.WARNING_TEXT_NON_EMPTY_TO_EMPTY,
                    ("signature_model", "player"),
                    {},
                ),
            )

    def test_sanity_checks_warnings_only(self) -> None:
        """
        Test ``configure-tables`` sanity checks.
        """
        config = {
            "tables": {
                "model": {
                    "vocabulary_table": True,
                },
                "manufacturer": {
                    "ignore": True,
                },
                "player": {
                    "num_rows_per_pass": 0,
                },
            },
        }
        with MockTableCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        ) as tc:
            tc.do_next("manufacturer")
            tc.do_vocabulary("")
            tc.reset()
            tc.do_quit("")
            self.assertEqual(
                tc.messages[0],
                (
                    TableCmd.NOTE_TEXT_CHANGING,
                    ("manufacturer", "ignore", "vocabulary"),
                    {},
                ),
            )
            self.assertEqual(
                tc.messages[1], (TableCmd.WARNING_TEXT_POTENTIAL_PROBLEMS, (), {})
            )
            self.assertEqual(
                tc.messages[2],
                (
                    TableCmd.WARNING_TEXT_NON_EMPTY_TO_EMPTY,
                    ("signature_model", "player"),
                    {},
                ),
            )

    def test_sanity_checks_errors_only(self) -> None:
        """
        Test ``configure-tables`` sanity checks.
        """
        config = {
            "tables": {
                "model": {
                    "vocabulary_table": True,
                },
                "manufacturer": {
                    "ignore": True,
                },
                "player": {
                    "num_rows_per_pass": 0,
                },
            },
        }
        with MockTableCmd(
            DbCmd.Settings(self.dsn, self.schema_name, config, self.metadata, None)
        ) as tc:
            tc.do_next("signature_model")
            tc.do_empty("")
            tc.reset()
            tc.do_quit("")
            self.assertEqual(
                tc.messages[0],
                (
                    TableCmd.NOTE_TEXT_CHANGING,
                    ("signature_model", "generate", "empty"),
                    {},
                ),
            )
            self.assertEqual(
                tc.messages[1], (TableCmd.WARNING_TEXT_PROBLEMS_EXIST, (), {})
            )
            self.assertEqual(
                tc.messages[2],
                (
                    TableCmd.WARNING_TEXT_VOCAB_TO_NON_VOCAB,
                    ("model", "manufacturer"),
                    {},
                ),
            )

    def test_see_roles(self) -> None:
        """Test that we can see roles for a particular column."""
        table = "model"
        column = "name"
        with self._get_cmd(
            {"tables": {table: {"columns": {column: {"roles": ["start", "source"]}}}}}
        ) as gc:
            gc.do_next(table)
            self.assertSetEqual(gc.get_roles_from_columns(column), {"start", "source"})
        with self._get_cmd(
            {"tables": {table: {"columns": {column: {"roles": ["start"]}}}}}
        ) as gc:
            gc.do_next(table)
            self.assertSetEqual(gc.get_roles_from_columns(column), {"start"})


class TrickyTests(ConfigureTablesTests):
    """Testing configure-tables with arguments that could cause SQL errors."""

    dump_file_path = "tricky.sql"
    database_name = "tricky"
    schema_name = "public"

    def do_and_test_peek_tricky(self, tc: MockTableCmd) -> None:
        """Peek the "names" table and check the output."""
        tc.reset()
        tc.do_peek("")
        self.assertSetEqual(set(tc.headings), {"id", "offset", "count", "sensible"})
        self.assertSetEqual(
            set(tc.rows), {(1, 10, 5, "reasonable"), (2, None, 6, "clear-headed")}
        )

    def test_peek_with_tricky_names(self) -> None:
        """
        Peek with column names that are function names (#66).
        """
        with self._get_cmd({}) as tc:
            tc.do_next("names")
            self.do_and_test_peek_tricky(tc)

    def test_count_with_tricky_names(self) -> None:
        """
        Count with column names that are function names (#66).
        """
        with self._get_cmd({}) as tc:
            tc.do_next("names")
            self.do_and_test_peek_tricky(tc)
            tc.do_counts("")
            self.assertSequenceEqual(tc.rows, [["offset", 1], ["sensible", 0]])

    def test_incorrect_orm_yaml_columns(self) -> None:
        """
        Peek with incorrect columns in orm.yaml (#70).
        """
        self.metadata = dict_to_metadata(
            {
                "tables": {
                    "names": {
                        "columns": {
                            "id": {
                                "primary": True,
                                "nullable": False,
                                "type": "INTEGER",
                            },
                            "sensible": {
                                "primary": False,
                                "nullable": False,
                                "type": "TEXT",
                            },
                            "nonexistent": {
                                "primary": False,
                                "nullable": False,
                                "type": "TEXT",
                            },
                        }
                    }
                }
            }
        )
        with self._get_cmd({}) as tc:
            tc.reset()
            tc.do_peek("")
            self.assertIn("SQL query", "/".join(m for (m, _a, _kw) in tc.messages))

    def test_repeated_field_does_not_throw_exception(self) -> None:
        """
        Select with repeated fields (#70).
        """
        with MockTableCmd(
            DbCmd.Settings(
                self.dsn,
                self.schema_name,
                config={},
                metadata=self.metadata,
                parquet_dir=None,
            ),
            print_tables=True,
        ) as tc:
            tc.reset()
            tc.do_select('sensible AS same, "offset" AS same FROM names')
            self.assertIn(
                "Failed to display", "/".join(m for (m, _a, _kw) in tc.messages)
            )

    def assert_message_correctly_formatted(
        self, m: str, a: Sized, kw: dict[str, Any]
    ) -> None:
        """
        Assert that the ``{...}`` interpolations in ``m`` match the arguments given.

        :param m: The message.
        :param a: List of positional arguments.
        :param kw: Dict of keyword arguments.
        """
        interpolations = set(re.findall(r"\{([A-Za-z0-9_]+)\}", m))
        positions = set(range(len(a)))
        keywords = set(kw.keys())
        self.assertSetEqual(interpolations, positions | keywords)

    def test_sql_error_does_not_throw_exception(self) -> None:
        """
        Select with a SQL error.
        """
        with self._get_cmd({}) as tc:
            tc.reset()
            tc.do_select("+++")
            self.assertIn("SQL query", "/".join(m for (m, _a, _kw) in tc.messages))
            for m, a, kw in tc.messages:
                self.assert_message_correctly_formatted(m, a, kw)
