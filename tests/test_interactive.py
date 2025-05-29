""" Tests for the base module. """
import copy
import random
import re
from sqlalchemy import select

from datafaker.interactive import DbCmd, TableCmd, GeneratorCmd, MissingnessCmd

from tests.utils import RequiresDBTestCase, GeneratesDBTestCase


class TestDbCmdMixin(DbCmd):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reset()
    def reset(self):
        self.messages: list[tuple[str, list, dict[str, any]]] = []
        self.headings: list[str] = []
        self.rows: list[list[str]] = []
        self.column_items: list[str] = []
        self.columns: dict[str, list[str]] = {}
    def print(self, text: str, *args, **kwargs):
        self.messages.append((text, args, kwargs))
    def print_table(self, headings: list[str], rows: list[list[str]]):
        self.headings = headings
        self.rows = rows
    def print_table_by_columns(self, columns: dict[str, list[str]]):
        self.columns = columns
    def columnize(self, items: list[str]):
        self.column_items.append(items)
    def ask_save(self) -> str:
        return "yes"


class TestTableCmd(TableCmd, TestDbCmdMixin):
    """ TableCmd but mocked """


class ConfigureTablesTests(RequiresDBTestCase):
    """Testing configure-tables."""
    def _get_cmd(self, config) -> TestTableCmd:
        return TestTableCmd(self.dsn, self.schema_name, self.metadata, config)


class ConfigureTablesSrcTests(ConfigureTablesTests):
    """Testing configure-tables with src.dump."""
    dump_file_path = "src.dump"
    database_name = "src"
    schema_name = "public"

    def test_table_name_prompts(self) -> None:
        """Test that the prompts follow the names of the tables."""
        config = {}
        with self._get_cmd(config) as tc:
            table_names = list(self.metadata.tables.keys())
            for t in table_names:
                self.assertIn(t, tc.prompt)
                tc.do_next("")
            self.assertListEqual(tc.messages, [(TableCmd.ERROR_NO_MORE_TABLES, (), {})])
            tc.reset()
            for t in reversed(table_names):
                self.assertIn(t, tc.prompt)
                tc.do_previous("")
            self.assertListEqual(tc.messages, [(TableCmd.ERROR_ALREADY_AT_START, (), {})])
            tc.reset()
            bad_table_name = "notarealtable"
            tc.do_next(bad_table_name)
            self.assertListEqual(tc.messages, [(TableCmd.ERROR_NO_SUCH_TABLE, (bad_table_name,), {})])
            tc.reset()
            good_table_name = table_names[2]
            tc.do_next(good_table_name)
            self.assertListEqual(tc.messages, [])
            self.assertIn(good_table_name, tc.prompt)

    def test_column_display(self) -> None:
        """Test that we can see the names of the columns."""
        config = {}
        with self._get_cmd(config) as tc:
            tc.do_next("unique_constraint_test")
            tc.do_columns("")
            self.assertListEqual(
                tc.rows,
                [
                    ["id", "INTEGER", True, False, ""],
                    ["a", "BOOLEAN", False, False, ""],
                    ["b", "BOOLEAN", False, False, ""],
                    ["c", "TEXT", False, False, ""],
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
            self.assertFalse(tables["unique_constraint_test"].get("vocabulary_table", False))
            self.assertFalse(tables["unique_constraint_test"].get("ignore", False))
            self.assertTrue(tables["unique_constraint_test"].get("primary_private", False))

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
            self.assertFalse(tables["unique_constraint_test"].get("vocabulary_table", False))
            self.assertFalse(tables["unique_constraint_test"].get("ignore", False))
            self.assertTrue(tables["unique_constraint_test"].get("primary_private", False))

    def test_configure_tables(self) -> None:
        """Test that we can change columns to ignore, vocab or generate."""
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
                }
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
            self.assertFalse(tables["unique_constraint_test"].get("vocabulary_table", False))
            self.assertFalse(tables["unique_constraint_test"].get("ignore", False))
            self.assertFalse(tables["unique_constraint_test"].get("primary_private", False))
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

    def test_print_data(self) -> None:
        """Test that we can print random rows from the table and random data from columns."""
        person_table = self.metadata.tables["person"]
        with self.engine.connect() as conn:
            person_rows = conn.execute(select(person_table)).mappings().fetchall()
            person_data = {
                row["person_id"]: row
                for row in person_rows
            }
            name_set = {row["name"] for row in person_rows}
        person_headings = ["person_id", "name", "research_opt_out", "stored_from"]
        with self._get_cmd({}) as tc:
            tc.do_next("person")
            tc.do_data("")
            self.assertListEqual(tc.headings, person_headings)
            self.assertEqual(len(tc.rows), 10)  # default number of rows is 10
            for row in tc.rows:
                expected = person_data[row[0]]
                self.assertListEqual(row, [expected[h] for h in person_headings])
            tc.reset()
            rows_to_get_count = 6
            tc.do_data(str(rows_to_get_count))
            self.assertListEqual(tc.headings, person_headings)
            self.assertEqual(len(tc.rows), rows_to_get_count)
            for row in tc.rows:
                expected = person_data[row[0]]
                self.assertListEqual(row, [expected[h] for h in person_headings])
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
            tc.do_data(f"{to_get_count} name 13")
            self.assertEqual(len(tc.column_items), 1)
            self.assertEqual(set(tc.column_items[0]), set(filter(lambda n: 13 <= len(n), name_set)))
            tc.reset()
            tc.do_data(f"{to_get_count} name 16")
            self.assertEqual(len(tc.column_items), 1)
            self.assertEqual(set(tc.column_items[0]), set(filter(lambda n: 16 <= len(n), name_set)))

    def test_list_tables(self):
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
            for (text, args, kwargs) in tc.messages:
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
    """ Testing configure-tables with the instrument.sql database. """
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def test_sanity_checks_both(self):
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
            self.assertEqual(tc.messages[1], (TableCmd.WARNING_TEXT_PROBLEMS_EXIST, (), {}))
            self.assertEqual(tc.messages[2], (TableCmd.WARNING_TEXT_VOCAB_TO_NON_VOCAB, ("model", "manufacturer"), {}))
            self.assertEqual(tc.messages[3], (TableCmd.WARNING_TEXT_POTENTIAL_PROBLEMS, (), {}))
            self.assertEqual(tc.messages[4], (TableCmd.WARNING_TEXT_NON_EMPTY_TO_EMPTY, ("signature_model", "player"), {}))

    def test_sanity_checks_warnings_only(self):
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
        with TestTableCmd(self.dsn, self.schema_name, self.metadata, config) as tc:
            tc.do_next("manufacturer")
            tc.do_vocabulary("")
            tc.reset()
            tc.do_quit("")
            self.assertEqual(tc.messages[0], (TableCmd.NOTE_TEXT_CHANGING, ("manufacturer", "ignore", "vocabulary"), {}))
            self.assertEqual(tc.messages[1], (TableCmd.WARNING_TEXT_POTENTIAL_PROBLEMS, (), {}))
            self.assertEqual(tc.messages[2], (TableCmd.WARNING_TEXT_NON_EMPTY_TO_EMPTY, ("signature_model", "player"), {}))

    def test_sanity_checks_errors_only(self):
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
        with TestTableCmd(self.dsn, self.schema_name, self.metadata, config) as tc:
            tc.do_next("signature_model")
            tc.do_empty("")
            tc.reset()
            tc.do_quit("")
            self.assertEqual(tc.messages[0], (TableCmd.NOTE_TEXT_CHANGING, ("signature_model", "generate", "empty"), {}))
            self.assertEqual(tc.messages[1], (TableCmd.WARNING_TEXT_PROBLEMS_EXIST, (), {}))
            self.assertEqual(tc.messages[2], (TableCmd.WARNING_TEXT_VOCAB_TO_NON_VOCAB, ("model", "manufacturer"), {}))


class TestGeneratorCmd(GeneratorCmd, TestDbCmdMixin):
    """ GeneratorCmd but mocked """
    def get_proposals(self) -> dict[str, tuple[int, str, str, list[str]]]:
        """
        Returns a dict of generator name to a tuple of (index, fit_string, [list,of,samples])"""
        return {
            kw["name"]: (kw["index"], kw["fit"], kw["sample"].split(", "))
            for (s, _, kw) in self.messages
            if s == self.PROPOSE_GENERATOR_SAMPLE_TEXT
        }


class ConfigureGeneratorsTests(RequiresDBTestCase):
    """ Testing configure-generators. """
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def _get_cmd(self, config) -> TestGeneratorCmd:
        return TestGeneratorCmd(self.dsn, self.schema_name, self.metadata, config)

    def test_null_configuration(self):
        """ Test that the tables having null configuration does not break. """
        config = {
            "tables": None,
        }
        with self._get_cmd(config) as gc:
            TABLE = "model"
            gc.do_next(f"{TABLE}.name")
            gc.do_propose("")
            gc.do_compare("")
            gc.do_set("1")
            gc.do_quit("")
            self.assertEqual(len(gc.config["tables"][TABLE]["row_generators"]), 1)

    def test_null_table_configuration(self):
        """ Test that a table having null configuration does not break. """
        config = {
            "tables": {
                "model": None,
            }
        }
        with self._get_cmd(config) as gc:
            TABLE = "model"
            gc.do_next(f"{TABLE}.name")
            gc.do_propose("")
            gc.do_set("1")
            gc.do_quit("")
            self.assertEqual(len(gc.config["tables"][TABLE]["row_generators"]), 1)

    def test_set_generator_mimesis(self):
        """ Test that we can set one generator to a mimesis generator. """
        with self._get_cmd({}) as gc:
            TABLE = "model"
            COLUMN = "name"
            GENERATOR = "person.first_name"
            gc.do_next(f"{TABLE}.{COLUMN}")
            gc.do_propose("")
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[f"generic.{GENERATOR}"][0]))
            gc.do_quit("")
            self.assertEqual(len(gc.config["tables"][TABLE]["row_generators"]), 1)
            self.assertDictEqual(
                gc.config["tables"][TABLE]["row_generators"][0],
                {"name": f"generic.{GENERATOR}", "columns_assigned": [COLUMN]},
            )

    def test_set_generator_distribution(self):
        """ Test that we can set one generator to gaussian. """
        with self._get_cmd({}) as gc:
            TABLE = "string"
            COLUMN = "frequency"
            GENERATOR = "dist_gen.normal"
            gc.do_next(f"{TABLE}.{COLUMN}")
            gc.do_propose("")
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[GENERATOR][0]))
            gc.do_quit("")
            row_gens = gc.config["tables"][TABLE]["row_generators"]
            self.assertEqual(len(row_gens), 1)
            row_gen = row_gens[0]
            self.assertEqual(row_gen["name"], GENERATOR)
            self.assertListEqual(row_gen["columns_assigned"], [COLUMN])
            self.assertDictEqual(row_gen["kwargs"], {
                "mean": f'SRC_STATS["auto__{TABLE}"][0]["mean__{COLUMN}"]',
                "sd": f'SRC_STATS["auto__{TABLE}"][0]["stddev__{COLUMN}"]',
            })
            self.assertEqual(len(gc.config["src-stats"]), 1)
            self.assertDictEqual(gc.config["src-stats"][0], {
                "name": f"auto__{TABLE}",
                "query": f"SELECT AVG({COLUMN}) AS mean__{COLUMN}, STDDEV({COLUMN}) AS stddev__{COLUMN} FROM {TABLE}",
            })

    def test_set_generator_choice(self):
        """ Test that we can set one generator to uniform choice. """
        with self._get_cmd({}) as gc:
            TABLE = "string"
            COLUMN = "frequency"
            GENERATOR = "dist_gen.choice"
            gc.do_next(f"{TABLE}.{COLUMN}")
            gc.do_propose("")
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[GENERATOR][0]))
            gc.do_quit("")
            row_gens = gc.config["tables"][TABLE]["row_generators"]
            self.assertEqual(len(row_gens), 1)
            row_gen = row_gens[0]
            self.assertEqual(row_gen["name"], GENERATOR)
            self.assertListEqual(row_gen["columns_assigned"], [COLUMN])
            self.assertDictEqual(row_gen["kwargs"], {
                "a": f'SRC_STATS["auto__{TABLE}__{COLUMN}"]',
            })
            self.assertEqual(len(gc.config["src-stats"]), 1)
            self.assertDictEqual(gc.config["src-stats"][0], {
                "name": f"auto__{TABLE}__{COLUMN}",
                "query": f"SELECT {COLUMN} AS value FROM {TABLE} GROUP BY value ORDER BY COUNT({COLUMN}) DESC",
            })

    def test_old_generators_remain(self):
        """ Test that we can set one generator and keep an old one. """
        config = {
            "tables": {
                "string": {
                    "row_generators": [{
                        "name": "dist_gen.normal",
                        "columns_assigned": ["frequency"],
                        "kwargs": {
                            "mean": 'SRC_STATS["auto__string"][0]["mean__frequency"]',
                            "sd": 'SRC_STATS["auto__string"][0]["stddev__frequency"]',
                        },
                    }]
                }
            },
            "src-stats": [{
                "name": "auto__string",
                "query": 'SELECT AVG(frequency) AS mean__frequency, STDDEV(frequency) AS stddev__frequency FROM string',
            }]
        }
        with self._get_cmd(config) as gc:
            TABLE = "model"
            COLUMN = "name"
            GENERATOR = "person.first_name"
            gc.do_next(f"{TABLE}.{COLUMN}")
            gc.do_propose("")
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[f"generic.{GENERATOR}"][0]))
            gc.do_quit("")
            self.assertEqual(len(gc.config["tables"][TABLE]["row_generators"]), 1)
            self.assertDictEqual(
                gc.config["tables"][TABLE]["row_generators"][0],
                {"name": f"generic.{GENERATOR}", "columns_assigned": [COLUMN]},
            )
            row_gens = gc.config["tables"]["string"]["row_generators"]
            self.assertEqual(len(row_gens), 1)
            row_gen = row_gens[0]
            self.assertEqual(row_gen["name"], "dist_gen.normal")
            self.assertListEqual(row_gen["columns_assigned"], ["frequency"])
            self.assertDictEqual(row_gen["kwargs"], {
                "mean": 'SRC_STATS["auto__string"][0]["mean__frequency"]',
                "sd": 'SRC_STATS["auto__string"][0]["stddev__frequency"]',
            })
            self.assertEqual(len(gc.config["src-stats"]), 1)
            self.assertDictEqual(gc.config["src-stats"][0], {
                "name": "auto__string",
                "query": "SELECT AVG(frequency) AS mean__frequency, STDDEV(frequency) AS stddev__frequency FROM string",
            })
    
    def test_aggregate_queries_merge(self):
        """
        Test that we can set a generator that requires select aggregate clauses
        and keep an old one, resulting in a merged query.
        """
        config = {
            "tables": {
                "string": {
                    "row_generators": [{
                        "name": "dist_gen.normal",
                        "columns_assigned": ["frequency"],
                        "kwargs": {
                            "mean": 'SRC_STATS["auto__string"][0]["mean__frequency"]',
                            "sd": 'SRC_STATS["auto__string"][0]["stddev__frequency"]',
                        },
                    }]
                }
            },
            "src-stats": [{
                "name": "auto__string",
                "query": 'SELECT AVG(frequency) AS mean__frequency, STDDEV(frequency) AS stddev__frequency FROM string',
            }]
        }
        with self._get_cmd(copy.deepcopy(config)) as gc:
            COLUMN = "position"
            GENERATOR = "dist_gen.uniform_ms"
            gc.do_next(f"string.{COLUMN}")
            gc.do_propose("")
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[f"{GENERATOR}"][0]))
            gc.do_quit("")
            row_gens: list[dict[str,any]] = gc.config["tables"]["string"]["row_generators"]
            self.assertEqual(len(row_gens), 2)
            if row_gens[0]["name"] == GENERATOR:
                row_gen0 = row_gens[0]
                row_gen1 = row_gens[1]
            else:
                row_gen0 = row_gens[1]
                row_gen1 = row_gens[0]
            self.assertEqual(row_gen0["name"], GENERATOR)
            self.assertEqual(row_gen1["name"], "dist_gen.normal")
            self.assertListEqual(row_gen0["columns_assigned"], [COLUMN])
            self.assertDictEqual(row_gen0["kwargs"], {
                "mean": f'SRC_STATS["auto__string"][0]["mean__{COLUMN}"]',
                "sd": f'SRC_STATS["auto__string"][0]["stddev__{COLUMN}"]',
            })
            self.assertListEqual(row_gen1["columns_assigned"], ["frequency"])
            self.assertDictEqual(row_gen1["kwargs"], {
                "mean": 'SRC_STATS["auto__string"][0]["mean__frequency"]',
                "sd": 'SRC_STATS["auto__string"][0]["stddev__frequency"]',
            })
            self.assertEqual(len(gc.config["src-stats"]), 1)
            self.assertEqual(gc.config["src-stats"][0]["name"], "auto__string")
            select_match = re.match(r'SELECT (.*) FROM string', gc.config["src-stats"][0]["query"])
            self.assertIsNotNone(select_match, "src_stats[0].query is not an aggregate select")
            self.assertSetEqual(set(select_match.group(1).split(", ")), {
                "AVG(frequency) AS mean__frequency",
                "STDDEV(frequency) AS stddev__frequency",
                f"AVG({COLUMN}) AS mean__{COLUMN}",
                f"STDDEV({COLUMN}) AS stddev__{COLUMN}",
            })

    def test_next_completion(self):
        """ Test tab completion for the next command. """
        with self._get_cmd({}) as gc:
            self.assertSetEqual(
                set(gc.complete_next("m", "next m", 5, 6)),
                {"manufacturer", "model"},
            )
            self.assertSetEqual(
                set(gc.complete_next("model", "next model", 5, 10)),
                {"model", "model."},
            )
            self.assertSetEqual(
                set(gc.complete_next("string.", "next string.", 5, 11)),
                {"string.id", "string.model_id", "string.position", "string.frequency"},
            )
            self.assertSetEqual(
                set(gc.complete_next("string.p", "next string.p", 5, 12)),
                {"string.position"},
            )
            self.assertListEqual(gc.complete_next("string.q", "next string.q", 5, 12), [])
            self.assertListEqual(gc.complete_next("ww", "next ww", 5, 7), [])

    def test_compare_reports_privacy(self):
        """
        Test that compare reports whether the current table is primary private,
        secondary private or not private.
        """
        config = {
            "tables": {
                "model": {
                    "primary_private": True,
                }
            },
        }
        with self._get_cmd(config) as gc:
            gc.do_next("manufacturer")
            gc.reset()
            gc.do_compare("")
            (text, args, _kwargs) = gc.messages[0]
            self.assertEqual(text, gc.NOT_PRIVATE_TEXT)
            gc.do_next("model")
            gc.reset()
            gc.do_compare("")
            (text, args, _kwargs) = gc.messages[0]
            self.assertEqual(text, gc.PRIMARY_PRIVATE_TEXT)
            gc.do_next("string")
            gc.reset()
            gc.do_compare("")
            (text, args, _kwargs) = gc.messages[0]
            self.assertEqual(text, gc.SECONDARY_PRIVATE_TEXT)
            self.assertSequenceEqual(args, [["model"]])

    def test_existing_configuration_remains(self):
        """
        Test setting a generator does not remove other information.
        """
        config = {
            "tables": {
                "string": {
                    "primary_private": True,
                }
            },
            "src-stats": [{
                "name": "kraken",
                "query": 'SELECT MAX(frequency) AS max_frequency FROM string',
            }]
        }
        with self._get_cmd(config) as gc:
            COLUMN = "position"
            GENERATOR = "dist_gen.uniform_ms"
            gc.do_next(f"string.{COLUMN}")
            gc.do_propose("")
            proposals = gc.get_proposals()
            gc.do_set(str(proposals[f"{GENERATOR}"][0]))
            gc.do_quit("")
            src_stats = {
                stat["name"]: stat["query"]
                for stat in gc.config["src-stats"]
            }
            self.assertEqual(src_stats["kraken"], config["src-stats"][0]["query"])
            self.assertTrue(gc.config["tables"]["string"]["primary_private"])

    def test_empty_tables_are_not_configured(self):
        """ Test that tables marked as empty are not configured. """
        config = {
            "tables": {
                "string": {
                    "num_rows_per_pass": 0,
                }
            },
        }
        with self._get_cmd(copy.deepcopy(config)) as gc:
            gc.do_tables("")
            table_names = { m[1][0] for m in gc.messages }
            self.assertIn("model", table_names)
            self.assertNotIn("string", table_names)


class TestMissingnessCmd(MissingnessCmd, TestDbCmdMixin):
    """ MissingnessCmd but mocked """

class ConfigureMissingnessTests(RequiresDBTestCase):
    """ Testing configure-missing. """
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def _get_cmd(self, config) -> TestMissingnessCmd:
        return TestMissingnessCmd(self.dsn, self.schema_name, self.metadata, config)

    def test_set_missingness_to_sampled(self):
        """ Test that we can set one table to sampled missingness. """
        with self._get_cmd({}) as mc:
            TABLE = "signature_model"
            mc.do_next(TABLE)
            mc.do_counts("")
            self.assertListEqual(mc.messages, [(MissingnessCmd.ROW_COUNT_MSG, (6,), {})])
            self.assertListEqual(mc.rows, [['player_id', 3], ['based_on', 2]])
            mc.do_sampled("")
            mc.do_quit("")
            self.assertDictEqual(
                mc.config,
                { "tables": {TABLE: {"missingness_generators": [{
                    "columns": ["player_id", "based_on"],
                    "kwargs": {"patterns": 'SRC_STATS["missing_auto__signature_model__0"]'},
                    "name": "column_presence.sampled",
                }]}},
                    "src-stats": [{
                      "name": "missing_auto__signature_model__0",
                      "query": ("SELECT COUNT(*) AS row_count, player_id__is_null, based_on__is_null FROM"
                                " (SELECT player_id IS NULL AS player_id__is_null, based_on IS NULL AS based_on__is_null FROM"
                                " signature_model ORDER BY RANDOM() LIMIT 1000) AS __t GROUP BY player_id__is_null, based_on__is_null")
                    }]
                }
            )


class ConfigureMissingnessTests(GeneratesDBTestCase):
    """ Testing configure-missing with generation. """
    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def _get_cmd(self, config) -> TestMissingnessCmd:
        return TestMissingnessCmd(self.dsn, self.schema_name, self.metadata, config)

    def test_create_with_missingness(self):
        """ Test that we can sample real missingness and reproduce it. """
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
        with self.engine.connect() as conn:
            stmt = select(self.metadata.tables[table_name])
            rows = conn.execute(stmt).mappings().fetchall()
            patterns: set[int] = set()
            for row in rows:
                p = 0 if row["player_id"] is None else 1
                b = 0 if row["based_on"] is None else 2
                patterns.add(p + b)
            # all pattern possibilities should be present
            self.assertSetEqual(patterns, {0, 1, 2, 3})
