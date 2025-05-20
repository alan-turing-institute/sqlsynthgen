from abc import ABC, abstractmethod
import cmd
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
import logging
from prettytable import PrettyTable
import re
import sqlalchemy
from sqlalchemy import Column, MetaData, Table, text

from sqlsynthgen.generators import everything_factory, Generator, PredefinedGenerator
from sqlsynthgen.utils import create_db_engine, primary_private_fks, table_is_private

logger = logging.getLogger(__name__)

def or_default(v, d):
    return d if v is None else v

class TableType(Enum):
    NORMAL = "normal"
    IGNORE = "ignore"
    VOCABULARY = "vocabulary"
    PRIVATE = "private"
    EMPTY = "empty"

TYPE_LETTER = {
    TableType.NORMAL: " ",
    TableType.IGNORE: "I",
    TableType.VOCABULARY: "V",
    TableType.PRIVATE: "P",
    TableType.EMPTY: "e",
}

TYPE_PROMPT = {
    TableType.NORMAL: "(table: {}) ",
    TableType.IGNORE: "(table: {} (ignore)) ",
    TableType.VOCABULARY: "(table: {} (vocab)) ",
    TableType.PRIVATE: "(table: {} (private)) ",
    TableType.EMPTY: "(table: {} (empty))",
}

@dataclass
class TableEntry:
    name: str  # name of the table


class AskSaveCmd(cmd.Cmd):
    intro = "Do you want to save this configuration?"
    prompt = "(yes/no/cancel) "
    file = None
    def __init__(self):
        super().__init__()
        self.result = ""
    def do_yes(self, _arg):
        self.result = "yes"
        return True
    def do_no(self, _arg):
        self.result = "no"
        return True
    def do_cancel(self, _arg):
        self.result = "cancel"
        return True


class DbCmd(ABC, cmd.Cmd):
    ERROR_NO_MORE_TABLES = "Error: There are no more tables"
    ERROR_ALREADY_AT_START = "Error: Already at the start"
    ERROR_NO_SUCH_TABLE = "Error: '{0}' is not the name of a table in this database"
    ROW_COUNT_MSG = "Total row count: {}"

    @abstractmethod
    def make_table_entry(self, name: str, table_config: Mapping) -> TableEntry:
        ...

    def __init__(self, src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.table_entries: list[TableEntry] = []
        tables_config: Mapping = config.get("tables", {})
        if type(tables_config) is not dict:
            tables_config = {}
        for name in metadata.tables.keys():
            table_config = tables_config.get(name, {})
            if type(table_config) is not dict:
                table_config = {}
            entry = self.make_table_entry(name, table_config)
            if entry is not None:
                self.table_entries.append(entry)
        self.table_index = 0
        self.engine = create_db_engine(src_dsn, schema_name=src_schema)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.engine.dispose()

    def print(self, text: str, *args, **kwargs):
        print(text.format(*args, **kwargs))
    def print_table(self, headings: list[str], rows: list[list[str]]):
        output = PrettyTable()
        output.field_names = headings
        for row in rows:
            output.add_row(row)
        print(output)
    def print_table_by_columns(self, columns: dict[str, list[str]]):
        output = PrettyTable()
        row_count = max([len(col) for col in columns.values()])
        for field_name, data in columns.items():
            output.add_column(field_name, data + [None] * (row_count - len(data)))
        print(output)
    def print_results(self, result):
        self.print_table(
            list(result.keys()),
            [list(row) for row in result.all()]
        )
    def ask_save(self):
        ask = AskSaveCmd()
        ask.cmdloop()
        return ask.result

    def set_table_index(self, index) -> bool:
        if 0 <= index and index < len(self.table_entries):
            self.table_index = index
            self.set_prompt()
            return True
        return False
    def next_table(self, report="No more tables"):
        if not self.set_table_index(self.table_index + 1):
            self.print(report)
            return False
        return True
    def table_name(self):
        return self.table_entries[self.table_index].name
    def table_metadata(self) -> Table:
        return self.metadata.tables[self.table_name()]
    def report_columns(self):
        self.print_table(["name", "type", "primary", "nullable", "foreign key"], [
            [name, str(col.type), col.primary_key, col.nullable, ", ".join([fk.column.name for fk in col.foreign_keys])]
            for name, col in self.table_metadata().columns.items()
        ])
    def get_table_config(self, table_name: str) -> dict[str, any]:
        ts = self.config.get("tables", None)
        if type(ts) is not dict:
            return {}
        t = ts.get(table_name)
        return t if type(t) is dict else {}
    def set_table_config(self, table_name: str, config: dict[str, any]):
        ts = self.config.get("tables", None)
        if type(ts) is not dict:
            self.config["tables"] = {table_name: config}
            return
        ts[table_name] = config
    def _remove_prefix_src_stats(self, prefix: str) -> list[dict[str, any]]:
        src_stats = self.config.get("src-stats", [])
        new_src_stats = []
        for stat in src_stats:
            if not stat.get("name", "").startswith(prefix):
                new_src_stats.append(stat)
        self.config["src-stats"] = new_src_stats
        return new_src_stats
    def get_nonnull_columns(self, table_name: str):
        metadata_table = self.metadata.tables[table_name]
        return [
            str(name)
            for name, column in metadata_table.columns.items()
            if column.nullable
        ]

    def do_counts(self, _arg):
        "Report the column names with the counts of nulls in them"
        if len(self.table_entries) <= self.table_index:
            return
        table_name = self.table_name()
        nonnull_columns = self.get_nonnull_columns(table_name)
        colcounts = [
            ", COUNT({0}) AS {0}".format(nnc)
            for nnc in nonnull_columns
        ]
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT COUNT(*) AS row_count{colcounts} FROM {table}".format(
                    table=table_name,
                    colcounts="".join(colcounts),
                ))
            ).first()
            if result is None:
                self.print("Could not count rows in table {0}", table_name)
                return
            row_count = result.row_count
            self.print(self.ROW_COUNT_MSG, row_count)
            self.print_table(["Column", "NULL count"], [
                [name, row_count - count]
                for name, count in result._mapping.items()
                if name != "row_count"
            ])

    def do_select(self, arg):
        "Run a select query over the database and show the first 50 results"
        MAX_SELECT_ROWS = 50
        with self.engine.connect() as connection:
            try:
                result = connection.execute(
                    text("SELECT " + arg)
                )
            except sqlalchemy.exc.DatabaseError as exc:
                self.print("Failed to execute: {}", exc)
                return
            row_count = result.rowcount
            self.print(self.ROW_COUNT_MSG, row_count)
            if 50 < row_count:
                self.print("Showing the first {} rows", MAX_SELECT_ROWS)
            fields = list(result.keys())
            rows = [
                row.tuple()
                for row in result.fetchmany(MAX_SELECT_ROWS)
            ]
            self.print_table(fields, rows)

    def do_peek(self, arg: str):
        """Use 'peek col1 col2 col3' to see a sample of values from columns col1, col2 and col3 in the current table."""
        MAX_PEEK_ROWS = 25
        if len(self.table_entries) <= self.table_index:
            return
        table_name = self.table_name()
        col_names = arg.split()
        nonnulls = [cn + " IS NOT NULL" for cn in col_names]
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT {cols} FROM {table} WHERE {nonnull} LIMIT {max}".format(
                    cols=",".join(col_names),
                    table=table_name,
                    nonnull=" AND ".join(nonnulls),
                    max=MAX_PEEK_ROWS,
                ))
            )
            rows = [
                row.tuple()
                for row in result.fetchmany(MAX_PEEK_ROWS)
            ]
            self.print_table(list(result.keys()), rows)

    def complete_peek(self, text: str, _line: str, _begidx: int, _endidx: int):
        if len(self.table_entries) <= self.table_index:
            return []
        return [
            col
            for col in self.table_metadata().columns.keys()
            if col.startswith(text)
        ]


@dataclass
class TableCmdTableEntry(TableEntry):
    old_type: TableType
    new_type: TableType

class TableCmd(DbCmd):
    intro = "Interactive table configuration (ignore, vocabulary, private, normal or empty). Type ? for help.\n"
    doc_leader = """Use the commands 'ignore', 'vocabulary',
'private', 'empty' or 'normal' to set the table's type. Use 'next' or
'previous' to change table. Use 'tables' and 'columns' for
information about the database. Use 'data', 'peek', 'select' or
'count' to see some data contained in the current table. Use 'quit'
to exit this program."""
    prompt = "(tableconf) "
    file = None

    def make_table_entry(self, name: str, table: Mapping) -> TableEntry:
        if table.get("ignore", False):
            return TableCmdTableEntry(name, TableType.IGNORE, TableType.IGNORE)
        if table.get("vocabulary_table", False):
            return TableCmdTableEntry(name, TableType.VOCABULARY, TableType.VOCABULARY)
        if table.get("primary_private", False):
            return TableCmdTableEntry(name, TableType.PRIVATE, TableType.PRIVATE)
        if table.get("num_rows_per_pass", 1) == 0:
            return TableCmdTableEntry(name, TableType.EMPTY, TableType.EMPTY)
        return TableCmdTableEntry(name, TableType.NORMAL, TableType.NORMAL)

    def __init__(self, src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
        super().__init__(src_dsn, src_schema, metadata, config)
        self.set_prompt()

    def set_prompt(self):
        if self.table_index < len(self.table_entries):
            entry = self.table_entries[self.table_index]
            self.prompt = TYPE_PROMPT[entry.new_type].format(entry.name)
        else:
            self.prompt = "(table) "
    def set_type(self, t_type: TableType):
        if self.table_index < len(self.table_entries):
            entry = self.table_entries[self.table_index]
            entry.new_type = t_type
    def _copy_entries(self) -> None:
        for entry in self.table_entries:
            if entry.old_type != entry.new_type:
                table = self.get_table_config(entry.name)
                if entry.old_type == TableType.EMPTY and table.get("num_rows_per_pass", 1) == 0:
                    table["num_rows_per_pass"] = 1
                if entry.new_type == TableType.IGNORE:
                    table["ignore"] = True
                    table.pop("vocabulary_table", None)
                    table.pop("primary_private", None)
                elif entry.new_type == TableType.VOCABULARY:
                    table.pop("ignore", None)
                    table["vocabulary_table"] = True
                    table.pop("primary_private", None)
                elif entry.new_type == TableType.PRIVATE:
                    table.pop("ignore", None)
                    table.pop("vocabulary_table", None)
                    table["primary_private"] = True
                elif entry.new_type == TableType.EMPTY:
                    table.pop("ignore", None)
                    table.pop("vocabulary_table", None)
                    table.pop("primary_private", None)
                    table["num_rows_per_pass"] = 0
                else:
                    table.pop("ignore", None)
                    table.pop("vocabulary_table", None)
                    table.pop("primary_private", None)
                self.set_table_config(entry.name, table)

    def do_quit(self, _arg):
        "Check the updates, save them if desired and quit the configurer."
        count = 0
        for entry in self.table_entries:
            if entry.old_type != entry.new_type:
                count += 1
                self.print(
                    "Changing {0} from {1} to {2}",
                    entry.name,
                    entry.old_type.value,
                    entry.new_type.value,
                )
        if count == 0:
            self.print("You have made no changes.")
        reply = self.ask_save()
        if reply == "yes":
            self._copy_entries()
            return True
        if reply == "no":
            return True
        return False
    def do_tables(self, arg):
        "list the tables with their types"
        for entry in self.table_entries:
            old = entry.old_type
            new = entry.new_type
            becomes = "   " if old == new else "->" + TYPE_LETTER[new]
            self.print("{0}{1} {2}", TYPE_LETTER[old], becomes, entry.name)
    def do_next(self, arg):
        "'next' = go to the next table, 'next tablename' = go to table 'tablename'"
        if arg:
            # Find the index of the table called _arg, if any
            index = next((i for i,entry in enumerate(self.table_entries) if entry.name == arg), None)
            if index is None:
                self.print(self.ERROR_NO_SUCH_TABLE, arg)
                return
            self.set_table_index(index)
            return
        self.next_table(self.ERROR_NO_MORE_TABLES)
    def complete_next(self, text, line, begidx, endidx):
        return [
            entry.name
            for entry in self.table_entries
            if entry.name.startswith(text)
        ]
    def do_previous(self, _arg):
        "Go to the previous table"
        if not self.set_table_index(self.table_index - 1):
            self.print(self.ERROR_ALREADY_AT_START)
    def do_ignore(self, _arg):
        "Set the current table as ignored, and go to the next table"
        self.set_type(TableType.IGNORE)
        self.print("Table {} set as ignored", self.table_name())
        self.next_table()
    def do_vocabulary(self, _arg):
        "Set the current table as a vocabulary table, and go to the next table"
        self.set_type(TableType.VOCABULARY)
        self.print("Table {} set to be a vocabulary table", self.table_name())
        self.next_table()
    def do_private(self, _arg):
        "Set the current table as a primary private table (such as the table of patients)"
        self.set_type(TableType.PRIVATE)
        self.print("Table {} set to be a primary private table", self.table_name())
        self.next_table()
    def do_normal(self, _arg):
        "Set the current table as neither a vocabulary table nor ignored nor primary private, and go to the next table"
        self.set_type(TableType.NORMAL)
        self.print("Table {} normal", self.table_name())
        self.next_table()
    def do_empty(self, _arg):
        "Set the current table as empty; no generators will be run for it"
        self.set_type(TableType.EMPTY)
        self.print("Table {} empty", self.table_name())
        self.next_table()
    def do_columns(self, _arg):
        "Report the column names and metadata"
        self.report_columns()
    def do_data(self, arg: str):
        """
        Report some data.
        'data' = report a random ten lines,
        'data 20' = report a random 20 lines,
        'data 20 ColumnName' = report a random twenty entries from ColumnName,
        'data 20 ColumnName 30' = report a random twenty entries from ColumnName of length at least 30,
        """
        args = arg.split()
        column = None
        number = None
        arg_index = 0
        min_length = 0
        table_metadata = self.table_metadata()
        if arg_index < len(args) and args[arg_index].isdigit():
            number = int(args[arg_index])
            arg_index += 1
        if arg_index < len(args) and args[arg_index] in table_metadata.columns:
            column = args[arg_index]
            arg_index += 1
        if arg_index < len(args) and args[arg_index].isdigit():
            min_length = int(args[arg_index])
            arg_index += 1
        if arg_index != len(args):
            self.print(
                """Did not understand these arguments
The format is 'data [entries] [column-name [minimum-length]]' where [] means optional text.
Type 'columns' to find out valid column names for this table.
Type 'help data' for examples."""
            )
            return
        if column is None:
            if number is None:
                number = 10
            self.print_row_data(number)
        else:
            if number is None:
                number = 48
            self.print_column_data(column, number, min_length)
    def complete_data(self, text, line, begidx, endidx):
        previous_parts = line[:begidx - 1].split()
        if len(previous_parts) != 2:
            return []
        table_metadata = self.table_metadata()
        return [
            k for k in table_metadata.columns.keys()
            if k.startswith(text)
        ]

    def print_column_data(self, column: str, count: int, min_length: int):
        where = f"WHERE {column} IS NOT NULL"
        if 0 < min_length:
            where = "WHERE LENGTH({column}) >= {len}".format(
                column=column,
                len=min_length,
            )
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT {column} FROM {table} {where} ORDER BY RANDOM() LIMIT {count}".format(
                    table=self.table_name(),
                    column=column,
                    count=count,
                    where=where,
                ))
            )
            self.columnize([str(x[0]) for x in result.all()])

    def print_row_data(self, count: int):
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT * FROM {table} ORDER BY RANDOM() LIMIT {count}".format(
                    table=self.table_name(),
                    count=count,
                ))
            )
            if result is None:
                self.print("No rows in this table!")
                return
            self.print_results(result)


def update_config_tables(src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
    with TableCmd(src_dsn, src_schema, metadata, config) as tc:
        tc.cmdloop()
        return tc.config


@dataclass
class MissingnessType:
    SAMPLED="column_presence.sampled"
    SAMPLED_QUERY=(
        "SELECT COUNT(*) AS row_count, {result_names} FROM "
        "(SELECT {column_is_nulls} FROM {table} ORDER BY RANDOM() LIMIT {count})"
        " AS __t GROUP BY {result_names}"
    )
    name: str
    query: str
    columns: list[str]
    @classmethod
    def sampled_query(cls, table, count, column_names):
        result_names = ", ".join([
            "{0}__is_null".format(c)
            for c in column_names
        ])
        column_is_nulls = ", ".join([
            "{0} IS NULL AS {0}__is_null".format(c)
            for c in column_names
        ])
        return cls.SAMPLED_QUERY.format(
            result_names=result_names,
            column_is_nulls=column_is_nulls,
            table=table,
            count=count,
        )


@dataclass
class MissingnessCmdTableEntry(TableEntry):
    old_type: MissingnessType
    new_type: MissingnessType


class MissingnessCmd(DbCmd):
    intro = "Interactive missingness configuration. Type ? for help.\n"
    doc_leader = """Use commands 'sampled' and 'none' to choose the missingness style for
the current table. Use commands 'next' and 'previous' to change the
current table. Use 'tables' to list the tables and 'count' to show
how many NULLs exist in each column. Use 'peek' or 'select' to see
data from the database. Use 'quit' to exit this tool."""
    prompt = "(missingness) "
    file = None
    PATTERN_RE = re.compile(r'SRC_STATS\["([^"]*)"\]')

    def find_missingness_query(self, missingness_generator: Mapping):
        kwargs = missingness_generator.get("kwargs", {})
        patterns = kwargs.get("patterns", "")
        pattern_match = self.PATTERN_RE.match(patterns)
        if pattern_match:
            key = pattern_match.group(1)
            for src_stat in self.config["src-stats"]:
                if src_stat.get("name") == key:
                    return src_stat.get("query", None)
            return None
    def make_table_entry(self, name: str, table: Mapping) -> TableEntry:
        if table.get("ignore", False):
            return None
        if table.get("vocabulary_table", False):
            return None
        if table.get("num_passes", 1) == 0:
            return None
        mgs = table.get("missingness_generators", [])
        old = None
        nonnull_columns = self.get_nonnull_columns(name)
        if not nonnull_columns:
            return None
        if not mgs:
            old = MissingnessType(
                name="none",
                query="",
                columns=[],
            )
        elif len(mgs) == 1:
            mg = mgs[0]
            mg_name = mg.get("name", None)
            if mg_name is not None:
                query = self.find_missingness_query(mg)
                if query is not None:
                    old = MissingnessType(
                        name=mg_name,
                        query=query,
                        columns=mg.get("columns_assigned", []),
                    )
        if old is None:
            return None
        return MissingnessCmdTableEntry(
            name=name,
            old_type=old,
            new_type=old,
        )

    def __init__(self, src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
        super().__init__(src_dsn, src_schema, metadata, config)
        self.set_prompt()

    def set_prompt(self):
        if self.table_index < len(self.table_entries):
            entry: MissingnessCmdTableEntry = self.table_entries[self.table_index]
            nt = entry.new_type
            if nt is None:
                self.prompt = "(missingness for {0}) ".format(entry.name)
            else:
                self.prompt = "(missingness for {0}: {1}) ".format(entry.name, nt.name)
        else:
            self.prompt = "(missingness) "
    def set_type(self, t_type: TableType):
        if self.table_index < len(self.table_entries):
            entry = self.table_entries[self.table_index]
            entry.new_type = t_type
    def _copy_entries(self) -> None:
        src_stats = self._remove_prefix_src_stats("missing_auto__")
        for entry in self.table_entries:
            entry: MissingnessCmdTableEntry
            table = self.get_table_config(entry.name)
            if entry.new_type is None or entry.new_type.name == "none":
                table.pop("missingness_generators", None)
            else:
                src_stat_key = "missing_auto__{0}__0".format(entry.name)
                table["missingness_generators"] = [{
                    "name": entry.new_type.name,
                    "kwargs": {"patterns": 'SRC_STATS["{0}"]'.format(src_stat_key)},
                    "columns": entry.new_type.columns,
                }]
                src_stats.append({
                    "name": src_stat_key,
                    "query": entry.new_type.query,
                })
            self.set_table_config(entry.name, table)

    def do_quit(self, _arg):
        "Check the updates, save them if desired and quit the configurer."
        count = 0
        for entry in self.table_entries:
            if entry.old_type != entry.new_type:
                count += 1
                if entry.old_type is None:
                    self.print("Putting generator {0} on table {1}", entry.name, entry.new_type.name)
                elif entry.new_type is None:
                    self.print("Deleting generator {1} from table {0}", entry.name, entry.old_type.name)
                else:
                    self.print(
                        "Changing {0} from {1} to {2}",
                        entry.name,
                        entry.old_type.name,
                        entry.new_type.name,
                    )
        if count == 0:
            self.print("You have made no changes.")
        reply = self.ask_save()
        if reply == "yes":
            self._copy_entries()
            return True
        if reply == "no":
            return True
        return False
    def do_tables(self, arg):
        "list the tables with their types"
        for entry in self.table_entries:
            old = "-" if entry.old_type is None else entry.old_type.name
            new = "-" if entry.new_type is None else entry.new_type.name
            desc = new if old == new else "{0}->{1}".format(old, new)
            self.print("{0} {1}", entry.name, desc)
    def do_next(self, arg):
        "'next' = go to the next table, 'next tablename' = go to table 'tablename'"
        if arg:
            # Find the index of the table called _arg, if any
            index = next((i for i,entry in enumerate(self.table_entries) if entry.name == arg), None)
            if index is None:
                self.print(self.ERROR_NO_SUCH_TABLE, arg)
                return
            self.set_table_index(index)
            return
        self.next_table(self.ERROR_NO_MORE_TABLES)
    def complete_next(self, text, line, begidx, endidx):
        return [
            entry.name
            for entry in self.table_entries
            if entry.name.startswith(text)
        ]
    def do_previous(self, _arg):
        "Go to the previous table"
        if not self.set_table_index(self.table_index - 1):
            self.print(self.ERROR_ALREADY_AT_START)
    def _set_type(self, name, query):
        if len(self.table_entries) <= self.table_index:
            return
        entry: MissingnessCmdTableEntry = self.table_entries[self.table_index]
        entry.new_type = MissingnessType(
            name=name,
            query=query,
            columns=self.get_nonnull_columns(entry.name),
        )
    def _set_none(self):
        if len(self.table_entries) <= self.table_index:
            return
        entry: MissingnessCmdTableEntry = self.table_entries[self.table_index]
        entry.new_type = None
    def do_sampled(self, arg: str):
        """
        Set the current table missingness as 'sampled', and go to the next table.
        "sampled 3000" means sample 3000 rows at random and choose the missingness
        to be the same as one of those 3000 at random.
        "sampled" means the same, but with a default number of rows sampled (1000).
        """
        if len(self.table_entries) <= self.table_index:
            self.print("Error! not on a table")
            return
        entry: MissingnessCmdTableEntry = self.table_entries[self.table_index]
        if arg == "":
            count = 1000
        elif arg.isdecimal():
            count = int(arg)
        else:
            self.print("Error: sampled can be used alone or with an integer argument. {0} is not permitted", arg)
            return
        self._set_type(
            MissingnessType.SAMPLED,
            MissingnessType.sampled_query(
                entry.name,
                count,
                self.get_nonnull_columns(entry.name),
            ),
        )
        self.print("Table {} set to sampled missingness", self.table_name())
        self.next_table()
    def do_none(self, _arg):
        "Set the current table to have no missingness, and go to the next table"
        self._set_none()
        self.print("Table {} set to have no missingness", self.table_name())
        self.next_table()


def update_missingness(src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
    with MissingnessCmd(src_dsn, src_schema, metadata, config) as mc:
        mc.cmdloop()
        return mc.config


@dataclass
class GeneratorInfo:
    column: str
    is_primary_key: bool
    old_gen: Generator | None
    new_gen: Generator | None

@dataclass
class GeneratorCmdTableEntry(TableEntry):
    generators: list[GeneratorInfo]

class GeneratorCmd(DbCmd):
    intro = "Interactive generator configuration. Type ? for help.\n"
    doc_leader = """Use command 'propose' for a list of generators applicable to the
current column, then command 'compare' to see how these perform
against the source data, then command 'set' to choose your favourite.
Use 'unset' to remove the column's generator. Use commands 'next' and
'previous' to change which column we are examining. Use 'info'
for useful information about the current column. Use 'tables' and
'list' to see available tables and columns. Use 'columns' to see
information about the columns in the current table. Use 'peek',
'count' or 'select' to fetch data from the source database. Use
'quit' to exit this program."""
    prompt = "(generatorconf) "
    file = None

    PROPOSE_SOURCE_SAMPLE_TEXT = "Sample of actual source data: {0}..."
    PROPOSE_SOURCE_EMPTY_TEXT = "Source database has no data in this column."
    PROPOSE_GENERATOR_SAMPLE_TEXT = "{index}. {name}: {fit} {sample} ..."
    PRIMARY_PRIVATE_TEXT = "Primary Private"
    SECONDARY_PRIVATE_TEXT = "Secondary Private on columns {0}"
    NOT_PRIVATE_TEXT = "Not private"

    def make_table_entry(self, table_name: str, table: Mapping) -> TableEntry | None:
        if table.get("ignore", False):
            return None
        if table.get("vocabulary_table", False):
            return None
        if table.get("num_passes", 1) == 0:
            return None
        metadata_table = self.metadata.tables[table_name]
        columns = frozenset(metadata_table.columns.keys())
        col2gen: dict[str, Generator] = {}
        multiple_columns_assigned: dict[str, list[str]] = {}
        for rg in table.get("row_generators", []):
            gen_name = rg.get("name", None)
            if gen_name:
                ca = rg.get("columns_assigned", [])
                single_ca = None
                if isinstance(ca, str):
                    if ca not in columns:
                        logger.warning(
                            "table '%s' has '%s' assigned to column '%s' which is not in this table",
                            table_name, gen_name, ca,
                        )
                    elif ca in col2gen:
                        logger.warning(
                            "table '%s' has column '%s' assigned to multiple times",
                            table_name, ca,
                        )
                    else:
                        single_ca = ca
                else:
                    if len(ca) == 1:
                        single_ca = str(ca[0])
                if single_ca is not None:
                    col2gen[single_ca] = PredefinedGenerator(table, rg, self.config)
                else:
                    multiple_columns_assigned[gen_name] = ca
        generator_infos: list[GeneratorInfo] = []
        for name, col in metadata_table.columns.items():
            gen = col2gen.get(name, None)
            generator_infos.append(GeneratorInfo(
                column=str(name),
                is_primary_key=col.primary_key,
                old_gen=gen,
                new_gen=gen,
            ))
        if multiple_columns_assigned:
            self.print(
                "The following mulit-column generators for table {0} are defined in the configuration file and cannot be configured with this command",
                table_name,
            )
            for (gen_name, cols) in multiple_columns_assigned.items():
                self.print("   {0}: {1}", gen_name, cols)
        if len(generator_infos) == 0:
            return None
        return GeneratorCmdTableEntry(
            name=table_name,
            generators=generator_infos
        )

    def __init__(self, src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
        super().__init__(src_dsn, src_schema, metadata, config)
        self.generator_index = 0
        self.generators_valid_indices = None
        self.set_prompt()

    def set_table_index(self, index):
        ret = super().set_table_index(index)
        if ret:
            self.generator_index = 0
            self.set_prompt()
        return ret

    def previous_table(self):
        ret = self.set_table_index(self.table_index - 1)
        if ret:
            table = self.get_table()
            if table is None:
                self.print("Internal error! table {0} does not have any generators!", self.table_index)
                return False
            self.generator_index = len(table.generators) - 1
        return ret

    def get_table(self) -> GeneratorCmdTableEntry | None:
        if self.table_index < len(self.table_entries):
            return self.table_entries[self.table_index]
        return None

    def get_table_and_generator(self) -> tuple[str | None, GeneratorInfo | None]:
        if self.table_index < len(self.table_entries):
            entry = self.table_entries[self.table_index]
            if self.generator_index < len(entry.generators):
                return (entry.name, entry.generators[self.generator_index])
            return (entry.name, None)
        return (None, None)

    def get_column_name(self) -> str | None:
        (_, generator_info) = self.get_table_and_generator()
        return generator_info.column if generator_info else None

    def column_metadata(self) -> Column | None:
        table = self.table_metadata()
        column_name = self.get_column_name()
        if table is None or column_name is None:
            return None
        return table.columns[column_name]

    def set_prompt(self):
        (table_name, gen_info) = self.get_table_and_generator()
        if table_name is None:
            self.prompt = "(generators) "
            return
        if gen_info is None:
            self.prompt = "({table}) ".format(table=table_name)
            return
        if gen_info.is_primary_key:
            column = f"{gen_info.column}[pk]"
        else:
            column = gen_info.column
        if gen_info.new_gen:
            self.prompt = "({table}.{column} ({generator})) ".format(
                table=table_name,
                column=column,
                generator=gen_info.new_gen.function_name(),
            )
        else:
            self.prompt = "({table}.{column}) ".format(
                table=table_name,
                column=column,
            )

    def _remove_auto_src_stats(self) -> list[dict[str, any]]:
        return self._remove_prefix_src_stats("auto__")

    def _copy_entries(self) -> None:
        src_stats = self._remove_auto_src_stats()
        tes: list[GeneratorCmdTableEntry] = self.table_entries
        for entry in tes:
            rgs = []
            new_gens: list[Generator] = []
            for generator in entry.generators:
                if generator.new_gen is not None:
                    new_gens.append(generator.new_gen)
                    cqs = generator.new_gen.custom_queries()
                    for cq_key, cq in cqs.items():
                        src_stats.append({
                            "name": cq_key,
                            "query": cq,
                        })
                    rg = {
                        "name": generator.new_gen.function_name(),
                        "columns_assigned": [generator.column],
                    }
                    kwn = generator.new_gen.nominal_kwargs()
                    if kwn:
                        rg["kwargs"] = kwn
                    rgs.append(rg)
            aq = self._get_aggregate_query(new_gens, entry.name)
            if aq:
                src_stats.append({
                    "name": f"auto__{entry.name}",
                    "query": aq,
                })
            table_config = self.get_table_config(entry.name)
            if rgs:
                table_config["row_generators"] = rgs
            elif "row_generators" in table_config:
                del table_config["row_generators"]
            self.set_table_config(entry.name, table_config)
        self.config["src-stats"] = src_stats

    def do_quit(self, _arg):
        "Check the updates, save them if desired and quit the configurer."
        count = 0
        for entry in self.table_entries:
            header_shown = False
            g_entry: GeneratorCmdTableEntry = entry
            for gen in g_entry.generators:
                if gen.old_gen != gen.new_gen:
                    if not header_shown:
                        header_shown = True
                        self.print("Table {0}:", entry.name)
                    count += 1
                    self.print(
                        "...changing {0} from {1} to {2}",
                        gen.column,
                        gen.old_gen.function_name() if gen.old_gen else "nothing",
                        gen.new_gen.function_name() if gen.new_gen else "nothing",
                    )
        if count == 0:
            self.print("You have made no changes.")
        reply = self.ask_save()
        if reply == "yes":
            self._copy_entries()
            return True
        if reply == "no":
            return True
        return False

    def do_tables(self, arg):
        "list the tables"
        for entry in self.table_entries:
            gen_count = len(entry.generators)
            how_many = "one generator" if gen_count == 1 else f"{gen_count} generators"
            self.print("{0} ({1})", entry.name, how_many)

    def do_list(self, arg):
        "list the generators in the current table"
        if len(self.table_entries) <= self.table_index:
            self.print("Error: no table {0}", self.table_index)
            return
        for gen in self.table_entries[self.table_index].generators:
            old = "" if gen.old_gen is None else gen.old_gen.function_name()
            if gen.old_gen == gen.new_gen:
                becomes = ""
                if old == "":
                    old = "(not set)"
            elif gen.new_gen is None:
                becomes = "(delete)"
            else:
                becomes = f"->{gen.new_gen.function_name()}"
            primary = "[primary-key]" if gen.is_primary_key else ""
            self.print("{0}{1}{2} {3}", old, becomes, primary, gen.column)

    def do_columns(self, _arg):
        "Report the column names and metadata"
        self.report_columns()

    def do_info(self, _arg):
        "Show information about the current column"
        cm = self.column_metadata()
        if cm is None:
            return
        self.print(
            "Column {0} in table {1} has type {2} ({3}).",
            cm.name,
            cm.table.name,
            str(cm.type),
            "nullable" if cm.nullable else "not nullable",
        )
        if cm.primary_key:
            self.print("It is a primary key, which usually does not need a generator")
        elif cm.foreign_keys:
            fk_names = [fk.column.name for fk in cm.foreign_keys]
            self.print("It is a foreign key referencing table {0}", ", ".join(fk_names))
            if len(fk_names) == 1:
                self.print("You do not need a generator if you just want a uniform choice over the referenced table's rows")

    def _get_table_index(self, table_name: str) -> int | None:
        for n, entry in enumerate(self.table_entries):
            if entry.name == table_name:
                return n
        return None

    def _get_generator_index(self, table_index, column_name):
        entry: GeneratorCmdTableEntry = self.table_entries[table_index]
        for n, gen in enumerate(entry.generators):
            if gen.column == column_name:
                return n
        return None

    def do_next(self, arg):
        """
        Go to the next generator.
        Or, go to a named table: 'next tablename'.
        Or go to a column: 'next tablename.columnname'.
        """
        if arg:
            parts = arg.split(".", 1)
            table_index = self._get_table_index(parts[0])
            if table_index is None:
                self.print("No such (non-vocabulary, non-ignored) table name {0}", parts[0])
                return
            gen_index = None
            if 1 < len(parts) and parts[1]:
                gen_index = self._get_generator_index(table_index, parts[1])
                if gen_index is None:
                    self.print("we cannot set the generator for column {0}", parts[1])
                    return
            self.set_table_index(table_index)
            if gen_index is not None:
                self.generator_index = gen_index
                self.set_prompt()
            return
        self._go_next()

    def _go_next(self):
        table = self.get_table()
        if table is None:
            self.print("No more tables")
        next_gi = self.generator_index + 1
        if next_gi == len(table.generators):
            self.next_table()
            return
        self.generator_index = next_gi
        self.set_prompt()

    def complete_next(self, text: str, _line: str, _begidx: int, _endidx: int):
        parts = text.split(".", 1)
        table_name = parts[0]
        if 1 < len(parts):
            column_name = parts[1]
            table_index = self._get_table_index(table_name)
            if table_index is None:
                return []
            table_entry: GeneratorCmdTableEntry = self.table_entries[table_index]
            return [
                f"{table_name}.{gen.column}"
                for gen in table_entry.generators
                if gen.column.startswith(column_name)
            ]
        table_names = [
            entry.name
            for entry in self.table_entries
            if entry.name.startswith(table_name)
        ]
        if table_name in table_names:
            table_names.append(f"{table_name}.")
        return table_names

    def do_previous(self, _arg):
        "Go to the previous generator"
        if self.generator_index == 0:
            self.previous_table()
        else:
            self.generator_index -= 1
        self.set_prompt()

    def _generators_valid(self) -> bool:
        return self.generators_valid_indices == (self.table_index, self.generator_index)

    def _get_generator_proposals(self) -> list[Generator]:
        if not self._generators_valid():
            self.generators = None
        if self.generators is None:
            column = self.column_metadata()
            if column is None:
                logger.error("No such column")
                return []
            gens = everything_factory.get_generators(column, self.engine)
            gens.sort(key=lambda g: g.fit(9999))
            self.generators = gens
            self.generators_valid_indices = (self.table_index, self.generator_index)
        return self.generators

    def _print_privacy(self):
        table = self.table_metadata()
        if table is None:
            return
        if table_is_private(self.config, table.name):
            self.print(self.PRIMARY_PRIVATE_TEXT)
            return
        pfks = primary_private_fks(self.config, table)
        if not pfks:
            self.print(self.NOT_PRIVATE_TEXT)
            return
        self.print(self.SECONDARY_PRIVATE_TEXT, pfks)

    def do_compare(self, arg: str):
        """
        Compare the real data with some generators.

        'compare': just look at some source data from this column.
        'compare 5 6 10': compare a sample of the source data with a sample
        from generators 5, 6 and 10. You can find out which numbers
        correspond to which generators using the 'propose' command.
        """
        self._print_privacy()
        args = arg.split()
        limit = 20
        comparison = {
            "source": self._get_column_data(limit, to_str=str),
        }
        gens: list[Generator] = self._get_generator_proposals()
        table_name = self.table_name()
        for argument in args:
            if argument.isdigit():
                n = int(argument)
                if 0 < n and n <= len(gens):
                    gen = gens[n - 1]
                    comparison[f"{n}. {gen.function_name()}"] = gen.generate_data(limit)
                    self._print_values_queried(table_name, n, gen)
        self.print_table_by_columns(comparison)

    def _print_values_queried(self, table_name: str, n: int, gen: Generator):
        """
        Print the values queried from the database for this generator.
        """
        if not gen.select_aggregate_clauses() and not gen.custom_queries():
            self.print(
                "{0}. {1} requires no data from the source database.",
                n,
                gen.function_name(),
            )
        else:
            self.print(
                "{0}. {1} requires the following data from the source database:",
                n,
                gen.function_name(),
            )
            self._print_select_aggregate_query(table_name, gen)
            self._print_custom_queries(gen)

    def _print_custom_queries(self, gen: Generator) -> None:
        """
        Print all the custom queries and all the values they get in this case.
        """
        cqs = gen.custom_queries()
        if not cqs:
            return
        kwa = gen.actual_kwargs()
        cq_key2args = {}
        src_stat_re = re.compile(f'SRC_STATS\\["([^"]+)"\\]')
        for argname, src_stat in gen.nominal_kwargs().items():
            if argname in kwa:
                src_stat_groups = src_stat_re.match(src_stat)
                if src_stat_groups:
                    cq_key = src_stat_groups.group(1)
                    if cq_key not in cq_key2args:
                        cq_key2args[cq_key] = []
                    cq_key2args[cq_key].append(kwa[argname])
        for cq_key, cq in cqs.items():
            self.print("{0}; providing the following values: {1}", cq, cq_key2args[cq_key])

    def _get_aggregate_query(self, gens: list[Generator], table_name: str) -> str | None:
        clauses = [
            f"{q} AS {n}"
            for gen in gens
            for n, q in or_default(gen.select_aggregate_clauses(), {}).items()
        ]
        if not clauses:
            return None
        return f"SELECT {', '.join(clauses)} FROM {table_name}"

    def _print_select_aggregate_query(self, table_name, gen: Generator) -> None:
        """
        Prints the select aggregate query and all the values it gets in this case.
        """
        sacs = gen.select_aggregate_clauses()
        if not sacs:
            return
        kwa = gen.actual_kwargs()
        vals = []
        src_stat2kwarg = { v: k for k, v in gen.nominal_kwargs().items() }
        for n in sacs.keys():
            src_stat = f'SRC_STATS["auto__{table_name}"][0]["{n}"]'
            if src_stat in src_stat2kwarg:
                ak = src_stat2kwarg[src_stat]
                if ak in kwa:
                    vals.append(kwa[ak])
                else:
                    logger.warning("actual_kwargs for %s does not report %s", gen.function_name(), ak)
            else:
                logger.warning('nominal_kwargs for %s does not have a value SRC_STATS["auto__%s"][0]["%s"]', gen.function_name(), table_name, n)
        select_q = self._get_aggregate_query([gen], table_name)
        self.print("{0}; providing the following values: {1}", select_q, vals)

    def _get_column_data(self, count: int, to_str=repr, min_length: int = 0):
        column = str(self.get_column_name())
        where = f"WHERE {column} IS NOT NULL"
        if 0 < min_length:
            where = "WHERE LENGTH({column}) >= {len}".format(
                column=column,
                len=min_length,
            )
        with self.engine.connect() as connection:
            result = connection.execute(
                text("SELECT {column} FROM {table} {where} ORDER BY RANDOM() LIMIT {count}".format(
                    table=self.table_name(),
                    column=column,
                    count=count,
                    where=where,
                ))
            )
            return [to_str(x[0]) for x in result.all()]

    def do_propose(self, _arg):
        """
        Display a list of possible generators for this column.

        They will be listed in order of fit, the most likely matches first.
        The results can be compared (against a sample of the real data in
        the column and against each other) with the 'compare' command.
        """
        limit = 5
        gens = self._get_generator_proposals()
        sample = self._get_column_data(limit)
        if sample:
            self.print(self.PROPOSE_SOURCE_SAMPLE_TEXT, ",".join(sample))
        else:
            self.print(self.PROPOSE_SOURCE_EMPTY_TEXT)
        for index, gen in enumerate(gens):
            fit = gen.fit()
            if fit is None:
                fit_s = "(no fit)"
            elif fit < 100:
                fit_s = f"(fit: {fit:.3g})"
            else:
                fit_s = f"(fit: {fit:.0f})"
            self.print(
                self.PROPOSE_GENERATOR_SAMPLE_TEXT,
                index=index + 1,
                name=gen.function_name(),
                fit=fit_s,
                sample=", ".join(map(repr, gen.generate_data(limit)))
            )

    def do_set(self, arg: str):
        """
        Set one of the proposals as a generator.
        Takes a single integer argument.
        """
        if not arg.isdigit():
            self.print("set requires a single integer argument; 'set 3' sets the third generator that 'propose' lists.")
            return
        if not self._generators_valid():
            self.print("Please run 'propose' before 'set'")
            return
        gens = self._get_generator_proposals()
        index = int(arg)
        if index < 1:
            self.print("set's argument must be at least 1")
            return
        if len(gens) < index:
            self.print("There are currently only {0} generators proposed, please select one of them.", index)
            return
        (table, gen_info) = self.get_table_and_generator()
        if table is None:
            self.print("Error: no table")
            return
        if gen_info is None:
            self.print("Error: no column")
            return
        gen_info.new_gen = gens[index - 1]
        self._go_next()

    def do_unset(self, _arg):
        """
        Removes any generator set for this column.
        """
        (table, gen_info) = self.get_table_and_generator()
        if table is None:
            self.print("Error: no table")
            return
        if gen_info is None:
            self.print("Error: no column")
            return
        gen_info.new_gen = None
        self._go_next()

def update_config_generators(src_dsn: str, src_schema: str, metadata: MetaData, config: Mapping):
    with GeneratorCmd(src_dsn, src_schema, metadata, config) as gc:
        gc.cmdloop()
        return gc.config
