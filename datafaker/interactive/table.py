"""Table configuration command shell."""
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from sqlalchemy import func, select

from datafaker.dialects import Random
from datafaker.interactive.base import (
    TYPE_LETTER,
    TYPE_PROMPT,
    DbCmd,
    TableEntry,
    TableType,
)


@dataclass
class TableCmdTableEntry(TableEntry):
    """Table entry for the table command shell."""

    old_type: TableType
    old_name_column: str | None
    new_type: TableType
    new_name_column: str | None


# pylint: disable=too-many-public-methods
class TableCmd(DbCmd):
    """Command shell allowing the user to set the type of each table."""

    intro = (
        "Interactive table configuration (ignore,"
        " vocabulary, private, generate or empty). Type ? for help.\n"
    )
    doc_leader = """Use the commands 'ignore', 'vocabulary',
'private', 'empty' or 'generate' to set the table's type. Use 'next' or
'previous' to change table. Use 'tables' and 'columns' for
information about the database. Use 'data', 'peek', 'select' or
'count' to see some data contained in the current table. Use 'quit'
to exit this program."""
    prompt = "(tableconf) "
    file = None
    WARNING_TEXT_VOCAB_TO_NON_VOCAB = (
        "Vocabulary table {0} references non-vocabulary table {1}"
    )
    WARNING_TEXT_NON_EMPTY_TO_EMPTY = (
        "Empty table {1} referenced from non-empty table {0}. Foreign keys"
        " from {0} to {1} cannot be chosen by the default generator unless"
        " {1} has stories."
    )
    WARNING_TEXT_PROBLEMS_EXIST = "WARNING: The following table types have problems:"
    WARNING_TEXT_POTENTIAL_PROBLEMS = (
        "NOTE: The following table types might cause problems later:"
    )
    NOTE_TEXT_NO_CHANGES = "You have made no changes."
    NOTE_TEXT_CHANGING = "Changing {0} from {1} to {2}"
    NOTE_TEXT_TABLE_WITH_NAME = "{0} (name column: {1})"

    def make_table_entry(
        self, table_name: str, table_config: Mapping
    ) -> TableCmdTableEntry | None:
        """
        Make a table entry for the named table.

        :param name: The name of the table.
        :param table: The part of ``config.yaml`` corresponding to this table.
        :return: The newly-constructed table entry.
        """
        nc = table_config.get("name_column", None)
        if not isinstance(nc, str):
            nc = None
        if table_config.get("ignore", False):
            return TableCmdTableEntry(
                table_name, TableType.IGNORE, nc, TableType.IGNORE, nc
            )
        if table_config.get("vocabulary_table", False):
            return TableCmdTableEntry(
                table_name, TableType.VOCABULARY, nc, TableType.VOCABULARY, nc
            )
        if table_config.get("primary_private", False):
            return TableCmdTableEntry(
                table_name, TableType.PRIVATE, nc, TableType.PRIVATE, nc
            )
        if table_config.get("num_rows_per_pass", 1) == 0:
            return TableCmdTableEntry(
                table_name, TableType.EMPTY, nc, TableType.EMPTY, nc
            )
        return TableCmdTableEntry(
            table_name, TableType.GENERATE, nc, TableType.GENERATE, nc
        )

    def __init__(
        self,
        settings: DbCmd.Settings,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialise a TableCmd."""
        super().__init__(settings, *args, **kwargs)
        self.set_prompt()

    @property
    def table_entries(self) -> list[TableCmdTableEntry]:
        """Get the list of table entries."""
        return cast(list[TableCmdTableEntry], self._table_entries)

    def _find_entry_by_table_name(self, table_name: str) -> TableCmdTableEntry | None:
        """Get the table entry of the table with the given name."""
        entry = super()._find_entry_by_table_name(table_name)
        if entry is None:
            return None
        return cast(TableCmdTableEntry, entry)

    def set_prompt(self) -> None:
        """Set the prompt according to the current table and its type."""
        if self.table_index < len(self.table_entries):
            entry = self.table_entries[self.table_index]
            self.prompt = TYPE_PROMPT[entry.new_type].format(entry.name)
        else:
            self.prompt = "(table) "

    def _copy_entries(self) -> None:
        """Alter the configuration to match the new table entries."""
        for entry in self.table_entries:
            if (
                entry.old_type != entry.new_type
                or entry.old_name_column != entry.new_name_column
            ):
                table = self.get_table_config(entry.name)
                if (
                    entry.old_type == TableType.EMPTY
                    and table.get("num_rows_per_pass", 1) == 0
                ):
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
                if entry.new_name_column:
                    table["name_column"] = entry.new_name_column
                else:
                    table.pop("name_column", None)
                self.set_table_config(entry.name, table)

    def _get_referenced_tables(self, from_table_name: str) -> set[str]:
        """Get all the tables referenced by this table's foreign keys."""
        from_meta = self.metadata.tables[from_table_name]
        return {
            fk.column.table.name for col in from_meta.columns for fk in col.foreign_keys
        }

    def _sanity_check_failures(self) -> list[tuple[str, str, str]]:
        """Find tables that reference each other that should not given their types."""
        failures = []
        for from_entry in self.table_entries:
            from_t = from_entry.new_type
            if from_t == TableType.VOCABULARY:
                referenced = self._get_referenced_tables(from_entry.name)
                for ref in referenced:
                    to_entry = self._find_entry_by_table_name(ref)
                    if (
                        to_entry is not None
                        and to_entry.new_type != TableType.VOCABULARY
                    ):
                        failures.append(
                            (
                                self.WARNING_TEXT_VOCAB_TO_NON_VOCAB,
                                from_entry.name,
                                to_entry.name,
                            )
                        )
        return failures

    def _sanity_check_warnings(self) -> list[tuple[str, str, str]]:
        """Find tables that reference each other that might cause problems given their types."""
        warnings = []
        for from_entry in self.table_entries:
            from_t = from_entry.new_type
            if from_t in {TableType.GENERATE, TableType.PRIVATE}:
                referenced = self._get_referenced_tables(from_entry.name)
                for ref in referenced:
                    to_entry = self._find_entry_by_table_name(ref)
                    if to_entry is not None and to_entry.new_type in {
                        TableType.EMPTY,
                        TableType.IGNORE,
                    }:
                        warnings.append(
                            (
                                self.WARNING_TEXT_NON_EMPTY_TO_EMPTY,
                                from_entry.name,
                                to_entry.name,
                            )
                        )
        return warnings

    def _print_change_note(self, entry: TableCmdTableEntry) -> int:
        """
        Print a note if the table entry has changed.

        :return: The number of notes written
        """
        if (
            entry.old_type == entry.new_type
            and entry.old_name_column == entry.new_name_column
        ):
            return 0
        if entry.old_name_column:
            old = self.NOTE_TEXT_TABLE_WITH_NAME.format(
                entry.old_type.value, entry.old_name_column
            )
        else:
            old = entry.old_type.value
        if entry.new_name_column:
            new = self.NOTE_TEXT_TABLE_WITH_NAME.format(
                entry.new_type.value, entry.new_name_column
            )
        else:
            new = entry.new_type.value
        self.print(self.NOTE_TEXT_CHANGING, entry.name, old, new)
        return 1

    def do_quit(self, _arg: str) -> bool:
        """Check the updates, save them if desired and quit the configurer."""
        count = 0
        for entry in self.table_entries:
            count += self._print_change_note(entry)
        if count == 0:
            self.print(self.NOTE_TEXT_NO_CHANGES)
        failures = self._sanity_check_failures()
        if failures:
            self.print(self.WARNING_TEXT_PROBLEMS_EXIST)
            for text, from_t, to_t in failures:
                self.print(text, from_t, to_t)
        warnings = self._sanity_check_warnings()
        if warnings:
            self.print(self.WARNING_TEXT_POTENTIAL_PROBLEMS)
            for text, from_t, to_t in warnings:
                self.print(text, from_t, to_t)
        reply = self.ask_save()
        if reply == "yes":
            self._copy_entries()
            return True
        if reply == "no":
            return True
        return False

    def do_tables(self, _arg: str) -> None:
        """List the tables with their types."""
        for entry in self.table_entries:
            old = entry.old_type
            new = entry.new_type
            becomes = "   " if old == new else "->" + TYPE_LETTER[new]
            self.print("{0}{1} {2}", TYPE_LETTER[old], becomes, entry.name)

    def do_next(self, arg: str) -> None:
        """'next' = go to the next table, 'next tablename' = go to table 'tablename'."""
        if arg:
            # Find the index of the table called _arg, if any
            index = self.find_entry_index_by_table_name(arg)
            if index is None:
                self.print(self.ERROR_NO_SUCH_TABLE, arg)
                return
            self._set_table_index(index)
            return
        self.next_table(self.INFO_NO_MORE_TABLES)

    def complete_next(
        self, text: str, _line: str, _begidx: int, _endidx: int
    ) -> list[str]:
        """Get the completions for tables and columns."""
        return [
            entry.name for entry in self.table_entries if entry.name.startswith(text)
        ]

    def do_previous(self, _arg: str) -> None:
        """Go to the previous table."""
        if not self._set_table_index(self.table_index - 1):
            self.print(self.ERROR_ALREADY_AT_START)

    def set_table_type(self, arg: str, new_type: TableType, type_name: str) -> None:
        """
        Set the current table type from a command argument list.

        :param arg: The arg passed to the ``do_`` function.
        :param new_type: The type to set.
        :param type_name: The human-readable name of the type being set.
        """
        if len(self.table_entries) <= self.table_index:
            return
        entry = self.table_entries[self.table_index]
        args = arg.split()
        new_name_column: str | None = None
        if len(args) == 2 and args[0] == "name":
            new_name_column = args[1]
            args = []
        if args:
            self.print(
                f"""Did not understand these arguments.
Correct formats are:
{new_type.value}  -- just set the type to {type_name}
{new_type.value} name ColumnName  -- set the type and the naming column of the table"""
            )
        entry.new_type = new_type
        entry.new_name_column = new_name_column
        self.print("Table {} set as {}", self.table_name(), type_name)
        self.next_table()

    def do_ignore(self, arg: str) -> None:
        """
        Set the current table as ignored, and go to the next table.

        "ignore name <column_name>" sets the table to ignored but also sets
        the column <column_name> to be the column from which we can get
        human-readable names for the rows.
        """
        self.set_table_type(arg, TableType.IGNORE, "ignored")

    def get_table_type_completions(
        self, text: str, line: str, begidx: int
    ) -> list[str]:
        """Get the completions for ignore/vocabulary/generate etcetera."""
        previous_parts = line[: begidx - 1].split()
        if len(previous_parts) == 1:
            return [x for x in ["name"] if x.startswith(text)]
        if len(previous_parts) == 2 and previous_parts[1] == "name":
            return self.get_column_completions(text)
        return []

    def complete_ignore(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Get the completions for ignore."""
        return self.get_table_type_completions(text, line, begidx)

    def do_vocabulary(self, arg: str) -> None:
        """
        Set the current table as a vocabulary table, and go to the next table.

        "vocabulary name <column_name>" sets the table to vocabulary but also
        sets the column <column_name> to be the column from which we can get
        human-readable names for the rows.
        """
        self.set_table_type(arg, TableType.VOCABULARY, "vocabulary")

    def complete_vocabulary(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Get the completions for vocabulary."""
        return self.get_table_type_completions(text, line, begidx)

    def do_private(self, arg: str) -> None:
        """
        Set the current table as a primary private table (such as the table of patients).

        "vocabulary name <column_name>" sets the table to vocabulary but also
        sets the column <column_name> to be the column from which we can get
        human-readable names for the rows.
        """
        self.set_table_type(arg, TableType.PRIVATE, "primary private")

    def complete_private(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Get the completions for private."""
        return self.get_table_type_completions(text, line, begidx)

    def do_generate(self, arg: str) -> None:
        """
        Set the current table as to be generated, and go to the next table.

        "vocabulary name <column_name>" sets the table to vocabulary but also
        sets the column <column_name> to be the column from which we can get
        human-readable names for the rows.
        """
        self.set_table_type(arg, TableType.GENERATE, "generate")

    def complete_generate(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Get the completions for generate."""
        return self.get_table_type_completions(text, line, begidx)

    def do_empty(self, arg: str) -> None:
        """
        Set the current table as empty; no generators will be run for it.

        "empty name <column_name>" sets the table to empty but also sets
        the column <column_name> to be the column from which we can get
        human-readable names for the rows.
        """
        self.set_table_type(arg, TableType.EMPTY, "empty")

    def complete_empty(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Get the completions for empty."""
        return self.get_table_type_completions(text, line, begidx)

    def do_columns(self, _arg: str) -> None:
        """Report the column names and metadata."""
        self.report_columns()

    def do_data(self, arg: str) -> None:
        """
        Report some data.

        'data' = report a random ten lines,
        'data 20' = report a random 20 lines,
        'data 20 ColumnName' = report a random twenty entries from ColumnName,
        'data 20 ColumnName 30' = report a random twenty entries from
        ColumnName of length at least 30,
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

    def complete_data(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Get completions for arguments to ``data``."""
        previous_parts = line[: begidx - 1].split()
        if len(previous_parts) != 2:
            return []
        return self.get_column_completions(text)

    def print_column_data(self, column_name: str, count: int, min_length: int) -> None:
        """
        Print a sample of data from a certain column of the current table.

        :param column_name: The name of the column to report on.
        :param count: The number of rows to sample.
        :param min_length: The minimum length of text to choose from (0 for any text).
        """
        table = self.table_metadata()
        column = table.columns[column_name]
        if 0 < min_length:
            where_clause = func.length(column) >= min_length
        else:
            where_clause = column.isnot(None)
        stmt = select(column).where(where_clause).order_by(Random()).limit(count)
        with self.sync_engine.connect() as connection:
            result = connection.execute(stmt)
            self.columnize([str(x[0]) for x in result.all()])

    def print_row_data(self, count: int) -> None:
        """
        Print a sample or rows from the current table.

        :param count: The number of rows to report.
        """
        stmt = select(self.table_metadata()).order_by(Random()).limit(count)
        with self.sync_engine.connect() as connection:
            result = connection.execute(stmt)
            if result is None:
                self.print("No rows in this table!")
                return
            self.print_results(result)
