"""Base configuration command shells."""
import cmd
from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any, Optional, Type

import sqlalchemy
from prettytable.colortable import ColorTable, Theme
from sqlalchemy import Engine, ForeignKey, MetaData, Table, func, or_, select
from sqlalchemy.exc import DatabaseError, SQLAlchemyError
from typing_extensions import Self

from datafaker.db_utils import (
    create_db_engine,
    fk_refers_to_ignored_table,
    get_sync_engine,
)
from datafaker.dialects import Random
from datafaker.theme import get_active_theme
from datafaker.utils import T, get_property, split_column_full_name


def or_default(v: T | None, d: T) -> T:
    """Return v if it isn't None, otherwise d."""
    return d if v is None else v


class TableType(Enum):
    """Types of table to be configured."""

    GENERATE = "generate"
    IGNORE = "ignore"
    VOCABULARY = "vocabulary"
    PRIVATE = "private"
    EMPTY = "empty"


TYPE_LETTER = {
    TableType.GENERATE: "G",
    TableType.IGNORE: "I",
    TableType.VOCABULARY: "V",
    TableType.PRIVATE: "P",
    TableType.EMPTY: "e",
}

TYPE_PROMPT = {
    TableType.GENERATE: "(table: {}) ",
    TableType.IGNORE: "(table: {} (ignore)) ",
    TableType.VOCABULARY: "(table: {} (vocab)) ",
    TableType.PRIVATE: "(table: {} (private)) ",
    TableType.EMPTY: "(table: {} (empty)) ",
}


@dataclass
class TableEntry:
    """Base class for table entries for interactive commands."""

    name: str  # name of the table


class AskSaveCmd(cmd.Cmd):
    """Interactive shell for whether to save and quit."""

    intro = "Do you want to save this configuration?"
    prompt = "(yes/no/cancel) "
    file = None

    def __init__(self) -> None:
        """Initialise a save command."""
        super().__init__()
        self.result = ""

    def do_yes(self, _arg: str) -> bool:
        """Save the new config.yaml."""
        self.result = "yes"
        return True

    def do_no(self, _arg: str) -> bool:
        """Exit without saving."""
        self.result = "no"
        return True

    def do_cancel(self, _arg: str) -> bool:
        """Do not exit."""
        self.result = "cancel"
        return True


def fk_column_name(fk: ForeignKey) -> str:
    """Display name for a foreign key."""
    if fk_refers_to_ignored_table(fk):
        return f"{fk.target_fullname} (ignored)"
    return str(fk.target_fullname)


# pylint: disable=too-many-public-methods
class DbCmd(ABC, cmd.Cmd):
    """Base class for interactive configuration commands."""

    INFO_NO_MORE_TABLES = "There are no more tables"
    ERROR_ALREADY_AT_START = "Error: Already at the start"
    ERROR_NO_SUCH_TABLE = "Error: '{0}' is not the name of a table in this database"
    ERROR_NO_SUCH_TABLE_OR_COLUMN = (
        "Error: '{0}' is not the name of a table"
        " in this database or a column in this table"
    )
    ERROR_FAILED_SQL = 'SQL query "{query}" caused exception {exc}'
    ERROR_FAILED_DISPLAY = "Error: Failed to display: {}"
    ERROR_UNKNOWN_COLUMNS = "Error: Unknown columns: {}"
    ROW_COUNT_MSG = "Total row count: {}"

    @abstractmethod
    def make_table_entry(
        self, table_name: str, table_config: Mapping
    ) -> TableEntry | None:
        """
        Make a table entry suitable for this interactive command.

        :param name: The name of the table to make an entry for.
        :param table_config: The part of the ``config.yaml`` referring to this table.
        :return: The table entry or None if this table should not be interacted with.
        """

    @dataclass
    class Settings:
        """Settings for the source database."""

        dsn: str
        schema: str | None
        config: MutableMapping[str, Any]
        metadata: MetaData
        parquet_dir: Path | None

    def __init__(
        self,
        settings: Settings,
    ):
        """
        Initialise a DbCmd.

        :param src_dsn: Settings for the source database.
        """
        super().__init__()
        self.config: MutableMapping[str, Any] = settings.config
        self.metadata = settings.metadata
        self._table_entries: list[TableEntry] = []
        theme = get_active_theme()
        self._table_theme = Theme(
            default_color=theme.data,
            vertical_color=theme.line,
            horizontal_color=theme.line,
            junction_color=theme.line,
        )
        tables_config: MutableMapping = self.config.get("tables", {})
        if not isinstance(tables_config, MutableMapping):
            tables_config = {}
        for name in self.metadata.tables.keys():
            table_config = tables_config.get(name, {})
            if not isinstance(table_config, MutableMapping):
                table_config = {}
            entry = self.make_table_entry(name, table_config)
            if entry is not None:
                self._table_entries.append(entry)
        self.table_index = 0
        self.engine = create_db_engine(
            settings.dsn,
            schema_name=settings.schema,
            parquet_dir=settings.parquet_dir,
        )

    @property
    def sync_engine(self) -> Engine:
        """Get the synchronous version of the engine."""
        return get_sync_engine(self.engine)

    def __enter__(self) -> Self:
        """Enter a ``with`` statement."""
        return self

    def __exit__(
        self,
        _exc_type: Optional[Type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        """Dispose of this object."""
        self.engine.dispose()

    def print(self, text: str, *args: Any, **kwargs: Any) -> None:
        """Print text, formatted with positional and keyword arguments."""
        print(text.format(*args, **kwargs))

    def print_table(
        self, headings: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> None:
        """
        Print a table.

        :param headings: List of headings for the table.
        :param rows: List of rows of values.
        """
        output = ColorTable(theme=self._table_theme)
        output.field_names = headings
        for row in rows:
            # Hopefully PrettyTable will accept Sequence in the future, not list
            output.add_row(list(row))
        print(output)

    def print_table_by_columns(self, columns: Mapping[str, Sequence[str]]) -> None:
        """
        Print a table.

        :param columns: Dict of column names to the values in the column.
        """
        output = ColorTable(theme=self._table_theme)
        row_count = max(len(col) for col in columns.values())
        for field_name, data in columns.items():
            output.add_column(field_name, list(data) + [None] * (row_count - len(data)))
        print(output)

    def print_results(self, result: sqlalchemy.CursorResult) -> None:
        """Print the rows resulting from a database query."""
        theme = get_active_theme()
        self.print_table(
            [f"{theme.column}{heading}" for heading in result.keys()],
            [list(row) for row in result.all()],
        )

    def ask_save(self) -> str:
        """
        Ask the user if they want to save.

        :return: ``yes``, ``no`` or ``cancel``.
        """
        ask = AskSaveCmd()
        ask.cmdloop()
        return ask.result

    @abstractmethod
    def set_prompt(self) -> None:
        """Set the prompt according to the current state."""

    def _set_table_index(self, index: int) -> bool:
        """
        Move to a different table.

        :param index: Index of the table to move to.
        :return: True if there is a table with such an index to move to.
        """
        if 0 <= index < len(self._table_entries):
            self.table_index = index
            self.set_prompt()
            return True
        return False

    def next_table(self, report: str = "No more tables") -> bool:
        """
        Move to the next table.

        :param report: The text to print if there is no next table.
        :return: True if there is another table to move to.
        """
        if not self._set_table_index(self.table_index + 1):
            self.print(report)
            return False
        return True

    def table_name(self) -> str:
        """Get the name of the current table."""
        return str(self._table_entries[self.table_index].name)

    def table_metadata(self) -> Table:
        """Get the metadata of the current table."""
        return self.metadata.tables[self.table_name()]

    def _get_column_names(self) -> list[str]:
        """Get the names of the current columns."""
        return [col.name for col in self.table_metadata().columns]

    def get_roles(self, table: str, column: str) -> set[str]:
        """Get the roles for the named column in the named table."""
        return set(
            get_property(
                self.config,
                ["tables", table, "columns", column, "roles"],
                [],
            )
        )

    def report_columns(self) -> None:
        """Print information about the current columns."""
        table = self.table_metadata()
        theme = get_active_theme()
        self.print_table(
            [
                f"{theme.reset}{heading}"
                for heading in [
                    "name",
                    "type",
                    "primary",
                    "nullable",
                    "foreign key",
                    "roles",
                ]
            ],
            [
                [
                    name,
                    str(col.type),
                    col.primary_key,
                    col.nullable,
                    ", ".join([fk_column_name(fk) for fk in col.foreign_keys]),
                    ", ".join(self.get_roles(table.name, name)),
                ]
                for name, col in table.columns.items()
            ],
        )

    def get_table_config(self, table_name: str) -> MutableMapping[str, Any]:
        """Get the configuration of the named table."""
        ts = self.config.get("tables", None)
        if not isinstance(ts, MutableMapping):
            return {}
        t = ts.get(table_name)
        return t if isinstance(t, MutableMapping) else {}

    def set_table_config(
        self, table_name: str, config: MutableMapping[str, Any]
    ) -> None:
        """Set the configuration of the named table."""
        ts = self.config.get("tables", None)
        if not isinstance(ts, MutableMapping):
            self.config["tables"] = {table_name: config}
            return
        ts[table_name] = config

    def _remove_prefix_src_stats(self, prefix: str) -> list[MutableMapping[str, Any]]:
        """Remove all source stats with the given prefix from the configuration."""
        src_stats = self.config.get("src-stats", [])
        new_src_stats = []
        for stat in src_stats:
            if not stat.get("name", "").startswith(prefix):
                new_src_stats.append(stat)
        self.config["src-stats"] = new_src_stats
        return new_src_stats

    def get_nullable_columns(self, table_name: str) -> list[sqlalchemy.Column]:
        """Get the names of the nullable columns in the named table."""
        metadata_table = self.metadata.tables[table_name]
        return [column for column in metadata_table.columns.values() if column.nullable]

    def find_entry_index_by_table_name(self, table_name: str) -> int | None:
        """Get the index of the table entry of the named table."""
        return next(
            (
                i
                for i, entry in enumerate(self._table_entries)
                if entry.name == table_name
            ),
            None,
        )

    def _find_entry_by_table_name(self, table_name: str) -> TableEntry | None:
        """Get the table entry of the named table."""
        for e in self._table_entries:
            if e.name == table_name:
                return e
        return None

    def do_counts(self, _arg: str) -> None:
        """Report the column names with the counts of nulls in them."""
        if len(self._table_entries) <= self.table_index:
            return
        table_name = self.table_name()
        nullable_columns = self.get_nullable_columns(table_name)
        tbl = self.table_metadata()
        count_exprs = [func.count().label("row_count")] + [  # pylint: disable=E1102
            func.count(tbl.c[col.name]).label(col.name)  # pylint: disable=E1102
            for col in nullable_columns
        ]
        stmt = select(*count_exprs).select_from(tbl)
        with self.sync_engine.connect() as connection:
            result = connection.execute(stmt).mappings().first()
            if result is None:
                self.print("Could not count rows in table {0}", table_name)
                return
            row_count = result.get("row_count", 0)
            self.print(self.ROW_COUNT_MSG, row_count)
            theme = get_active_theme()
            self.print_table(
                [f"{theme.reset}Column", f"{theme.reset}NULL count"],
                [
                    [name, row_count - count]
                    for name, count in result.items()
                    if name != "row_count"
                ],
            )

    def do_select(self, arg: str) -> None:
        """Run a select query over the database and show the first 50 results."""
        max_select_rows = 50
        query = "SELECT " + arg
        with self.sync_engine.connect() as connection:
            try:
                result = connection.execute(sqlalchemy.text(query))
            except DatabaseError as exc:
                self.print(self.ERROR_FAILED_SQL, exc=exc, query=query)
                return
            row_count = result.rowcount
            self.print(self.ROW_COUNT_MSG, row_count)
            if 50 < row_count:
                self.print("Showing the first {} rows", max_select_rows)
            fields = list(result.keys())
            rows = result.fetchmany(max_select_rows)
            theme = get_active_theme()
            try:
                self.print_table(
                    [f"{theme.column}{f}" for f in fields],
                    rows,
                )
            except ValueError as exc:
                self.print(self.ERROR_FAILED_DISPLAY, exc)
                return

    def complete_select(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Completions for the ``select`` command."""
        if len(self._table_entries) <= self.table_index:
            return []
        join_types = ["inner", "outer", "left", "right", "full", "cross", "self"]
        table_clause = ["from", "join"]
        sql_ops = (
            join_types + table_clause + ["where", "order", "like", "limit", "random()"]
        )
        previous_word = ([""] + line[:begidx].rsplit(maxsplit=1))[-1].lower()
        if previous_word in join_types:
            return [j for j in ["outer", "OUTER", "join", "JOIN"] if j.startswith(text)]
        sql_ops += [s.upper() for s in sql_ops]
        sql_completions = [word for word in sql_ops if word.startswith(text)]
        if previous_word in table_clause:
            return sql_completions + self.get_table_completions(text)
        return sql_completions + self.get_table_or_column_completions(text)

    def do_peek(self, arg: str) -> None:
        """
        View some data from the current table.

        Use 'peek col1 col2 col3' to see a sample of values from
        columns col1, col2 and col3 in the current table.
        Use 'peek' to see a sample of the current column(s).
        Rows that are enitrely null are suppressed.
        """
        max_peek_rows = 25
        if len(self._table_entries) <= self.table_index:
            return
        col_names = arg.split()
        if not col_names:
            col_names = self._get_column_names()
        table = self.table_metadata()
        unknown_cols = [cn for cn in col_names if cn not in table.columns]
        if unknown_cols:
            self.print(self.ERROR_UNKNOWN_COLUMNS, ", ".join(unknown_cols))
            return
        col_exprs = [table.columns[cn] for cn in col_names]
        nonnull_clauses = [ce.isnot(None) for ce in col_exprs]
        stmt = (
            select(*col_exprs)
            .select_from(table)
            .where(or_(*nonnull_clauses))
            .order_by(Random())
            .limit(max_peek_rows)
        )
        with self.sync_engine.connect() as connection:
            try:
                result = connection.execute(stmt)
            except SQLAlchemyError as exc:
                self.print(self.ERROR_FAILED_SQL, exc=exc, query=stmt)
                return
            theme = get_active_theme()
            self.print_table(
                [f"{theme.column}{k}" for k in result.keys()],
                result.fetchmany(max_peek_rows),
            )

    def get_column_completions(self, text: str) -> list[str]:
        """Get current table column name completions."""
        table_metadata = self.table_metadata()
        return [k for k in table_metadata.columns.keys() if k.startswith(text)]

    def get_table_completions(self, text: str) -> list[str]:
        """Get table name completions."""
        return [
            entry.name for entry in self._table_entries if entry.name.startswith(text)
        ]

    def get_table_index(self, table_name: str) -> int | None:
        """Get the index of the named table in the table entries list."""
        for n, entry in enumerate(self._table_entries):
            if entry.name == table_name:
                return n
        return None

    def get_table_or_column_completions(self, text: str) -> list[str]:
        """
        Completions for text that are columns or tables.

        Columns are either columns in this table or table.columns pairs.

        :param text: The text to be completed (should not include spaces).
        :return: The list of all column names or table names or perhaps
         a table name followed by a dot.
        """
        (first_part, last_part) = split_column_full_name(text)
        if first_part:
            # first_part is table, last_part is column
            table_index = self.get_table_index(first_part)
            if table_index is None:
                return []
            table_entry = self._table_entries[table_index]
            return [
                f"{first_part}.{column}"
                for column in self.metadata.tables[table_entry.name].columns.keys()
                if column.startswith(last_part)
            ]
        # first_part is None, last_part might be table or column.
        table_names = self.get_table_completions(last_part)
        if last_part in table_names:
            # we have a complete table name, so allow the completion with a dot to show this
            table_names.append(f"{last_part}.")
        if self.table_index < len(self._table_entries):
            current_table = self._table_entries[self.table_index]
            column_names = [
                column
                for column in self.metadata.tables[current_table.name].columns.keys()
                if column.startswith(last_part)
            ]
        else:
            column_names = []
        return table_names + column_names

    def complete_peek(
        self, text: str, _line: str, _begidx: int, _endidx: int
    ) -> list[str]:
        """Completions for the ``peek`` command."""
        if len(self._table_entries) <= self.table_index:
            return []
        return self.get_column_completions(text)
