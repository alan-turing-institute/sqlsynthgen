"""Generator configuration shell."""  # pylint: disable=too-many-lines
import copy
import functools
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, cast

from sqlalchemy import Column, and_, select
from sqlalchemy.types import Integer

from datafaker.db_utils import MaybeAsyncEngine, primary_private_fks, table_is_private
from datafaker.dialects import Random
from datafaker.evaluators.column_evaluator import ColumnEvaluator
from datafaker.evaluators.evaluation_profile import EvaluationProfile
from datafaker.evaluators.proposal_ranking import (
    ProposalRanking,
    ProposalRankingDisplay,
    format_ranking_display,
    rank_proposals,
)
from datafaker.interactive.base import DbCmd, TableEntry, fk_column_name, or_default
from datafaker.proposers import everything_factory
from datafaker.proposers.base import PredefinedProposer, Proposer, get_column_type
from datafaker.proposers.continuous import (
    GaussianProposer,
    LogNormalProposer,
    UniformProposer,
)
from datafaker.theme import Theme, get_active_theme
from datafaker.utils import (
    get_columns_assigned,
    get_property,
    get_row_generators,
    logger,
    schema_qualified_name,
    set_property,
    split_column_full_name,
    text_list,
)


@dataclass
class ProposerInfo:
    """A generator and the columns it assigns to."""

    columns: list[str]
    proposer: Proposer | None


@dataclass
class GeneratorCmdTableEntry(TableEntry):
    """
    List of proposers set for a table.

    Includes the original setting and the currently configured
    proposers.
    """

    old_proposers: list[ProposerInfo]
    new_proposers: list[ProposerInfo]


def get_aggregate_query(
    proposers: Sequence[Proposer], table_name: str, engine: MaybeAsyncEngine
) -> str | None:
    """
    Get a SQL query from a list of proposers.

    :param proposers: List of proposers.
    :param table_name: The name of the table the proposers are operating on.
    :param engine: The engine that needs to be queried (it won't be queried
      during this call).
    :return: A string representing the query to be executed, or None if no
      query is required.
    """
    clauses = [
        f'{q["clause"]} AS {n}'
        for proposer in proposers
        for n, q in or_default(proposer.select_aggregate_clauses(), {}).items()
    ]
    if not clauses:
        return None
    qualified = schema_qualified_name(table_name, engine)
    alias = f' AS "{table_name}"' if engine.dialect.name == "duckdb" else ""
    return f'SELECT {", ".join(clauses)} FROM "{qualified}"{alias}'


class Role(str, Enum):
    """Roles that can be set on columns."""

    START = "start"
    SOURCE = "source"


def is_role(item: str) -> bool:
    """
    Test if ``item`` is a ``Role``.

    Like ``item in Role`` in Python 3.12.
    """
    return item in Role.__members__.values()


@dataclass
class RoleEntry:
    """The old and altered role set for a column."""

    old: set[Role]
    new: set[Role]


class RoleCommand(ABC):
    """Command for ``role``."""

    @abstractmethod
    def do(self, column_roles: RoleEntry | None) -> RoleEntry | None:
        """Perform the command."""


class RoleCommandList(RoleCommand):
    """Command for ``role list``."""

    def __init__(self, error_text: str, print_fn: Callable[[str], None]) -> None:
        """Initialize RoleCommandSet."""
        self.print = print_fn
        self.error_text = error_text

    def do(self, column_roles: RoleEntry | None) -> RoleEntry | None:
        """Perform the command."""
        if column_roles is None or not column_roles.new:
            self.print(self.error_text)
        else:
            self.print(", ".join(role.value for role in column_roles.new))


class RoleCommandSet(RoleCommand):
    """Command for ``role set <role>``."""

    def __init__(self, role: Role) -> None:
        """Initialize RoleCommandSet."""
        self.role = role

    def do(self, column_roles: RoleEntry | None) -> RoleEntry | None:
        """Perform the command."""
        if column_roles is None:
            return RoleEntry(set(), {self.role})
        column_roles.new.add(self.role)
        return column_roles


class RoleCommandDelete(RoleCommand):
    """Command for ``role delete <role>``."""

    def __init__(self, role: Role) -> None:
        """Initialize RoleCommandDelete."""
        self.role = role

    def do(self, column_roles: RoleEntry | None) -> RoleEntry | None:
        """Perform the command."""
        if column_roles is None:
            return None
        if self.role in column_roles.new:
            column_roles.new.remove(self.role)
        return column_roles


def make_role_entry(roles: Sequence[str]) -> RoleEntry:
    """Make a role entry from a list of strings."""
    role_set = {Role(r) for r in roles if is_role(r)}
    return RoleEntry(copy.copy(role_set), role_set)


# pylint: disable=too-many-public-methods
class GeneratorCmd(DbCmd):
    """Interactive command shell for setting generators."""

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

    PROPOSE_SOURCE_SAMPLE_TEXT = "Sample of actual source data: {1}{0}..."
    PROPOSE_SOURCE_EMPTY_TEXT = "Source database has no data in this column."
    RANKED_SAMPLE_TEXT = (
        "{theme_reset}{index}. {theme_func}{name}:"
        " {theme_data}{sample}{theme_reset} ..."
    )
    PRIMARY_PRIVATE_TEXT = "Primary Private"
    SECONDARY_PRIVATE_TEXT = "Secondary Private on columns {0}"
    NOT_PRIVATE_TEXT = "Not private"
    REQUIRES_NO_SOURCE_DATA_TEXT = (
        "{0}. {2}{1}{3} requires no data from the source database."
    )
    REQUIRES_SOURCE_DATA_TEXT = (
        "{0}. {2}{1}{3} requires the following data from the source database:"
    )
    PROVIDING_VALUES_TEXT = "{2}{0}{3}; providing the following values: {4}{1}"
    NO_ROLES_TEXT = "No roles."
    ERROR_NO_SUCH_TABLE = "No such (non-vocabulary, non-ignored) table name {0}"
    ERROR_NO_SUCH_COLUMN = "No such column {0} in this table"
    ERROR_COLUMN_ALREADY_MERGED = "Column {0} is already merged"
    ERROR_COLUMN_ALREADY_UNMERGED = "Column {0} is not merged"
    ERROR_CANNOT_UNMERGE_ALL = "You cannot unmerge all the generator's columns"
    PROPOSE_NOTHING = "No proposed generators, sorry."

    # Cap on rows shown by default 'propose' (front 1 can otherwise hold more
    # candidates than are worth scanning in a terminal table). 'propose all'
    # is unaffected.
    MAX_PROPOSERS_SHOWN = 10

    SRC_STAT_RE = re.compile(
        r'\bSRC_STATS\["([^"]+)"\](\["results"\]\[0\]\["([^"]+)"\])?'
    )

    def make_table_entry(
        self, table_name: str, table_config: Mapping
    ) -> GeneratorCmdTableEntry | None:
        """
        Make a table entry.

        :param table_name: The name of the table.
        :param table: The portion of the ``config.yaml`` file describing this table.
        :return: The newly constructed table entry, or None if this table is to be ignored.
        """
        if table_config.get("ignore", False):
            return None
        if table_config.get("vocabulary_table", False):
            return None
        if table_config.get("num_rows_per_pass", 1) == 0:
            return None
        columns = [
            str(colname) for colname in self.metadata.tables[table_name].columns.keys()
        ]
        column_set = frozenset(columns)
        columns_assigned_so_far: set[str] = set()

        new_proposer_infos: list[ProposerInfo] = []
        for gen_name, rg in get_row_generators(table_config):
            colset: set[str] = set(get_columns_assigned(rg))
            for unknown in colset - column_set:
                logger.warning(
                    "table '%s' has '%s' assigned to column '%s' which is not in this table",
                    table_name,
                    gen_name,
                    unknown,
                )
            for mult in columns_assigned_so_far & colset:
                logger.warning(
                    "table '%s' has column '%s' assigned to multiple times",
                    table_name,
                    mult,
                )
            actual_collist = [c for c in columns if c in colset]
            if actual_collist:
                new_proposer_infos.append(
                    ProposerInfo(
                        columns=actual_collist.copy(),
                        proposer=PredefinedProposer(table_name, rg, self.config),
                    )
                )
                columns_assigned_so_far |= colset
        old_proposer_infos = [
            ProposerInfo(columns=gi.columns.copy(), proposer=gi.proposer)
            for gi in new_proposer_infos
        ]
        for colname in columns:
            if colname not in columns_assigned_so_far:
                new_proposer_infos.append(
                    ProposerInfo(
                        columns=[colname],
                        proposer=None,
                    )
                )
        if len(new_proposer_infos) == 0:
            return None

        return GeneratorCmdTableEntry(
            name=table_name,
            old_proposers=old_proposer_infos,
            new_proposers=new_proposer_infos,
        )

    def __init__(
        self,
        settings: DbCmd.Settings,
    ) -> None:
        """
        Initialise a ``GeneratorCmd``.

        :param settings: Settings for the source database.
        """
        super().__init__(settings)
        self.proposers: list[Proposer] | None = None
        self.proposer_index = 0
        self.proposers_valid_columns: Optional[tuple[int, list[str]]] = None
        self.set_prompt()
        self.roles: dict[str, dict[str, RoleEntry]] = {
            table_name: {
                column_name: make_role_entry(column_settings["roles"])
                for column_name, column_settings in get_property(
                    table_settings, "columns", {}
                ).items()
                if "roles" in column_settings
            }
            for table_name, table_settings in get_property(
                settings.config, "tables", {}
            ).items()
        }

    @property
    def table_entries(self) -> list[GeneratorCmdTableEntry]:
        """Get the talbe entries, cast to ``GeneratorCmdTableEntry``."""
        return cast(list[GeneratorCmdTableEntry], self._table_entries)

    def _find_entry_by_table_name(
        self, table_name: str
    ) -> GeneratorCmdTableEntry | None:
        """
        Find the table entry by name.

        :param table_name: The name of the table to find.
        :return: The table entry, or None if no such table name exists.
        """
        entry = super()._find_entry_by_table_name(table_name)
        if entry is None:
            return None
        return cast(GeneratorCmdTableEntry, entry)

    def _set_table_index(self, index: int) -> bool:
        """
        Move to a new table.

        :param index: table index to move to.
        """
        ret = super()._set_table_index(index)
        if ret:
            self.proposer_index = 0
            self.set_prompt()
        return ret

    def _previous_table(self) -> bool:
        """
        Move to the table before the current one.

        :return: True if there is a previous table to go to.
        """
        ret = self._set_table_index(self.table_index - 1)
        if ret:
            table = self.get_table()
            if table is None:
                self.print(
                    "Internal error! table {0} does not have any generators!",
                    self.table_index,
                )
                return False
            self.proposer_index = len(table.new_proposers) - 1
        else:
            self.print(self.ERROR_ALREADY_AT_START)
        return ret

    def get_table(self) -> GeneratorCmdTableEntry | None:
        """Get the current table entry."""
        if self.table_index < len(self.table_entries):
            return self.table_entries[self.table_index]
        return None

    def _get_table_and_proposer(self) -> tuple[str | None, ProposerInfo | None]:
        """Get a pair; the table name then the generator information."""
        if self.table_index < len(self.table_entries):
            entry = self.table_entries[self.table_index]
            if self.proposer_index < len(entry.new_proposers):
                return (entry.name, entry.new_proposers[self.proposer_index])
            return (entry.name, None)
        return (None, None)

    def _get_column_names(self) -> list[str]:
        """Get the (unqualified) names for all the current columns."""
        (_, proposer_info) = self._get_table_and_proposer()
        return proposer_info.columns if proposer_info else []

    def _column_metadata(self) -> list[Column]:
        """Get the metadata for all the current columns."""
        table = self.table_metadata()
        if table is None:
            return []
        return [table.columns[name] for name in self._get_column_names()]

    def set_prompt(self) -> None:
        """Set the prompt according to the current table, column and generator."""
        (table_name, prop_info) = self._get_table_and_proposer()
        if table_name is None:
            self.prompt = "(generators) "
            return
        if prop_info is None:
            self.prompt = f"({table_name}) "
            return
        table = self.table_metadata()
        columns = [
            c + "[pk]" if table.columns[c].primary_key else c for c in prop_info.columns
        ]
        gen = f" ({prop_info.proposer.name()})" if prop_info.proposer else ""
        self.prompt = f"({table_name}.{','.join(columns)}{gen}) "

    def _remove_auto_src_stats(self) -> list[MutableMapping[str, Any]]:
        """
        Remove all automatic source stats.

        We assume every source stats query whose name begins with ``auto__`
        :return: The new ``src_stats`` configuration.
        """
        return self._remove_prefix_src_stats("auto__")

    def _copy_generators(self) -> None:
        """Set generator and query information in the configuration."""
        src_stats = self._remove_auto_src_stats()
        for entry in self.table_entries:
            rgs = []
            new_gens: list[Proposer] = []
            for proposer in entry.new_proposers:
                if proposer.proposer is not None:
                    new_gens.append(proposer.proposer)
                    cqs = proposer.proposer.custom_queries()
                    for cq_key, cq in cqs.items():
                        src_stats.append(
                            {
                                "name": cq_key,
                                "query": cq["query"],
                                "comments": cq.get("comments", []),
                            }
                        )
                    rg: dict[str, Any] = {
                        "name": proposer.proposer.function_name(),
                        "columns_assigned": proposer.columns,
                    }
                    kwn = proposer.proposer.nominal_kwargs()
                    if kwn:
                        rg["kwargs"] = kwn
                    asn = proposer.proposer.nominal_args()
                    if asn:
                        rg["args"] = asn
                    rgs.append(rg)
            aq = get_aggregate_query(new_gens, entry.name, self.engine)
            if aq:
                src_stats.append(
                    {
                        "name": f"auto__{entry.name}",
                        "query": aq,
                        "comments": [
                            q["comment"]
                            for gen in new_gens
                            for q in gen.select_aggregate_clauses().values()
                            if "comment" in q and q["comment"] is not None
                        ],
                    }
                )
            table_config = self.get_table_config(entry.name)
            if rgs:
                table_config["row_generators"] = rgs
            elif "row_generators" in table_config:
                del table_config["row_generators"]
            self.set_table_config(entry.name, table_config)
        self.config["src-stats"] = src_stats

    def _copy_roles(self) -> None:
        """Set role information in the configuration."""
        for table_name, table_roles in self.roles.items():
            for column_name, entry in table_roles.items():
                if entry.new or entry.old:
                    set_property(
                        self.config,
                        ["tables", table_name, "columns", column_name, "roles"],
                        [n.value for n in entry.new],
                    )

    def _copy_entries(self) -> None:
        """Set generator, query and role information in the configuration."""
        self._copy_generators()
        self._copy_roles()

    def _find_old_proposer(
        self, entry: GeneratorCmdTableEntry, columns: Iterable[str]
    ) -> Proposer | None:
        """Find any proposer that previously assigned to these exact same columns."""
        fc = frozenset(columns)
        for gen in entry.old_proposers:
            if frozenset(gen.columns) == fc:
                return gen.proposer
        return None

    def _print_generator_changes(self) -> int:
        """
        Print any changes to generators that have been requested.

        :return: Number of changes found.
        """
        count = 0
        for entry in self.table_entries:
            header_shown = False
            for gen in entry.new_proposers:
                old_gen = self._find_old_proposer(entry, gen.columns)
                new_gen = None if gen is None else gen.proposer
                if old_gen != new_gen:
                    if not header_shown:
                        header_shown = True
                        self.print("Table {0}:", entry.name)
                    count += 1
                    self.print(
                        "...changing {0} from {1} to {2}",
                        ", ".join(gen.columns),
                        old_gen.name() if old_gen else "nothing",
                        gen.proposer.name() if gen.proposer else "nothing",
                    )
        return count

    def _print_role_changes(self) -> int:
        """
        Print any changes to roles that have been requested.

        :return: Number of changes found.
        """
        count = 0
        for table_name, table_entry in self.roles.items():
            for column_name, entry in table_entry.items():
                if entry.old != entry.new:
                    count += 1
                    self.print(
                        "Changing role set of column {0} of table {1} from {2} to {3}",
                        column_name,
                        table_name,
                        text_list((e.value for e in entry.old), "empty"),
                        text_list((e.value for e in entry.new), "empty"),
                    )
        return count

    def do_quit(self, arg: str) -> bool:
        """Check the updates, save them if desired and quit the configurer."""
        count = self._print_generator_changes()
        count += self._print_role_changes()
        if count == 0:
            self.print("You have made no changes.")
        if arg in {"yes", "no"}:
            reply = arg
        else:
            reply = self.ask_save()
        if reply == "yes":
            self._copy_entries()
            logger.debug("Changed entries copied")
            return True
        if reply == "no":
            logger.debug("Configuration unchanged")
            return True
        return False

    def do_tables(self, _arg: str) -> None:
        """List the tables."""
        for entry in self.table_entries:
            gen_count = len(entry.new_proposers)
            how_many = "one generator" if gen_count == 1 else f"{gen_count} generators"
            self.print("{0} ({1})", entry.name, how_many)

    def do_list(self, _arg: str) -> None:
        """List the generators in the current table."""
        if len(self.table_entries) <= self.table_index:
            self.print("Error: no table {0}", self.table_index)
            return
        g_entry = self.table_entries[self.table_index]
        table = self.table_metadata()
        for gen in g_entry.new_proposers:
            old_gen = self._find_old_proposer(g_entry, gen.columns)
            old = "" if old_gen is None else old_gen.name()
            if old_gen == gen.proposer:
                becomes = ""
                if old == "":
                    old = "(not set)"
            elif gen.proposer is None:
                becomes = "(delete)"
            else:
                becomes = f"->{gen.proposer.name()}"
            primary = ""
            if len(gen.columns) == 1 and table.columns[gen.columns[0]].primary_key:
                primary = "[primary-key]"
            self.print("{0}{1}{2} {3}", old, becomes, primary, gen.columns)

    def do_columns(self, _arg: str) -> None:
        """Report the column names and metadata."""
        self.report_columns()

    def get_roles(self, table: str, column: str) -> set[str]:
        """Get the roles for the named column in the named table."""
        if table not in self.roles:
            return set()
        t = self.roles[table]
        if column not in t:
            return set()
        return {r.value for r in t[column].new}

    def do_info(self, _arg: str) -> None:
        """Show information about the current column."""
        for cm in self._column_metadata():
            self.print(
                "Column {0} in table {1} has type {2} ({3}).",
                cm.name,
                cm.table.name,
                str(cm.type),
                "nullable" if cm.nullable else "not nullable",
            )
            if cm.primary_key:
                self.print(
                    "It is a primary key, which usually does not"
                    " need a generator (it will auto-increment)"
                )
            if cm.foreign_keys:
                fk_names = [fk_column_name(fk) for fk in cm.foreign_keys]
                self.print(
                    "It is a foreign key referencing column {0}", ", ".join(fk_names)
                )
                if len(fk_names) == 1 and not cm.primary_key:
                    self.print(
                        "You do not need a generator if you just want"
                        " a uniform choice over the referenced table's rows"
                    )

    def _get_proposer_index(self, table_index: int, column_name: str) -> int | None:
        """
        Get the index number of a column within the list of proposers in this table.

        :param table_index: The index of the table in which to search.
        :param column_name: The name of the column to search for.
        :return: The index in the ``new_proposers`` attribute of the table entry
        containing the specified column, or None if this does not exist.
        """
        entry = self.table_entries[table_index]
        for n, prop in enumerate(entry.new_proposers):
            if column_name in prop.columns:
                return n
        return None

    def _get_table_and_column(self, target: str) -> tuple[int | None, int | None]:
        """
        Turn a table or column name into a table and column index.

        :param target: Either a table name, a column name or a table and
          a column name separated by a dot.
        :return: (None, None) if ``target`` did not refer to a table
          or a column. (None, index) refers to a column in the current table.
          (table_index, column_index) refers to a column within a different
          table.
        """
        table_index = self.get_table_index(target)
        if table_index is not None:
            return (table_index, 0)
        # The whole thing is not a table
        prop_index = self._get_proposer_index(self.table_index, target)
        if prop_index is not None:
            return (None, prop_index)
        # The whole thing isn't a column either, so let's split it:
        (first_part, last_part) = split_column_full_name(target)
        if not first_part:
            # It doesn't split, so that's the end
            self.print(self.ERROR_NO_SUCH_TABLE_OR_COLUMN, last_part)
            return (None, None)
        table_index = self.get_table_index(first_part)
        if table_index is None:
            self.print(self.ERROR_NO_SUCH_TABLE, first_part)
            return (None, None)
        prop_index = self._get_proposer_index(table_index, last_part)
        if prop_index is None:
            self.print(self.ERROR_NO_SUCH_COLUMN, last_part)
            return (None, None)
        return (table_index, prop_index)

    def go_to(self, target: str) -> bool:
        """
        Go to a particular column.

        :return: True on success.
        """
        (table_index, prop_index) = self._get_table_and_column(target)
        if prop_index is None:
            return False
        if table_index is not None:
            self._set_table_index(table_index)
        self.proposer_index = prop_index
        self.set_prompt()
        return True

    def do_next(self, arg: str) -> None:
        """
        Go to the next generator. or a specified generator.

        Go to a named table: 'next tablename',
        go to a column: 'next tablename.columnname',
        or go to a column within this table: 'next columnname'.
        """
        if arg:
            self.go_to(arg)
        else:
            self._go_next()

    def do_n(self, arg: str) -> None:
        """Go to the next generator, or a specified generator."""
        self.do_next(arg)

    def complete_n(self, text: str, line: str, begidx: int, endidx: int) -> list[str]:
        """Complete the ``n`` command's arguments."""
        return self.complete_next(text, line, begidx, endidx)

    def _go_next(self) -> None:
        """Go to the next column."""
        table = self.get_table()
        if table is None:
            self.print("No more tables")
            return
        next_gi = self.proposer_index + 1
        if next_gi == len(table.new_proposers):
            self.next_table(self.INFO_NO_MORE_TABLES)
            return
        self.proposer_index = next_gi
        self.set_prompt()

    def complete_next(
        self, text: str, _line: str, _begidx: int, _endidx: int
    ) -> list[str]:
        """Completions for the arguments of the ``next`` command."""
        (first_part, last_part) = split_column_full_name(text)
        if first_part:
            # first_part is table, last_part is column
            table_index = self.get_table_index(first_part)
            if table_index is None:
                return []
            table_entry = self.table_entries[table_index]
            return [
                f"{first_part}.{column}"
                for prop in table_entry.new_proposers
                for column in prop.columns
                if column.startswith(last_part)
            ]
        # first_part is None, last_part might be table or column.
        table_names = [
            entry.name
            for entry in self.table_entries
            if entry.name.startswith(last_part)
        ]
        if last_part in table_names:
            # we have a complete table name, so allow the completion with a dot to show this
            table_names.append(f"{last_part}.")
        current_table = self.get_table()
        if current_table:
            column_names = [
                col
                for prop in current_table.new_proposers
                for col in prop.columns
                if col.startswith(last_part)
            ]
        else:
            column_names = []
        return table_names + column_names

    def do_previous(self, _arg: str) -> None:
        """Go to the previous generator."""
        if self.proposer_index == 0:
            self._previous_table()
        else:
            self.proposer_index -= 1
        self.set_prompt()

    def do_b(self, arg: str) -> None:
        """Synonym for previous."""
        self.do_previous(arg)

    def _proposers_valid(self) -> bool:
        """Test if ``self.proposers`` is still correct for the current columns."""
        return self.proposers_valid_columns == (
            self.table_index,
            self._get_column_names(),
        )

    def _get_proposer_proposals(self) -> list[Proposer]:
        """
        Get a list of acceptable proposers, sorted by decreasing fit to the actual data.

        The cached list from the most recent 'propose'/'propose all' is
        reused if still valid for the current table/columns, so that 'set
        <number>' numbering matches whichever list was last displayed.
        """
        if not self._proposers_valid() or self.proposers is None:
            columns = self._column_metadata()
            props = everything_factory(self.config, self.metadata).get_proposers(
                columns, self.sync_engine
            )
            sorted_props = sorted(props, key=lambda g: g.fit(9999))
            self.proposers = sorted_props
            self.proposers_valid_columns = (
                self.table_index,
                self._get_column_names().copy(),
            )
        return self.proposers

    # Proposers that emit continuous float output (via random.uniform/
    # normalvariate/lognormvariate) with no integer rounding. That's a
    # legitimate fit for a Numeric/decimal column, but not for a genuinely
    # Integer-typed one: inserting a non-whole float into an Integer column
    # is backend-dependent at best (silently stored as-is on SQLite,
    # truncated or rejected on MySQL) and raises an error on PostgreSQL.
    _CONTINUOUS_FLOAT_PROPOSER_TYPES = (
        GaussianProposer,
        UniformProposer,
        LogNormalProposer,
    )

    def _is_identifier_type_mismatch(
        self,
        proposer: Proposer,
        columns: list[Column],
        profile: EvaluationProfile | None,
    ) -> bool:
        """Check whether `proposer` produces float output invalid for an Integer column.

        Deliberately narrow: only flags this for an ``IDENTIFIER``-profile
        (near-unique) column. For a ``CATEGORICAL``-profile Integer column
        (e.g. a small set of repeated codes/years), the fidelity metric
        already scores a continuous-float proposer at essentially zero on
        its own - it almost never reproduces one of the small set of
        observed discrete values - so no extra guard is needed there. For
        an ``IDENTIFIER``-profile column, the opposite happens: fidelity
        rewards a continuous distribution fit directly to the real column's
        numeric range, which can score almost perfectly while still being
        structurally invalid to insert into an Integer column - that's the
        case this guards against.
        """
        if profile is not EvaluationProfile.IDENTIFIER:
            return False
        if not isinstance(proposer, self._CONTINUOUS_FLOAT_PROPOSER_TYPES):
            return False
        if len(columns) != 1:
            return False
        return isinstance(get_column_type(columns[0]), Integer)

    def _print_privacy(self) -> None:
        """Print the privacy status of the current table."""
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

    def do_compare(self, arg: str) -> None:
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
            "source": [
                x[0] if len(x) == 1 else ", ".join(x)
                for x in self._get_column_data(limit, to_str=str)
            ]
        }
        props: list[Proposer] = self._get_proposer_proposals()
        theme = get_active_theme()
        table_name = self.table_name()
        for argument in args:
            if argument.isdigit():
                n = int(argument)
                if 0 < n <= len(props):
                    prop = props[n - 1]
                    comparison[
                        f"{n}. {theme.function}{prop.name()}"
                    ] = prop.generate_data(limit)
                    self._print_values_queried(table_name, n, prop)
        self.print_table_by_columns(comparison)

    def do_c(self, arg: str) -> None:
        """Synonym for compare."""
        self.do_compare(arg)

    def _print_values_queried(self, table_name: str, n: int, prop: Proposer) -> None:
        """
        Print the values queried from the database for this proposer.

        :param table_name: The name of the table the proposer applies to.
        :param n: A number to print at the start of the output.
        :param gen: The proposer to report.
        """
        theme = get_active_theme()
        if not prop.select_aggregate_clauses() and not prop.custom_queries():
            self.print(
                self.REQUIRES_NO_SOURCE_DATA_TEXT,
                n,
                prop.name(),
                theme.function,
                theme.reset,
            )
        else:
            self.print(
                self.REQUIRES_SOURCE_DATA_TEXT,
                n,
                prop.name(),
                theme.function,
                theme.reset,
            )
            self._print_select_aggregate_query(table_name, prop)
            self._print_custom_queries(prop)

    def _print_custom_queries(self, prop: Proposer) -> None:
        """
        Print all the custom queries and all the values they get in this case.

        :param prop: The proposer to print the custom queries for.
        """
        cqs = prop.custom_queries()
        if not cqs:
            return
        cq_key2args: dict[str, Any] = {}
        nominal = prop.nominal_kwargs()
        actual = prop.actual_kwargs()
        self._get_custom_queries_from(
            cq_key2args,
            nominal,
            actual,
        )
        theme = get_active_theme()
        for cq_key, cq in cqs.items():
            if cq_key not in cq_key2args:
                self.print("There is an error in {0}", cq_key)
                continue
            self.print(
                self.PROVIDING_VALUES_TEXT,
                cq["query"],
                cq_key2args[cq_key],
                theme.query,
                theme.reset,
                theme.data,
            )

    def _get_custom_queries_from(
        self, out: dict[str, Any], nominal: Any, actual: Any
    ) -> None:
        if isinstance(nominal, str):
            src_stat_groups = self.SRC_STAT_RE.search(nominal)
            # Do we have a SRC_STAT reference?
            if src_stat_groups:
                # Get its name
                cq_key = src_stat_groups.group(1)
                # Are we pulling a specific part of this result?
                sub = src_stat_groups.group(3)
                if sub:
                    if cq_key in out:
                        out[cq_key][sub] = actual
                    else:
                        out[cq_key] = {sub: actual}
                else:
                    out[cq_key] = actual
        elif isinstance(nominal, Sequence) and isinstance(actual, Sequence):
            for i in range(min(len(nominal), len(actual))):
                self._get_custom_queries_from(out, nominal[i], actual[i])
        elif isinstance(nominal, Mapping) and isinstance(actual, Mapping):
            for k, v in nominal.items():
                if k in actual:
                    self._get_custom_queries_from(out, v, actual[k])

    def _print_select_aggregate_query(self, table_name: str, prop: Proposer) -> None:
        """
        Print the select aggregate query and all the values it gets in this case.

        This is not the entire query that will be executed, but only the part of it
        that is required by a certain proposer.
        :param table_name: The table name.
        :param prop: The proposer to limit the aggregate query to.
        """
        sacs = prop.select_aggregate_clauses()
        if not sacs:
            return
        kwa = prop.actual_kwargs()
        vals = []
        src_stat2kwarg = {v: k for k, v in prop.nominal_kwargs().items()}
        for n in sacs.keys():
            src_stat = f'SRC_STATS["auto__{table_name}"]["results"][0]["{n}"]'
            if src_stat in src_stat2kwarg:
                ak = src_stat2kwarg[src_stat]
                if ak in kwa:
                    vals.append(kwa[ak])
                else:
                    logger.warning(
                        "actual_kwargs for %s does not report %s", prop.name(), ak
                    )
            else:
                logger.warning(
                    (
                        "nominal_kwargs for %s does not have a value"
                        ' SRC_STATS["auto__%s"]["results"][0]["%s"]'
                    ),
                    prop.name(),
                    table_name,
                    n,
                )
        select_q = get_aggregate_query([prop], table_name, self.engine)
        theme = get_active_theme()
        self.print(
            "{2}{0}{3}; providing the following values: {4}{1}",
            select_q,
            vals,
            theme.query,
            theme.reset,
            theme.data,
        )

    def _get_column_data(
        self, count: int, to_str: Callable[[Any], str] = repr
    ) -> list[list[str]]:
        column_names = self._get_column_names()
        table = self.table_metadata()
        columns = [table.columns[c] for c in column_names]
        nonnull_clauses = [col.isnot(None) for col in columns]
        stmt = (
            select(*columns)
            .select_from(table)
            .where(and_(*nonnull_clauses))
            .order_by(Random())
            .limit(count)
        )
        with self.sync_engine.connect() as connection:
            result = connection.execute(stmt)
            return [[to_str(x) for x in xs] for xs in result.all()]

    def do_propose(  # pylint: disable=too-many-locals too-many-branches too-many-statements
        self, _arg: str
    ) -> None:
        """
        Display a ranked table of candidate generators for this column.

        Each candidate generator is tried against a sample of the real
        data and scored on how well it matches (fidelity), how much new
        data it produces (novelty) and how varied its own output is
        (diversity). The single best-scoring candidate is shown above the
        table as 'Recommended: N. <name>'.

        To keep the table short, two kinds of candidate are left out by
        default - run 'propose all' to see everything instead:

        - a candidate that's strictly worse than some other candidate on
          every one of fidelity/novelty/diversity at once ("dominated",
          beyond the shown MAX_PROPOSERS_SHOWN cap);
        - a candidate whose output type doesn't actually fit a genuinely
          unique Integer column (e.g. a continuous-float generator) - this
          one is also never the recommendation, even under 'propose all'.

        The results can be compared (against a sample of the real data in
        the column and against each other) with the 'compare' command. Type
        'help ranking' for how the score is computed, or 'help pareto' for
        what the Front column means - or see the "Evaluating and ranking
        proposals" section of the docs for the full detail.
        """
        theme = get_active_theme()
        limit = 5
        include_all = _arg.strip().lower() == "all"
        props = self._get_proposer_proposals()
        sample = self._get_column_data(limit)
        if not props:
            self.print(self.PROPOSE_NOTHING)

        columns = self._column_metadata()
        column_evaluator = ColumnEvaluator(columns, self.sync_engine)
        results = [column_evaluator.evaluate(proposer) for proposer in props]

        profile = column_evaluator.profile
        column_name = (
            column_evaluator.column.name
            if column_evaluator.column is not None
            else None
        )
        real_uniqueness = column_evaluator.real_uniqueness
        is_numeric_column = column_evaluator.column_is_numeric
        is_primary_key = column_evaluator.is_primary_key
        is_unique_constrained = column_evaluator.is_unique_constrained
        type_incompatible = [
            self._is_identifier_type_mismatch(result.proposer, columns, profile)
            for result in results
        ]
        ranking = rank_proposals(
            results,
            profile,
            theme,
            column_name=column_name,
            real_uniqueness=real_uniqueness,
            is_numeric_column=is_numeric_column,
            is_primary_key=is_primary_key,
            is_unique_constrained=is_unique_constrained,
            type_incompatible=type_incompatible,
        )
        display = format_ranking_display(ranking, results, profile)

        if display.recommendation is not None:
            self.print("\n")
            if display.no_uniqueness_guarantee:
                # There's no actual pick here ("Recommended: none - ..."),
                # so highlighting it the same way as a real recommendation
                # would falsely claim confidence in a pick that doesn't exist.
                self.print(display.recommendation)
            else:
                self.print(f"{theme.recommend}{display.recommendation}{theme.reset}")
        if display.weak_recommendation_warning is not None:
            self.print(display.weak_recommendation_warning)

        self.print(display.profile_summary)

        indices_to_show = self._select_indices_to_show(display, ranking, include_all)
        rows_to_show = [display.rows[i] for i in indices_to_show]

        self.print_table(
            [
                "#",
                "Proposer",
                "Front",
                "Score",
                "Keyword",
                "Fidelity",
                "Novelty",
                "Diversity",
                "Copies",
                "Uniqueness",
                "Penalty",
            ],
            rows_to_show,
        )

        self.print("\n")
        self._print_source_sample(sample, theme)
        for i in indices_to_show:
            prop = props[i]
            self.print(
                self.RANKED_SAMPLE_TEXT,
                index=i + 1,
                name=prop.name(),
                sample="; ".join(map(repr, prop.generate_data(limit))),
                theme_func=theme.function,
                theme_data=theme.data,
                theme_reset=theme.reset,
            )

        print("\n")

    def _print_source_sample(self, sample: list[list[str]], theme: Theme) -> None:
        """Print a sample of the real column values, or a note that there are none."""
        if sample:
            rep = [x[0] if len(x) == 1 else ",".join(x) for x in sample]
            self.print(self.PROPOSE_SOURCE_SAMPLE_TEXT, "; ".join(rep), theme.data)
        else:
            self.print(self.PROPOSE_SOURCE_EMPTY_TEXT)

    def _select_indices_to_show(
        self,
        display: ProposalRankingDisplay,
        ranking: ProposalRanking,
        include_all: bool,
    ) -> list[int]:
        """Pick which candidate rows to show in the propose table.

        By default, only show Pareto front 1: a front-2+ candidate is, by
        definition, strictly worse than some front-1 one on fidelity,
        novelty AND diversity simultaneously, so it adds nothing to a
        shortlist. A candidate whose output type doesn't match the column
        (see ``GeneratorCmd._is_identifier_type_mismatch``) is also hidden
        by default, even if it's front 1 - the statistical score has no way
        to detect that mismatch at all, so a high score there doesn't mean
        it's safe to use. 'propose all' bypasses both to show every
        candidate. The returned indices let the sample listing after the
        table map each shown row back to its proposer without parsing the
        (possibly theme-colored) '#' cell text.
        """
        indices_to_show = list(range(len(display.rows)))
        if include_all:
            return indices_to_show

        indices_to_show = [
            i
            for i in indices_to_show
            if display.fronts[i] == 1
            and not (
                i < len(display.type_incompatible) and display.type_incompatible[i]
            )
        ]
        # The recommendation is picked by combined score across *all*
        # candidates, not just front 1 (see rank_proposals) - a
        # dominated candidate can still win once the resample penalty
        # and keyword boost are applied, since those are computed after
        # fronts are derived from the raw fidelity/novelty/diversity
        # values. Force it into the table even when front-2+, otherwise
        # "Recommended: N. x" can point at a row the user never sees.
        recommended_outside_front_1 = (
            ranking.recommended_index is not None
            and ranking.recommended_index not in indices_to_show
        )
        if recommended_outside_front_1:
            indices_to_show.append(ranking.recommended_index)
        capped = len(indices_to_show) > self.MAX_PROPOSERS_SHOWN
        if capped:
            # Keep the highest-scoring candidates - front 1 alone can
            # still hold more rows than are worth scanning in a
            # terminal table (nothing dominates them, but they're not
            # all equally worth showing). Always keep the recommendation
            # even if its score wouldn't otherwise make the cut, since
            # it's referenced by name right above this table.
            ranked = sorted(
                indices_to_show, key=lambda i: ranking.scores[i], reverse=True
            )
            keep = set(ranked[: self.MAX_PROPOSERS_SHOWN])
            if ranking.recommended_index is not None:
                keep.add(ranking.recommended_index)
            indices_to_show = sorted(keep)
        else:
            indices_to_show.sort()
        if len(indices_to_show) < len(display.rows):
            if capped:
                qualifier = (
                    f" (Pareto front 1, top {self.MAX_PROPOSERS_SHOWN} by score,"
                    " plus the recommendation)"
                )
            elif recommended_outside_front_1:
                qualifier = " (Pareto front 1, plus the recommendation)"
            else:
                qualifier = " (Pareto front 1 only)"
            self.print(
                "Showing {0} of {1} candidates" + qualifier + "."
                " Run 'propose all' to see the rest.",
                len(indices_to_show),
                len(display.rows),
            )
        return indices_to_show

    def do_p(self, arg: str) -> None:
        """Synonym for propose."""
        self.do_propose(arg)

    def help_ranking(self) -> None:
        """Explain how 'propose' scores and ranks candidate generators."""
        self.print(
            "Each candidate's synthetic output is compared against a sample"
            " of the real column on three measures: Fidelity (does it"
            " statistically match the real data), Novelty (does it produce"
            " values not already in the real data) and Diversity (how"
            " varied is its own output). These combine into one Score,"
            " weighted differently depending on the column's profile - e.g."
            " an identifier-like column weighs fidelity much more heavily"
            " than a free-text one.\n"
            "\n"
            "Two adjustments apply on top of that: a column-name hint"
            " (e.g. a column called 'first_name' boosts a matching"
            " generic.person.* candidate) and a privacy penalty that"
            " discounts a candidate for reproducing real values verbatim,"
            " scaled by how unique the real column actually is.\n"
            "\n"
            "The single highest-scoring candidate is shown as 'Recommended'"
            ". Type 'help pareto' for what the Front column means, or see"
            ' the "Evaluating and ranking proposals" section of the docs'
            " for the full formula."
        )

    def help_pareto(self) -> None:
        """Explain the 'Front' column shown by 'propose'."""
        self.print(
            "A candidate is in Pareto front 1 if no other candidate beats"
            " it on Fidelity, Novelty AND Diversity all at once - front 2"
            " means some front-1 candidate beats it on all three, and so"
            " on. By default 'propose' only shows front 1 (plus the"
            " recommendation, even if it's in a later front) to keep the"
            " table short; run 'propose all' to see every front.\n"
            "\n"
            "You don't need to understand this to use 'propose' - the"
            " Recommended line and Score column are usually enough. Front"
            " is mainly useful for seeing which candidates are genuine"
            " trade-offs against each other (e.g. one is more novel,"
            " another fits better) rather than one simply being worse than"
            " another."
        )

    def get_proposer_by_name(self, gen_name: str) -> Proposer | None:
        """Find a proposer by name from the list of proposals."""
        for gen in self._get_proposer_proposals():
            if gen.name() == gen_name:
                return gen
        return None

    def do_set(self, arg: str) -> None:
        """Set one of the proposals as a generator."""
        if arg.isdigit() and not self._proposers_valid():
            self.print("Please run 'propose' before 'set <number>'")
            return
        gens = self._get_proposer_proposals()
        new_gen: Proposer | None
        if arg.isdigit():
            index = int(arg)
            if index < 1:
                self.print("set's integer argument must be at least 1")
                return
            if len(gens) < index:
                self.print(
                    "There are currently only {0} generators proposed, please select one of them.",
                    len(gens),
                )
                return
            new_gen = gens[index - 1]
        else:
            new_gen = self.get_proposer_by_name(arg)
            if new_gen is None:
                self.print("'{0}' is not an appropriate generator for this column", arg)
                return
        self.set_proposer(new_gen)
        self._go_next()

    def set_proposer(self, prop: Proposer | None) -> None:
        """Set the current column's generator."""
        (table, gen_info) = self._get_table_and_proposer()
        if table is None:
            self.print("Error: no table")
            return
        if gen_info is None:
            self.print("Error: no column")
            return
        gen_info.proposer = prop

    def do_s(self, arg: str) -> None:
        """Synonym for set."""
        self.do_set(arg)

    def do_unset(self, _arg: str) -> None:
        """Remove any generator set for this column."""
        self.set_proposer(None)
        self._go_next()

    def merge_columns(self, arg: str) -> bool:
        """
        Add this column(s) to the specified column(s).

        After this, one generator will cover them all.
        """
        cols = arg.split()
        if not cols:
            self.print("Error: merge requires a column argument")
        table_entry: GeneratorCmdTableEntry | None = self.get_table()
        if table_entry is None:
            self.print(self.ERROR_NO_SUCH_TABLE)
            return False
        cols_available = functools.reduce(
            lambda x, y: x | y,
            [frozenset(gen.columns) for gen in table_entry.new_proposers],
        )
        cols_to_merge = frozenset(cols)
        unknown_cols = cols_to_merge - cols_available
        if unknown_cols:
            for uc in unknown_cols:
                self.print(self.ERROR_NO_SUCH_COLUMN, uc)
            return False
        gen_info = table_entry.new_proposers[self.proposer_index]
        stated_current_columns = cols_to_merge & frozenset(gen_info.columns)
        if stated_current_columns:
            for c in stated_current_columns:
                self.print(self.ERROR_COLUMN_ALREADY_MERGED, c)
            return False
        # Remove cols_to_merge from each proposer
        new_new_proposers: list[ProposerInfo] = []
        for gen in table_entry.new_proposers:
            if gen is gen_info:
                # Add columns to this proposer
                self.proposer_index = len(new_new_proposers)
                new_new_proposers.append(
                    ProposerInfo(
                        columns=gen.columns + cols,
                        proposer=None,
                    )
                )
            else:
                # Remove columns if applicable
                new_columns = [c for c in gen.columns if c not in cols_to_merge]
                is_changed = len(new_columns) != len(gen.columns)
                if new_columns:
                    # We have not removed this proposer completely
                    new_new_proposers.append(
                        ProposerInfo(
                            columns=new_columns,
                            proposer=None if is_changed else gen.proposer,
                        )
                    )
        table_entry.new_proposers = new_new_proposers
        self.set_prompt()
        return True

    def do_merge(self, arg: str) -> None:
        """Add this column(s) to the specified column(s), so one generator covers them all."""
        self.merge_columns(arg)

    def complete_merge(
        self, text: str, _line: str, _begidx: int, _endidx: int
    ) -> list[str]:
        """Complete column names."""
        table_entry: GeneratorCmdTableEntry | None = self.get_table()
        if table_entry is None:
            return []
        return [
            column
            for i, gen in enumerate(table_entry.new_proposers)
            if i != self.proposer_index
            for column in gen.columns
            if column.startswith(text)
        ]

    def do_unmerge(self, arg: str) -> None:
        """Remove this column(s) from this generator, make them a separate generator."""
        cols = arg.split()
        if not cols:
            self.print("Error: merge requires a column argument")
        table_entry: GeneratorCmdTableEntry | None = self.get_table()
        if table_entry is None:
            self.print(self.ERROR_NO_SUCH_TABLE)
            return
        prop_info = table_entry.new_proposers[self.proposer_index]
        current_columns = frozenset(prop_info.columns)
        cols_to_unmerge = frozenset(cols)
        unknown_cols = cols_to_unmerge - current_columns
        if unknown_cols:
            for uc in unknown_cols:
                self.print(self.ERROR_NO_SUCH_COLUMN, uc)
            return
        stated_unmerged_columns = cols_to_unmerge - current_columns
        if stated_unmerged_columns:
            for c in stated_unmerged_columns:
                self.print(self.ERROR_COLUMN_ALREADY_UNMERGED, c)
            return
        if cols_to_unmerge == current_columns:
            self.print(self.ERROR_CANNOT_UNMERGE_ALL)
            return
        # Remove unmerged columns
        for um in cols_to_unmerge:
            prop_info.columns.remove(um)
        # The existing proposer will not work
        prop_info.proposer = None
        # And put them into a new (empty) proposer
        table_entry.new_proposers.insert(
            self.proposer_index + 1,
            ProposerInfo(
                columns=cols,
                proposer=None,
            ),
        )
        self.set_prompt()

    def complete_unmerge(
        self, text: str, _line: str, _begidx: int, _endidx: int
    ) -> list[str]:
        """Complete column names to unmerge."""
        table_entry: GeneratorCmdTableEntry | None = self.get_table()
        if table_entry is None:
            return []
        return [
            column
            for column in table_entry.new_proposers[self.proposer_index].columns
            if column.startswith(text)
        ]

    def get_current_columns(self) -> set[str]:
        """Get the current colums."""
        table_entry: GeneratorCmdTableEntry | None = self.get_table()
        if table_entry is None:
            return set()
        prop_info = table_entry.new_proposers[self.proposer_index]
        return set(prop_info.columns)

    def set_merged_columns(self, first_col: str, other_cols: str) -> bool:
        """
        Merge columns, after unmerging everything we don't want.

        :param first_col: The first column we want in the merge, must already
        be in this column set.
        :param other_cols: all the columns we want merged other than
        first_col, in order, space-separated.
        :return: True if the merge worked, false if there was an error
        """
        existing = self.get_current_columns()
        existing.discard(first_col)
        for to_remove in existing:
            self.do_unmerge(to_remove)
        return self.merge_columns(other_cols)

    # pylint: disable=too-many-return-statements
    def parse_role(self, arg: str) -> tuple[str | None, RoleCommand] | None:
        """
        Parse the `role` command arguments.

        :param arg: The arguments; the text after ``role ``.
        :return: A pair of the column name and the command specifed by
         the arguments. Each is None if unspecified. None overall is returned
         if the parse fails.
        """
        args = arg.split()
        if len(args) == 0:
            self.print("role requires a subcommand: list, set or delete.")
            return None
        column: str | None = None
        command: RoleCommand | None = None
        n = 0
        while n < len(args):
            param = args[n + 1] if n + 1 < len(args) else None
            match args[n].lower():
                case "on":
                    if param is None:
                        self.print("'on' requires a column name")
                        return None
                    column = param
                    n += 2
                case "list":
                    command = RoleCommandList(
                        self.NO_ROLES_TEXT,
                        self.print,
                    )
                    n += 1
                case "set":
                    if param is None:
                        self.print("'role set' requires a role name")
                        return None
                    if not is_role(param):
                        self.print("role must be one of {0}", ", ".join(Role))
                        return None
                    command = RoleCommandSet(Role(param))
                    n += 2
                case "delete":
                    if param is None:
                        self.print("'delete' requires a role name")
                        return None
                    if not is_role(param):
                        self.print("role must be one of {0}", ", ".join(Role))
                        return None
                    command = RoleCommandDelete(Role(param))
                    n += 2
                case _:
                    self.print("Did not understand role command '{0}'", args[n])
                    self.print("should be 'on', 'list', 'set' or 'delete'")
                    return None
        if command is None:
            self.print("'role' requires a command 'set', 'list' or 'delete'")
            return None
        return (column, command)

    def do_role(self, arg: str) -> None:
        """
        List, Set or Delete roles on this column.

        List the roles on this column: role list
        Set role 'start' on this column: role add start
        Delete role 'start' on this column: role delete start
        Add 'on mycolumn' or 'on mytable.mycolumn' to
        check or alter a different column, for example
        role on mytable.mycolumn list
        """
        parse = self.parse_role(arg)
        if parse is None:
            return
        (column, command) = parse
        if not column:
            cols = self.get_current_columns()
            if len(cols) != 1:
                self.print(
                    "We are not on one column, so an 'on columnname' must be specified"
                )
                return
            column = list(cols)[0]
        (table, col) = split_column_full_name(column)
        if not table:
            entry = self.get_table()
            if entry is None:
                self.print("Error, no tables!")
                return
            table = entry.name
        if table not in self.metadata.tables:
            self.print("Error: no such table '{0}'", table)
            return
        tm = self.metadata.tables[table]
        if col not in tm.columns:
            self.print("Error: no such column '{0}' in table '{1}'", col, table)
            return
        table_roles: dict[str, RoleEntry] = self.roles.get(table, {})
        column_roles: RoleEntry | None = table_roles.get(col, None)
        new = command.do(column_roles)
        if new:
            table_roles[col] = new
            self.roles[table] = table_roles

    def complete_role(
        self, text: str, line: str, begidx: int, _endidx: int
    ) -> list[str]:
        """Complete column names."""
        prev = line[:begidx].rsplit(maxsplit=1)[-1]
        if prev == "on":
            return self.get_table_or_column_completions(text)
        if prev in {"set", "delete"}:
            return [
                com.value
                for com in Role.__members__.values()
                if com.value.startswith(text)
            ]
        return [com for com in ["list", "set", "delete"] if com.startswith(text)]


def try_setting_generator(gc: GeneratorCmd, proposers: Iterable[str]) -> bool:
    """
    Set the current proposer by name if possible.

    :param gc: The interactive ``GeneratorCmd`` to use.
    :param proposers: A list of names of proposers to try, in order.
    :return: True if one of the proposers was successfully set, False otherwise.
    """
    for prop in proposers:
        new_prop = gc.get_proposer_by_name(prop)
        if new_prop is not None:
            gc.set_proposer(new_prop)
            return True
    return False
