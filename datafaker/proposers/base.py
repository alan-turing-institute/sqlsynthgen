"""Basic Proposers and their factories."""

import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Union

import mimesis
import mimesis.locales
from sqlalchemy import Column, Engine, Join, Table, func, select
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import aliased
from sqlalchemy.types import Integer, Numeric, String, TypeEngine
from typing_extensions import Self

from datafaker.dialects import StdDev
from datafaker.providers import DistributionProvider
from datafaker.utils import T, logger

NumericType = Union[int, float]


dist_gen = DistributionProvider()
generic = mimesis.Generic(locale=mimesis.locales.Locale.EN_GB)


class ProposerError(Exception):
    """Error thrown from Datafaker Proposers."""


class Proposer(ABC):
    """
    Random data generator proposer.

    A proposer is specific to a particular column (set) in a particular table
    in a particluar database.

    A proposer knows how to fetch its summary data from the database, how to calculate
    its fit (if apropriate) and which function actually does the generation.

    It also knows these summary statistics for the column it was instantiated on,
    and therefore knows how to generate fake data for that column.
    """

    @abstractmethod
    def function_name(self) -> str:
        """Get the name of the generator function to call to generate the data."""

    def name(self) -> str:
        """
        Get the name of the proposer.

        Usually the same as the function name, but can be different to distinguish
        between proposers that have the same function but different queries.
        """
        return self.function_name()

    @abstractmethod
    def nominal_kwargs(self) -> dict[str, str]:
        """
        Get the kwargs the generator wants to be called with.

        The values will tend to be references to something in the src-stats.yaml
        file.
        For example {"avg_age": 'SRC_STATS["auto__patient"]["results"][0]["age_mean"]'} will
        provide the value stored in src-stats.yaml as
        SRC_STATS["auto__patient"]["results"][0]["age_mean"] as the "avg_age" argument
        to the generator function.
        """

    def nominal_args(self) -> list[str]:
        """Get the args the generator wants to be called with."""
        return []

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """
        Get the SQL clauses to add to a SELECT ... FROM {table} query.

        Will add to SRC_STATS["auto__{table}"]
        For example {
            "count": {
                "clause": "COUNT(*)",
                "comment": "number of rows in table {table}"
            }, "avg_thiscolumn": {
                "clause": "AVG(thiscolumn)",
                "comment": "Average value of thiscolumn in table {table}"
        }}
        will make the clause become:
        "SELECT COUNT(*) AS count, AVG(thiscolumn) AS avg_thiscolumn FROM thistable"
        and this will populate SRC_STATS["auto__thistable"]["results"][0]["count"] and
        SRC_STATS["auto__thistable"]["results"][0]["avg_thiscolumn"] in the src-stats.yaml file.
        """
        return {}

    def custom_queries(self) -> dict[str, dict[str, Any]]:
        """
        Get the SQL queries to add to SRC_STATS.

        Should be used for queries that do not follow the SELECT ... FROM table format
        using aggregate queries, because these should use select_aggregate_clauses.

        For example {"myquery": {
            "query": "SELECT one, too AS two FROM mytable WHERE too > 1",
            "comments": ["big enough one and two from table mytable"]
        }}
        will populate SRC_STATS["myquery"]["results"][0]["one"]
        and SRC_STATS["myquery"]["results"][0]["two"]
        in the src-stats.yaml file.

        Keys should be chosen to minimize the chances of clashing with other queries,
        for example "auto__{table}__{column}__{queryname}"

        In order for your query to work for all database types, alias each table
        referenced in both FROM and SELECT clauses, only use the aliased value,
        and double quote all table names.

        The following are OK:

        - SELECT AVG(column) FROM "table" WHERE column > 3
        - SELECT AVG(a.column) FROM "table" AS a WHERE a.column > 3

        The following are not OK:

        - SELECT AVG("table".column) FROM "table" WHERE "table".column > 3
        - SELECT AVG(a.column) FROM table AS a WHERE a.column > 3
        """
        return {}

    @abstractmethod
    def actual_kwargs(self) -> dict[str, Any]:
        """
        Get the kwargs (summary statistics) this generator is instantiated with.

        This must match `nominal_kwargs` in structure.
        """

    @abstractmethod
    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""

    def fit(self, default: float = -1) -> float:
        """
        Return a value representing how well the distribution fits the real source data.

        0.0 means "perfectly".
        Returns default if no fitness has been defined.
        """
        return default

    def guarantees_fresh_uniqueness(self) -> bool:
        """
        Whether this proposer is structurally guaranteed never to collide.

        True only for a proposer whose *construction* rules out ever
        reproducing a real or previously-generated value (e.g. a sequence
        continuing past the observed maximum) - not merely one that happens
        to have a low *observed* collision rate in a given sample. Used to
        let this kind of proposer take precedence for a column that needs
        guaranteed-unique values, since the fidelity/novelty/diversity
        metrics can only ever read "deliberately diverges from the observed
        range" as a poor statistical fit, never as the intended behavior.
        """
        return False


class PredefinedProposer(Proposer):
    """
    Proposer built from an existing config.yaml.

    Does not actually propose, it just represents generators
    that have been defined previously.
    """

    SELECT_AGGREGATE_RE = re.compile(
        r"SELECT (.*) FROM ((?:[A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*)"
    )
    AS_CLAUSE_RE = re.compile(r" *(.+) +AS +([A-Za-z_][A-Za-z0-9_]*) *")
    SRC_STAT_NAME_RE = re.compile(r'\bSRC_STATS\["([^]]*)"\].*')

    def _get_src_stats_mentioned(self, val: Any) -> set[str]:
        if not val:
            return set()
        if isinstance(val, str):
            ss = self.SRC_STAT_NAME_RE.match(val)
            if ss:
                ss_name = ss.group(1)
                logger.debug("Found SRC_STATS reference %s", ss_name)
                return set([ss_name])
            logger.debug("Value %s does not seem to be a SRC_STATS reference", val)
            return set()
        if isinstance(val, list):
            return set.union(*(self._get_src_stats_mentioned(v) for v in val))
        if isinstance(val, dict):
            return set.union(*(self._get_src_stats_mentioned(v) for v in val.values()))
        return set()

    def __init__(
        self,
        table_name: str,
        generator_object: Mapping[str, Any],
        config: Mapping[str, Any],
    ):
        """
        Initialise a proposer from a config.yaml.

        :param config: The entire configuration.
        :param generator_object: The part of the configuration at tables.*.row_generators
        """
        logger.debug(
            "Creating a PredefinedProposer %s from table %s",
            generator_object["name"],
            table_name,
        )
        self._table_name = table_name
        self._name: str = generator_object["name"]
        self._kwn: dict[str, str] = generator_object.get("kwargs", {})
        self._asn: list[str] = generator_object.get("args", [])
        self._src_stats_mentioned = self._get_src_stats_mentioned(self._kwn)
        self._select_aggregate_clauses: dict[str, dict[str, str | Any]] = {}
        self._custom_queries = {}
        for sstat in config.get("src-stats", []):
            name: str = sstat["name"]
            dpq = sstat.get("dp-query", None)
            query = sstat.get(
                "query", dpq
            )  # ... should we really be combining query and dp-query?
            comments = sstat.get("comments", [])
            if name in self._src_stats_mentioned:
                logger.debug("Found a src-stats entry for %s", name)
                # This query is one that this generator is interested in
                sam = None if query is None else self.SELECT_AGGREGATE_RE.match(query)
                # sam.group(2) is the table name from the FROM clause of the query
                if sam and name == f"auto__{sam.group(2).split('.')[-1]}":
                    # name is auto__{table_name}, so it's a select_aggregate,
                    # so we split up its clauses
                    sacs = [
                        self.AS_CLAUSE_RE.match(clause)
                        for clause in sam.group(1).split(",")
                    ]
                    # Work out what select_aggregate_clauses this represents
                    for sac in sacs:
                        if sac is not None:
                            comment = comments.pop() if comments else None
                            self._select_aggregate_clauses[sac.group(2)] = {
                                "clause": sac.group(1),
                                "comment": comment,
                            }
                else:
                    # some other name, so must be a custom query
                    logger.debug("Custom query %s is '%s'", name, query)
                    self._custom_queries[name] = {
                        "query": query,
                        "comments": comments,
                    }

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return self._name

    def nominal_kwargs(self) -> dict[str, str]:
        """Get the keyword arguments to be entered into ``config.yaml``."""
        return self._kwn

    def nominal_args(self) -> list[str]:
        """Get the position arguments to be entered into ``config.yaml``."""
        return self._asn

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """Get the query fragments the generators need to call."""
        return self._select_aggregate_clauses

    def custom_queries(self) -> dict[str, dict[str, Any]]:
        """Get the queries the generators need to call."""
        return self._custom_queries

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        # Run the queries from nominal_kwargs
        # ...
        logger.error("PredefinedProposer.actual_kwargs not implemented yet")
        return {}

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        # Call the function if we can. This could be tricky...
        # ...
        logger.error("PredefinedProposer.generate_data not implemented yet")
        return []


class ProposerFactory(ABC):
    """A factory for making proposers appropriate for a database column."""

    @abstractmethod
    def get_proposers(
        self,
        columns: list[Column],
        engine: Engine,
    ) -> Sequence[Proposer]:
        """Get the proposers appropriate to these columns."""


def fit_from_buckets(xs: Sequence[NumericType], ys: Sequence[NumericType]) -> float:
    """Calculate the fit by comparing a pair of lists of buckets."""
    sum_diff_squared = sum(map(lambda t, a: (t - a) * (t - a), xs, ys))
    count = len(ys)
    return sum_diff_squared / (count * count)


class ForeignKeyRelationship:
    """How a table is related."""

    def __init__(self, fk_column: Column, target_column: Column) -> None:
        """
        Initialise a ``ForeignKeyRelationship``.

        :param fk_column: The column containing the foreign key (the source).
        :param target_column: The column that ``fk_column`` matches in
          the target table.
        """
        self.fk_column = fk_column
        table = aliased(target_column.table)
        self.target_column = table.columns[target_column.name]

    def __repr__(self):
        """Render the relationship as a JOIN."""
        return (
            f"<JOIN {self.target_column.table.original.name} ON"
            f" {self.fk_column} = {self.target_column.name}>"
        )

    def target_table(self) -> Table:
        """Get the related table."""
        return self.target_column.table

    def target_table_name(self) -> str:
        """
        Get the name of the target table.

        Don't use ``target_table().name`` because this will give the
        name of the aliased table.
        """
        return str(self.target_column.table.original.name)


def _get_one(seq: Iterable[T]) -> T | None:
    """Get an arbitrary member of the sequence, or None if it is empty."""
    for elt in seq:
        return elt
    return None


def make_foreign_key_relationship(fk_column: Column) -> ForeignKeyRelationship | None:
    """Make a ``ForeignKeyRelationship`` from a foreign key column."""
    if (elt := _get_one(fk_column.foreign_keys)) is None:
        return None
    return ForeignKeyRelationship(fk_column, elt.column)


@dataclass
class RelatedColumn:
    """A column in a related table, and how it is related."""

    target: Column
    relationship: ForeignKeyRelationship | None

    def add_from_to_query(self, query: Any) -> Any:
        """Take a query and add a ``select_from`` and maybe ``join`` as appropriate."""
        if self.relationship is None:
            return query.select_from(self.target.table)
        query = query.select_from(self.relationship.fk_column.table)
        return self.add_join_to_query(query)

    def add_join_to_query(self, query: Any) -> Any:
        """Take a query and maybe add ``join`` as appropriate."""
        rel = self.relationship
        if rel is None:
            return query
        return query.join(
            rel.target_column.table,
            onclause=rel.fk_column == rel.target_column,
        )

    def target_table(self) -> Table | None:
        """Get the related table, or None if it's the same table."""
        if self.relationship is None:
            return None
        return self.relationship.target_table()

    def target_table_name(self) -> str | None:
        """Get the name of the target table, or None if it's the same table."""
        if self.relationship is None:
            return None
        return self.relationship.target_table_name()


class Buckets:
    """
    Measured buckets for a real distribution.

    Finds the real distribution of continuous data so that we can measure
    the fit of generators against it.
    """

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        engine: Engine,
        table: Table | Join,
        column: Any,
        mean: float,
        stddev: float,
        count: int,
        anchor_relation: RelatedColumn | None = None,
    ):
        """Initialise a Buckets object."""
        self.mean = mean
        self.stddev = stddev
        query = self._get_bucket_query(table, column, anchor_relation)
        with engine.connect() as connection:
            raw_buckets = connection.execute(query.group_by("b"))
            self.buckets: Sequence[int] = [0] * 10
            for rb in raw_buckets:
                try:
                    x = float(rb.b)
                    if x.is_integer():
                        bucket = min(9, max(0, int(x) + 1))
                        self.buckets[bucket] += rb.f / count
                except TypeError:
                    # We get a type error if there are no rows returned at all
                    # because rb.b is None in this case.
                    # We could just test for None explicitly, but this way
                    # catches errors if SQLAlchemy returns something that
                    # isn't a number for some other unknown reason.
                    pass

    def _get_bucket_query(
        self,
        table: Table | Join,
        column: Any,
        anchor_relation: RelatedColumn | None = None,
    ) -> Any:
        """Make the query for half-sd buckets."""
        bottom = self.mean - 2 * self.stddev
        width = self.stddev / 2
        query = select(
            func.count(column).label("f"),  # pylint: disable=not-callable
            ((func.floor(column) - bottom) / width).label("b"),
        ).select_from(table)
        if anchor_relation is not None:
            query = anchor_relation.add_join_to_query(query)
        return query

    @classmethod
    def make_buckets(
        cls,
        engine: Engine,
        table: Table | Join,
        column: Any,
        anchor_relation: RelatedColumn | None = None,
    ) -> Self | None:
        """
        Construct a Buckets object.

        Calculates the mean and standard deviation of the values in the column
        specified and makes ten buckets, centered on the mean and each half
        a standard deviation wide (except for the end two that extend to
        infinity). Each bucket will be set to the count of the number of values
        in the column within that bucket.

        :param engine: SQLAlchemy engine.
        :param table: SQLAlchemy table (or joined tables) to pull data from.
        :param column: SQLAlchemy column or expression to measure.
        :param anchor_relation: A relationship to another column whose table
          needs to be joined.
        """
        query = select(
            func.avg(column).label("mean"),
            StdDev(column).label("stddev"),
            func.count(column).label("count"),  # pylint: disable=not-callable
        ).select_from(table)
        if anchor_relation is not None:
            query = anchor_relation.add_join_to_query(query)
        with engine.connect() as connection:
            result = connection.execute(query).first()
            if result is None or result.stddev is None or getattr(result, "count") < 2:
                return None
        try:
            buckets = cls(
                engine,
                table,
                column,
                result.mean,
                result.stddev,
                getattr(result, "count"),
                anchor_relation,
            )
        except DatabaseError as exc:
            logger.debug("Failed to instantiate Buckets object: %s", exc)
            return None
        return buckets

    def fit_from_counts(self, bucket_counts: Sequence[float]) -> float:
        """Figure out the fit from bucket counts from the generator distribution."""
        return fit_from_buckets(self.buckets, bucket_counts)

    def fit_from_values(self, values: list[float]) -> float:
        """Figure out the fit from samples from the generator distribution."""
        buckets = [0] * 10
        x = self.mean - 2 * self.stddev
        w = self.stddev / 2
        for v in values:
            b = min(9, max(0, int((v - x) / w)))
            buckets[b] += 1
        return self.fit_from_counts(buckets)


class MultiProposerFactory(ProposerFactory):
    """A composite factory."""

    def __init__(self, *factories: ProposerFactory):
        """Initialise a MultiProposerFactory."""
        super().__init__()
        self.factories = factories

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the proposers appropriate to these columns."""
        return [
            generator
            for factory in self.factories
            for generator in factory.get_proposers(columns, engine)
        ]


def get_column_type(column: Column) -> TypeEngine:
    """Get the type of the column, generic if possible."""
    try:
        return column.type.as_generic()
    except NotImplementedError:
        return column.type


class ConstantProposer(Proposer):
    """Proposer for a generator that always produces the same value."""

    def __init__(self, value: Any) -> None:
        """Initialise the ConstantProposer."""
        super().__init__()
        self.value = value
        self.repr = repr(value)

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.constant"

    def nominal_kwargs(self) -> dict[str, str]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {"value": self.repr}

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {"value": self.value}

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [self.value for _ in range(count)]


class ConstantProposerFactory(ProposerFactory):
    """Propose just the null generator."""

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate for these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        if column.nullable:
            return [ConstantProposer(None)]
        c_type = get_column_type(column)
        if isinstance(c_type, String):
            return [ConstantProposer("")]
        if isinstance(c_type, Numeric):
            return [ConstantProposer(0.0)]
        if isinstance(c_type, Integer):
            return [ConstantProposer(0)]
        return []
