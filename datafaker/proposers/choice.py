"""Generator factories for making generators for choices of values."""

import decimal
import math
import typing
from abc import abstractmethod
from typing import Any, Sequence, Union

from sqlalchemy import Column, CursorResult, Engine, desc, func, select

from datafaker.dialects import Random
from datafaker.proposers.base import (
    Proposer,
    ProposerFactory,
    dist_gen,
    fit_from_buckets,
)

NumericType = Union[int, float]

# How many distinct values can we have before we consider a
# choice distribution to be infeasible?
MAXIMUM_CHOICES = 500


def zipf_distribution(total: int, bins: int) -> typing.Generator[int, None, None]:
    """
    Get a zipf distribution for a certain number of items.

    :param total: The total number of items to be distributed.
    :param bins: The total number of bins to distribute the items into.
    :return: A generator of the number of items in each bin, from the
    largest to the smallest.
    """
    basic_dist = list(map(lambda n: 1 / n, range(1, bins + 1)))
    bd_remaining = sum(basic_dist)
    for b in basic_dist:
        # yield b/bd_remaining of the `total` remaining
        if bd_remaining == 0:
            yield 0
        else:
            x = math.floor(0.5 + total * b / bd_remaining)
            bd_remaining -= x * bd_remaining / total
            total -= x
            yield x


def _choice_stmt(  # pylint: disable=R0913,R0917
    column: Column,
    store_counts: bool,
    sample_count: int | None,
    suppress_count: int,
) -> Any:
    """Build a SQLAlchemy SELECT for gathering choice value distributions.

    Compiles to dialect-correct SQL: LIMIT/random() on PostgreSQL/DuckDB,
    TOP/newid() on MS-SQL.  MS-SQL also forbids ORDER BY inside a subquery
    without TOP; this function never emits such a clause.
    """
    if sample_count is not None:
        sample_sub = (
            select(column.label("value"))
            .where(column.isnot(None))
            .order_by(Random())
            .limit(sample_count)
            .subquery("_inner")
        )
        counted_sub = (
            select(
                sample_sub.c.value,
                func.count(sample_sub.c.value).label("count"),  # pylint: disable=E1102
            )
            .group_by(sample_sub.c.value)
            .subquery("_counted")
        )
    else:
        counted_sub = (
            select(
                column.label("value"),
                func.count(column).label("count"),  # pylint: disable=E1102
            )
            .where(column.isnot(None))
            .group_by(column)
            .subquery("_counted")
        )
    out_cols = [counted_sub.c.value]
    if store_counts:
        out_cols.append(counted_sub.c["count"])
    stmt = select(*out_cols).select_from(counted_sub)
    if suppress_count > 0:
        stmt = stmt.where(counted_sub.c["count"] > suppress_count)
    else:
        stmt = stmt.order_by(desc(counted_sub.c["count"]))
    return stmt


class ChoiceProposer(Proposer):
    """Base proposer for all proposers producing choices of items."""

    STORE_COUNTS = False

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        column: Column,
        values: list[Any],
        counts: list[int],
        sample_count: int | None = None,
        suppress_count: int = 0,
        dialect: Any = None,
    ) -> None:
        """Initialise a ChoiceProposer."""
        super().__init__()
        self.column = column
        self.values = values
        estimated_counts = self.get_estimated_counts(counts)
        self._fit = fit_from_buckets(counts, estimated_counts)

        extra_comment = " and their counts" if self.STORE_COUNTS else ""
        stmt = _choice_stmt(
            column,
            self.STORE_COUNTS,
            sample_count,
            suppress_count,
        )
        compile_opts: dict[str, Any] = {"compile_kwargs": {"literal_binds": True}}
        if dialect is not None:
            compile_opts["dialect"] = dialect
        self._query = str(stmt.compile(**compile_opts))

        if suppress_count == 0:
            if sample_count is None:
                self._comment = (
                    f"All the values{extra_comment} that appear in column {column.name}"
                    f" of table {column.table.name}"
                )
                self._annotation = None
            else:
                self._comment = (
                    f"The values{extra_comment} that appear in column {column.name}"
                    f" of a random sample of {sample_count} rows of table {column.table.name}"
                )
                self._annotation = "sampled"
        else:
            if sample_count is None:
                self._comment = (
                    f"All the values{extra_comment} that appear in column {column.name}"
                    f" of table {column.table.name} more than {suppress_count} times"
                )
                self._annotation = "suppressed"
            else:
                self._comment = (
                    f"The values{extra_comment} that appear more than {suppress_count} times"
                    f" in column {column.name}, out of a random sample of {sample_count} rows"
                    f" of table {column.table.name}"
                )
                self._annotation = "sampled and suppressed"

    @abstractmethod
    def get_estimated_counts(self, counts: list[int]) -> list[int]:
        """Get the counts that we would expect if this distribution was the correct one."""

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "a": f'SRC_STATS["auto__{self.column.table.name}__{self.column.name}"]["results"]',
        }

    def name(self) -> str:
        """Get the name of the generator."""
        n = super().name()
        if self._annotation is None:
            return n
        return f"{n} [{self._annotation}]"

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {
            "a": self.values,
        }

    def custom_queries(self) -> dict[str, dict[str, Any]]:
        """Get the queries the generators need to call."""
        qs = super().custom_queries()
        return {
            **qs,
            f"auto__{self.column.table.name}__{self.column.name}": {
                "query": self._query,
                "comments": [self._comment],
            },
        }

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        return default if self._fit is None else self._fit


class ZipfChoiceProposer(ChoiceProposer):
    """Generator producing items in a Zipf distribution."""

    def get_estimated_counts(self, counts: list[int]) -> list[int]:
        """Get the counts that we would expect if this distribution was the correct one."""
        return list(zipf_distribution(sum(counts), len(counts)))

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.zipf_choice"

    def generate_data(self, count: int) -> list[float]:
        """Generate ``count`` random data points for this column."""
        return [
            dist_gen.zipf_choice_direct(self.values, len(self.values))
            for _ in range(count)
        ]


def uniform_distribution(total: int, bins: int) -> typing.Generator[int, None, None]:
    """
    Construct a distribution putting ``total`` items uniformly into ``bins`` bins.

    If they don't fit exactly evenly, the earlier bins will have one more
    item than the later bins so the total is as required.
    """
    p = total // bins
    n = total % bins
    for _ in range(0, n):
        yield p + 1
    for _ in range(n, bins):
        yield p


class UniformChoiceProposer(ChoiceProposer):
    """A proposer producing values, each roughly as frequently as each other."""

    def get_estimated_counts(self, counts: list[int]) -> list[int]:
        """Get the counts that we would expect if this distribution was the correct one."""
        return list(uniform_distribution(sum(counts), len(counts)))

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.choice"

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [dist_gen.choice_direct(self.values) for _ in range(count)]


class WeightedChoiceProposer(ChoiceProposer):
    """Choice proposer that matches the source data's frequency."""

    STORE_COUNTS = True

    def get_estimated_counts(self, counts: list[int]) -> list[int]:
        """Get the counts that we would expect if this distribution was the correct one."""
        return counts

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.weighted_choice"

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [dist_gen.weighted_choice(self.values) for _ in range(count)]


class ValueGatherer:
    """
    Gathers values from a query of values and counts.

    The query must return columns ``v`` for a value and ``f`` for the
    count of how many of those values there are.
    These values will be gathered into a number of properties:
    ``values``: the list of ``v`` values, ``counts``: the list of ``f`` counts
    in the same order as ``v``, ``cvs``: list of dicts with keys ``value`` and
    ``count`` giving these values and counts. ``counts_not_suppressed``,
    ``values_not_suppressed`` and ``cvs_not_suppressed`` are the
    equivalents with the counts less than or equal to ``suppress_count``
    removed.

    :param suppress_count: value with a count of this or fewer will be excluded
    from the suppressed values.
    """

    def __init__(self, results: CursorResult, suppress_count: int = 0) -> None:
        """Initialise a ValueGatherer."""
        values = []  # All values found
        counts = []  # The number or each value
        cvs: list[dict[str, Any]] = []  # list of dicts with keys "v" and "count"
        values_not_suppressed = []  # All values found more than SUPPRESS_COUNT times
        counts_not_suppressed = []  # The number for each value not suppressed
        cvs_not_suppressed: list[
            dict[str, Any]
        ] = []  # list of dicts with keys "v" and "count"
        for result in results:
            c = result.f
            if c != 0:
                counts.append(c)
                v = result.v
                if isinstance(v, decimal.Decimal):
                    v = float(v)
                values.append(v)
                cvs.append({"value": v, "count": c})
            if suppress_count < c:
                counts_not_suppressed.append(c)
                v = result.v
                if isinstance(v, decimal.Decimal):
                    v = float(v)
                values_not_suppressed.append(v)
                cvs_not_suppressed.append({"value": v, "count": c})
        self.values = values
        self.counts = counts
        self.cvs = cvs
        self.values_not_suppressed = values_not_suppressed
        self.counts_not_suppressed = counts_not_suppressed
        self.cvs_not_suppressed = cvs_not_suppressed


class ChoiceProposerFactory(ProposerFactory):
    """All generators that want an average and standard deviation."""

    SAMPLE_COUNT = MAXIMUM_CHOICES
    SUPPRESS_COUNT = 7

    def get_proposers(  # pylint: disable=too-many-locals
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        src_table = column.table
        dialect = engine.dialect
        src_table = column.table  # preserves schema for schema-qualified databases
        generators = []
        with engine.connect() as connection:
            stmt_count = (
                select(
                    column.label("v"),
                    func.count(column).label("f"),  # pylint: disable=E1102
                )
                .select_from(src_table)
                .group_by(column)
                .order_by(desc(func.count(column)))  # pylint: disable=E1102
                .limit(MAXIMUM_CHOICES + 1)
            )
            results = connection.execute(stmt_count)
            if results is not None and results.rowcount <= MAXIMUM_CHOICES:
                vg = ValueGatherer(results, self.SUPPRESS_COUNT)
                if vg.counts:
                    generators += [
                        ZipfChoiceProposer(
                            column,
                            vg.values,
                            vg.counts,
                            dialect=dialect,
                        ),
                        UniformChoiceProposer(
                            column,
                            vg.values,
                            vg.counts,
                            dialect=dialect,
                        ),
                        WeightedChoiceProposer(
                            column,
                            vg.cvs,
                            vg.counts,
                            dialect=dialect,
                        ),
                    ]
                if vg.counts_not_suppressed:
                    generators += [
                        ZipfChoiceProposer(
                            column,
                            vg.values_not_suppressed,
                            vg.counts_not_suppressed,
                            suppress_count=self.SUPPRESS_COUNT,
                            dialect=dialect,
                        ),
                        UniformChoiceProposer(
                            column,
                            vg.values_not_suppressed,
                            vg.counts_not_suppressed,
                            suppress_count=self.SUPPRESS_COUNT,
                            dialect=dialect,
                        ),
                        WeightedChoiceProposer(
                            column=column,
                            values=vg.cvs_not_suppressed,
                            counts=vg.counts_not_suppressed,
                            suppress_count=self.SUPPRESS_COUNT,
                            dialect=dialect,
                        ),
                    ]
            inner = (
                select(column.label("v"))
                .select_from(src_table)
                .order_by(Random())
                .limit(self.SAMPLE_COUNT)
                .subquery("_inner")
            )
            stmt_sample = (
                select(
                    inner.c.v, func.count(inner.c.v).label("f")  # pylint: disable=E1102
                )  # pylint: disable=E1102
                .select_from(inner)
                .group_by(inner.c.v)
                .order_by(desc(func.count(inner.c.v)))  # pylint: disable=E1102
            )
            sampled_results = connection.execute(stmt_sample)
            if sampled_results is not None:
                vg = ValueGatherer(sampled_results, self.SUPPRESS_COUNT)
                if vg.counts:
                    generators += [
                        ZipfChoiceProposer(
                            column,
                            vg.values,
                            vg.counts,
                            sample_count=self.SAMPLE_COUNT,
                            dialect=dialect,
                        ),
                        UniformChoiceProposer(
                            column,
                            vg.values,
                            vg.counts,
                            sample_count=self.SAMPLE_COUNT,
                            dialect=dialect,
                        ),
                        WeightedChoiceProposer(
                            column,
                            vg.cvs,
                            vg.counts,
                            sample_count=self.SAMPLE_COUNT,
                            dialect=dialect,
                        ),
                    ]
                if vg.counts_not_suppressed:
                    generators += [
                        ZipfChoiceProposer(
                            column,
                            vg.values_not_suppressed,
                            vg.counts_not_suppressed,
                            sample_count=self.SAMPLE_COUNT,
                            suppress_count=self.SUPPRESS_COUNT,
                            dialect=dialect,
                        ),
                        UniformChoiceProposer(
                            column,
                            vg.values_not_suppressed,
                            vg.counts_not_suppressed,
                            sample_count=self.SAMPLE_COUNT,
                            suppress_count=self.SUPPRESS_COUNT,
                            dialect=dialect,
                        ),
                        WeightedChoiceProposer(
                            column=column,
                            values=vg.cvs_not_suppressed,
                            counts=vg.counts_not_suppressed,
                            sample_count=self.SAMPLE_COUNT,
                            suppress_count=self.SUPPRESS_COUNT,
                            dialect=dialect,
                        ),
                    ]
        return generators
