"""Generator factories for making generators of continuous distributions."""

import itertools
from abc import abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import (
    Column,
    Dialect,
    Engine,
    FromClause,
    RowMapping,
    Select,
    Table,
    case,
    func,
    literal,
    null,
    select,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.types import Integer, Numeric
from typing_extensions import Self

from datafaker.dialects import IsNotNull, LogNatural, NullIf, Random, StdDev
from datafaker.proposers.base import (
    Buckets,
    NumericType,
    Proposer,
    ProposerFactory,
    dist_gen,
    get_column_type,
)
from datafaker.utils import logger


class ContinuousDistributionProposer(Proposer):
    """Base class for generators producing continuous distributions."""

    expected_buckets: Sequence[NumericType] = []

    def __init__(
        self,
        table: Table,
        column: Column,
        buckets: Buckets,
        dialect: Dialect,
    ):
        """Initialise a ContinuousDistributionProposer."""
        super().__init__()
        self.table = table
        self.column = column
        self.buckets = buckets
        self._dialect = dialect

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "mean": (
                f'SRC_STATS["auto__{self.table.name}"]["results"]'
                f'[0]["mean__{self.column.name}"]'
            ),
            "sd": (
                f'SRC_STATS["auto__{self.table.name}"]["results"]'
                f'[0]["stddev__{self.column.name}"]'
            ),
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        if self.buckets is None:
            return {}
        return {
            "mean": self.buckets.mean,
            "sd": self.buckets.stddev,
        }

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """Get the query fragments the generators need to call."""
        clauses = super().select_aggregate_clauses()
        sd = StdDev(self.column).compile(
            dialect=self._dialect, compile_kwargs={"literal_binds": True}
        )
        mean = func.avg(self.column).compile(
            dialect=self._dialect, compile_kwargs={"literal_binds": True}
        )
        return {
            **clauses,
            f"mean__{self.column.name}": {
                "clause": str(mean),
                "comment": f"Mean of {self.column.name} from table {self.table.name}",
            },
            f"stddev__{self.column.name}": {
                "clause": str(sd),
                "comment": f"Standard deviation of {self.column.name} from table {self.table.name}",
            },
        }

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        if self.buckets is None:
            return default
        return self.buckets.fit_from_counts(self.expected_buckets)


class GaussianProposer(ContinuousDistributionProposer):
    """Generator producing numbers in a Gaussian (normal) distribution."""

    expected_buckets = [
        0.0227,
        0.0441,
        0.0918,
        0.1499,
        0.1915,
        0.1915,
        0.1499,
        0.0918,
        0.0441,
        0.0227,
    ]

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.normal"

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [
            dist_gen.normal(self.buckets.mean, self.buckets.stddev)
            for _ in range(count)
        ]


class UniformProposer(ContinuousDistributionProposer):
    """Generator producing numbers in a uniform distribution."""

    expected_buckets = [
        0,
        0.06698,
        0.14434,
        0.14434,
        0.14434,
        0.14434,
        0.14434,
        0.14434,
        0.06698,
        0,
    ]

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.uniform_ms"

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [
            dist_gen.uniform_ms(self.buckets.mean, self.buckets.stddev)
            for _ in range(count)
        ]


class ContinuousDistributionProposerFactory(ProposerFactory):
    """All generators that want an average and standard deviation."""

    def _get_generators_from_buckets(
        self,
        engine: Engine,
        src_table: Table,
        column: Column,
        buckets: Buckets,
    ) -> Sequence[Proposer]:
        dialect = engine.dialect
        return [
            GaussianProposer(src_table, column, buckets, dialect=dialect),
            UniformProposer(src_table, column, buckets, dialect=dialect),
        ]

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, Numeric) and not isinstance(ct, Integer):
            return []
        table = column.table
        buckets = Buckets.make_buckets(engine, table, column)
        if buckets is None:
            return []
        return self._get_generators_from_buckets(engine, table, column, buckets)


class LogNormalProposer(Proposer):
    """Generator producing numbers in a log-normal distribution."""

    # R:
    # > xs<-seq(-2,2,0.5)*sqrt((exp(1)-1)*exp(1))+exp(0.5)
    # > ys <- plnorm(xs)
    # > c(ys, 1) - c(0,ys)
    #  [1] 0.00000000 0.00000000 0.00000000 0.28589471 0.40556775 0.15086088
    #  [7] 0.06716451 0.03428958 0.01924848 0.03697409
    expected_buckets = [
        0,
        0,
        0,
        0.28589471,
        0.40556775,
        0.15086088,
        0.06716451,
        0.03428958,
        0.01924848,
        0.03697409,
    ]

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        table: Table,
        column: Column,
        buckets: Buckets,
        logmean: float,
        logstddev: float,
        dialect: Dialect,
    ):
        """Initialise a LogNormalProposer."""
        super().__init__()
        self.table = table
        self.column = column
        self.buckets = buckets
        self.logmean = logmean
        self.logstddev = logstddev
        self._dialect = dialect

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.lognormal"

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [dist_gen.lognormal(self.logmean, self.logstddev) for _ in range(count)]

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "logmean": (
                f'SRC_STATS["auto__{self.table.name}"]["results"][0]'
                f'["logmean__{self.column.name}"]'
            ),
            "logsd": (
                f'SRC_STATS["auto__{self.table.name}"]["results"][0]'
                f'["logstddev__{self.column.name}"]'
            ),
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {
            "logmean": self.logmean,
            "logsd": self.logstddev,
        }

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """Get the query fragments the generators need to call."""
        clauses = super().select_aggregate_clauses()
        return {
            **clauses,
            f"logmean__{self.column.name}": {
                "clause": (
                    f"AVG(CASE WHEN 0<{self.column.name} THEN LN({self.column.name})"
                    " ELSE NULL END)"
                ),
                "comment": f"Mean of logs of {self.column.name} from table {self.table.name}",
            },
            f"logstddev__{self.column.name}": {
                "clause": (
                    f"{'STDEV' if self._dialect.name == 'mssql' else 'STDDEV'}"
                    f"(CASE WHEN 0<{self.column.name}"
                    f" THEN LN({self.column.name}) ELSE NULL END)"
                ),
                "comment": (
                    f"Standard deviation of logs of {self.column.name}"
                    f" from table {self.table.name}"
                ),
            },
        }

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        if self.buckets is None:
            return default
        return self.buckets.fit_from_counts(self.expected_buckets)


class ContinuousLogDistributionProposerFactory(ContinuousDistributionProposerFactory):
    """All generators that want an average and standard deviation of log data."""

    def _get_generators_from_buckets(
        self,
        engine: Engine,
        src_table: Table,
        column: Column,
        buckets: Buckets,
    ) -> Sequence[Proposer]:
        col = case(
            (column > 0, func.log(column)),
            else_=null(),
        )
        stmt = select(
            func.avg(col).label("logmean"),
            func.stddev_samp(col).label("logstddev"),
        ).select_from(src_table)
        with engine.connect() as connection:
            result = connection.execute(stmt).first()
            if result is None or result.logstddev is None:
                return []
        return [
            LogNormalProposer(
                src_table,
                column,
                buckets,
                float(result.logmean),
                float(result.logstddev),
                dialect=engine.dialect,
            )
        ]


class MultivariateNormalProposer(Proposer):
    """Generator of multiple values drawn from a multivariate normal distribution."""

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        dialect: Dialect,
        table: Table,
        columns: list[Column],
        query: Any,
        covariates: RowMapping,
        function_name: str,
    ) -> None:
        """Initialise a MultivariateNormalProposer."""
        self._dialect = dialect
        self._table = table
        self._columns = columns
        self._query = query
        self._covariates = covariates
        self._function_name = function_name

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen." + self._function_name

    def src_stats_key(self) -> str:
        """Get the name for the query results in SRC_STATS."""
        return f"auto__cov__{self._table.name}__{self._columns[0].name}"

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "cov": f'SRC_STATS["{self.src_stats_key()}"]["results"][0]',
        }

    def custom_queries(self) -> dict[str, Any]:
        """Get the queries the generators need to call."""
        cols = ", ".join([c.name for c in self._columns])
        return {
            self.src_stats_key(): {
                "comments": [
                    f"Means and covariate matrix for the columns {cols},"
                    " so that we can produce the relatedness between these in the fake data."
                ],
                "query": str(
                    self._query.compile(
                        dialect=self._dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                ),
            }
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {"cov": self._covariates}

    def generate_data(self, count: int) -> list[Any]:
        """Generate 'count' random data points for this column."""
        return [
            getattr(dist_gen, self._function_name)(self._covariates)
            for _ in range(count)
        ]

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        return default


class MultivariateNormalGeneratorFactoryBase(ProposerFactory):
    """Generator factory that makes distributions and maybe partitions."""

    @abstractmethod
    def query_predicate(self, column: Column) -> Any:
        """Get the SQLAlchemy expression for whether this column should be queried."""

    @abstractmethod
    def query_var(self, column: Column) -> Any:
        """Get the SQLAlchemy expression of the value to query for this column."""

    @abstractmethod
    def query_comment(self) -> str:
        """
        Return the human-readable comment for this generator.

        Should have a ``{columns}`` reference to the list of columns,
        which will be a string like ``apples, pears and bananas``.
        """

    def get_named_tables(self) -> Mapping[str, Column]:
        """
        Get a mapping showing which tables have naming columns.

        A naming column is a column that provides a nice name for the row.
        We could call tables containing such a column as a "named table".
        :return: A map mapping names of named tables to their naming
        columns.
        """
        return {}


@dataclass
class JoinToOn:
    """Represents a JOIN clause."""

    table: FromClause
    onclause: Any


# pylint: disable=too-many-instance-attributes
class CovariateQuery:
    """Query-constructing object for making a covariate matrix."""

    def __init__(
        self,
        table: Table,
        factory: MultivariateNormalGeneratorFactoryBase,
    ) -> None:
        """
        Initialize the query for the basics for multivariate normal/lognormal parameters.

        :param table: The name of the table to be queried.
        :param factory: The generator factory, perhaps with overridden
        ``query_var`` and ``query_predicate`` methods.
        :param dialect: The SQLAlchemy dialect name (e.g. ``mssql.dialect()``).
        """
        self.table: Table = table
        self._columns: Sequence[Column] = []
        self._predicates: Iterable[Any] = []
        self._constant_clauses: dict[int, Column] = {}
        self.suppress_count = 1
        self._sample_count: int | None = None
        self._factory = factory
        self._predicate_fn = lambda x: x + " IS NOT NULL"

    def get_query_comment(self) -> str:
        """
        Return the human-readable comment for this generator.

        Should have a ``{columns}`` reference to the list of columns,
        which will be a string like ``apples, pears and bananas``.
        """
        return self._factory.query_comment()

    def columns(self, cols: Sequence[Column]) -> Self:
        """
        Set the included columns.

        :param cols: The columns in the multivariate distribution.
        """
        self._columns = cols
        return self

    def set_suppress_count(self, count: int) -> Self:
        """
        Set the suppression count.

        No set of categories will be included in the results that have
        this many or fewer rows in the source.

        :param count: a group smaller than this will be suppressed.
        """
        self.suppress_count = count
        return self

    def sample_count(self, count: int) -> Self:
        """
        Set the sample count.

        This many rows will be sampled at random from this partition.

        :param count: this many samples will be taken from this partition.
        """
        self._sample_count = count
        return self

    def predicates(self, predicates: Iterable[Any]) -> Self:
        """
        Set the predicates to filter the queried table by.

        :param predicates: Additional where clauses.
        """
        self._predicates = predicates
        return self

    def constant_clauses(self, clauses: dict[int, Column]) -> Self:
        """
        Set constant clauses.

        :param constant_clauses: Extra output columns in the outer SELECT clause.
        This is in the form of a dict from an integer index to the name of the
        column being extracted. The index is the position of this constant in
        the list of non-null outputs.
        """
        self._constant_clauses = clauses
        return self

    def _get_constants_and_joins(
        self, named_tables: Mapping[str, Column], subquery: Any
    ) -> tuple[list[Column], list[JoinToOn]]:
        """
        Extra JOINs to give names to foreign keys.

        This enables information governance people can understand the results
        better.
        :param named_tables: A mapping of tables that have names to columns
         that supply those names.
        :return: A pair; the first is constants in the SELECT clause, the
         second is foreign keys to tables to join to the outer query in order
         to make names appear in the output.
        """
        # Column names -> Foreign Keys to named_tables
        col_to_named_fks = {
            col.name: [
                fk for fk in col.foreign_keys if fk.column.table.name in named_tables
            ]
            for col in self._constant_clauses.values()
        }
        # Column names -> single FK to named_tables
        col_to_named_fk = {col: fks[0] for col, fks in col_to_named_fks.items() if fks}
        name_joins: list[JoinToOn] = []
        constants: list[Any] = []
        for index, col in self._constant_clauses.items():
            col_name = col.name
            fk_parent_col = subquery.c[f"k{index}"]
            constants.append(fk_parent_col)
            if col_name in col_to_named_fk:
                fk = col_to_named_fk[col_name]
                name_table = aliased(fk.column.table)
                name_table_id = name_table.c[fk.column.name]
                name_joins.append(JoinToOn(name_table, fk_parent_col == name_table_id))
                name_column = named_tables[fk.column.table.name]
                constants.append(
                    name_table.c[name_column.name].label(f"k{index}_{col_name}__name")
                )
        return constants, name_joins

    def get(self) -> Any:
        """
        Get the SQL query.

        :return: The SQLAlchemy query for this partition.
        """
        middle = self._middle_query(self._inner_query()).subquery("_q")
        means = [middle.c[f"m{i}"] for i in range(len(self._columns))]
        covs = [
            (
                (
                    middle.c[f"s{ix}_{iy}"]
                    - middle.c["count"] * middle.c[f"m{ix}"] * middle.c[f"m{iy}"]
                )
                / NullIf(middle.c["count"] - 1, literal(0))
            ).label(f"c{ix}_{iy}")
            for iy in range(len(self._columns))
            for ix in range(iy + 1)
        ]
        rank = len(self._columns)
        named_tables = self._factory.get_named_tables()
        constants, name_joins = self._get_constants_and_joins(named_tables, middle)
        query = select(
            literal(rank).label("rank"), middle.c["count"], *constants, *means, *covs
        ).select_from(middle)
        for j in name_joins:
            query = query.join(j.table, onclause=j.onclause)
        # if there are any numeric columns we need at least
        # two rows to make any (co)variances at all
        if self._columns:
            query = query.where(middle.c["count"] > self.suppress_count)
        return query

    def _inner_query(self) -> Select:
        """Get the rows from the table that we are interested in."""
        constants = [col.label(f"k{i}") for i, col in self._constant_clauses.items()]
        values = [col.label(f"v{i}") for i, col in enumerate(self._columns)]
        sel = select(*constants, *values).select_from(self.table)
        preds = itertools.chain(
            (self._factory.query_predicate(col) for col in self._columns),
            self._predicates,
        )
        if preds:
            sel = sel.filter(*preds)
        if self._sample_count is not None:
            sel = sel.order_by(Random()).limit(self._sample_count)
        return sel

    def _middle_query(self, inner_query: Any) -> Any:
        """Get the basic statistics (and constants) from the inner query."""
        inner = inner_query.subquery("_sampled")
        col_count = len(self._columns)
        multiples = [
            func.sum(
                self._factory.query_var(inner.c[f"v{ix}"])
                * self._factory.query_var(inner.c[f"v{iy}"])
            ).label(f"s{ix}_{iy}")
            for iy in range(col_count)
            for ix in range(iy + 1)
        ]
        avgs = [
            func.avg(self._factory.query_var(inner.c[f"v{i}"])).label(f"m{i}")
            for i in range(col_count)
        ]
        constants = [inner.c[f"k{k}"] for k in self._constant_clauses.keys()]
        query = select(
            func.count().label("count"),  # pylint: disable=not-callable
            *multiples,
            *avgs,
            *constants,
        ).select_from(inner)
        if len(self._constant_clauses) == 0:
            return query
        return query.group_by(
            *[inner.c[f"k{k}"] for k in self._constant_clauses.keys()]
        )


class MultivariateNormalProposerFactory(MultivariateNormalGeneratorFactoryBase):
    """Normal distribution generator factory."""

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "multivariate_normal"

    def query_predicate(self, column: Column) -> Any:
        """Get the SQLAlchemy expression for whether this column should be queried."""
        return IsNotNull(column)

    def query_var(self, column: Column) -> Any:
        """Get the SQL expression of the value to query for this column."""
        return column

    def query_comment(self) -> str:
        """Return the human-readable comment for this generator."""
        return (
            "This query provides the covariate matrix for a multivariate"
            " normal distribution over the columns {columns}."
        )

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators for these columns."""
        # For the case of one column we'll use GaussianGenerator
        if len(columns) < 2:
            return []
        # All columns must be numeric
        for c in columns:
            ct = get_column_type(c)
            if not isinstance(ct, Numeric) and not isinstance(ct, Integer):
                return []
        table = columns[0].table
        cq = CovariateQuery(table, self).columns(columns)
        query = cq.get()
        with engine.connect() as connection:
            try:
                covariates = connection.execute(query).mappings().first()
            except SQLAlchemyError as e:
                logger.debug("SQL query %s failed with error %s", query, e)
                return []
            if not covariates or covariates["c0_0"] is None:
                return []
            return [
                MultivariateNormalProposer(
                    connection.dialect,
                    table,
                    columns,
                    query,
                    covariates,
                    self.function_name(),
                )
            ]


class MultivariateLogNormalProposerFactory(MultivariateNormalProposerFactory):
    """Multivariate lognormal generator factory."""

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "multivariate_lognormal"

    def query_predicate(self, column: Column) -> Any:
        """Get the SQLAlchemy expression for whether this column should be queried."""
        return coalesce(column > 0, False)

    def query_var(self, column: Column) -> Any:
        """Get the expression to query for, for this column."""
        return LogNatural(column)

    def query_comment(self) -> str:
        """Return the human-readable comment for this generator."""
        return (
            "This query provides the covariate matrix for a multivariate"
            " lognormal distribution over the columns {columns}."
        )
