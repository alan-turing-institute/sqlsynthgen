"""Powerful generators for numbers, choices and related missingness."""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import chain, combinations
from typing import Any, Union

import sqlalchemy
from sqlalchemy import (
    Column,
    Connection,
    Dialect,
    Engine,
    MetaData,
    RowMapping,
    Table,
    case,
    func,
    select,
    text,
)
from sqlalchemy.exc import DatabaseError

from datafaker.db_utils import is_numeric_sql_type
from datafaker.dialects import IsNotNull, IsNull, IsNumeric, IsPositive, LogNatural
from datafaker.proposers.base import Proposer, dist_gen, get_column_type
from datafaker.proposers.continuous import (
    CovariateQuery,
    MultivariateNormalProposerFactory,
)
from datafaker.utils import T, get_property, logger, text_list

NumericType = Union[int, float]

# How many distinct values can we have before we consider a
# choice distribution to be infeasible?
MAXIMUM_CHOICES = 500


@dataclass
class RowPartition:
    """A partition where all the rows have the same pattern of NULLs."""

    query: str
    query_comment: str
    # list of numeric columns
    included_numeric: list[Column]
    # map of indices to column names that are being grouped by.
    # The indices are indices of where they need to be inserted into
    # the generator outputs.
    included_choice: dict[int, str]
    # map of column names to clause that defines the partition
    # such as "mycolumn IS NULL"
    excluded_columns: dict[str, str]
    # map of constant outputs that need to be inserted into the
    # list of included column values (so once the generator has
    # been run and the included_choice values have been
    # added): {index: None}. This is adding excluded column
    # values to the output.
    constant_outputs: dict[int, Any]
    # The actual covariates from the source database
    covariates: Sequence[RowMapping]

    def overall_comment(self) -> str:
        """Get a comment about the row generator that uses this query."""
        columns = (
            [c.name for c in self.included_numeric]
            + list(self.included_choice.values())
            + list(self.excluded_columns.keys())
        )
        return self.query_comment.format(columns=text_list(columns))

    def constant_comments(self) -> list[str]:
        """Get comments about the kN values in the results."""
        return [
            f"k{index} is the value in column {col}."
            for index, col in self.included_choice.items()
        ]

    def comments(self) -> list[str]:
        """Make a appropriate comments for this partition."""
        caveat = ""
        if self.included_choice:
            icvs = self.included_choice.values()
            if len(icvs) == 1:
                caveat = f" for each value found in the column {list(icvs)[0]}"
            else:
                caveat = f" for each combination of values found in the columns {text_list(icvs)}"
        if not self.included_numeric:
            return [
                self.overall_comment(),
                f"Number of rows for which {text_list(self.excluded_columns.values())}{caveat}",
            ] + self.constant_comments()
        if not self.excluded_columns:
            where = ""
        else:
            where = f" where {text_list(self.excluded_columns.values())}"
        if len(self.included_numeric) == 1:
            return [
                self.overall_comment(),
                "These results are the mean (m0) and variance (c0_0) for the numbers"
                f" in the column {self.included_numeric[0].name}{where}{caveat}.",
            ] + self.constant_comments()
        cov_explainers = [
            [
                f"m{index} refers to the mean"
                f" of the values in the column {col_name.name}",
                f"c{index}_{index} refers to the variance"
                f" of the values in the column {col_name.name}",
            ]
            for index, col_name in enumerate(self.included_numeric)
        ]
        if len(self.included_numeric) == 2:
            cov_explainers.append(
                [
                    "c0_1 refers to the covariance between columns"
                    f" {self.included_numeric[0].name} and {self.included_numeric[1].name}"
                ]
            )
        else:
            cov_explainers.append(
                [
                    "The cN_M results refer to the covariances between these numeric columns"
                ]
            )
        return (
            [
                self.overall_comment(),
                "These results are the means (mN) and covariate matrix (cN_M)"
                " for the columns"
                f" {text_list(col.name for col in self.included_numeric)}"
                f"{where}{caveat} so that we can"
                " produce the relatedness between these in the fake data.",
            ]
            + [exp for ce in cov_explainers for exp in ce]
            + self.constant_comments()
        )


@dataclass
class NullableColumn:
    """A reference to a nullable column whose nullability is part of a partitioning."""

    column: Column
    # The bit (power of two) of the number of the partition in the partition sizes list
    bitmask: int


class PartitionCountQuery:
    """Query, result and comment for the row counts of the null pattern partitions."""

    def __init__(
        self,
        connection: Connection,
        query: Any,
        nullable_columns: Iterable[NullableColumn],
        overall_comment: str,
    ) -> None:
        """
        Initialise the partition count query.

        :param connection: Database connection.
        :param query: The SQLAlchemy query getting the row counts of the null pattern partitions.
        :param table_name: The name of the table being queried.
        :param nullable_columns: The columns that are being checked for nullness.
        """
        self.query = query
        rows = connection.execute(query).mappings().fetchall()
        self.results = [dict(row) for row in rows]
        self.comments = [
            overall_comment,
            "These results list the number of rows in the source sample that have each"
            ' combination of these columns being NULL. Each result has an "index", which'
            ' refers to the combination of NULLs and a "count", which refers to the'
            " number of rows in the source sample with this combination."
            ' The "index" is 0 if all the columns are NULL, otherwise it is the'
            " sum of the following values for those columns that are not NULL:",
        ] + [f"{nc.column.name}: {nc.bitmask}" for nc in nullable_columns]


class NullPartitionedNormalProposer(Proposer):
    """
    A generator of mixed numeric and non-numeric data.

    Generates data that matches the source data in
    missingness, choice of non-numeric data and numeric
    data.

    For the numeric data to be generated, samples of rows for each
    combination of non-numeric values and missingness. If any such
    combination has only one line in the source data (or sample of
    the source data if sampling), it will not be generated as a
    covariate matrix cannot be generated from one source row
    (although if the data is all non-numeric values and nulls, single
    rows are used because no covariate matrix is required for this).
    """

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        dialect: Dialect,
        query_name: str,
        partitions: dict[int, RowPartition],
        function_name: str = "grouped_multivariate_lognormal",
        name_suffix: str | None = None,
        partition_count_query: PartitionCountQuery | None = None,
    ):
        """Initialise a NullPartitionedNormalGenerator."""
        self._dialect = dialect
        self._query_name = query_name
        self._partitions = partitions
        self._function_name = function_name
        self._partition_count_query = partition_count_query
        if name_suffix:
            self._name = f"null-partitioned {function_name} [{name_suffix}]"
        else:
            self._name = f"null-partitioned {function_name}"

    def name(self) -> str:
        """Get the name of the generator."""
        return self._name

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.alternatives"

    def _nominal_kwargs_with_combinations(
        self, index: int, partition: RowPartition
    ) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml`` for a single partition."""
        count = (
            'sum(r["count"] for r in'
            f' SRC_STATS["auto__cov__{self._query_name}__alt_{index}"]["results"])'
        )
        if not partition.included_numeric and not partition.included_choice:
            return {
                "count": count,
                "name": '"constant"',
                "params": {"value": [None] * len(partition.constant_outputs)},
            }
        covariates = {
            "covs": f'SRC_STATS["auto__cov__{self._query_name}__alt_{index}"]["results"]'
        }
        if not partition.constant_outputs:
            return {
                "count": count,
                "name": f'"{self._function_name}"',
                "params": covariates,
            }
        return {
            "count": count,
            "name": '"with_constants_at"',
            "params": {
                "constants_at": partition.constant_outputs,
                "subgen": f'"{self._function_name}"',
                "params": covariates,
            },
        }

    def _count_query_name(self) -> str:
        return f"auto__cov__{self._query_name}__counts"

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "alternative_configs": [
                self._nominal_kwargs_with_combinations(index, self._partitions[index])
                for index in range(len(self._partitions))
            ],
            "counts": f'SRC_STATS["{self._count_query_name()}"]["results"]',
        }

    def custom_queries(self) -> dict[str, Any]:
        """Get the queries the generators need to call."""
        partitions = {
            f"auto__cov__{self._query_name}__alt_{index}": {
                "comments": partition.comments(),
                "query": partition.query,
            }
            for index, partition in self._partitions.items()
        }
        if not self._partition_count_query:
            return partitions
        pc_query = self._partition_count_query.query.compile(
            dialect=self._dialect, compile_kwargs={"literal_binds": True}
        )
        return {
            self._count_query_name(): {
                "comments": self._partition_count_query.comments,
                "query": str(pc_query),
            },
            **partitions,
        }

    def _actual_kwargs_with_combinations(
        self, partition: RowPartition
    ) -> dict[str, Any]:
        count = sum(row["count"] for row in partition.covariates)
        if not partition.included_numeric and not partition.included_choice:
            return {
                "count": count,
                "name": "constant",
                "params": {"value": [None] * len(partition.excluded_columns)},
            }
        covariates = {
            "covs": partition.covariates,
        }
        if not partition.constant_outputs:
            return {
                "count": count,
                "name": self._function_name,
                "params": covariates,
            }
        return {
            "count": count,
            "name": "with_constants_at",
            "params": {
                "constants_at": partition.constant_outputs,
                "subgen": self._function_name,
                "params": covariates,
            },
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        if self._partition_count_query is None:
            counts = None
        else:
            counts = self._partition_count_query.results
        return {
            "alternative_configs": [
                self._actual_kwargs_with_combinations(self._partitions[index])
                for index in range(len(self._partitions))
            ],
            "counts": counts,
        }

    def generate_data(self, count: int) -> list[Any]:
        """Generate 'count' random data points for this column."""
        kwargs = self.actual_kwargs()
        return [dist_gen.alternatives(**kwargs) for _ in range(count)]

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        return default


def is_numeric(col: Column) -> bool:
    """Test if this column stores a numeric value."""
    return is_numeric_sql_type(get_column_type(col)) and not col.foreign_keys


def powerset(xs: list[T]) -> Iterable[Iterable[T]]:
    """Get a list of all sublists of ``input``."""
    return chain.from_iterable(combinations(xs, n) for n in range(len(xs) + 1))


# pylint: disable=too-many-instance-attributes
class NullPatternPartition:
    """Get the definition of a partition (in other words, what makes it not another partition)."""

    def __init__(
        self, columns: Iterable[Column], partition_nonnulls: Iterable[NullableColumn]
    ):
        """Initialise a pattern of nulls which can be queried for."""
        self.index = sum(nc.bitmask for nc in partition_nonnulls)
        nonnull_columns = {nc.column.name for nc in partition_nonnulls}
        self.included_numeric: list[Column] = []
        self.included_choice: dict[int, str] = {}
        self.constant_clauses: dict[int, Column] = {}
        self.excluded: dict[str, str] = {}
        self.predicates: list[Any] = []
        self.nones: dict[int, None] = {}
        for col_index, column in enumerate(columns):
            col_name = column.name
            if col_name in nonnull_columns or not column.nullable:
                if is_numeric(column):
                    self.included_numeric.append(column)
                else:
                    index = len(self.included_numeric) + len(self.included_choice)
                    self.included_choice[index] = col_name
                    self.constant_clauses[index] = column
                self.predicates.append(IsNotNull(column))
            else:
                self.excluded[col_name] = f"{col_name} IS NULL"
                self.predicates.append(IsNull(column))
                self.nones[col_index] = None


class NullPartitionedNormalProposerFactory(MultivariateNormalProposerFactory):
    """Produces null partitioned generators, for complex interdependent data."""

    SAMPLE_COUNT = MAXIMUM_CHOICES
    SUPPRESS_COUNT = 7
    EMPTY_RESULT = [
        RowMapping(
            parent=sqlalchemy.engine.result.SimpleResultMetaData(["count"]),
            processors=None,
            key_to_index={"count": 0},
            data=(0,),
        )
    ]

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "grouped_multivariate_normal"

    def query_predicate(self, column: Column) -> Any:
        """Get a SQLAlchemy expression that is true when ``column`` is available for analysis."""
        if is_numeric(column):
            # x <> x + 1 ensures that x is not infinity or NaN
            return IsNumeric(column)
        return IsNotNull(column)

    def query_var(self, column: Column) -> Any:
        """Return the expression we are querying for in this column."""
        return column

    def query_comment(self) -> str:
        """
        Return the human-readable comment for this generator.

        Should have a ``{columns}`` reference to the list of columns,
        which will be a string like ``apples, pears and bananas``.
        """
        return (
            "This query contributes to the multivariate normal generator"
            " that covers the columns {columns}."
        )

    def get_named_tables(self) -> Mapping[str, Column]:
        """
        Get a mapping showing which tables have naming columns.

        Based on the configuration file.

        :return: A map mapping names of named tables to their naming
         columns.
        """
        return self._named_tables

    def __init__(self, config: Mapping[str, Any], metadata: MetaData) -> None:
        """Initialize the null partitioned generator factory."""
        tables: dict[str, Any] = get_property(config, "tables", {})
        named_tables: list[tuple[str, str]] = [
            (table_name, table_conf["name_column"])
            for table_name, table_conf in tables.items()
            if isinstance(table_conf, Mapping) and "name_column" in table_conf
        ]
        delkeys: set[str] = set()
        for table_name, column_name in named_tables:
            if table_name not in metadata.tables:
                logger.warning("Configured table %s not present in database.")
                delkeys.add(table_name)
            elif column_name not in metadata.tables[table_name].columns:
                logger.warning(
                    "name_column %s configured in table %s is not a column in this table.",
                    column_name,
                    table_name,
                )
                delkeys.add(table_name)
        self._named_tables = {
            t: metadata.tables[t].columns[c]
            for t, c in named_tables
            if t not in delkeys
        }

    def get_nullable_columns(self, columns: list[Column]) -> list[NullableColumn]:
        """Get a list of nullable columns together with bitmasks."""
        out: list[NullableColumn] = []
        for col in columns:
            if col.nullable:
                out.append(
                    NullableColumn(
                        column=col,
                        bitmask=2 ** len(out),
                    )
                )
        return out

    def _get_query_predicate(self, nc: NullableColumn) -> Any:
        return case(
            (self.query_predicate(nc.column), nc.bitmask),
            else_=0,
        )

    def get_partition_count_query(
        self,
        ncs: list[NullableColumn],
        table: Table,
        suppress_count: int = 0,
    ) -> Any:
        """
        Get a SQLAlchemy expression returning columns ``count`` and ``index``.

        Each row returned represents one of the null pattern partitions.
        ``index`` is the bitmask of all those nullable columns that are not null for
        this partition, and ``count`` is the total number of rows in this partition.
        """
        index_exp = sum(self._get_query_predicate(nc) for nc in ncs)
        subq = select(index_exp.label("index")).select_from(table).subquery()
        sel = select(
            func.count().label("count"),  # pylint: disable=not-callable
            subq.c["index"],
        ).group_by("index")
        if 1 < suppress_count:
            sb = sel.subquery("_q")
            sel = select(sb.c["count", "index"]).where(sb.c["count"] > suppress_count)
        return sel

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def _get_generator(
        self,
        connection: Connection,
        cov_query: CovariateQuery,
        columns: list[Column],
        nullable_columns: list[NullableColumn],
        name_suffix: str | None = None,
    ) -> NullPartitionedNormalProposer | None:
        partitions: dict[int, RowPartition] = {}
        for partition_nonnulls in powerset(nullable_columns):
            partition_def = NullPatternPartition(columns, partition_nonnulls)
            cov_query.columns(
                partition_def.included_numeric,
            ).predicates(
                partition_def.predicates,
            ).constant_clauses(
                partition_def.constant_clauses,
            )
            partitions[partition_def.index] = RowPartition(
                query=str(
                    cov_query.get().compile(
                        dialect=connection.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                ),
                query_comment=cov_query.get_query_comment(),
                included_numeric=partition_def.included_numeric,
                included_choice=partition_def.included_choice,
                excluded_columns=partition_def.excluded,
                constant_outputs=partition_def.nones,
                covariates=[],
            )
        if not self._execute_partition_queries(connection, partitions):
            return None
        query = self.get_partition_count_query(
            nullable_columns,
            cov_query.table,
            cov_query.suppress_count,
        )
        return NullPartitionedNormalProposer(
            connection.dialect,
            f"{cov_query.table}__{columns[0].name}",
            partitions,
            self.function_name(),
            name_suffix=name_suffix,
            partition_count_query=PartitionCountQuery(
                connection,
                query,
                nullable_columns,
                cov_query.get_query_comment().format(
                    columns=text_list(c.name for c in columns)
                ),
            ),
        )

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get any appropriate generators for these columns."""
        if len(columns) < 2:
            return []
        nullable_columns = self.get_nullable_columns(columns)
        if not nullable_columns:
            return []
        table = columns[0].table
        gens: list[Proposer | None] = []
        try:
            with engine.connect() as connection:
                cov_query = CovariateQuery(table, self)
                gens.append(
                    self._get_generator(
                        connection,
                        cov_query,
                        columns,
                        nullable_columns,
                    )
                )
                cov_query = cov_query.sample_count(self.SAMPLE_COUNT)
                gens.append(
                    self._get_generator(
                        connection,
                        cov_query,
                        columns,
                        nullable_columns,
                        name_suffix="sampled",
                    )
                )
                cov_query = CovariateQuery(
                    table,
                    self,
                ).set_suppress_count(self.SUPPRESS_COUNT)
                gens.append(
                    self._get_generator(
                        connection,
                        cov_query,
                        columns,
                        nullable_columns,
                        name_suffix="suppressed",
                    )
                )
                cov_query = cov_query.sample_count(self.SAMPLE_COUNT)
                gens.append(
                    self._get_generator(
                        connection,
                        cov_query,
                        columns,
                        nullable_columns,
                        name_suffix="sampled and suppressed",
                    )
                )
        except DatabaseError as exc:
            logger.debug("SQL query failed with error %s [%s]", exc, exc.statement)
            return []
        return [gen for gen in gens if gen]

    def _execute_partition_queries(
        self,
        connection: Connection,
        partitions: dict[int, RowPartition],
    ) -> bool:
        """
        Execute the query in each partition, filling in the covariates.

        :return: False if all the partitions fail, True if any of them work.
        """
        found_nonzero = False
        for rp in partitions.values():
            query = rp.query
            covs = connection.execute(text(query)).mappings().fetchall()
            if not covs or covs.count == 0 or covs[0]["count"] is None:
                rp.covariates = self.EMPTY_RESULT
            else:
                rp.covariates = covs
                found_nonzero = True
        return found_nonzero


class NullPartitionedLogNormalProposerFactory(NullPartitionedNormalProposerFactory):
    """
    A generator for numeric and non-numeric columns.

    Any values could be null, the distributions of the nonnull numeric columns
    depend on each other and the other non-numeric column values.
    """

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "grouped_multivariate_lognormal"

    def query_predicate(self, column: Column) -> Any:
        """Get the SQL expression testing if the value in this column should be used."""
        if is_numeric(column):
            return IsPositive(column)
        return IsNotNull(column)

    def query_var(self, column: Column) -> Any:
        """Get the variable or expression we are querying for this column."""
        return LogNatural(column)

    def query_comment(self) -> str:
        """Return the human-readable comment for this generator."""
        return (
            "This query contributes to the multivariate lognormal generator"
            " that covers the columns {columns}."
        )
