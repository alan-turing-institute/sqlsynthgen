"""Proposers for date intervals."""

import datetime
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import Column, Engine, MetaData, func, select
from sqlalchemy.types import Date, DateTime

from datafaker.dialects import SecondsDifference, StdDev, random_sample
from datafaker.proposers.base import (
    Buckets,
    ForeignKeyRelationship,
    Proposer,
    ProposerFactory,
    RelatedColumn,
    get_column_type,
    make_foreign_key_relationship,
)
from datafaker.providers import AnchoredProvider
from datafaker.utils import get_property, procrustes


def _set_roles_for_column(
    out: dict[str, list[RelatedColumn]],
    fk: ForeignKeyRelationship | None,
    column: Column,
    columns_config: Mapping,
) -> None:
    """
    Set new entries in ``out`` based on the roles ``column`` has.

    :param out: Mapping to be updated of role to related columns.
    :param fk: Foreign key relationship to the table ``column`` appears in
        (this will be set in new entries added to ``out``).
    :param column: The column to be checked for roles.
    :param columns_config: The ``tables: <table-name>: columns:`` section
        of the ``config.yaml`` file.
    """
    roles: list[Any] = get_property(columns_config, [column.name, "roles"], [])
    for role in roles:
        rc = RelatedColumn(column, fk)
        if role not in out:
            out[role] = [rc]
        else:
            out[role].append(rc)


def _get_all_relative_roles(
    config: Mapping,
    column: Column,
) -> dict[str, list[RelatedColumn]]:
    """
    Work out where the roles are relative to this table.

    :param config: The configuration from ``config.yaml``.
    :param column: The column we are to propose for.
    :return: dictionary of ``role_name`` -> ``(fk or None, column_name)``
        where ``fk`` is the actual foreign key from the table, and ``None``
        means a column from the same table as the input column(s)
        has the required role.
    """
    table = column.table
    tables_config: dict[str, Any] = get_property(config, "tables", {})
    columns_config: dict[str, Any] = get_property(
        tables_config, [str(table.name), "columns"], {}
    )
    role_to_fk_columns: dict[str, list[RelatedColumn]] = {}
    for col in table.columns:
        _set_roles_for_column(role_to_fk_columns, None, col, columns_config)
        # look for roles in related tables
        if (fk_relationship := make_foreign_key_relationship(col)) is not None:
            ft_conf: dict[str, Any] = get_property(
                tables_config, [fk_relationship.target_table_name(), "columns"], {}
            )
            for fcol in fk_relationship.target_table().columns:
                _set_roles_for_column(
                    role_to_fk_columns, fk_relationship, fcol, ft_conf
                )
    return role_to_fk_columns


def _not_anchored_on_ourselves(column: Column, fk_col: RelatedColumn) -> bool:
    """
    Return whether these columns are the exact same thing.

    :param column: The column we are wanting to generate for.
    :param fk_col: The column we are considering using as an anchor.
    :return: False if ``fk_col`` targets the same column as ``column``
      not through a foreign key, so True if ``fk_col`` is through a FK
      or if it doesn't target the same column.
    """
    return not (fk_col.relationship is None and fk_col.target == column)


def _not_anchoring_nonnullable_to_nullable(
    column: Column, fk_col: RelatedColumn
) -> bool:
    """
    Return True if ``column`` is nullable or ``fk_col`` is not.

    We are counting ``fk_col`` as nullabe if either the target is nullable
    or the foreign key itself.

    This is required if ``fk_col`` can be copied to ``col``.
    """
    fk_nullable = (
        fk_col.relationship is not None
        and fk_col.relationship.fk_column.nullable is not False
    )
    fk_col_nullable = fk_nullable or fk_col.target.nullable
    return column.nullable or not fk_col_nullable


def _not_copying_incompatible_fk(column: Column, fk_col: RelatedColumn) -> bool:
    """
    Return False if ``column`` and ``fk_col`` are incompatible foreign keys.

    So, returns True if neither argument is a foreign key, or if both are and they
    reference the same table. So, True implies that copying might be possible,
    False asserts that copying is not possible.
    """
    target = fk_col.target
    if not column.foreign_keys:
        return not target.foreign_keys
    if not target.foreign_keys:
        return False
    tfks = {c.target_fullname for c in target.foreign_keys}
    cfks = {c.target_fullname for c in column.foreign_keys}
    return tfks == cfks


def _get_roles(
    config: Mapping,
    column: Column,
    role_name: str,
) -> list[RelatedColumn]:
    """
    Get all the ``role_name`` roles in this table or a related table.

    :param role_name: The role to return.
    :return: The list of all columns in the same table as ``column`` or in
      tables directly related to that table that have the named role. This
      list has the following columns filtered out: Nullable columns (if
      ``column`` is nonnullable), foreign keys if ``column`` is not a
      foreign key, non-foreign keys if ``column`` is a foreign key
      or foreign keys if ``column`` is a foreign key to a different table.
    """
    roles = _get_all_relative_roles(config, column)
    if role_name not in roles:
        return []
    return [
        fk_col
        for fk_col in roles[role_name]
        if _not_anchored_on_ourselves(column, fk_col)
        and _not_anchoring_nonnullable_to_nullable(column, fk_col)
    ]


def coerce_to_datetime(
    d: datetime.date | datetime.datetime,
    tzdonor: datetime.datetime,
) -> datetime.datetime:
    """Return a datetime, given either a date or datetime."""
    if isinstance(d, datetime.datetime):
        if d.tzinfo is None:
            if tzdonor.tzinfo is not None:
                return d.replace(tzinfo=tzdonor.tzinfo)
        elif tzdonor.tzinfo is None:
            return d.replace(tzinfo=None)
        return d
    return datetime.datetime.combine(d, datetime.time(), tzinfo=tzdonor.tzinfo)


class DateAfterProposer(Proposer):
    """Proposer that proposes dates that are after a preexisting date."""

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        metadata: MetaData,
        sd: float,
        mean: float,
        column: Column,
        anchor_related: RelatedColumn,
        engine: Engine,
        buckets: Buckets | None = None,
    ):
        """
        Initialise a date after proposer.

        :param metadata: The metadata of the source database.
        :param sd: The standard deviation of the number of seconds of the interval measured.
        :param mean: The mean of the number of seconds of the interval measured.
        :param column: The column to generate.
        :param anchor: The anchor column, which must be in the same table as column
         or a related table.
        :param engine: The source database.
        :param buckets: The buckets for the intervals in the source database.
        """
        super().__init__()
        self._sd = sd
        self._mean = mean
        self._anchor_related = anchor_related
        self._column = column
        self._engine = engine
        self._provider = AnchoredProvider(metadata=metadata)
        if buckets is None:
            self._fit = None
            return
        intervals = self.generate_intervals(400)
        self._fit = buckets.fit_from_values(
            [(b - coerce_to_datetime(a, b)).total_seconds() for (a, b) in intervals]
        )

    @classmethod
    def get_query(
        cls,
        column: Column,
        anchor_related: RelatedColumn,
    ) -> Any:
        """Create the query for the summary of interval lengths."""
        anchor = anchor_related.target
        return anchor_related.add_from_to_query(
            select(
                func.avg(SecondsDifference(column, anchor)).label("mean"),
                StdDev(SecondsDifference(column, anchor)).label("sd"),
            )
        )

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        if self._anchor_related.relationship is None:
            return "generic.anchored_provider.normal_date"
        return "generic.anchored_provider.normal_date_fk"

    def name(self) -> str:
        """Get the name of the generator."""
        fname = self.function_name()
        aname = self._anchor_related.target.name
        rel = self._anchor_related.relationship
        if rel is None:
            return f"{fname} [anchored to {aname}]"
        atable = rel.target_table_name()
        fk_name = rel.fk_column.name
        return f"{fname} [anchored to {aname} of table {atable} (via {fk_name})]"

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        column = self._column
        anchor = self._anchor_related.target
        rel = self._anchor_related.relationship
        if rel is None:
            return {
                "mean_seconds": (
                    f'SRC_STATS["auto__{column.table.name}"]'
                    f'["results"][0]["mean__{column.name}"]'
                ),
                "sd_seconds": (
                    f'SRC_STATS["auto__{column.table.name}"]'
                    f'["results"][0]["stddev__{column.name}"]'
                ),
                "anchor": f'GENERATED_ROW["{anchor.name}"]',
            }
        key = f"auto__interval__{column.table.name}__{column.name}"
        fk_col = rel.fk_column.name
        on_col = rel.target_column.name
        return {
            "dst_db_conn": "dst_db_conn",
            "anchor_column": f'"{anchor.name}"',
            "table": f'"{rel.target_table_name()}"',
            "mean_seconds": (f'SRC_STATS["{key}"]["results"][0]["mean"]'),
            "sd_seconds": (f'SRC_STATS["{key}"]["results"][0]["sd"]'),
            "anchor_row": f'GENERATED_ROW["{fk_col}"]',
            "on_column": f'"{on_col}"',
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        if self._anchor_related.relationship is None:
            return {
                "mean_seconds": self._sd,
                "sd_seconds": self._mean,
                "anchor": "1970-01-01",
            }
        anchor = self._anchor_related.target
        on_col = self._anchor_related.relationship.target_column.name
        return {
            "anchor_column": anchor.name,
            "table": anchor.table.name,
            "mean_seconds": self._sd,
            "sd_seconds": self._mean,
            "on_column": on_col,
        }

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """
        Get the query fragments the generators need to call.

        This is for anchors in the same table.
        """
        if self._anchor_related.relationship is not None:
            return {}
        column = self._column
        anchor = self._anchor_related.target
        mean_q = func.avg(SecondsDifference(column, anchor))
        sd_q = StdDev(SecondsDifference(column, anchor))

        return {
            f"mean__{column.name}": {
                "clause": str(
                    mean_q.compile(
                        dialect=self._engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                ),
                "comment": (
                    f"Mean of interval between {anchor.name} of {anchor.table.name}"
                    f" and {column.name} from table {column.table.name}."
                ),
            },
            f"stddev__{column.name}": {
                "clause": str(
                    sd_q.compile(
                        dialect=self._engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                ),
                "comment": (
                    f"Standard deviation of interval between {anchor.name}"
                    f" and {column.name} from table {column.table.name}."
                ),
            },
        }

    def custom_queries(self) -> dict[str, dict[str, Any]]:
        """
        Get the query fragments the generators need to call.

        This is for anchors in a related table.
        """
        column = self._column
        rel = self._anchor_related
        if rel.relationship is None:
            return {}
        query = DateAfterProposer.get_query(column, rel)
        return {
            f"auto__interval__{column.table.name}__{column.name}": {
                "comments": [
                    "Mean and standard deviation of the length of time between"
                    f" column {rel.target.name} of table {rel.target.table.name}"
                    f" and column {column.name} of table {column.table.name}"
                    f" (joined through {rel.relationship.fk_column.name})"
                ],
                "query": str(
                    query.compile(
                        dialect=self._engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                ),
            }
        }

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        return default if self._fit is None else self._fit

    def generate_data(self, count: int) -> list[datetime.datetime]:
        """Generate ``count`` random data points for this column."""
        return [b for (_, b) in self.generate_intervals(count)]

    def generate_intervals(
        self,
        count: int,
    ) -> list[tuple[datetime.datetime, datetime.datetime]]:
        """
        Generate ``count`` intervals.

        :param count: The number of intervals to generate.
        :return: Each pair consists of an anchor and the generated datetime.
        """
        anchor = self._anchor_related.target
        query = select(anchor).select_from(anchor.table)
        query = random_sample(query, count)
        with self._engine.connect() as conn:
            anchors = conn.execute(query).scalars().all()
        anchors = procrustes(
            anchors,
            count,
            datetime.datetime.fromisoformat("1970-01-01"),
        )
        return [
            (anchor, self._provider.normal_date(self._sd, self._mean, anchor))
            for anchor in anchors
        ]


class DateAfterProposerFactory(ProposerFactory):
    """Makes proposers for dates after another anchor date."""

    def __init__(self, config: Mapping, metadata: MetaData):
        """Initialize ``DateAfterProposerFactory``."""
        super().__init__()
        self._config = config
        self._metadata = metadata

    def _make_date_after_proposers(
        self,
        engine: Engine,
        column: Column,
        anchor_related: RelatedColumn,
    ) -> list[DateAfterProposer]:
        """Create a ``DateAfterProposer`` object."""
        query = DateAfterProposer.get_query(column, anchor_related)
        with engine.connect() as connection:
            result = connection.execute(query).first()
            if result is None or result.sd is None:
                return []
        buckets = Buckets.make_buckets(
            engine,
            column.table,
            SecondsDifference(column, anchor_related.target),
            anchor_related,
        )
        return [
            DateAfterProposer(
                self._metadata,
                result.sd,
                result.mean,
                column,
                anchor_related,
                engine=engine,
                buckets=buckets,
            )
        ]

    def get_proposers(
        self,
        columns: list[Column],
        engine: Engine,
    ) -> Sequence[Proposer]:
        """Get all proposers of dates that might be anchored to another column."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, (Date, DateTime)):
            return []
        other_start_columns = _get_roles(self._config, column, "start")
        return [
            prop
            for anchor in other_start_columns
            for prop in self._make_date_after_proposers(engine, column, anchor)
        ]


class CopyProposer(Proposer):
    """Proposer that proposes copying another column."""

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        metadata: MetaData,
        column: Column,
        anchor_related: RelatedColumn,
        engine: Engine,
    ):
        """
        Initialise a copy proposer.

        :param metadata: The metadata of the source database.
        :param column: The column to generate.
        :param anchor: The anchor column, which must be in the same table as column
         or a related table.
        :param engine: The source database.
        """
        super().__init__()
        self._anchor_related = anchor_related
        self._column = column
        self._engine = engine
        self._provider = AnchoredProvider(metadata=metadata)

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        if self._anchor_related.relationship is None:
            return "generic.anchored_provider.copy"
        return "generic.anchored_provider.copy_fk"

    def name(self) -> str:
        """Get the name of the generator."""
        fname = self.function_name()
        aname = self._anchor_related.target.name
        rel = self._anchor_related.relationship
        if rel is None:
            return f"{fname} [from {aname}]"
        atable = rel.target_table_name()
        on_col = rel.fk_column.name
        return f"{fname} [from {aname} of table {atable} (via {on_col})]"

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        anchor = self._anchor_related.target
        rel = self._anchor_related.relationship
        if rel is None:
            return {
                "anchor": f'GENERATED_ROW["{anchor.name}"]',
            }
        fk_col = rel.fk_column
        on_col = rel.target_column
        return {
            "dst_db_conn": "dst_db_conn",
            "anchor_column": f'"{anchor.name}"',
            "table": f'"{rel.target_table_name()}"',
            "anchor_row": f'GENERATED_ROW["{fk_col.name}"]',
            "on_column": f'"{on_col.name}"',
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        anchor = self._anchor_related.target
        if self._anchor_related.relationship is None:
            return {
                "anchor": "1970-01-01",
            }
        on_col = self._anchor_related.relationship.target_column
        return {
            "anchor_column": anchor.name,
            "table": anchor.table.name,
            "on_column": on_col.name,
        }

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """No source data required."""
        return {}

    def custom_queries(self) -> dict[str, dict[str, Any]]:
        """No source data required."""
        return {}

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        return default

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        anchor = self._anchor_related.target
        query = select(anchor).select_from(anchor.table)
        query = random_sample(query, count)
        with self._engine.connect() as conn:
            return list(conn.execute(query).scalars().all())


class CopyProposerFactory(ProposerFactory):
    """Proposer that proposes copying another column."""

    def __init__(self, config: Mapping, metadata: MetaData):
        """Initialize ``CopyProposerFactory``."""
        super().__init__()
        self._config = config
        self._metadata = metadata

    def make_copy_fk_proposers(
        self, engine: Engine, column: Column, anchor: RelatedColumn
    ) -> list[CopyProposer]:
        """Create a ``CopyProposer`` object."""
        return [
            CopyProposer(
                self._metadata,
                column,
                anchor,
                engine=engine,
            )
        ]

    def _can_copy_type(self, copy_from, copy_to) -> bool:
        return type(copy_from) is type(copy_to)

    def get_proposers(
        self,
        columns: list[Column],
        engine: Engine,
    ) -> Sequence[Proposer]:
        """Get all proposers of dates that might be anchored to another column."""
        if len(columns) != 1:
            return []
        column = columns[0]
        source_cols = _get_roles(self._config, column, "source")
        other_source_columns = [
            source_col
            for source_col in source_cols
            if _not_copying_incompatible_fk(column, source_col)
            and self._can_copy_type(source_col.target.type, column.type)
        ]
        return [
            prop
            for anchor in other_source_columns
            for prop in self.make_copy_fk_proposers(engine, column, anchor)
        ]
