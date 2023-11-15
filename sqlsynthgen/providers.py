"""This module contains Mimesis Provider sub-classes."""
import datetime as dt
import random
from typing import Any, Optional, Union, cast

from mimesis import Datetime, Text
from mimesis.providers.base import BaseDataProvider, BaseProvider
from sqlalchemy import Connection
from sqlalchemy.sql import functions, select


class ColumnValueProvider(BaseProvider):
    """A Mimesis provider of random values from the source database."""

    class Meta:
        """Meta-class for ColumnValueProvider settings."""

        name = "column_value_provider"

    @staticmethod
    def column_value(
        db_connection: Connection, orm_class: Any, column_name: str
    ) -> Any:
        """Return a random value from the column specified."""
        query = select(orm_class).order_by(functions.random()).limit(1)
        random_row = db_connection.execute(query).first()

        if random_row:
            return getattr(random_row, column_name)
        return None


class BytesProvider(BaseDataProvider):
    """A Mimesis provider of binary data."""

    class Meta:
        """Meta-class for BytesProvider settings."""

        name = "bytes_provider"

    def bytes(self) -> bytes:
        """Return a UTF-8 encoded sentence."""
        return Text(self.locale).sentence().encode("utf-8")


class TimedeltaProvider(BaseProvider):
    """A Mimesis provider of timedeltas."""

    class Meta:
        """Meta-class for TimedeltaProvider settings."""

        name = "timedelta_provider"

    @staticmethod
    def timedelta(
        min_dt: dt.timedelta = dt.timedelta(seconds=0),
        # ints bigger than this cause trouble
        max_dt: dt.timedelta = dt.timedelta(seconds=2**32),
    ) -> dt.timedelta:
        """Return a random timedelta object."""
        min_s = min_dt.total_seconds()
        max_s = max_dt.total_seconds()
        seconds = random.randint(int(min_s), int(max_s))
        return dt.timedelta(seconds=seconds)


class TimespanProvider(BaseProvider):
    """A Mimesis provider for timespans.

    A timespan consits of start datetime, end datetime, and the timedelta in between.
    Returns a 3-tuple.
    """

    class Meta:
        """Meta-class for TimespanProvider settings."""

        name = "timespan_provider"

    @staticmethod
    def timespan(
        earliest_start_year: int,
        last_start_year: int,
        min_dt: dt.timedelta = dt.timedelta(seconds=0),
        # ints bigger than this cause trouble
        max_dt: dt.timedelta = dt.timedelta(seconds=2**32),
    ) -> tuple[dt.datetime, dt.datetime, dt.timedelta]:
        """Return a timespan as a 3-tuple of (start, end, delta)."""
        delta = TimedeltaProvider().timedelta(min_dt, max_dt)
        start = Datetime().datetime(start=earliest_start_year, end=last_start_year)
        end = start + delta
        return start, end, delta


class WeightedBooleanProvider(BaseProvider):
    """A Mimesis provider for booleans with a given probability for True."""

    class Meta:
        """Meta-class for WeightedBooleanProvider settings."""

        name = "weighted_boolean_provider"

    def bool(self, probability: float) -> bool:
        """Return True with given `probability`, otherwise False."""
        return self.random.uniform(0, 1) < probability


class SQLGroupByProvider(BaseProvider):
    """A Mimesis provider that samples from the results of a SQL `GROUP BY` query."""

    class Meta:
        """Meta-class for SQLGroupByProvider settings."""

        name = "sql_group_by_provider"

    def sample(
        self,
        group_by_result: list[dict[str, Any]],
        weights_column: str,
        value_columns: Optional[Union[str, list[str]]] = None,
        filter_dict: Optional[dict[str, Any]] = None,
    ) -> Union[Any, dict[str, Any], tuple[Any, ...]]:
        """Random sample a row from the result of a SQL `GROUP BY` query.

        The result of the query is assumed to be in the format that sqlsynthgen's
        make-stats outputs.

        For example, if one executes the following src-stats query

        .. code-block:: sql

          SELECT COUNT(*) AS num, nationality, gender, age
          FROM person
          GROUP BY nationality, gender, age

        and calls it the `count_demographics` query, one can then use

        .. code-block:: python

          generic.sql_group_by_provider.sample(
              SRC_STATS["count_demographics"],
              weights_column="num",
              value_columns=["gender", "nationality"],
              filter_dict={"age": 23},
          )

        to restrict the results of the query to only people aged 23, and random sample a
        pair of `gender` and `nationality` values (returned as a tuple in that order),
        with the weights of the sampling given by the counts `num`.

        Arguments:
            group_by_result: Result of the query. A list of rows, with each row being a
                dictionary with names of columns as keys.
            weights_column: Name of the column which holds the weights based on which to
                sample. Typically the result of a `COUNT(*)`.
            value_columns: Name(s) of the column(s) to include in the result. Either a
                string for a single column, an iterable of strings for multiple
                columns, or `None` for all columns (default).
            filter_dict: Dictionary of `{name_of_column: value_it_must_have}`, to
                restrict the sampling to a subset of `group_by_result`. Optional.

        Returns:
            * a single value if `value_columns` is a single column name,
            * a tuple of values in the same order as `value_columns` if `value_columns`
              is an iterable of strings.
            * a dictionary of {name_of_column: value} if `value_columns` is `None`
        """
        if filter_dict is not None:

            def filter_func(row: dict) -> bool:
                for key, value in filter_dict.items():
                    if row[key] != value:
                        return False
                return True

            group_by_result = [row for row in group_by_result if filter_func(row)]
            if not group_by_result:
                raise ValueError("No group_by_result left after filter")

        weights = [cast(int, row[weights_column]) for row in group_by_result]
        weights = [w if w >= 0 else 1 for w in weights]
        random_choice = random.choices(group_by_result, weights)[0]
        if isinstance(value_columns, str):
            return random_choice[value_columns]
        if value_columns is not None:
            values = tuple(random_choice[col] for col in value_columns)
            return values
        return random_choice


class NullProvider(BaseProvider):
    """A Mimesis provider that always returns `None`."""

    class Meta:
        """Meta-class for NullProvider settings."""

        name = "null_provider"

    @staticmethod
    def null() -> None:
        """Return `None`."""
        return None
