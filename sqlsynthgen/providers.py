"""This module contains Mimesis Provider sub-classes."""
import datetime as dt
import random
from typing import Any

from mimesis import Datetime, Text
from mimesis.providers.base import BaseDataProvider, BaseProvider
from sqlalchemy.sql import text


class ColumnValueProvider(BaseProvider):
    """A Mimesis provider of random values from the source database."""

    class Meta:
        """Meta-class for ColumnValueProvider settings."""

        name = "column_value_provider"

    def column_value(
        self, db_connection: Any, schema: str, table: str, column: str
    ) -> Any:
        """Return a random value from the column specified."""
        query_str = f"SELECT {column} FROM {schema}.{table} ORDER BY random() LIMIT 1"
        key = db_connection.execute(text(query_str)).fetchone()[0]
        return key


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

    def timedelta(
        self,
        min_dt: Any = dt.timedelta(seconds=0),
        # ints bigger than this cause trouble
        max_dt: Any = dt.timedelta(seconds=2**32),
    ) -> dt.timedelta:
        """Return a random timedelta object."""
        min_s = min_dt.total_seconds()
        max_s = max_dt.total_seconds()
        seconds = random.randint(min_s, max_s)
        return dt.timedelta(seconds=seconds)


class TimespanProvider(BaseProvider):
    """A Mimesis provider for timespans, consisting of start datetime, end datetime, and
    the timedelta in between. Returns a 3-tuple.
    """

    class Meta:
        """Meta-class for TimespanProvider settings."""

        name = "timespan_provider"

    def timespan(
        self,
        earliest_start_year: Any,
        last_start_year: Any,
        min_dt: Any = dt.timedelta(seconds=0),
        # ints bigger than this cause trouble
        max_dt: Any = dt.timedelta(seconds=2**32),
    ) -> tuple[dt.datetime, dt.datetime, dt.timedelta]:
        """Return a timespan as a 3-tuple of (start, end, delta)."""
        delta = TimedeltaProvider().timedelta(min_dt, max_dt)
        start = Datetime().datetime(start=earliest_start_year, end=last_start_year)
        end = start + delta
        return start, end, delta
