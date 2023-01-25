"""This module contains Mimesis Provider sub-classes."""
from typing import Any

from mimesis import Text
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
