"""This module contains Mimesis Provider sub-classes."""
from typing import Any

from mimesis import Text
from mimesis.providers.base import BaseDataProvider, BaseProvider

# from mimesis.locales import Locale
from sqlalchemy.sql import text

# generic = Generic(locale=Locale.EN)


class ForeignKeyProvider(BaseProvider):
    """A Mimesis provider of foreign keys."""

    class Meta:
        """Meta-class for ForeignKeyProvider settings."""

        name = "foreign_key_provider"

    def key(self, db_connection: Any, schema: str, table: str, column: str) -> Any:
        """Return a random value from the table and column specified."""
        query_str = f"SELECT {column} FROM {schema}.{table} ORDER BY random() LIMIT 1"
        key = db_connection.execute(text(query_str)).fetchone()[0]
        return key


class BinaryProvider(BaseDataProvider):
    """A Mimesis provider of binary data."""

    class Meta:
        """Meta-class for ForeignKeyProvider settings."""

        name = "binary_provider"

    def bytes(self) -> bytes:
        """Return a UTF-8 encoded sentence."""
        return Text(self.locale).sentence().encode("utf-8")
