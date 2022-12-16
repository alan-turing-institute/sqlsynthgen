from typing import Any

from mimesis.providers.base import BaseProvider
from sqlalchemy.sql import text


class ForeignKeyProvider(BaseProvider):
    class Meta:
        name = "foreign_key_provider"

    def key(self, db_connection: Any, schema: str, table: str, column: str) -> Any:
        query_str = f"SELECT {column} FROM {schema}.{table} ORDER BY random() LIMIT 1"
        key = db_connection.execute(text(query_str)).fetchone()[0]
        return key
