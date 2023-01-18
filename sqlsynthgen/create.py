"""Functions and classes to create and populate the target database."""
from typing import Any

from sqlalchemy import create_engine, insert
from sqlalchemy.schema import CreateSchema

from sqlsynthgen.settings import get_settings


def create_db_tables(metadata: Any) -> Any:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()
    engine = create_engine(settings.dst_postgres_dsn)
    # Create schemas, if necessary.
    for table in metadata.sorted_tables:
        try:
            schema = table.schema
            if not engine.dialect.has_schema(engine, schema=schema):
                engine.execute(CreateSchema(schema, if_not_exists=True))
        except AttributeError:
            # This table didn't have a schema field
            pass
    metadata.create_all(engine)


def create_db_data(sorted_tables: list, sorted_generators: list) -> None:
    """Connect to a database and populate it with data."""
    settings = get_settings()
    engine = create_engine(settings.dst_postgres_dsn)

    with engine.connect() as conn:
        populate(conn, sorted_tables, sorted_generators)


def populate(conn: Any, tables: list, generators: list) -> None:
    """Populate a database schema with dummy data."""

    for table, generator in zip(tables, generators):
        stmt = insert(table).values(generator(conn).__dict__)
        conn.execute(stmt)
