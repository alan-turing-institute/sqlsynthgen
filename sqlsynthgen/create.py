"""Functions and classes to create and populate the target database."""
from typing import Any, List

from sqlalchemy import create_engine, event, insert
from sqlalchemy.schema import CreateSchema

from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import set_search_path


def create_db_tables(metadata: Any) -> Any:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()

    engine = create_engine(settings.dst_postgres_dsn)

    # Create schema, if necessary.
    if settings.dst_schema:
        schema_name = settings.dst_schema
        if not engine.dialect.has_schema(engine, schema=schema_name):
            engine.execute(CreateSchema(schema_name, if_not_exists=True))

        # Recreate the engine and, this time, set a connection callback
        engine = create_engine(settings.dst_postgres_dsn)

        if schema_name:

            @event.listens_for(engine, "connect", insert=True)
            def connect(dbapi_connection: Any, _: Any) -> None:
                set_search_path(dbapi_connection, schema_name)

    metadata.create_all(engine)


def create_db_vocab(sorted_vocab: List[Any]) -> None:
    """Load vocabulary tables from files."""
    settings = get_settings()
    dst_engine = create_engine(settings.dst_postgres_dsn)

    with dst_engine.connect() as dst_conn:
        if settings.dst_schema:
            dst_conn.execute(f"SET SEARCH_PATH TO {settings.dst_schema}")

        for vocab_table in sorted_vocab:
            vocab_table.load(dst_conn)


def create_db_data(
    sorted_tables: list, sorted_generators: list, num_passes: int
) -> None:
    """Connect to a database and populate it with data."""
    settings = get_settings()
    engine = create_engine(settings.dst_postgres_dsn)

    if settings.dst_schema:
        schema_name = settings.dst_schema

        @event.listens_for(engine, "connect", insert=True)
        def connect(dbapi_connection: Any, _: Any) -> None:
            set_search_path(dbapi_connection, schema_name)

    src_engine = create_engine(settings.src_postgres_dsn)

    with engine.connect() as dst_conn:
        with src_engine.connect() as src_conn:
            populate(src_conn, dst_conn, sorted_tables, sorted_generators, num_passes)


def populate(
    src_conn: Any, dst_conn: Any, tables: list, generators: list, num_passes: int
) -> None:
    """Populate a database schema with dummy data."""
    for table, generator in reversed(
        list(zip(reversed(tables), reversed(generators)))
    ):  # Run all the inserts for one table in a transaction
        with dst_conn.begin():
            for _ in range(num_passes):
                for __ in range(generator.num_rows_per_pass):
                    stmt = insert(table).values(generator(src_conn, dst_conn).__dict__)
                    dst_conn.execute(stmt)
