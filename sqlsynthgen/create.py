"""Functions and classes to create and populate the target database."""
import logging
from typing import Any, Dict, List

from sqlalchemy import create_engine, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateSchema

from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import create_engine_with_search_path


def create_db_tables(metadata: Any) -> Any:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()

    engine = create_engine(settings.dst_postgres_dsn)

    # Create schema, if necessary.
    if settings.dst_schema:
        schema_name = settings.dst_schema
        if not engine.dialect.has_schema(engine, schema=schema_name):
            engine.execute(CreateSchema(schema_name, if_not_exists=True))

        # Recreate the engine, this time with a schema specified
        engine = create_engine_with_search_path(
            settings.dst_postgres_dsn, schema_name  # type: ignore
        )

    metadata.create_all(engine)


def create_db_vocab(vocab_dict: Dict[str, Any]) -> None:
    """Load vocabulary tables from files."""
    settings = get_settings()

    dst_engine = (
        create_engine_with_search_path(
            settings.dst_postgres_dsn, settings.dst_schema  # type: ignore
        )
        if settings.dst_schema
        else create_engine(settings.dst_postgres_dsn)
    )

    with dst_engine.connect() as dst_conn:
        for vocab_table in vocab_dict.values():
            try:
                vocab_table.load(dst_conn)
            except IntegrityError:
                logging.exception(
                    "Loading the vocabulary table %s failed:", vocab_table
                )


def create_db_data(
    sorted_tables: list,
    generator_dict: dict,
    story_generator_list: list,
    num_passes: int,
) -> None:
    """Connect to a database and populate it with data."""
    settings = get_settings()

    dst_engine = (
        create_engine_with_search_path(
            settings.dst_postgres_dsn, settings.dst_schema  # type: ignore
        )
        if settings.dst_schema
        else create_engine(settings.dst_postgres_dsn)
    )
    src_engine = (
        create_engine_with_search_path(
            settings.src_postgres_dsn, settings.src_schema  # type: ignore
        )
        if settings.src_schema is not None
        else create_engine(settings.src_postgres_dsn)
    )

    with dst_engine.connect() as dst_conn, src_engine.connect() as src_conn:
        for _ in range(num_passes):
            populate(
                src_conn, dst_conn, sorted_tables, generator_dict, story_generator_list
            )


def _populate_table(
    table: Any,
    src_conn: Any,
    dst_conn: Any,
    generator: Any,
    stories: List[Dict[str, Any]],
) -> None:
    """Populate a table with synthetic data, using the given generator and stories."""
    for story in stories:
        if table.name not in story:
            continue
        for row_num, provided_values in enumerate(story[table.name]):
            if callable(provided_values):
                provided_values = provided_values()
            else:
                for key, value in provided_values.items():
                    if callable(value):
                        provided_values[key] = value()
            default_values = generator(src_conn, dst_conn).__dict__
            input_values = {**default_values, **provided_values}
            stmt = insert(table).values(input_values)
            cursor = dst_conn.execute(stmt)
            # We need to add all the default values etc. to provided_values, because
            # other parts of the story may refer to them.
            final_values = {**input_values, **dict(cursor.returned_defaults)}
            story[table.name][row_num] = final_values

    for _ in range(generator.num_rows_per_pass):
        stmt = insert(table).values(generator(src_conn, dst_conn).__dict__)
        dst_conn.execute(stmt)


def populate(
    src_conn: Any,
    dst_conn: Any,
    tables: list,
    generator_dict: dict,
    story_generator_list: list,
) -> None:
    """Populate a database schema with dummy data."""
    stories: List[Dict[str, Any]] = sum(
        [
            [sg["name"](dst_conn) for _ in range(sg["num_stories_per_pass"])]
            for sg in story_generator_list
        ],
        [],
    )
    for table in tables:
        if table.name not in generator_dict:
            # We don't have a generator for this table, probably because it's a
            # vocabulary table.
            continue
        generator = generator_dict[table.name]
        # Run all the inserts for one table in a transaction
        with dst_conn.begin():
            _populate_table(table, src_conn, dst_conn, generator, stories)
