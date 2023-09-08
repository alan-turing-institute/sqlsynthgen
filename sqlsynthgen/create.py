"""Functions and classes to create and populate the target database."""
import logging
from typing import Any, Dict, Generator, List, Tuple

from sqlalchemy import Connection, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateSchema, MetaData, Table

from sqlsynthgen.base import FileUploader, TableGenerator
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import create_db_engine, get_sync_engine

Story = Generator[Tuple[str, Dict[str, Any]], Dict[str, Any], None]


def create_db_tables(metadata: MetaData) -> None:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()

    engine = get_sync_engine(create_db_engine(settings.dst_dsn))  # type: ignore

    # Create schema, if necessary.
    if settings.dst_schema:
        schema_name = settings.dst_schema
        with engine.connect() as connection:
            if not engine.dialect.has_schema(connection, schema_name=schema_name):
                connection.execute(CreateSchema(schema_name, if_not_exists=True))

        # Recreate the engine, this time with a schema specified
        engine = get_sync_engine(
            create_db_engine(settings.dst_dsn, schema_name=schema_name)  # type: ignore
        )

    metadata.create_all(engine)


def create_db_vocab(vocab_dict: Dict[str, FileUploader]) -> None:
    """Load vocabulary tables from files."""
    settings = get_settings()

    dst_engine = get_sync_engine(
        create_db_engine(
            settings.dst_dsn, schema_name=settings.dst_schema  # type: ignore
        )
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
    sorted_tables: list[Table],
    table_generator_dict: dict[str, TableGenerator],
    story_generator_list: list[dict[str, Any]],
    num_passes: int,
) -> None:
    """Connect to a database and populate it with data."""
    settings = get_settings()

    dst_engine = get_sync_engine(
        create_db_engine(
            settings.dst_dsn, schema_name=settings.dst_schema  # type: ignore
        )
    )

    with dst_engine.connect() as dst_conn:
        for _ in range(num_passes):
            populate(
                dst_conn,
                sorted_tables,
                table_generator_dict,
                story_generator_list,
            )


def _populate_story(
    story: Story,
    table_dict: Dict[str, Table],
    table_generator_dict: Dict[str, TableGenerator],
    dst_conn: Connection,
) -> None:
    """Write to the database all the rows created by the given story."""
    # Loop over the rows generated by the story, insert them into their
    # respective tables. Ideally this would say
    # `for table_name, provided_values in story:`
    # but we have to loop more manually to be able to use the `send` function.
    try:
        table_name, provided_values = next(story)
        while True:
            table = table_dict[table_name]
            if table.name in table_generator_dict:
                table_generator = table_generator_dict[table.name]
                default_values = table_generator(dst_conn)
            else:
                default_values = {}
            insert_values = {**default_values, **provided_values}
            stmt = insert(table).values(insert_values).return_defaults()
            cursor = dst_conn.execute(stmt)
            # We need to return all the default values etc. to the generator,
            # because other parts of the story may refer to them.
            if cursor.returned_defaults:
                # pylint: disable=protected-access
                return_values = {
                    str(k): v for k, v in cursor.returned_defaults._mapping.items()
                }
                # pylint: enable=protected-access
            else:
                return_values = {}
            final_values = {**insert_values, **return_values}
            table_name, provided_values = story.send(final_values)
    except StopIteration:
        # The story has finished, it has no more rows to generate
        pass


def populate(
    dst_conn: Connection,
    tables: list[Table],
    table_generator_dict: dict[str, TableGenerator],
    story_generator_list: list[dict[str, Any]],
) -> None:
    """Populate a database schema with synthetic data."""
    table_dict = {table.name: table for table in tables}
    # Generate stories
    # Each story generator returns a python generator (an unfortunate naming clash with
    # what we call generators). Iterating over it yields individual rows for the
    # database. First, collect all of the python generators into a single list.
    stories: List[Story] = sum(
        [
            [sg["name"](dst_conn) for _ in range(sg["num_stories_per_pass"])]
            for sg in story_generator_list
        ],
        [],
    )
    for story in stories:
        # Run the inserts for each story within a transaction.
        with dst_conn.begin():
            _populate_story(story, table_dict, table_generator_dict, dst_conn)

    # Generate individual rows, table by table.
    for table in tables:
        if table.name not in table_generator_dict:
            # We don't have a generator for this table, probably because it's a
            # vocabulary table.
            continue
        table_generator = table_generator_dict[table.name]
        # Run all the inserts for one table in a transaction
        with dst_conn.begin():
            for _ in range(table_generator.num_rows_per_pass):
                stmt = insert(table).values(table_generator(dst_conn))
                dst_conn.execute(stmt)
