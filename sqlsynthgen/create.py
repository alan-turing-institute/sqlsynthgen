"""Functions and classes to create and populate the target database."""
from collections import Counter
import random
from typing import Any, Generator, Iterable, Iterator, Mapping, Sequence, Tuple

from sqlalchemy import Connection, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.schema import (
    CreateSchema,
    MetaData,
    Table,
)
from sqlsynthgen.base import FileUploader, TableGenerator
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import (
    create_db_engine,
    get_sync_engine,
    get_vocabulary_table_names,
    logger,
    reinstate_vocab_foreign_key_constraints,
    remove_vocab_foreign_key_constraints,
)

Story = Generator[Tuple[str, dict[str, Any]], dict[str, Any], None]
RowCounts = Counter[str]


def create_db_tables(metadata: MetaData) -> None:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()
    dst_dsn: str = settings.dst_dsn or ""
    assert dst_dsn != "", "Missing DST_DSN setting."

    engine = get_sync_engine(create_db_engine(dst_dsn))

    # Create schema, if necessary.
    if settings.dst_schema:
        schema_name = settings.dst_schema
        with engine.connect() as connection:
            connection.execute(CreateSchema(schema_name, if_not_exists=True))
            connection.commit()

        # Recreate the engine, this time with a schema specified
        engine = get_sync_engine(create_db_engine(dst_dsn, schema_name=schema_name))

    metadata.create_all(engine)


def create_db_vocab(metadata: MetaData, meta_dict: dict[str, Any], config: Mapping) -> int:
    """
    Load vocabulary tables from files.
    
    arguments:
    metadata: The schema of the database
    meta_dict: The simple description of the schema from --orm-file
    config: The configuration from --config-file
    """
    settings = get_settings()
    dst_dsn: str = settings.dst_dsn or ""
    assert dst_dsn != "", "Missing DST_DSN setting."

    dst_engine = get_sync_engine(
        create_db_engine(dst_dsn, schema_name=settings.dst_schema)
    )

    tables_loaded: list[str] = []

    remove_vocab_foreign_key_constraints(metadata, config, dst_engine)
    vocab_tables = get_vocabulary_table_names(config)
    for vocab_table_name in vocab_tables:
        vocab_table = metadata.tables[vocab_table_name]
        try:
            logger.debug("Loading vocabulary table %s", vocab_table_name)
            uploader = FileUploader(table=vocab_table)
            with Session(dst_engine) as session:
                session.begin()
                uploader.load(session.connection())
            session.commit()
            tables_loaded.append(vocab_table_name)
        except IntegrityError:
            logger.exception("Loading the vocabulary table %s failed:", vocab_table)
    reinstate_vocab_foreign_key_constraints(
        metadata,
        meta_dict,
        config,
        dst_engine,
    )
    return tables_loaded


def create_db_data(
    sorted_tables: Sequence[Table],
    table_generator_dict: Mapping[str, TableGenerator],
    story_generator_list: Sequence[Mapping[str, Any]],
    num_passes: int,
) -> RowCounts:
    """Connect to a database and populate it with data."""
    settings = get_settings()
    dst_dsn: str = settings.dst_dsn or ""
    assert dst_dsn != "", "Missing DST_DSN setting."

    return create_db_data_into(
        sorted_tables,
        table_generator_dict,
        story_generator_list,
        num_passes,
        dst_dsn,
        settings.dst_schema,
    )


def create_db_data_into(
    sorted_tables: Sequence[Table],
    table_generator_dict: Mapping[str, TableGenerator],
    story_generator_list: Sequence[Mapping[str, Any]],
    num_passes: int,
    db_dsn: str,
    schema_name: str | None,
) -> RowCounts:
    dst_engine = get_sync_engine(
        create_db_engine(db_dsn, schema_name=schema_name)
    )

    row_counts: Counter[str] = Counter()
    with dst_engine.connect() as dst_conn:
        for _ in range(num_passes):
            row_counts += populate(
                dst_conn,
                sorted_tables,
                table_generator_dict,
                story_generator_list,
            )
    return row_counts


class StoryIterator:
    def __init__(self,
        stories: Iterable[tuple[str, Story]],
        table_dict: Mapping[str, Table],
        table_generator_dict: Mapping[str, TableGenerator],
        dst_conn: Connection,
    ):
        self._stories: Iterator[tuple[str, Story]] = iter(stories)
        self._table_dict: Mapping[str, Table] = table_dict
        self._table_generator_dict: Mapping[str, TableGenerator] = table_generator_dict
        self._dst_conn: Connection = dst_conn
        try:
            name, self._story = next(self._stories)
            logger.info("Generating data for story '%s'", name)
            self._table_name, self._provided_values = next(self._story)
        except StopIteration:
            self._table_name = None

    def is_ended(self) -> bool:
        """
        Do we have another row to process?
        If so, insert() can be called.
        """
        return self._table_name is None

    def has_table(self, table_name: str):
        """
        Do we have a row for table table_name?
        """
        return table_name == self._table_name

    def table_name(self) -> str:
        """
        The name of the current table (or None if no more stories to process)
        """
        return self._table_name

    def insert(self) -> None:
        """
        Perform the insert. Call this after __init__ or next, and after checking
        that is_ended returns False.
        """
        table = self._table_dict[self._table_name]
        if table.name in self._table_generator_dict:
            table_generator = self._table_generator_dict[table.name]
            default_values = table_generator(self._dst_conn, random.random)
        else:
            default_values = {}
        insert_values = {**default_values, **self._provided_values}
        stmt = insert(table).values(insert_values).return_defaults()
        cursor = self._dst_conn.execute(stmt)
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
        self._final_values = {**insert_values, **return_values}

    def next(self) -> None:
        """
        Advance to the next table row.
        """
        while True:
            try:
                self._table_name, self._provided_values = self._story.send(self._final_values)
                return
            except StopIteration:
                try:
                    name, self._story = next(self._stories)
                    logger.info("Generating data for story '%s'", name)
                except StopIteration:
                    self._table_name = None
                    return


def populate(
    dst_conn: Connection,
    tables: Sequence[Table],
    table_generator_dict: Mapping[str, TableGenerator],
    story_generator_list: Sequence[Mapping[str, Any]],
) -> RowCounts:
    """Populate a database schema with synthetic data."""
    row_counts: Counter[str] = Counter()
    table_dict = {table.name: table for table in tables}
    # Generate stories
    # Each story generator returns a python generator (an unfortunate naming clash with
    # what we call generators). Iterating over it yields individual rows for the
    # database. First, collect all of the python generators into a single list.
    stories: list[tuple[str, Story]] = sum(
        [
            [
                (sg["name"], sg["function"](dst_conn))
                for _ in range(sg["num_stories_per_pass"])
            ]
            for sg in story_generator_list
        ],
        [],
    )
    story_iterator = StoryIterator(stories, table_dict, table_generator_dict, dst_conn)

    # Generate individual rows, table by table.
    for table in tables:
        # Do we have a story row to enter into this table?
        if story_iterator.has_table(table.name):
            story_iterator.insert()
            row_counts[table.name] = row_counts.get(table.name, 0) + 1
            story_iterator.next()
        if table.name not in table_generator_dict:
            # We don't have a generator for this table
            continue
        table_generator = table_generator_dict[table.name]
        if table_generator.num_rows_per_pass == 0:
            continue
        logger.debug('Generating data for table "%s".', table.name)
        # Run all the inserts for one table in a transaction
        with dst_conn.begin():
            for _ in range(table_generator.num_rows_per_pass):
                stmt = insert(table).values(table_generator(dst_conn, random.random))
                dst_conn.execute(stmt)
                row_counts[table.name] = row_counts.get(table.name, 0) + 1

    # Insert any remaining stories
    while not story_iterator.is_ended():
        story_iterator.insert()
        row_counts[table.name] = row_counts.get(table.name, 0) + 1
        story_iterator.next()

    return row_counts
