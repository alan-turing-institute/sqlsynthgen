"""Functions and classes to create and populate the target database."""
from collections import Counter
from pathlib import Path
from typing import Any, Generator, Iterable, Iterator, Mapping, Sequence, Tuple

import typer
import yaml
from sqlalchemy import Connection, insert, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema, MetaData, Table

from datafaker.base import FileUploader
from datafaker.db_utils import (
    create_db_engine_dst,
    get_sync_engine,
    get_vocabulary_table_names,
    reinstate_vocab_foreign_key_constraints,
    remove_vocab_foreign_key_constraints,
)
from datafaker.make import FunctionCall, StoryGeneratorInfo, get_generation_info
from datafaker.populate import (
    TableGenerator,
    call_function,
    get_symbols,
    get_table_generator_dict,
)
from datafaker.settings import get_destination_dsn, get_destination_schema, get_settings
from datafaker.utils import get_property, logger

Story = Generator[Tuple[str, dict[str, Any]], dict[str, Any], None]
RowCounts = Counter[str]


def create_db_tables(metadata: MetaData) -> None:
    """Create tables described by the sqlalchemy metadata object."""
    dst_dsn = get_destination_dsn()
    assert dst_dsn != "", "Missing DST_DSN setting."
    create_db_tables_into(metadata, dst_dsn, get_destination_schema())


def create_db_tables_into(
    metadata: MetaData, dst_dsn: str, schema_name: str | None = None
) -> None:
    """Create tables described by the sqlalchemy metadata object with explicit DSN."""
    engine = get_sync_engine(create_db_engine_dst(dst_dsn))
    # Create schema, if necessary.
    if schema_name is not None:
        with engine.connect() as connection:
            # Do not try to create a schema if the schema already exists.
            # This is necessary if the user does not have schema creation privileges
            # but does have a schema they are able to write to.
            if not inspect(connection).has_schema(schema_name):
                connection.execute(CreateSchema(schema_name, if_not_exists=True))
                connection.commit()

        # Recreate the engine, this time with a schema specified
        engine.dispose()
        engine = get_sync_engine(create_db_engine_dst(dst_dsn, schema_name=schema_name))

    metadata.create_all(engine)
    engine.dispose()


def create_db_vocab(
    metadata: MetaData,
    meta_dict: dict[str, Any],
    config: Mapping,
    base_path: Path = Path("."),
) -> list[str]:
    """
    Load vocabulary tables from files.

    :param metadata: The schema of the database
    :param meta_dict: The simple description of the schema from --orm-file
    :param config: The configuration from --config-file
    :return: List of table names loaded.
    """
    settings = get_settings()
    dst_dsn: str = settings.dst_dsn or ""
    assert dst_dsn != "", "Missing DST_DSN setting."

    dst_engine = get_sync_engine(
        create_db_engine_dst(
            get_destination_dsn(),
            schema_name=get_destination_schema(),
        )
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
                uploader.load(session.connection(), base_path=base_path)
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
    dst_engine.dispose()
    return tables_loaded


def create_db_data(
    sorted_tables: Sequence[Table],
    config: Mapping[str, Any],
    src_stats_filename: Path | None,
    num_passes: int,
    metadata: MetaData,
) -> RowCounts:
    """Connect to a database and populate it with data."""
    if src_stats_filename:
        try:
            with src_stats_filename.open(encoding="utf-8") as fh:
                src_stats = yaml.load(fh, yaml.SafeLoader)
        except FileNotFoundError as exc:
            logger.error(
                "No source stats file '%s', this should be the output of the 'make-stats' command",
                src_stats_filename,
            )
            raise typer.Exit(1) from exc
    else:
        src_stats = None
    return create_db_data_into(
        sorted_tables,
        config,
        src_stats,
        num_passes,
        get_destination_dsn(),
        get_destination_schema(),
        metadata,
    )


# pylint: disable=too-many-arguments too-many-positional-arguments
def create_db_data_into(
    sorted_tables: Sequence[Table],
    config: Mapping[str, Any],
    src_stats: dict[str, dict[str, Any]] | None,
    num_passes: int,
    db_dsn: str,
    schema_name: str | None,
    metadata: MetaData,
) -> RowCounts:
    """
    Populate the database.

    :param sorted_tables: The table names to populate, sorted so that foreign
        keys' targets are populated before the foreign keys themselves.
    :param config: The data from the ``config.yaml`` file.
    :param src_stats: The data from the ``src-stats.yaml`` file.
    :param num_passes: Number of passes to perform.
    :param db_dsn: Connection string for the destination database.
    :param schema_name: Destination schema name.
    :param metadata: Destination database metadata.
    """
    dst_engine = get_sync_engine(create_db_engine_dst(db_dsn, schema_name=schema_name))
    gen_info = get_generation_info(metadata, config)
    context = get_symbols(
        gen_info.row_generator_module_name,
        gen_info.story_generator_module_name,
        get_property(config, "object_instantiation", {}),
        src_stats,
        metadata,
    )
    context["sum"] = sum
    row_counts: Counter[str] = Counter()
    with dst_engine.connect() as dst_conn:
        context["dst_db_conn"] = dst_conn
        for _ in range(num_passes):
            row_counts += populate(
                dst_conn,
                sorted_tables,
                get_table_generator_dict(
                    dst_conn,
                    gen_info.tables,
                    gen_info.max_unique_constraint_tries,
                ),
                gen_info.story_generators,
                context,
            )
    dst_engine.dispose()
    return row_counts


def empty_story_generator() -> (
    Generator[tuple[str, dict[str, Any]], dict[str, Any], None]
):
    """Get a story generator that generates no values."""
    empt: list[tuple[str, dict[str, Any]]] = []
    for e in empt:
        _ = yield e


# pylint: disable=too-many-instance-attributes
class StoryIterator:
    """Iterates through all the rows produced by all the stories."""

    def __init__(
        self,
        stories: Iterable[StoryGeneratorInfo],
        table_dict: Mapping[str, Table],
        table_generator_dict: Mapping[str, TableGenerator],
        dst_conn: Connection,
        context: Mapping,
    ):
        """Initialise a Story Iterator."""
        self._story_infos: Iterator[StoryGeneratorInfo] = iter(stories)
        self._table_dict: Mapping[str, Table] = table_dict
        self._table_generator_dict: Mapping[str, TableGenerator] = table_generator_dict
        self._dst_conn: Connection = dst_conn
        self._table_name: str | None = None
        self._final_values: dict[str, Any] | None = None
        # Number of times the current story should be run
        self._story_counts = 1
        self._story_function_call: FunctionCall
        self._context = context
        self._story = empty_story_generator()
        self._provided_values: dict[str, Any]
        self.next()

    def _get_next_story(self) -> bool:
        """
        Iterate to the next ``_story_infos``.

        :return: False if there are no more.
        """
        try:
            sgi = next(self._story_infos)
            self._story_counts = sgi.num_stories_per_pass
            self._story_function_call = sgi.function_call
            logger.info(
                "Generating data for story '%s'", sgi.function_call.function_name
            )
            self._story = call_function(sgi.function_call, self._context)
            self._final_values = None
        except StopIteration:
            self._table_name = None
            return False
        return True

    def _get_values(self) -> None:
        """Get the values from the current story and advance the iterator."""
        if self._final_values is None:
            self._table_name, self._provided_values = next(self._story)
        else:
            self._table_name, self._provided_values = self._story.send(
                self._final_values
            )

    def is_ended(self) -> bool:
        """
        Check if we have another row to process.

        If so, insert() can be called.
        """
        return self._story_counts == -1

    def has_table(self, table_name: str) -> bool:
        """Check if we have a row for table ``table_name``."""
        return table_name == self._table_name

    def table_name(self) -> str | None:
        """
        Get the name of the current table.

        :return: The table name, or None if there are  no more stories
            to process.
        """
        return self._table_name

    def insert(self) -> None:
        """
        Put the row in the table.

        Call this after __init__ or next, and after checking that is_ended
        returns False.
        """
        if self._table_name is None:
            raise StopIteration("StoryIterator.insert after is_ended")
        table = self._table_dict[self._table_name]
        if table.name in self._table_generator_dict:
            table_generator = self._table_generator_dict[table.name]
            default_values = table_generator.generate_row(self._dst_conn)
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
        self._dst_conn.commit()
        cursor.close()

    def next(self) -> None:
        """Advance to the next row."""
        while True:
            try:
                self._get_values()
                return
            except StopIteration:
                self._final_values = None
                self._story_counts -= 1
                if 0 < self._story_counts:
                    # Reinitialize the same story again
                    self._story = call_function(
                        self._story_function_call, self._context
                    )
                elif not self._get_next_story():
                    self._story_counts = -1
                    return


def populate(
    dst_conn: Connection,
    tables: Sequence[Table],
    table_generator_dict: Mapping[str, TableGenerator],
    story_generator_infos: Sequence[StoryGeneratorInfo],
    context: Mapping,
) -> RowCounts:
    """Populate a database schema with synthetic data."""
    row_counts: Counter[str] = Counter()
    table_dict = {table.name: table for table in tables}
    # Generate stories
    # Each story generator returns a python generator (an unfortunate naming clash with
    # what we call generators). Iterating over it yields individual rows for the
    # database.
    story_iterator = StoryIterator(
        story_generator_infos,
        table_dict,
        table_generator_dict,
        dst_conn,
        context,
    )

    # Set the context for each row generator
    for tg in table_generator_dict.values():
        tg.set_context(context)

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
        logger.debug("Generating data for table '%s'", table.name)
        # Run all the inserts for one table in a transaction
        with dst_conn.begin():
            try:
                for _ in range(table_generator.num_rows_per_pass):
                    stmt = insert(table).values(table_generator.generate_row(dst_conn))
                    dst_conn.execute(stmt)
                    row_counts[table.name] = row_counts.get(table.name, 0) + 1
                dst_conn.commit()
            except:
                dst_conn.rollback()
                raise

    # Insert any remaining stories
    while not story_iterator.is_ended():
        story_iterator.insert()
        t = story_iterator.table_name()
        if t is None:
            raise AssertionError(
                "Internal error: story iterator returns None but not is_ended"
            )
        row_counts[t] = row_counts.get(t, 0) + 1
        story_iterator.next()

    return row_counts
