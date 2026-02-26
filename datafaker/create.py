"""Functions and classes to create and populate the target database."""
import pathlib
from collections import Counter
from types import ModuleType
from typing import Any, Generator, Iterable, Iterator, Mapping, Sequence, Tuple

from sqlalchemy import Connection, insert, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateColumn, CreateSchema, CreateTable, MetaData, Table

from datafaker.base import FileUploader, TableGenerator
from datafaker.settings import get_settings
from datafaker.utils import (
    create_db_engine_dst,
    get_sync_engine,
    get_vocabulary_table_names,
    logger,
    reinstate_vocab_foreign_key_constraints,
    remove_vocab_foreign_key_constraints,
)

Story = Generator[Tuple[str, dict[str, Any]], dict[str, Any], None]
RowCounts = Counter[str]


@compiles(CreateColumn, "duckdb")
def remove_serial(element: CreateColumn, compiler: Any, **kw: Any) -> str:
    """
    Intercede in compilation for column creation, removing PostgreSQL's ``SERIAL``.

    DuckDB does not understand ``SERIAL``, and we don't care about
    autoincrementing in datafaker. Ideally ``duckdb_engine`` would remove
    this for us, or DuckDB would implement ``SERIAL``
    :param element: The CreateColumn being executed.
    :param compiler: Actually a DDLCompiler, but that type is not exported.
    :param kw: Further arguments.
    :return: Corrected SQL.
    """
    text: str = compiler.visit_create_column(element, **kw)
    return text.replace(" SERIAL ", " INTEGER ")


@compiles(CreateTable, "duckdb")
def remove_on_delete_cascade(element: CreateTable, compiler: Any, **kw: Any) -> str:
    """
    Intercede in compilation for column creation, removing ``ON DELETE CASCADE``.

    DuckDB does not understand cascades, and we don't care about
    that in datafaker. Ideally ``duckdb_engine`` would remove this for us.
    :param element: The CreateTable being executed.
    :param compiler: Actually a DDLCompiler, but that type is not exported.
    :param kw: Further arguments.
    :return: Corrected SQL.
    """
    text: str = compiler.visit_create_table(element, **kw)
    return text.replace(" ON DELETE CASCADE", "")


def create_db_tables(metadata: MetaData) -> None:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()
    dst_dsn: str = settings.dst_dsn or ""
    assert dst_dsn != "", "Missing DST_DSN setting."
    create_db_tables_into(metadata, dst_dsn, settings.dst_schema)


def create_db_tables_into(
    metadata: MetaData, dst_dsn: str, schema_name: str | None = None
) -> None:
    """Create tables described by the sqlalchemy metadata object with explicit DSN."""
    engine = get_sync_engine(create_db_engine_dst(dst_dsn))
    # Create schema, if necessary.
    if schema_name:
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
    base_path: pathlib.Path = pathlib.Path("."),
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
        create_db_engine_dst(dst_dsn, schema_name=settings.dst_schema)
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
    return tables_loaded


def create_db_data(
    sorted_tables: Sequence[Table],
    df_module: ModuleType,
    num_passes: int,
    metadata: MetaData,
) -> RowCounts:
    """Connect to a database and populate it with data."""
    settings = get_settings()
    dst_dsn: str = settings.dst_dsn or ""
    assert dst_dsn != "", "Missing DST_DSN setting."

    return create_db_data_into(
        sorted_tables,
        df_module,
        num_passes,
        dst_dsn,
        settings.dst_schema,
        metadata,
    )


# pylint: disable=too-many-arguments too-many-positional-arguments
def create_db_data_into(
    sorted_tables: Sequence[Table],
    df_module: ModuleType,
    num_passes: int,
    db_dsn: str,
    schema_name: str | None,
    metadata: MetaData,
) -> RowCounts:
    """
    Populate the database.

    :param sorted_tables: The table names to populate, sorted so that foreign
        keys' targets are populated before the foreign keys themselves.
    :param table_generator_dict: A mapping  of table names to the generators
        used to make data for them.
    :param story_generator_list: A list of story generators to be run after the
        table generators on each pass.
    :param num_passes: Number of passes to perform.
    :param db_dsn: Connection string for the destination database.
    :param schema_name: Destination schema name.
    """
    dst_engine = get_sync_engine(create_db_engine_dst(db_dsn, schema_name=schema_name))

    row_counts: Counter[str] = Counter()
    with dst_engine.connect() as dst_conn:
        for _ in range(num_passes):
            row_counts += populate(
                dst_conn,
                sorted_tables,
                df_module.table_generator_dict,
                df_module.story_generator_list,
                metadata,
            )
    dst_engine.dispose()
    return row_counts


# pylint: disable=too-many-instance-attributes
class StoryIterator:
    """Iterates through all the rows produced by all the stories."""

    def __init__(
        self,
        stories: Iterable[tuple[str, Story]],
        table_dict: Mapping[str, Table],
        table_generator_dict: Mapping[str, TableGenerator],
        dst_conn: Connection,
    ):
        """Initialise a Story Iterator."""
        self._stories: Iterator[tuple[str, Story]] = iter(stories)
        self._table_dict: Mapping[str, Table] = table_dict
        self._table_generator_dict: Mapping[str, TableGenerator] = table_generator_dict
        self._dst_conn: Connection = dst_conn
        self._table_name: str | None
        self._final_values: dict[str, Any] | None = None
        try:
            name, self._story = next(self._stories)
            logger.info("Generating data for story '%s'", name)
            self._table_name, self._provided_values = next(self._story)
        except StopIteration:
            self._table_name = None

    def is_ended(self) -> bool:
        """
        Check if we have another row to process.

        If so, insert() can be called.
        """
        return self._table_name is None

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

    def insert(self, metadata: MetaData) -> None:
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
            default_values = table_generator(self._dst_conn, metadata)
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
                if self._final_values is None:
                    self._table_name, self._provided_values = next(self._story)
                    return
                self._table_name, self._provided_values = self._story.send(
                    self._final_values
                )
                return
            except StopIteration:
                try:
                    name, self._story = next(self._stories)
                    logger.info("Generating data for story '%s'", name)
                    self._final_values = None
                except StopIteration:
                    self._table_name = None
                    return


def populate(
    dst_conn: Connection,
    tables: Sequence[Table],
    table_generator_dict: Mapping[str, TableGenerator],
    story_generator_list: Sequence[Mapping[str, Any]],
    metadata: MetaData,
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
            story_iterator.insert(metadata)
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
        try:
            with dst_conn.begin():
                for _ in range(table_generator.num_rows_per_pass):
                    stmt = insert(table).values(table_generator(dst_conn, metadata))
                    dst_conn.execute(stmt)
                    row_counts[table.name] = row_counts.get(table.name, 0) + 1
                dst_conn.commit()
        except:
            dst_conn.rollback()
            raise

    # Insert any remaining stories
    while not story_iterator.is_ended():
        story_iterator.insert(metadata)
        t = story_iterator.table_name()
        if t is None:
            raise AssertionError(
                "Internal error: story iterator returns None but not is_ended"
            )
        row_counts[t] = row_counts.get(t, 0) + 1
        story_iterator.next()

    return row_counts
