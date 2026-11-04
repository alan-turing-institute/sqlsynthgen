"""Functions and classes to undo the operations in create.py."""
from typing import Any, Mapping, Optional

from sqlalchemy import MetaData, delete

from datafaker.db_utils import (
    create_db_engine,
    get_metadata,
    get_sync_engine,
    reinstate_vocab_foreign_key_constraints,
    remove_vocab_foreign_key_constraints,
    sorted_non_vocabulary_tables,
)
from datafaker.settings import get_destination_dsn, get_destination_schema
from datafaker.utils import get_vocabulary_table_names, logger


def remove_db_data(metadata: MetaData, config: Mapping[str, Any]) -> None:
    """Truncate the synthetic data tables but not the vocabularies."""
    remove_db_data_from(
        metadata,
        config,
        get_destination_dsn(),
        schema_name=get_destination_schema(),
    )


def remove_db_data_from(
    metadata: MetaData, config: Mapping[str, Any], db_dsn: str, schema_name: str | None
) -> None:
    """Truncate the synthetic data tables but not the vocabularies."""
    dst_engine = get_sync_engine(create_db_engine(db_dsn, schema_name=schema_name))

    with dst_engine.connect() as dst_conn:
        for table in reversed(sorted_non_vocabulary_tables(metadata, config)):
            logger.debug('Truncating table "%s".', table.name)
            dst_conn.execute(delete(table))
            dst_conn.commit()


def remove_db_vocab(
    metadata: MetaData, meta_dict: Mapping[str, Any], config: Mapping[str, Any]
) -> None:
    """Truncate the vocabulary tables."""
    dst_engine = get_sync_engine(
        create_db_engine(
            get_destination_dsn(),
            schema_name=get_destination_schema(),
        )
    )

    with dst_engine.connect() as dst_conn:
        remove_vocab_foreign_key_constraints(metadata, config, dst_conn)
        for table in get_vocabulary_table_names(config):
            logger.debug('Truncating vocabulary table "%s".', table)
            dst_conn.execute(delete(metadata.tables[table]))
            dst_conn.commit()
        reinstate_vocab_foreign_key_constraints(metadata, meta_dict, config, dst_conn)


def remove_db_tables(metadata: Optional[MetaData]) -> None:
    """Drop the tables in the destination schema."""
    schema_name = get_destination_schema()
    dst_engine = get_sync_engine(
        create_db_engine(
            get_destination_dsn(),
            schema_name=schema_name,
        )
    )
    if metadata is None:
        metadata = get_metadata(
            dst_engine,
            schema_name=schema_name,
            views_to_tables=False,
        )
    metadata.drop_all(dst_engine)
