"""Functions and classes to undo the operations in create.py."""
from typing import Any, Mapping

from sqlalchemy import delete, MetaData

from datafaker.settings import get_settings
from datafaker.utils import (
    create_db_engine,
    get_sync_engine,
    get_vocabulary_table_names,
    logger,
    remove_vocab_foreign_key_constraints,
    reinstate_vocab_foreign_key_constraints,
    sorted_non_vocabulary_tables,
)


def remove_db_data(
    metadata: MetaData, config: Mapping[str, Any]
) -> None:
    """Truncate the synthetic data tables but not the vocabularies."""
    settings = get_settings()
    assert settings.dst_dsn, "Missing destination database settings"
    remove_db_data_from(
        metadata,
        config,
        settings.dst_dsn,
        schema_name=settings.dst_schema
    )


def remove_db_data_from(
    metadata: MetaData, config: Mapping[str, Any], db_dsn: str, schema_name: str | None
) -> None:
    """Truncate the synthetic data tables but not the vocabularies."""
    dst_engine = get_sync_engine(
        create_db_engine(db_dsn, schema_name=schema_name)
    )

    with dst_engine.connect() as dst_conn:
        for table in reversed(sorted_non_vocabulary_tables(metadata, config)):
            logger.debug('Truncating table "%s".', table.name)
            dst_conn.execute(delete(table))
            dst_conn.commit()


def remove_db_vocab(metadata: MetaData, meta_dict: Mapping[str, Any], config: Mapping[str, Any]) -> None:
    """Truncate the vocabulary tables."""
    settings = get_settings()
    assert settings.dst_dsn, "Missing destination database settings"
    dst_engine = get_sync_engine(
        create_db_engine(settings.dst_dsn, schema_name=settings.dst_schema)
    )

    with dst_engine.connect() as dst_conn:
        remove_vocab_foreign_key_constraints(metadata, config, dst_conn)
        for table in get_vocabulary_table_names(config):
            logger.debug('Truncating vocabulary table "%s".', table)
            dst_conn.execute(delete(metadata.tables[table]))
            dst_conn.commit()
        reinstate_vocab_foreign_key_constraints(metadata, meta_dict, config, dst_conn)


def remove_db_tables(metadata: MetaData) -> None:
    """Drop the tables in the destination schema."""
    settings = get_settings()
    assert settings.dst_dsn, "Missing destination database settings"
    dst_engine = get_sync_engine(
        create_db_engine(settings.dst_dsn, schema_name=settings.dst_schema)
    )
    metadata.drop_all(dst_engine)
