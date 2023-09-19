"""Functions and classes to undo the operations in create.py."""
from types import ModuleType
from typing import Any, Mapping

from sqlalchemy import delete

from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import (
    create_db_engine,
    get_orm_metadata,
    get_sync_engine,
    logger,
)


def remove_db_data(
    orm_module: ModuleType, ssg_module: ModuleType, config: Mapping[str, Any]
) -> None:
    """Truncate the synthetic data tables but not the vocabularies."""
    settings = get_settings()
    assert settings.dst_dsn, "Missing destination database settings"
    tables_config = config.get("tables", {})
    metadata = get_orm_metadata(orm_module, tables_config)
    dst_engine = get_sync_engine(
        create_db_engine(settings.dst_dsn, schema_name=settings.dst_schema)
    )

    with dst_engine.connect() as dst_conn:
        for table in reversed(metadata.sorted_tables):
            # We presume that all tables that aren't vocab should be truncated
            if table.name not in ssg_module.vocab_dict:
                logger.debug("Truncating table %s", table.name)
                dst_conn.execute(delete(table))
                dst_conn.commit()


def remove_db_vocab(
    orm_module: ModuleType, ssg_module: ModuleType, config: Mapping[str, Any]
) -> None:
    """Truncate the vocabulary tables."""
    settings = get_settings()
    assert settings.dst_dsn, "Missing destination database settings"
    tables_config = config.get("tables", {})
    metadata = get_orm_metadata(orm_module, tables_config)
    dst_engine = get_sync_engine(
        create_db_engine(settings.dst_dsn, schema_name=settings.dst_schema)
    )

    with dst_engine.connect() as dst_conn:
        for table in reversed(metadata.sorted_tables):
            # We presume that all tables that are vocab should be truncated
            if table.name in ssg_module.vocab_dict:
                logger.debug("Truncating vocabulary table %s", table.name)
                dst_conn.execute(delete(table))
                dst_conn.commit()


def remove_db_tables(orm_module: ModuleType, config: Mapping[str, Any]) -> None:
    """Drop the tables in the destination schema."""
    settings = get_settings()
    assert settings.dst_dsn, "Missing destination database settings"
    tables_config = config.get("tables", {})
    metadata = get_orm_metadata(orm_module, tables_config)
    dst_engine = get_sync_engine(
        create_db_engine(settings.dst_dsn, schema_name=settings.dst_schema)
    )
    metadata.drop_all(dst_engine)
