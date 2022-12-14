"""Entrypoint for the sqlsynthgen package."""
from typing import Any

from sqlalchemy import create_engine, insert
from sqlalchemy.engine.base import Engine

from sqlsynthgen.settings import Settings
from sqlsynthgen.star import AdvanceDecision, metadata
from sqlsynthgen.star_gens import AdvanceDecisionGenerator

settings = Settings()


def main() -> None:
    """Create an empty schema and populate it with dummy data."""
    engine = create_engine(settings.postgres_dsn)
    populate(engine)

    return
    metadata.create_all(bind=engine)


def populate(engine: Any) -> None:
    """Populate a database schema with dummy data."""
    # for table in metadata.sorted_tables:
    # print(dir(table))
    # print(table.name)
    # print(table.columns[0].type)
    # return

    stmt = insert(AdvanceDecision).values(AdvanceDecisionGenerator().__dict__)
    with engine.connect() as conn:
        conn.execute(stmt)


if __name__ == "__main__":
    main()
