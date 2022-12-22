"""Entrypoint for the sqlsynthgen package."""
from typing import Any

from sqlalchemy import create_engine, insert

from sqlsynthgen.create_generators import create_generators_from_tables
from sqlsynthgen.settings import get_settings

# TODO Fix the below imports
from tests.examples.example_tables import metadata
from tests.examples.expected_output import sorted_generators as test_generators

# from sqlsynthgen.star import AdvanceDecision, metadata
# from sqlsynthgen.star_gens import AdvanceDecisionGenerator


def main() -> None:
    """Create an empty schema and populate it with dummy data."""

    settings = get_settings()
    engine = create_engine(settings.postgres_dsn)
    populate(engine)
    # metadata.create_all(bind=engine)


def populate(engine: Any) -> None:
    """Populate a database schema with dummy data."""
    # for table in metadata.sorted_tables:
    # print(dir(table))
    # print(table.name)
    # print(table.columns[0].type)
    # return

    for table, generator in zip(metadata.sorted_tables, test_generators):
        with engine.connect() as conn:
            stmt = insert(table).values(generator(conn).__dict__)
            conn.execute(stmt)


if __name__ == "__main__":
    # main()
    create_generators_from_tables("sqlsynthgen.star")
