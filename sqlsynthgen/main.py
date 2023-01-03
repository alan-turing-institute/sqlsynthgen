"""Entrypoint for the sqlsynthgen package."""
from typing import Any

import typer
from sqlalchemy import create_engine, insert

from sqlsynthgen.settings import get_settings

app = typer.Typer()


@app.command()
def make_tables_file() -> None:
    """Make a SQLAlchemy file of Table classes."""


def create_tables(metadata: Any) -> Any:
    """Create tables described by the sqlalchemy metadata object."""
    settings = get_settings()
    engine = create_engine(settings.postgres_dsn)
    metadata.create_all(engine)


@app.command()
def main() -> None:
    """Not implemented yet."""
    raise NotImplementedError


def generate(sorted_tables: list, sorted_generators: list) -> Any:
    """Connect to a database and populate it with data."""
    settings = get_settings()
    engine = create_engine(settings.postgres_dsn)

    with engine.connect() as conn:
        populate(conn, sorted_tables, sorted_generators)


def populate(conn: Any, tables: list, generators: list) -> None:
    """Populate a database schema with dummy data."""

    for table, generator in zip(tables, generators):
        stmt = insert(table).values(generator(conn).__dict__)
        conn.execute(stmt)


if __name__ == "__main__":
    app()
