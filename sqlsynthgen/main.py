"""Entrypoint for the sqlsynthgen package."""

import typer

app = typer.Typer()


@app.command()
def create_data() -> None:
    """Fill tables with synthetic data."""


@app.command()
def create_tables() -> None:
    """Create tables using the SQLAlchemy file."""


@app.command()
def make_generators_file() -> None:
    """Make a SQLSynthGun file of generator classes."""


@app.command()
def make_tables_file() -> None:
    """Make a SQLAlchemy file of Table classes."""


if __name__ == "__main__":
    app()
