"""Entrypoint for the sqlsynthgen package."""
from subprocess import run

import typer

from sqlsynthgen.settings import get_settings

app = typer.Typer()


@app.command()
def create_data() -> None:
    """Fill tables with synthetic data."""


@app.command()
def create_tables() -> None:
    """Create tables using the SQLAlchemy file."""


@app.command()
def make_generators() -> None:
    """Make a SQLSynthGun file of generator classes."""


@app.command()
def make_tables() -> None:
    """Make a SQLAlchemy file of Table classes."""
    settings = get_settings()

    command = ["sqlacodegen"]

    if settings.src_schema:
        command.append(f"--schema={settings.src_schema}")

    command.append(str(get_settings().src_postgres_dsn))

    completed_process = run(command, capture_output=True, encoding="utf-8", check=True)
    print(completed_process.stdout)


if __name__ == "__main__":
    app()
