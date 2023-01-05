"""Entrypoint for the SQLSynthGen package."""
from importlib import import_module
from pathlib import Path
from subprocess import run

import typer

from sqlsynthgen.create import create_db_tables, generate
from sqlsynthgen.make import make_generators_from_tables
from sqlsynthgen.settings import get_settings

app = typer.Typer()


@app.command()
def create_data(
    orm_file: str = typer.Argument(...),
    ssg_file: str = typer.Argument(...),
) -> None:
    """Fill tables with synthetic data."""

    orm_file_path = Path(orm_file)
    orm_module_path = ".".join(orm_file_path.parts[:-1] + (orm_file_path.stem,))
    orm_module = import_module(orm_module_path)

    ssg_file_path = Path(ssg_file)
    ssg_module_path = ".".join(ssg_file_path.parts[:-1] + (ssg_file_path.stem,))
    ssg_module = import_module(ssg_module_path)

    generate(orm_module.metadata.sorted_tables, ssg_module.sorted_generators)


@app.command()
def create_tables(orm_file: str = typer.Argument(...)) -> None:
    """Create tables using the SQLAlchemy file."""
    file_path = Path(orm_file)
    module_path = ".".join(file_path.parts[:-1] + (file_path.stem,))
    orm_module = import_module(module_path)
    create_db_tables(orm_module.metadata)


@app.command()
def make_generators(orm_file: str = typer.Argument(...)) -> None:
    """Make a SQLSynthGen file of generator classes."""
    file_path = Path(orm_file)
    module_path = ".".join(file_path.parts[:-1] + (file_path.stem,))
    result = make_generators_from_tables(module_path)
    print(result)


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
