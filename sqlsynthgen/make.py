"""Functions to make a module of generator classes."""
import inspect
import sys
from subprocess import CalledProcessError, run
from sys import stderr
from types import ModuleType
from typing import Any, Final, Optional

from mimesis.providers.base import BaseProvider
from sqlalchemy import create_engine
from sqlalchemy.sql import sqltypes

from sqlsynthgen import providers
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import download_table

HEADER_TEXT: str = "\n".join(
    (
        '"""This file was auto-generated by sqlsynthgen but can be edited manually."""',
        "from mimesis import Generic",
        "from mimesis.locales import Locale",
        "from sqlsynthgen.base import FileUploader",
        "",
        "generic = Generic(locale=Locale.EN)",
        "",
    )
)
for entry_name, entry in inspect.getmembers(providers, inspect.isclass):
    if issubclass(entry, BaseProvider) and entry.__module__ == "sqlsynthgen.providers":
        HEADER_TEXT += f"\nfrom sqlsynthgen.providers import {entry_name}"
        HEADER_TEXT += f"\ngeneric.add_provider({entry_name})"
HEADER_TEXT += "\n"

INDENTATION: Final[str] = " " * 4

SQL_TO_MIMESIS_MAP = {
    sqltypes.BigInteger: "generic.numeric.integer_number()",
    sqltypes.Boolean: "generic.development.boolean()",
    sqltypes.Date: "generic.datetime.date()",
    sqltypes.DateTime: "generic.datetime.datetime()",
    sqltypes.Float: "generic.numeric.float_number()",
    sqltypes.Integer: "generic.numeric.integer_number()",
    sqltypes.LargeBinary: "generic.bytes_provider.bytes()",
    sqltypes.Numeric: "generic.numeric.float_number()",
    sqltypes.String: "generic.text.color()",
    sqltypes.Text: "generic.text.color()",
}


def _orm_class_from_table_name(tables_module: Any, full_name: str) -> Optional[Any]:
    """Return the ORM class corresponding to a table name."""
    for mapper in tables_module.Base.registry.mappers:
        cls = mapper.class_
        if cls.__table__.fullname == full_name:
            return cls
    return None


def _add_custom_generators(content: str, table_config: dict) -> tuple[str, list[str]]:
    """Append the custom generators to content, for the given table."""
    generators_config = table_config.get("custom_generators", {})
    columns_covered = []
    for gen_conf in generators_config:
        name = gen_conf["name"]
        columns_assigned = gen_conf["columns_assigned"]
        args = gen_conf["args"]
        if isinstance(columns_assigned, str):
            columns_assigned = [columns_assigned]

        content += INDENTATION * 2
        content += ", ".join(map(lambda x: f"self.{x}", columns_assigned))
        try:
            columns_covered += columns_assigned
        except TypeError:
            # Might be a single string, rather than a list of strings.
            columns_covered.append(columns_assigned)
        content += f" = {name}("
        if args is not None:
            content += ", ".join(f"{key}={value}" for key, value in args.items())
        content += ")\n"
    return content, columns_covered


def _add_default_generator(content: str, tables_module: ModuleType, column: Any) -> str:
    """Append a default generator to content, for the given column."""
    content += INDENTATION * 2
    # If it's a primary key column, we presume that primary keys are populated
    # automatically.
    if column.primary_key:
        content += "pass"
    # If it's a foreign key column, pull random values from the column it
    # references.
    elif column.foreign_keys:
        if len(column.foreign_keys) > 1:
            raise NotImplementedError(
                "Can't handle multiple foreign keys for one column."
            )
        fkey = column.foreign_keys.pop()
        target_name_parts = fkey.target_fullname.split(".")
        target_table_name = ".".join(target_name_parts[:-1])
        target_column_name = target_name_parts[-1]
        target_orm_class = _orm_class_from_table_name(tables_module, target_table_name)
        if target_orm_class is None:
            raise ValueError(f"Could not find the ORM class for {target_table_name}.")
        content += (
            f"self.{column.name} = "
            f"generic.column_value_provider.column_value(dst_db_conn, "
            f"{tables_module.__name__}.{target_orm_class.__name__}, "
            f'"{target_column_name}"'
            ")"
        )

    # Otherwise generate values based on just the datatype of the column.
    else:
        provider = SQL_TO_MIMESIS_MAP[type(column.type)]
        content += f"self.{column.name} = {provider}"
    content += "\n"
    return content


def _add_generator_for_table(
    content: str, tables_module: ModuleType, table_config: dict, table: Any
) -> tuple[str, str]:
    """Add to the generator file `content` a generator for the given table."""
    new_class_name = table.name + "Generator"
    content += f"\n\nclass {new_class_name}:\n"
    content += f"{INDENTATION}num_rows_per_pass = {table_config.get('num_rows_per_pass', 1)}\n\n"
    content += f"{INDENTATION}def __init__(self, src_db_conn, dst_db_conn):\n"
    content, columns_covered = _add_custom_generators(content, table_config)
    for column in table.columns:
        if column.name not in columns_covered:
            # No generator for this column in the user config.
            content = _add_default_generator(content, tables_module, column)
    return content, new_class_name


def make_generators_from_tables(
    tables_module: ModuleType, generator_config: dict
) -> str:
    """Create sqlsynthgen generator classes from a sqlacodegen-generated file.

    Args:
      tables_module: A sqlacodegen-generated module.
      generator_config: Configuration to control the generator creation.

    Returns:
      A string that is a valid Python module, once written to file.
    """
    new_content = HEADER_TEXT
    new_content += f"\nimport {tables_module.__name__}"
    generator_module_name = generator_config.get("custom_generators_module", None)
    if generator_module_name is not None:
        new_content += f"\nimport {generator_module_name}"

    sorted_generators = "[\n"
    sorted_vocab = "[\n"

    settings = get_settings()
    engine = create_engine(settings.src_postgres_dsn)

    for table in tables_module.Base.metadata.sorted_tables:
        table_config = generator_config.get("tables", {}).get(table.name, {})

        if table_config.get("vocabulary_table") is True:

            orm_class = _orm_class_from_table_name(tables_module, table.fullname)
            if not orm_class:
                raise RuntimeError(f"Couldn't find {table.fullname} in {tables_module}")
            class_name = orm_class.__name__
            new_content += (
                f"\n\n{class_name.lower()}_vocab "
                f"= FileUploader({tables_module.__name__}.{class_name}.__table__)"
            )
            sorted_vocab += f"{INDENTATION}{class_name.lower()}_vocab,\n"

            download_table(table, engine)

        else:
            new_content, new_generator_name = _add_generator_for_table(
                new_content, tables_module, table_config, table
            )
            sorted_generators += f"{INDENTATION}{new_generator_name},\n"

    sorted_generators += "]"
    sorted_vocab += "]"

    new_content += "\n\n" + "sorted_generators = " + sorted_generators + "\n"
    new_content += "\n\n" + "sorted_vocab = " + sorted_vocab + "\n"

    return new_content


def make_tables_file(db_dsn: str, schema_name: Optional[str]) -> str:
    """Write a file with the SQLAlchemy ORM classes.

    Exists with an error if sqlacodegen is unsuccessful.
    """
    command = ["sqlacodegen"]

    if schema_name:
        command.append(f"--schema={schema_name}")

    command.append(db_dsn)

    try:
        completed_process = run(
            command, capture_output=True, encoding="utf-8", check=True
        )
    except CalledProcessError as e:
        print(e.stderr, file=stderr)
        sys.exit(e.returncode)

    # sqlacodegen falls back on Tables() for tables without PKs,
    # but we don't explicitly support Tables and behaviour is unpredictable.
    if " = Table(" in completed_process.stdout:
        print(
            "WARNING: Table without PK detected. sqlsynthgen may not be able to continue.",
            file=stderr,
        )

    return completed_process.stdout
