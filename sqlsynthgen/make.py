"""Functions to make a module of generator classes."""
import inspect
import sys
from pathlib import Path
from sys import stderr
from types import ModuleType
from typing import Any, Dict, Final, List, Optional, Type

import snsql
from jinja2 import Environment, FileSystemLoader, Template
from mimesis.providers.base import BaseProvider
from pydantic import PostgresDsn
from sqlacodegen.generators import DeclarativeGenerator
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.sql import sqltypes

from sqlsynthgen import providers
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import create_engine_with_search_path, download_table

PROVIDER_IMPORTS: List[str] = []
for entry_name, entry in inspect.getmembers(providers, inspect.isclass):
    if issubclass(entry, BaseProvider) and entry.__module__ == "sqlsynthgen.providers":
        PROVIDER_IMPORTS.append(entry_name)

TEMPLATE_DIRECTORY: Path = Path(__file__).parent / "templates/"
SSG_TEMPLATE_FILENAME: Final[str] = "ssg.py.template"


INDENTATION: Final[str] = " " * 4

SQL_TO_MIMESIS_MAP = {
    sqltypes.BigInteger: "generic.numeric.integer_number",
    sqltypes.Boolean: "generic.development.boolean",
    sqltypes.Date: "generic.datetime.date",
    sqltypes.DateTime: "generic.datetime.datetime",
    sqltypes.Float: "generic.numeric.float_number",
    sqltypes.Integer: "generic.numeric.integer_number",
    sqltypes.LargeBinary: "generic.bytes_provider.bytes",
    sqltypes.Numeric: "generic.numeric.float_number",
    sqltypes.String: "generic.text.color",
    sqltypes.Text: "generic.text.color",
}


def _orm_class_from_table_name(tables_module: Any, full_name: str) -> Optional[Any]:
    """Return the ORM class corresponding to a table name."""
    for mapper in tables_module.Base.registry.mappers:
        cls = mapper.class_
        if cls.__table__.fullname == full_name:
            return cls
    return None


def _add_custom_generators(table_config: dict) -> tuple[List[Dict], list[str]]:
    """Append the custom generators to content, for the given table."""
    column_info: List[Dict] = []
    generators_config = table_config.get("custom_generators", {})
    columns_covered = []
    for gen_conf in generators_config:
        column_data: Dict[str, Any] = {}

        name = gen_conf["name"]
        columns_assigned = gen_conf["columns_assigned"]
        args = gen_conf["args"]
        if isinstance(columns_assigned, str):
            columns_assigned = [columns_assigned]

        column_data["column_names"] = ", ".join(
            map(lambda x: f"self.{x}", columns_assigned)
        )
        try:
            columns_covered += columns_assigned
        except TypeError:
            # Might be a single string, rather than a list of strings.
            columns_covered.append(columns_assigned)

        column_data["generator_name"] = name
        if args is not None:
            column_data["generator_arguments"] = ", ".join(
                f"{key}={value}" for key, value in args.items()
            )

        column_info.append(column_data)
    return column_info, columns_covered


def _add_default_generator(tables_module: ModuleType, column: Any) -> Dict[str, Any]:
    """Append a default generator to content, for the given column."""
    column_data: Dict[str, Any] = {"primary_key": False}
    # If it's a primary key column, we presume that primary keys are populated
    # automatically.
    if column.primary_key:
        column_data["primary_key"] = True
    # If it's a foreign key column, pull random values from the column it
    # references.
    elif column.foreign_keys:
        if len(column.foreign_keys) > 1:
            raise NotImplementedError(
                "Can't handle multiple foreign keys for one column."
            )
        fkey = next(iter(column.foreign_keys))
        target_name_parts = fkey.target_fullname.split(".")
        target_table_name = ".".join(target_name_parts[:-1])
        target_column_name = target_name_parts[-1]
        target_orm_class = _orm_class_from_table_name(tables_module, target_table_name)
        if target_orm_class is None:
            raise ValueError(f"Could not find the ORM class for {target_table_name}.")

        column_data["column_names"] = f"self.{column.name}"
        column_data["generator_name"] = "generic.column_value_provider.column_value"
        column_data["generator_arguments"] = (
            f"dst_db_conn, {tables_module.__name__}.{target_orm_class.__name__},"
            + f' "{target_column_name}"'
        )

    # Otherwise generate values based on just the datatype of the column.
    else:
        provider = SQL_TO_MIMESIS_MAP[type(column.type)]
        column_data["column_names"] = f"self.{column.name}"
        column_data["generator_name"] = provider

    return column_data


def _add_generator_for_table(
    tables_module: ModuleType, table_config: dict, table: Any
) -> Dict[str, Any]:
    """Add to the generator file `content` a generator for the given table."""
    table_data: Dict[str, Any] = {}
    table_data["table_name"] = table.name
    table_data["class_name"] = table.name + "Generator"
    table_data["num_rows_per_pass"] = table_config.get("num_rows_per_pass", 1)

    columns_and_generators, columns_covered = _add_custom_generators(table_config)
    table_data["columns"] = columns_and_generators

    for column in table.columns:
        if column.name not in columns_covered:
            # No generator for this column in the user config.
            table_data["columns"].append(_add_default_generator(tables_module, column))
    return table_data


def make_generators_from_tables(
    tables_module: ModuleType,
    generator_config: dict,
    src_stats_filename: Optional[str],
    overwrite_files: bool = False,
) -> str:
    """Create sqlsynthgen generator classes from a sqlacodegen-generated file.

    Args:
      tables_module: A sqlacodegen-generated module.
      generator_config: Configuration to control the generator creation.
      src_stats_filename: A filename for where to read src stats from. Optional, if
          `None` this feature will be skipped

    Returns:
      A string that is a valid Python module, once written to file.
    """
    new_content: str = ""
    generator_module_name: str = generator_config.get("custom_generators_module", None)

    settings = get_settings()
    engine = (
        create_engine_with_search_path(
            settings.src_postgres_dsn, settings.src_schema  # type: ignore
        )
        if settings.src_schema
        else create_engine(settings.src_postgres_dsn)
    )

    table_data_list: List[Dict] = []
    for table in tables_module.Base.metadata.sorted_tables:
        table_config = generator_config.get("tables", {}).get(table.name, {})
        table_data: Dict = {"table_config": table_config}

        if table_config.get("vocabulary_table") is True:

            table_data = table_data | _make_generator_for_vocabulary_table(
                tables_module, table, engine, overwrite_files=overwrite_files
            )
        else:
            table_data = table_data | _add_generator_for_table(
                tables_module, table_config, table
            )

        table_data_list.append(table_data)

    return generate_ssg_content(
        {
            "provider_imports": PROVIDER_IMPORTS,
            "ssg_content": new_content,
            "tables_module": tables_module,
            "generator_module_name": generator_module_name,
            "src_stats_filename": src_stats_filename,
            "table_data_list": table_data_list,
        }
    )


def generate_ssg_content(template_context: Dict[str, Any]) -> str:
    """Generate the content of the ssg.py file as a string."""
    environment: Environment = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIRECTORY)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    ssg_template: Template = environment.get_template(SSG_TEMPLATE_FILENAME)
    return ssg_template.render(template_context)


def _make_generator_for_vocabulary_table(
    tables_module: ModuleType,
    table: Any,
    engine: Any,
    table_file_name: Optional[str] = None,
    overwrite_files: bool = False,
) -> Dict[str, Any]:
    orm_class: Optional[Type] = _orm_class_from_table_name(
        tables_module, table.fullname
    )
    if not orm_class:
        raise RuntimeError(f"Couldn't find {table.fullname} in {tables_module}")

    class_name: str = orm_class.__name__

    yaml_file_name: str = table_file_name or table.fullname + ".yaml"
    if Path(yaml_file_name).exists() and not overwrite_files:
        print(f"{str(yaml_file_name)} already exists. Exiting...", file=stderr)
        sys.exit(1)
    else:
        download_table(table, engine, yaml_file_name)

    return {
        "is_vocabulary_table": True,
        "class_name": class_name,
        "vocabulary_class": f"{class_name.lower()}_vocab",
        "table_name": table.name,
    }


def make_tables_file(db_dsn: PostgresDsn, schema_name: Optional[str]) -> str:
    """Write a file with the SQLAlchemy ORM classes.

    Exists with an error if sqlacodegen is unsuccessful.
    """
    engine = (
        create_engine_with_search_path(db_dsn, schema_name)
        if schema_name
        else create_engine(db_dsn)
    )

    metadata = MetaData()
    metadata.reflect(engine)

    generator = DeclarativeGenerator(metadata, engine, options=())
    code = str(generator.generate())

    # sqlacodegen falls back on Tables() for tables without PKs,
    # but we don't explicitly support Tables and behaviour is unpredictable.
    if " = Table(" in code:
        print(
            "WARNING: Table without PK detected. sqlsynthgen may not be able to continue.",
            file=stderr,
        )

    return code


def make_src_stats(
    dsn: PostgresDsn, config: dict, schema_name: Optional[str] = None
) -> dict:
    """Run the src-stats queries specified by the configuration.

    Query the src database with the queries in the src-stats block of the `config`
    dictionary, using the differential privacy parameters set in the `smartnoise-sql`
    block of `config`. Record the results in a dictionary and returns it.
    Args:
        dsn: postgres connection string
        config: a dictionary with the necessary configuration
        schema_name: name of the database schema

    Returns:
        The dictionary of src-stats.
    """
    if schema_name:
        engine = create_engine_with_search_path(dsn, schema_name)
    else:
        engine = create_engine(dsn, echo=False, future=True)

    use_smartnoise_sql = config.get("use-smartnoise-sql", True)
    if use_smartnoise_sql:
        dp_config = config.get("smartnoise-sql", {})
        snsql_metadata = {"": dp_config}

        def execute_query(conn: Any, stat_data: Dict[str, Any]) -> Any:
            privacy = snsql.Privacy(
                epsilon=stat_data["epsilon"], delta=stat_data["delta"]
            )
            reader = snsql.from_connection(
                conn.connection,
                engine="postgres",
                privacy=privacy,
                metadata=snsql_metadata,
            )
            private_result = reader.execute(stat_data["query"])
            # The first entry in the list names the columns, skip that.
            return private_result[1:]

    else:

        def execute_query(conn: Any, stat_data: Dict[str, Any]) -> Any:
            result = conn.execute(text(stat_data["query"])).fetchall()
            return [list(r) for r in result]

    with engine.connect() as conn:
        src_stats = {
            stat_data["name"]: execute_query(conn, stat_data)
            for stat_data in config.get("src-stats", [])
        }
    return src_stats
