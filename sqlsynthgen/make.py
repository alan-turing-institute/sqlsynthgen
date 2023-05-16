"""Functions to make a module of generator classes."""
import inspect
import sys
from dataclasses import dataclass, field
from pathlib import Path
from sys import stderr
from types import ModuleType
from typing import Any, Dict, Final, List, Optional, Tuple

import snsql
from black import FileMode, format_str
from jinja2 import Environment, FileSystemLoader, Template
from mimesis.providers.base import BaseProvider
from pydantic import PostgresDsn
from sqlacodegen.generators import DeclarativeGenerator
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.sql import sqltypes

from sqlsynthgen import providers
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import create_engine_with_search_path, download_table

PROVIDER_IMPORTS: Final[List[str]] = []
for entry_name, entry in inspect.getmembers(providers, inspect.isclass):
    if issubclass(entry, BaseProvider) and entry.__module__ == "sqlsynthgen.providers":
        PROVIDER_IMPORTS.append(entry_name)

TEMPLATE_DIRECTORY: Final[Path] = Path(__file__).parent / "templates/"
SSG_TEMPLATE_FILENAME: Final[str] = "ssg.py.j2"


@dataclass
class VocabularyTableGenerator:
    """Contains the ssg.py content related to vocabulary tables."""

    variable_name: str
    class_name: str
    table_name: str
    dictionary_entry: str


@dataclass
class ColumnGenerator:
    """Contains the ssg.py content related to columns of a table."""

    variable_names: str
    generator_function: str
    generator_arguments: str
    primary_key: bool = False


@dataclass
class TableGenerator:
    """Contains the ssg.py content related to regular tables."""

    class_name: str
    table_name: str
    rows_per_pass: int
    columns: List[ColumnGenerator] = field(default_factory=list)


@dataclass
class StoryGenerator:
    """Contains the ssg.py content related to story generators."""

    wrapper_name: str
    name: str
    arguments: Dict[str, str]
    num_stories_per_pass: int


def _orm_class_from_table_name(
    tables_module: Any, full_name: str
) -> Optional[Tuple[str, str]]:
    """Return the ORM class corresponding to a table name."""
    # If the class in tables_module is an SQLAlchemy ORM class
    for mapper in tables_module.Base.registry.mappers:
        cls = mapper.class_
        if cls.__table__.fullname == full_name:
            return cls.__name__, cls.__name__ + ".__table__"

    # If the class in tables_module is a SQLAlchemy Core Table
    guess = "t_" + full_name
    if guess in dir(tables_module):
        return guess, guess
    return None


def _get_row_generator(
    table_config: dict,
) -> tuple[List[ColumnGenerator], list[str]]:
    """Get the row generators information, for the given table."""
    column_info: List[ColumnGenerator] = []
    config = table_config.get("row_generators", {})
    columns_covered = []
    for gen_conf in config:
        name = gen_conf["name"]
        columns_assigned = gen_conf["columns_assigned"]
        args = gen_conf["args"]
        if isinstance(columns_assigned, str):
            columns_assigned = [columns_assigned]

        variable_names: str = ", ".join(map(lambda x: f"self.{x}", columns_assigned))
        try:
            columns_covered += columns_assigned
        except TypeError:
            # Might be a single string, rather than a list of strings.
            columns_covered.append(columns_assigned)

        generator_function: str = name

        generator_arguments: str = ""
        if args is not None:
            generator_arguments = ", ".join(
                f"{key}={value}" for key, value in args.items()
            )

        column_info.append(
            ColumnGenerator(
                variable_names=variable_names,
                generator_function=generator_function,
                generator_arguments=generator_arguments,
            )
        )
    return column_info, columns_covered


def _get_default_generator(tables_module: ModuleType, column: Any) -> ColumnGenerator:
    """Get default generator information, for the given column."""
    # If it's a primary key column, we presume that primary keys are populated
    # automatically.

    # If it's a foreign key column, pull random values from the column it
    # references.
    variable_names: str = ""
    generator_function: str = ""
    generator_arguments: str = ""

    if column.foreign_keys:
        if len(column.foreign_keys) > 1:
            raise NotImplementedError(
                "Can't handle multiple foreign keys for one column."
            )
        fkey = next(iter(column.foreign_keys))
        target_name_parts = fkey.target_fullname.split(".")
        target_table_name = ".".join(target_name_parts[:-1])
        target_column_name = target_name_parts[-1]
        class_and_name = _orm_class_from_table_name(tables_module, target_table_name)
        if not class_and_name:
            raise ValueError(f"Could not find the ORM class for {target_table_name}.")

        target_orm_class, _ = class_and_name

        variable_names = f"self.{column.name}"
        generator_function = "generic.column_value_provider.column_value"
        generator_arguments = (
            f"dst_db_conn, {tables_module.__name__}.{target_orm_class},"
            + f' "{target_column_name}"'
        )

    # Otherwise generate values based on just the datatype of the column.
    else:
        (
            variable_names,
            generator_function,
            generator_arguments,
        ) = _get_mimesis_function_for_colum(column)

    return ColumnGenerator(
        primary_key=column.primary_key,
        variable_names=variable_names,
        generator_function=generator_function,
        generator_arguments=generator_arguments,
    )


def _get_mimesis_function_for_colum(column: Any) -> Tuple[str, str, str]:
    variable_names: str = f"self.{column.name}"
    generator_arguments: str = ""
    generator_function: str = ""

    column_type = type(column.type)
    column_size: Optional[int] = getattr(column.type, "length", None)

    if column_type == sqltypes.BigInteger:
        generator_function = "generic.numeric.integer_number"
    elif column_type == sqltypes.Boolean:
        generator_function = "generic.development.boolean"
    elif column_type == sqltypes.Date:
        generator_function = "generic.datetime.date"
    elif column_type == sqltypes.DateTime:
        generator_function = "generic.datetime.datetime"
    elif column_type in {sqltypes.Float, sqltypes.Numeric}:
        generator_function = "generic.numeric.float_number"
    elif column_type == sqltypes.Integer:
        generator_function = "generic.numeric.integer_number"
    elif column_type == sqltypes.LargeBinary:
        generator_function = "generic.bytes_provider.bytes"
    elif column_type in {sqltypes.String, sqltypes.Text} and column_size is None:
        generator_function = "generic.text.color"
    elif column_type in {sqltypes.String, sqltypes.Text} and column_size is not None:
        generator_function = "generic.person.password"
        generator_arguments = str(column_size)

    return variable_names, generator_function, generator_arguments


def _get_generator_for_table(
    tables_module: ModuleType, table_config: dict, table: Any
) -> TableGenerator:
    """Get generator information for the given table."""
    table_data: TableGenerator = TableGenerator(
        table_name=table.name,
        class_name=table.name + "Generator",
        rows_per_pass=table_config.get("num_rows_per_pass", 1),
    )

    column_info_data, columns_covered = _get_row_generator(table_config)
    table_data.columns.extend(column_info_data)

    for column in table.columns:
        if column.name not in columns_covered:
            # No generator for this column in the user config.
            table_data.columns.append(_get_default_generator(tables_module, column))
    return table_data


def _get_story_generators(config: dict) -> List[StoryGenerator]:
    """Get story generators."""
    generators = []
    for gen in config.get("story_generators", []):
        wrapper_name = "run_" + gen["name"].replace(".", "_").lower()
        generators.append(
            StoryGenerator(
                wrapper_name=wrapper_name,
                name=gen["name"],
                arguments=gen["args"],
                num_stories_per_pass=gen["num_stories_per_pass"],
            )
        )
    return generators


def make_table_generators(
    tables_module: ModuleType,
    config: dict,
    src_stats_filename: Optional[str],
    overwrite_files: bool = False,
) -> str:
    """Create sqlsynthgen generator classes from a sqlacodegen-generated file.

    Args:
      tables_module: A sqlacodegen-generated module.
      config: Configuration to control the generator creation.
      src_stats_filename: A filename for where to read src stats from.
        Optional, if `None` this feature will be skipped
      overwrite_files: Whether to overwrite pre-existing vocabulary files

    Returns:
      A string that is a valid Python module, once written to file.
    """
    row_generator_module_name: str = config.get("row_generators_module", None)
    story_generator_module_name = config.get("story_generators_module", None)

    settings = get_settings()
    engine = (
        create_engine_with_search_path(
            settings.src_postgres_dsn, settings.src_schema  # type: ignore
        )
        if settings.src_schema
        else create_engine(settings.src_postgres_dsn)
    )

    tables: List[TableGenerator] = []
    vocabulary_tables: List[VocabularyTableGenerator] = []

    for table in tables_module.Base.metadata.sorted_tables:
        table_config = config.get("tables", {}).get(table.name, {})

        if table_config.get("vocabulary_table") is True:
            vocabulary_tables.append(
                _get_generator_for_vocabulary_table(
                    tables_module, table, engine, overwrite_files=overwrite_files
                )
            )
        else:
            tables.append(_get_generator_for_table(tables_module, table_config, table))

    story_generators = _get_story_generators(config)

    return generate_ssg_content(
        {
            "provider_imports": PROVIDER_IMPORTS,
            "tables_module": tables_module,
            "row_generator_module_name": row_generator_module_name,
            "story_generator_module_name": story_generator_module_name,
            "src_stats_filename": src_stats_filename,
            "tables": tables,
            "vocabulary_tables": vocabulary_tables,
            "story_generators": story_generators,
        }
    )


def generate_ssg_content(template_context: Dict[str, Any]) -> str:
    """Generate the content of the ssg.py file as a string."""
    environment: Environment = Environment(
        loader=FileSystemLoader(TEMPLATE_DIRECTORY),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    ssg_template: Template = environment.get_template(SSG_TEMPLATE_FILENAME)
    template_output: str = ssg_template.render(template_context)

    return format_str(template_output, mode=FileMode())


def _get_generator_for_vocabulary_table(
    tables_module: ModuleType,
    table: Any,
    engine: Any,
    table_file_name: Optional[str] = None,
    overwrite_files: bool = False,
) -> VocabularyTableGenerator:
    class_and_name: Optional[Tuple[str, str]] = _orm_class_from_table_name(
        tables_module, table.fullname
    )
    if not class_and_name:
        raise RuntimeError(f"Couldn't find {table.fullname} in {tables_module}")

    class_name, table_name = class_and_name

    yaml_file_name: str = table_file_name or table.fullname + ".yaml"
    if Path(yaml_file_name).exists() and not overwrite_files:
        print(f"{str(yaml_file_name)} already exists. Exiting...", file=stderr)
        sys.exit(1)
    else:
        download_table(table, engine, yaml_file_name)

    return VocabularyTableGenerator(
        class_name=class_name,
        dictionary_entry=table.name,
        variable_name=f"{class_name.lower()}_vocab",
        table_name=table_name,
    )


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
