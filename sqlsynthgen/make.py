"""Functions to make a module of generator classes."""
import asyncio
import inspect
import logging
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
from sqlacodegen.generators import DeclarativeGenerator
from sqlalchemy import MetaData, UniqueConstraint, text
from sqlalchemy.sql import sqltypes

from sqlsynthgen import providers
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import create_db_engine, download_table

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
class FunctionCall:
    """Contains the ssg.py content related function calls."""

    function_name: str
    argument_values: List[str]


@dataclass
class RowGenerator:
    """Contains the ssg.py content related to row generators of a table."""

    variable_names: List[str]
    function_call: FunctionCall
    primary_key: bool = False


@dataclass
class TableGenerator:
    """Contains the ssg.py content related to regular tables."""

    class_name: str
    table_name: str
    rows_per_pass: int
    row_gens: List[RowGenerator] = field(default_factory=list)
    unique_constraints: List[Any] = field(default_factory=list)


@dataclass
class StoryGenerator:
    """Contains the ssg.py content related to story generators."""

    wrapper_name: str
    function_call: FunctionCall
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


def _get_function_call(
    function_name: str,
    positional_arguments: Optional[List[Any]] = None,
    keyword_arguments: Optional[Dict[str, Any]] = None,
) -> FunctionCall:
    if positional_arguments is None:
        positional_arguments = []

    if keyword_arguments is None:
        keyword_arguments = {}

    argument_values: List[str] = [str(value) for value in positional_arguments]
    argument_values += [f"{key}={value}" for key, value in keyword_arguments.items()]

    return FunctionCall(function_name=function_name, argument_values=argument_values)


def _get_row_generator(
    table_config: dict,
) -> tuple[List[RowGenerator], list[str]]:
    """Get the row generators information, for the given table."""
    row_gen_info: List[RowGenerator] = []
    config = table_config.get("row_generators", {})
    columns_covered = []
    for gen_conf in config:
        name = gen_conf["name"]
        columns_assigned = gen_conf["columns_assigned"]
        keyword_arguments: Dict[str, Any] = gen_conf.get("kwargs", {})
        positional_arguments: List[str] = gen_conf.get("args", [])

        if isinstance(columns_assigned, str):
            columns_assigned = [columns_assigned]

        variable_names: List[str] = columns_assigned
        try:
            columns_covered += columns_assigned
        except TypeError:
            # Might be a single string, rather than a list of strings.
            columns_covered.append(columns_assigned)

        row_gen_info.append(
            RowGenerator(
                variable_names=variable_names,
                function_call=_get_function_call(
                    name, positional_arguments, keyword_arguments
                ),
            )
        )
    return row_gen_info, columns_covered


def _get_default_generator(tables_module: ModuleType, column: Any) -> RowGenerator:
    """Get default generator information, for the given column."""
    # If it's a primary key column, we presume that primary keys are populated
    # automatically.

    # If it's a foreign key column, pull random values from the column it
    # references.
    variable_names: List[str] = []
    generator_function: str = ""
    generator_arguments: List[str] = []

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

        variable_names = [column.name]
        generator_function = "generic.column_value_provider.column_value"
        generator_arguments = [
            "dst_db_conn",
            f"{tables_module.__name__}.{target_orm_class}",
            f'"{target_column_name}"',
        ]

    # Otherwise generate values based on just the datatype of the column.
    else:
        (
            variable_names,
            generator_function,
            generator_arguments,
        ) = _get_provider_for_column(column)

    return RowGenerator(
        primary_key=column.primary_key,
        variable_names=variable_names,
        function_call=_get_function_call(
            function_name=generator_function, positional_arguments=generator_arguments
        ),
    )


def _get_provider_for_column(column: Any) -> Tuple[List[str], str, List[str]]:
    """
    Get a default Mimesis provider and its arguments for a SQL column type.

    Args:
        column: SQLAlchemy column object

    Returns:
        Tuple[str, str, List[str]]: Tuple containing the variable names to assign to,
        generator function and any generator arguments.
    """
    variable_names: List[str] = [column.name]
    generator_arguments: List[str] = []

    column_type = type(column.type)
    column_size: Optional[int] = getattr(column.type, "length", None)

    mapping = {
        (sqltypes.Integer, False): "generic.numeric.integer_number",
        (sqltypes.Boolean, False): "generic.development.boolean",
        (sqltypes.Date, False): "generic.datetime.date",
        (sqltypes.DateTime, False): "generic.datetime.datetime",
        (sqltypes.Numeric, False): "generic.numeric.float_number",
        (sqltypes.LargeBinary, False): "generic.bytes_provider.bytes",
        (sqltypes.String, False): "generic.text.color",
        (sqltypes.String, True): "generic.person.password",
    }

    generator_function = mapping.get((column_type, column_size is not None), None)

    if not generator_function:
        for key, value in mapping.items():
            if issubclass(column_type, key[0]) and key[1] == (column_size is not None):
                generator_function = value
                break

    if not generator_function:
        raise ValueError(f"Unsupported SQLAlchemy type: {column_type}")

    if column_size:
        generator_arguments.append(str(column_size))

    return variable_names, generator_function, generator_arguments


def _enforce_unique_constraints(table_data: TableGenerator) -> None:
    """Wrap row generators of `table_data` in `UniqueGenerator`s to enforce constraints.

    The given `table_data` is modified in place.
    """
    # For each row generator that assigns values to a column that has a unique
    # constraint, wrap it in a UniqueGenerator that ensures the values generated are
    # unique.
    for row_gen in table_data.row_gens:
        # Set of column names that this row_gen assigns to.
        row_gen_column_set = set(row_gen.variable_names)
        for constraint in table_data.unique_constraints:
            # Set of column names that this constraint affects.
            constraint_column_set = set(c.name for c in constraint.columns)
            if not constraint_column_set & row_gen_column_set:
                # The intersection is empty, this constraint isn't relevant for this
                # row_gen.
                continue
            if not constraint_column_set.issubset(row_gen_column_set):
                msg = (
                    "A unique constraint (%s) isn't fully covered by one row "
                    "generator (%s). Enforcement of the constraint may not work."
                )
                logging.warning(msg, constraint.name, row_gen.variable_names)

            # Make a new function call that wraps the old one in a UniqueGenerator
            old_function_call = row_gen.function_call
            new_arguments = [
                "dst_db_conn",
                str(row_gen.variable_names),
                old_function_call.function_name,
            ] + old_function_call.argument_values
            # The self.unique_{constraint_name} will be a UniqueGenerator, initialized
            # in the __init__ of the table generator.
            new_function_call = FunctionCall(
                function_name=f"self.unique_{constraint.name}",
                argument_values=new_arguments,
            )
            row_gen.function_call = new_function_call


def _get_generator_for_table(
    tables_module: ModuleType, table_config: dict, table: Any
) -> TableGenerator:
    """Get generator information for the given table."""
    unique_constraints = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    table_data: TableGenerator = TableGenerator(
        table_name=table.name,
        class_name=table.name + "Generator",
        rows_per_pass=table_config.get("num_rows_per_pass", 1),
        unique_constraints=unique_constraints,
    )

    row_gen_info_data, columns_covered = _get_row_generator(table_config)
    table_data.row_gens.extend(row_gen_info_data)

    for column in table.columns:
        if column.name not in columns_covered:
            # No generator for this column in the user config.
            table_data.row_gens.append(_get_default_generator(tables_module, column))

    _enforce_unique_constraints(table_data)
    return table_data


def _get_story_generators(config: dict) -> List[StoryGenerator]:
    """Get story generators."""
    generators = []
    for gen in config.get("story_generators", []):
        wrapper_name = "run_" + gen["name"].replace(".", "_").lower()
        generators.append(
            StoryGenerator(
                wrapper_name=wrapper_name,
                function_call=_get_function_call(
                    function_name=gen["name"],
                    keyword_arguments=gen.get("kwargs"),
                    positional_arguments=gen.get("args"),
                ),
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
    engine = create_db_engine(
        settings.src_dsn, schema_name=settings.src_schema  # type: ignore
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

    max_unique_constraint_tries = config.get("max-unique-constraint-tries", None)
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
            "max_unique_constraint_tries": max_unique_constraint_tries,
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


def make_tables_file(db_dsn: str, schema_name: Optional[str]) -> str:
    """Write a file with the SQLAlchemy ORM classes.

    Exists with an error if sqlacodegen is unsuccessful.
    """
    engine = create_db_engine(db_dsn, schema_name=schema_name)

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


async def make_src_stats(
    dsn: str, config: dict, schema_name: Optional[str] = None
) -> dict:
    """Run the src-stats queries specified by the configuration.

    Query the src database with the queries in the src-stats block of the `config`
    dictionary, using the differential privacy parameters set in the `smartnoise-sql`
    block of `config`. Record the results in a dictionary and returns it.
    Args:
        dsn: database connection string
        config: a dictionary with the necessary configuration
        schema_name: name of the database schema

    Returns:
        The dictionary of src-stats.
    """
    use_smartnoise_sql = config.get("use-smartnoise-sql", True)
    # SmartNoiseSQL doesn't support asyncio
    use_asyncio = not use_smartnoise_sql
    engine = create_db_engine(dsn, schema_name=schema_name, use_asyncio=use_asyncio)

    if use_smartnoise_sql:
        logging.warning(
            "SmartNoiseSQL does not support asyncio, so make-stats queries"
            " will be run sequentially."
        )
        dp_config = config.get("smartnoise-sql", {})
        snsql_metadata = {"": dp_config}

        async def execute_query(engine: Any, query_block: Dict[str, Any]) -> Any:
            """Execute query using a synchronous SQLAlchemy engine."""
            privacy = snsql.Privacy(
                epsilon=query_block["epsilon"], delta=query_block["delta"]
            )
            with engine.connect() as conn:
                reader = snsql.from_connection(
                    conn.connection,
                    engine="postgres",
                    privacy=privacy,
                    metadata=snsql_metadata,
                )
                private_result = reader.execute(query_block["query"])
            # The first entry in the list names the columns, skip that.
            return private_result[1:]

    else:

        async def execute_query(engine: Any, query_block: Dict[str, Any]) -> Any:
            """Execute query using an asynchronous SQLAlchemy engine."""
            async with engine.connect() as conn:
                raw_result = await conn.execute(text(query_block["query"]))
                result = raw_result.fetchall()
            return [list(r) for r in result]

    query_blocks = config.get("src-stats", [])
    results = await asyncio.gather(
        *[execute_query(engine, query_block) for query_block in query_blocks]
    )
    src_stats = {
        query_block["name"]: result
        for query_block, result in zip(query_blocks, results)
    }
    return src_stats
