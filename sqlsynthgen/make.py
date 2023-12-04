"""Functions to make a module of generator classes."""
import asyncio
import inspect
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Mapping, Optional, Sequence, Tuple

import pandas as pd
import snsql
from black import FileMode, format_str
from jinja2 import Environment, FileSystemLoader, Template
from mimesis.providers.base import BaseProvider
from sqlacodegen.generators import DeclarativeGenerator
from sqlalchemy import Engine, MetaData, UniqueConstraint, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.schema import Column, Table
from sqlalchemy.sql import sqltypes

from sqlsynthgen import providers
from sqlsynthgen.settings import get_settings
from sqlsynthgen.utils import (
    create_db_engine,
    download_table,
    get_orm_metadata,
    get_sync_engine,
    logger,
)

PROVIDER_IMPORTS: Final[list[str]] = []
for entry_name, entry in inspect.getmembers(providers, inspect.isclass):
    if issubclass(entry, BaseProvider) and entry.__module__ == "sqlsynthgen.providers":
        PROVIDER_IMPORTS.append(entry_name)

TEMPLATE_DIRECTORY: Final[Path] = Path(__file__).parent / "templates/"
SSG_TEMPLATE_FILENAME: Final[str] = "ssg.py.j2"


@dataclass
class VocabularyTableGeneratorInfo:
    """Contains the ssg.py content related to vocabulary tables."""

    variable_name: str
    class_name: str
    table_name: str
    dictionary_entry: str


@dataclass
class FunctionCall:
    """Contains the ssg.py content related function calls."""

    function_name: str
    argument_values: list[str]


@dataclass
class RowGeneratorInfo:
    """Contains the ssg.py content related to row generators of a table."""

    variable_names: list[str]
    function_call: FunctionCall
    primary_key: bool = False


@dataclass
class TableGeneratorInfo:
    """Contains the ssg.py content related to regular tables."""

    class_name: str
    table_name: str
    rows_per_pass: int
    row_gens: list[RowGeneratorInfo] = field(default_factory=list)
    unique_constraints: list[UniqueConstraint] = field(default_factory=list)


@dataclass
class StoryGeneratorInfo:
    """Contains the ssg.py content related to story generators."""

    wrapper_name: str
    function_call: FunctionCall
    num_stories_per_pass: int


def _orm_class_from_table_name(
    tables_module: ModuleType, full_name: str
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
    positional_arguments: Optional[Sequence[Any]] = None,
    keyword_arguments: Optional[Mapping[str, Any]] = None,
) -> FunctionCall:
    if positional_arguments is None:
        positional_arguments = []

    if keyword_arguments is None:
        keyword_arguments = {}

    argument_values: list[str] = [str(value) for value in positional_arguments]
    argument_values += [f"{key}={value}" for key, value in keyword_arguments.items()]

    return FunctionCall(function_name=function_name, argument_values=argument_values)


def _get_row_generator(
    table_config: Mapping[str, Any],
) -> tuple[list[RowGeneratorInfo], list[str]]:
    """Get the row generators information, for the given table."""
    row_gen_info: list[RowGeneratorInfo] = []
    config: list[dict[str, Any]] = table_config.get("row_generators", {})
    columns_covered = []
    for gen_conf in config:
        name: str = gen_conf["name"]
        columns_assigned = gen_conf["columns_assigned"]
        keyword_arguments: Mapping[str, Any] = gen_conf.get("kwargs", {})
        positional_arguments: Sequence[str] = gen_conf.get("args", [])

        if isinstance(columns_assigned, str):
            columns_assigned = [columns_assigned]

        variable_names: list[str] = columns_assigned
        try:
            columns_covered += columns_assigned
        except TypeError:
            # Might be a single string, rather than a list of strings.
            columns_covered.append(columns_assigned)

        row_gen_info.append(
            RowGeneratorInfo(
                variable_names=variable_names,
                function_call=_get_function_call(
                    name, positional_arguments, keyword_arguments
                ),
            )
        )
    return row_gen_info, columns_covered


def _get_default_generator(
    tables_module: ModuleType, column: Column
) -> RowGeneratorInfo:
    """Get default generator information, for the given column."""
    # If it's a primary key column, we presume that primary keys are populated
    # automatically.

    # If it's a foreign key column, pull random values from the column it
    # references.
    variable_names: list[str] = []
    generator_function: str = ""
    generator_arguments: list[str] = []

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

    return RowGeneratorInfo(
        primary_key=column.primary_key,
        variable_names=variable_names,
        function_call=_get_function_call(
            function_name=generator_function, positional_arguments=generator_arguments
        ),
    )


def _get_provider_for_column(column: Column) -> Tuple[list[str], str, list[str]]:
    """
    Get a default Mimesis provider and its arguments for a SQL column type.

    Args:
        column: SQLAlchemy column object

    Returns:
        Tuple[str, str, list[str]]: Tuple containing the variable names to assign to,
        generator function and any generator arguments.
    """
    variable_names: list[str] = [column.name]
    generator_arguments: list[str] = []

    column_type = type(column.type)
    column_size: Optional[int] = getattr(column.type, "length", None)

    mapping = {
        (sqltypes.Integer, False): "generic.numeric.integer_number",
        (sqltypes.Boolean, False): "generic.development.boolean",
        (sqltypes.Date, False): "generic.datetime.date",
        (sqltypes.DateTime, False): "generic.datetime.datetime",
        (sqltypes.Numeric, False): "generic.numeric.float_number",
        (sqltypes.LargeBinary, False): "generic.bytes_provider.bytes",
        (sqltypes.Uuid, False): "generic.cryptographic.uuid",
        (postgresql.UUID, False): "generic.cryptographic.uuid",
        (sqltypes.String, False): "generic.text.color",
        (sqltypes.String, True): "generic.person.password",
    }

    generator_function = mapping.get((column_type, column_size is not None), None)

    # Try if we know how to generate for a superclass of this type.
    if not generator_function:
        for key, value in mapping.items():
            if issubclass(column_type, key[0]) and key[1] == (column_size is not None):
                generator_function = value
                break

    # If we still don't have a generator, use null and warn.
    if not generator_function:
        generator_function = "generic.null_provider.null"
        logger.warning(
            "Unsupported SQLAlchemy type %s for column %s. "
            "Setting this column to NULL always, "
            "you may want to configure a row generator for it instead.",
            column_type,
            column.name,
        )
    elif column_size:
        generator_arguments.append(str(column_size))

    return variable_names, generator_function, generator_arguments


def _enforce_unique_constraints(table_data: TableGeneratorInfo) -> None:
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
                logger.warning(msg, constraint.name, row_gen.variable_names)

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


def _constraint_sort_key(constraint: UniqueConstraint) -> str:
    """Extract a string out of a UniqueConstraint that is unique to that constraint.

    We sort the constraints so that the output of make_tables is deterministic, this is
    the sort key.
    """
    return (
        constraint.name
        if isinstance(constraint.name, str)
        else "_".join(map(str, constraint.columns))
    )


def _get_generator_for_table(
    tables_module: ModuleType, table_config: Mapping[str, Any], table: Table
) -> TableGeneratorInfo:
    """Get generator information for the given table."""
    unique_constraints = sorted(
        (
            constraint
            for constraint in table.constraints
            if isinstance(constraint, UniqueConstraint)
        ),
        key=_constraint_sort_key,
    )
    table_data: TableGeneratorInfo = TableGeneratorInfo(
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


def _get_story_generators(config: Mapping) -> list[StoryGeneratorInfo]:
    """Get story generators."""
    generators = []
    for gen in config.get("story_generators", []):
        wrapper_name = "run_" + gen["name"].replace(".", "_").lower()
        generators.append(
            StoryGeneratorInfo(
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


def make_table_generators(  # pylint: disable=too-many-locals
    tables_module: ModuleType,
    config: Mapping,
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
    src_dsn: str = settings.src_dsn or ""
    assert src_dsn != "", "Missing SRC_DSN setting."

    tables_config = config.get("tables", {})
    metadata = get_orm_metadata(tables_module, tables_config)
    engine = get_sync_engine(create_db_engine(src_dsn, schema_name=settings.src_schema))

    tables: list[TableGeneratorInfo] = []
    vocabulary_tables: list[VocabularyTableGeneratorInfo] = []
    for table in metadata.sorted_tables:
        table_config = tables_config.get(table.name, {})

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


def generate_ssg_content(template_context: Mapping[str, Any]) -> str:
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
    table: Table,
    engine: Engine,
    table_file_name: Optional[str] = None,
    overwrite_files: bool = False,
) -> VocabularyTableGeneratorInfo:
    class_and_name: Optional[Tuple[str, str]] = _orm_class_from_table_name(
        tables_module, table.fullname
    )
    if not class_and_name:
        raise RuntimeError(f"Couldn't find {table.fullname} in {tables_module}")

    class_name, table_name = class_and_name

    yaml_file_name: str = table_file_name or table.fullname + ".yaml"
    if Path(yaml_file_name).exists() and not overwrite_files:
        logger.error("%s already exists. Exiting...", yaml_file_name)
        sys.exit(1)
    else:
        logger.debug("Downloading vocabulary table %s", table.name)
        download_table(table, engine, yaml_file_name)
        logger.debug("Done downloading %s", table.name)

    return VocabularyTableGeneratorInfo(
        class_name=class_name,
        dictionary_entry=table.name,
        variable_name=f"{class_name.lower()}_vocab",
        table_name=table_name,
    )


def make_tables_file(
    db_dsn: str, schema_name: Optional[str], config: Mapping[str, Any]
) -> str:
    """Write a file with the SQLAlchemy ORM classes.

    Exits with an error if sqlacodegen is unsuccessful.
    """
    tables_config = config.get("tables", {})
    engine = get_sync_engine(create_db_engine(db_dsn, schema_name=schema_name))

    def reflect_if(table_name: str, _: Any) -> bool:
        table_config = tables_config.get(table_name, {})
        ignore = table_config.get("ignore", False)
        return not ignore

    metadata = MetaData()
    metadata.reflect(
        engine,
        only=reflect_if,
    )

    for table_name in metadata.tables.keys():
        table_config = tables_config.get(table_name, {})
        ignore = table_config.get("ignore", False)
        if ignore:
            logger.warning(
                "Table %s is supposed to be ignored but there is a foreign key "
                "reference to it. "
                "You may need to create this table manually at the dst schema before "
                "running create-tables.",
                table_name,
            )

    generator = DeclarativeGenerator(metadata, engine, options=())
    code = str(generator.generate())

    # sqlacodegen falls back on Tables() for tables without PKs,
    # but we don't explicitly support Tables and behaviour is unpredictable.
    if " = Table(" in code:
        logger.warning(
            "Table without PK detected. sqlsynthgen may not be able to continue.",
        )

    return format_str(code, mode=FileMode())


async def make_src_stats(
    dsn: str, config: Mapping, schema_name: Optional[str] = None
) -> dict[str, list[dict]]:
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
    use_asyncio = config.get("use-asyncio", False)
    engine = create_db_engine(dsn, schema_name=schema_name, use_asyncio=use_asyncio)

    async def execute_query(query_block: Mapping[str, Any]) -> Any:
        """Execute query in query_block."""
        logger.debug("Executing query %s", query_block["name"])
        query = text(query_block["query"])
        if isinstance(engine, AsyncEngine):
            async with engine.connect() as conn:
                raw_result = await conn.execute(query)
        else:
            with engine.connect() as conn:
                raw_result = conn.execute(query)

        if "dp-query" in query_block:
            result_df = pd.DataFrame(raw_result.mappings())
            logger.debug("Executing dp-query for %s", query_block["name"])
            dp_query = query_block["dp-query"]
            snsql_metadata = {"": {"": {"query_result": query_block["snsql-metadata"]}}}
            privacy = snsql.Privacy(
                epsilon=query_block["epsilon"], delta=query_block["delta"]
            )
            reader = snsql.from_df(result_df, privacy=privacy, metadata=snsql_metadata)
            private_result = reader.execute(dp_query)
            header = tuple(str(x) for x in private_result[0])
            final_result = [dict(zip(header, row)) for row in private_result[1:]]
        else:
            final_result = [
                {str(k): v for k, v in row.items()}
                for row in raw_result.mappings().fetchall()
            ]
        return final_result

    query_blocks = config.get("src-stats", [])
    results = await asyncio.gather(
        *[execute_query(query_block) for query_block in query_blocks]
    )
    src_stats = {
        query_block["name"]: result
        for query_block, result in zip(query_blocks, results)
    }

    for name, result in src_stats.items():
        if not result:
            logger.warning("src-stats query %s returned no results", name)
    return src_stats
