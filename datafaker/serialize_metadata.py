"""Convert between a Python dict describing a database schema and a SQLAlchemy MetaData."""
import typing
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import Any

import parsy
import snowflake.sqlalchemy.custom_types as snowflake_types
from sqlalchemy import Column, Dialect, Engine, ForeignKey, MetaData, Table
from sqlalchemy.dialects import mssql, oracle, postgresql
from sqlalchemy.sql import schema, sqltypes

from datafaker.db_utils import constraint_name
from datafaker.utils import (
    T,
    get_property,
    make_foreign_key_name,
    split_column_full_name,
    unqualify_fk_target,
)

TableT = dict[str, typing.Any]


# We will change this to parsy.Parser when parsy exports its types properly
ParserType = typing.Any


def generate(s: str) -> Callable[[T], T]:
    """Return a ty friendly parsy generate decorator."""
    return lambda fn: parsy.generate(fn).desc(s)  # pylint: disable=no-member


def simple(type_: type) -> ParserType:
    """
    Get a parser for a simple sqltypes type.

    For example, simple(sqltypes.UUID) takes the string "UUID" and outputs
    a UUID class, or fails with any other string.
    """
    return parsy.string(type_.__name__).result(type_)


def integer() -> ParserType:
    """Get a parser for an integer, outputting that integer."""
    return parsy.regex(r"-?[0-9]+").map(int)


def integer_arguments() -> ParserType:
    """
    Get a parser for a list of integers.

    The integers are surrounded by brackets and separated by
    a comma and space.
    """
    return (
        parsy.string("(") >> (integer().sep_by(parsy.string(", "))) << parsy.string(")")
    )


def numeric_type(type_: type) -> ParserType:
    """
    Make a parser for a SQL numeric type.

    Parses TYPE_NAME, TYPE_NAME(2) or TYPE_NAME(2,3)
    passing any arguments to the TYPE_NAME constructor.
    """
    return parsy.string(type_.__name__) >> integer_arguments().optional([]).combine(
        type_
    )


def string_type(type_: type) -> ParserType:
    """
    Make a parser for a SQL string type.

    Parses TYPE_NAME, TYPE_NAME(32), TYPE_NAME COLLATE "fr"
    or TYPE_NAME(32) COLLATE "fr" (PostgreSQL style, quoted collation name)
    or TYPE_NAME(32) COLLATE SQL_Latin1_General_CP1_CI_AS
    (MS-SQL style, unquoted collation name)
    """

    @generate(type_.__name__)
    def st_parser() -> typing.Generator[ParserType, None, typing.Any]:
        """Parse the specific type."""
        yield parsy.string(type_.__name__)
        length: int | None = yield (
            parsy.string("(") >> integer() << parsy.string(")")
        ).optional()
        collation: str | None = yield parsy.alt(
            # PostgreSQL: COLLATE "name" (quoted)
            parsy.string(' COLLATE "') >> parsy.regex(r'[^"]*') << parsy.string('"'),
            # MS-SQL: COLLATE name (unquoted identifier)
            parsy.string(" COLLATE ") >> parsy.regex(r"\S+"),
        ).optional()
        return type_(length=length, collation=collation)

    return st_parser


def time_type(type_: type, tz_type: type) -> ParserType:
    """
    Make a parser for a SQL date/time type.

    Parses TYPE_NAME, TYPE_NAME(32), TYPE_NAME WITH TIME ZONE
    or TYPE_NAME(32) WITH TIME ZONE, also WITHOUT

    :param type_: The SQLAlchemy type we would like to parse.
    :param tz_type: The type to instantiate when precision or timezone is
    provided (e.g. ``postgresql.types.TIMESTAMP``).
    :return: ``type_`` if neither precision nor timezone are provided in the
    parsed text, ``tz_type(precision, timezone)`` otherwise.
    """

    @generate(type_.__name__)
    def pgt_parser() -> typing.Generator[ParserType, None, typing.Any]:
        """Parse the actual type."""
        yield parsy.string(type_.__name__)
        precision: int | None = yield (
            parsy.string("(") >> integer() << parsy.string(")")
        ).optional()
        timezone: str | None = yield (
            parsy.string(" WITH")
            >> (parsy.string(" ").result(True) | parsy.string("OUT ").result(False))
            << parsy.string("TIME ZONE")
        ).optional(False)
        if precision is None and not timezone:
            # normal sql type
            return type_
        return tz_type(precision=precision, timezone=timezone)

    return pgt_parser


@parsy.generate("VARBINARY")  # type: ignore
def _mssql_varbinary_parser() -> typing.Generator[ParserType, None, typing.Any]:
    """Parse VARBINARY, VARBINARY(n), or VARBINARY(max/MAX)."""
    yield parsy.string("VARBINARY")
    length: int | None = yield (
        parsy.string("(")
        >> ((parsy.string("max") | parsy.string("MAX")).result(None) | integer())
        << parsy.string(")")
    ).optional()
    return mssql.VARBINARY(length=length)


SIMPLE_TYPE_PARSER = parsy.alt(
    parsy.string("DOUBLE PRECISION").result(
        sqltypes.DOUBLE_PRECISION
    ),  # must be before DOUBLE
    numeric_type(sqltypes.FLOAT),
    simple(sqltypes.DOUBLE),
    simple(sqltypes.INTEGER),
    simple(sqltypes.SMALLINT),
    simple(sqltypes.BIGINT),
    # DATETIME2 and DATETIMEOFFSET must come before DATETIME — parsy.alt() is
    # ordered and does not backtrack once a parser has consumed input, so the
    # longer names must be tried first.
    numeric_type(mssql.DATETIMEOFFSET),
    numeric_type(mssql.DATETIME2),
    simple(sqltypes.DATETIME),
    simple(sqltypes.DATE),
    simple(sqltypes.CLOB),
    simple(oracle.NCLOB),
    simple(sqltypes.UUID),
    simple(sqltypes.BLOB),
    simple(sqltypes.BOOLEAN),
    # PostgreSQL-specific types — mapped to cross-dialect equivalents so that
    # an orm.yaml produced from a PostgreSQL source can be used with MS-SQL.
    # PostgreSQL recreates these correctly; MSSQL gets a functional fallback.
    parsy.string("TSVECTOR").result(
        sqltypes.Text
    ),  # no MS-SQL equivalent; degrade to Text
    parsy.string("BYTEA").result(sqltypes.LargeBinary),  # MS-SQL: VARBINARY(MAX)
    parsy.string("CIDR").result(
        sqltypes.String(43)
    ),  # no MS-SQL equivalent; store as VARCHAR(43)
    # PostgreSQL SERIAL pseudo-types — map to plain integers.  datafaker does
    # not rely on server-side autoincrement; the @compiles hook in dialects.py
    # strips IDENTITY from MS-SQL DDL so explicit INSERTs work without
    # SET IDENTITY_INSERT.  BIGSERIAL/SMALLSERIAL listed before SERIAL so
    # the common "SERIAL" prefix is tried last (defensive ordering).
    parsy.string("BIGSERIAL").result(sqltypes.BIGINT),
    parsy.string("SMALLSERIAL").result(sqltypes.SMALLINT),
    parsy.string("SERIAL").result(sqltypes.INTEGER),
    numeric_type(sqltypes.NUMERIC),
    numeric_type(sqltypes.DECIMAL),
    numeric_type(postgresql.BIT),
    numeric_type(sqltypes.REAL),  # was postgresql.REAL; sqltypes.REAL is cross-dialect
    # MS-SQL-specific types
    simple(mssql.UNIQUEIDENTIFIER),
    _mssql_varbinary_parser,
    numeric_type(mssql.BINARY),
    simple(mssql.MONEY),
    simple(mssql.SMALLMONEY),
    simple(mssql.IMAGE),
    simple(mssql.TINYINT),
    simple(mssql.SMALLDATETIME),
    simple(mssql.NTEXT),
    simple(mssql.SQL_VARIANT),
    simple(mssql.ROWVERSION),
    string_type(sqltypes.CHAR),
    string_type(sqltypes.NCHAR),
    string_type(sqltypes.VARCHAR),
    string_type(sqltypes.NVARCHAR),
    string_type(sqltypes.TEXT),
    parsy.string("TIMESTAMP_NTZ").result(sqltypes.DATETIME),
    parsy.string("TIMESTAMPNTZ").result(sqltypes.DATETIME),
    parsy.string("TIMESTAMP_LTZ").result(snowflake_types.TIMESTAMP_LTZ),
    parsy.string("TIMESTAMPLTZ").result(snowflake_types.TIMESTAMP_LTZ),
    parsy.string("TIMESTAMP WITH LOCAL TIME ZONE").result(
        snowflake_types.TIMESTAMP_LTZ
    ),
    parsy.string("TIMESTAMP_TZ").result(lambda: sqltypes.DATETIME(timezone=True)),
    parsy.string("TIMESTAMPTZ").result(lambda: sqltypes.DATETIME(timezone=True)),
    time_type(sqltypes.TIMESTAMP, postgresql.types.TIMESTAMP),
    time_type(sqltypes.TIME, postgresql.types.TIME),
)


@parsy.generate
def type_parser() -> ParserType:
    """
    Make a parser for a simple type or an array.

    Arrays produce a PostgreSQL-specific type.
    """
    base = yield SIMPLE_TYPE_PARSER
    dimensions = yield parsy.string("[]").many().map(len)
    if dimensions == 0:
        return base
    return postgresql.ARRAY(base, dimensions=dimensions)


def column_to_dict(column: Column, dialect: Dialect) -> dict[str, typing.Any]:
    """
    Produce a dict description of a column.

    :param column: The SQLAlchemy column to translate.
    :param dialect: The SQL dialect in which to render the type name.
    """
    type_ = column.type
    if isinstance(type_, postgresql.DOMAIN):
        # Instead of creating a restricted type, we'll just use the base type.
        # It might be better to use the actual type if we could find a good way
        # to compile it and also parse the compiled string.
        type_ = type_.data_type
    if isinstance(type_, postgresql.ENUM):
        compiled = "TEXT"
    else:
        compiled = dialect.type_compiler_instance.process(type_)
    result: dict[str, Any] = {
        "type": compiled,
        "primary": column.primary_key,
        "nullable": column.nullable,
    }
    foreign_keys = [fk_target_string(fk) for fk in column.foreign_keys]
    if foreign_keys:
        result["foreign_keys"] = foreign_keys
    return result


def fk_target_string(fk: ForeignKey) -> str:
    """
    Get the name of the foreign key target.

    The schema is not included, only table and column.
    """
    c = fk.column
    return f"{c.table.name}.{c.name}"


def dict_to_column(
    table_name: str,
    col_name: str,
    rep: dict,
    ignore_fk: typing.Callable[[str], bool],
    table_names: typing.Optional[frozenset] = None,
) -> Column:
    """
    Produce column from aspects of its dict description.

    :param table_name: The name of the table the column appears in.
    :param col_name: The name of the column.
    :param rep: The dict description of the column.
    :ignore_fk: A predicate, called with the name of any foreign key target
    (in other words, the name of any table referred to by this column). If it
    returns True, this foreign key constraint will not be applied to the
    returned column. This is useful in a situation where we want a foreign
    key constraint to be present when we are determining what generators
    might be appropriate for it, but we don't want the foreign key constraint
    actually applied to the destination database because (for example) the
    target table will be ignored.
    """
    type_sql = rep["type"]
    try:
        type_ = type_parser.parse(type_sql)
    except parsy.ParseError as e:
        print(f"Failed to parse {type_sql}")
        raise e
    if "foreign_keys" in rep:
        args = [
            ForeignKey(
                unqualify_fk_target(fk, table_names),
                name=make_foreign_key_name(table_name, col_name),
                ondelete="CASCADE",
            )
            for fk in rep["foreign_keys"]
            if not ignore_fk(fk)
        ]
    else:
        args = []
    return Column(
        *args,
        name=col_name,
        type_=type_,
        primary_key=rep.get("primary", False),
        nullable=rep.get("nullable", None),
        autoincrement=False,
    )


def dict_to_unique(rep: dict) -> schema.UniqueConstraint:
    """Make a uniqueness constraint from its dict representation."""
    return schema.UniqueConstraint(*rep.get("columns", []), name=rep.get("name", None))


def unique_to_dict(constraint: schema.UniqueConstraint) -> dict:
    """Render a dict representation of a uniqueness constraint."""
    return {
        "name": constraint_name(constraint),
        "columns": [str(col.name) for col in constraint.columns],
    }


def table_to_dict(table: Table, dialect: Dialect) -> TableT:
    """Convert a SQL Alchemy Table object into a Python dict."""
    return {
        "columns": {
            str(column.key): column_to_dict(column, dialect)
            for column in table.columns.values()
        },
        "unique": [
            unique_to_dict(constraint)
            for constraint in table.constraints
            if isinstance(constraint, schema.UniqueConstraint)
        ],
    }


def dict_to_table(
    name: str,
    meta: MetaData,
    table_dict: TableT,
    ignore_fk: typing.Callable[[str], bool],
    table_names: typing.Optional[frozenset] = None,
) -> Table:
    """Create a Table from its description."""
    return Table(
        name,
        meta,
        *[
            dict_to_column(name, colname, col, ignore_fk, table_names)
            for (colname, col) in table_dict.get("columns", {}).items()
        ],
        *[dict_to_unique(constraint) for constraint in table_dict.get("unique", [])],
    )


def metadata_to_dict(
    meta: MetaData,
    schema_name: str | None,
    engine: Engine,
    parquet_dir: Path | None,
) -> dict[str, typing.Any]:
    """
    Convert a metadata object into a Python dict.

    The output will be ready for output to ``orm.yaml``.
    """
    d = {
        "tables": {
            str(table.name): table_to_dict(table, engine.dialect)
            for table in meta.tables.values()
        },
        "dsn": str(engine.url),
        "schema": schema_name,
    }
    if parquet_dir is not None:
        d["parquet-dir"] = str(parquet_dir)
    return d


def should_ignore_fk(tables_dict: dict[str, TableT], fk: str) -> bool:
    """
    Test if this foreign key points to an ignored table.

    If so, this foreign key should be ignored.
    :param tables_dict: The ``tables`` value from ``config.yaml``.
    :param fk: The name of the foreign key.
    """
    (table, _column) = split_column_full_name(fk)
    # FK targets may be schema-qualified (e.g. "mimic100.concept").
    # Try the fully-qualified name first so users can be explicit in config
    # (e.g. "mimic100.concept: ignore: true"); fall back to the bare table
    # name for configs that don't include a schema prefix.
    td = tables_dict.get(table)
    if td is None:
        bare = table.rsplit(".", maxsplit=1)[-1]
        td = tables_dict.get(bare, {})
    return get_property(td, "ignore", False)


def _always_false(_: str) -> bool:
    return False


def dict_to_metadata(
    obj: dict, config_for_output: Mapping[Any, Any] | None = None
) -> MetaData:
    """
    Convert a dict to a SQL Alchemy MetaData object.

    :param config_for_output: The configuration object. Should be None if
    the metadata object is being used for connecting to the source database.
    If it is being used for connecting to the destination database this
    configuration will be used to make sure that there is no foreign key
    constraint to an ignored table.
    """
    tables_dict = obj.get("tables", {})
    ignore_fk: typing.Callable[[str], bool]
    if config_for_output and "tables" in config_for_output:
        tables_config = config_for_output["tables"]
        ignore_fk = partial(should_ignore_fk, tables_config)
    else:
        ignore_fk = _always_false
    table_names = frozenset(tables_dict.keys())
    meta = MetaData()
    for k, td in tables_dict.items():
        dict_to_table(k, meta, td, ignore_fk, table_names)
    return meta
