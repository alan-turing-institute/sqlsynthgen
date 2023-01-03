"""Functions to create a module of generator classes."""
import importlib
from typing import Final

from sqlalchemy.sql import sqltypes

HEADER_TEXT: Final[str] = "\n".join(
    (
        '"""This file was auto-generated by sqlsynthgen but can be edited manually."""',
        "from mimesis import Generic",
        "from mimesis.locales import Locale",
        "from sqlsynthgen.providers import BinaryProvider, ForeignKeyProvider",
        "",
        "generic = Generic(locale=Locale.EN)",
        "generic.add_provider(ForeignKeyProvider)",
        "generic.add_provider(BinaryProvider)",
        "",
    )
)

INDENTATION: Final[str] = " " * 4


def create_generators_from_tables(tables_module_name: str) -> str:
    """Creates sqlsynthgen generator classes from a sqlacodegen-generated file.

    Args:
      tables_module_name: The name of a sqlacodegen-generated module
        as you would provide to importlib.import_module.

    Returns:
      A string that is a valid Python module, once written to file.
    """

    new_content = HEADER_TEXT

    sorted_generators = "[\n"

    sql_to_mimesis_map = {
        sqltypes.BigInteger: "generic.numeric.integer_number()",
        sqltypes.Boolean: "generic.development.boolean()",
        sqltypes.DateTime: "generic.datetime.datetime()",
        sqltypes.Date: "generic.datetime.date()",
        sqltypes.Integer: "generic.numeric.integer_number()",
        sqltypes.Text: "generic.text.color()",
        sqltypes.Float: "generic.numeric.float_number()",
        sqltypes.LargeBinary: "generic.binary_provider.bytes()",
    }

    tables_module = importlib.import_module(tables_module_name)
    for table in tables_module.metadata.sorted_tables:
        new_class_name = table.name + "Generator"
        sorted_generators += INDENTATION + new_class_name + ",\n"
        new_content += (
            "\n\nclass "
            + new_class_name
            + ":\n"
            + INDENTATION
            + "def __init__(self, db_connection):\n"
        )

        for column in table.columns:
            # We presume that primary keys are populated automatically
            if column.primary_key:
                continue

            if column.foreign_keys:
                if len(column.foreign_keys) > 1:
                    raise NotImplementedError("Can't handle multiple foreign keys.")
                fkey = column.foreign_keys.pop()
                fk_column_path = fkey.target_fullname
                fk_schema, fk_table, fk_column = fk_column_path.split(".")
                new_content += (
                    f"{INDENTATION*2}self.{column.name} = "
                    f"generic.foreign_key_provider.key(db_connection, "
                    f'"{fk_schema}", "{fk_table}", "{fk_column}"'
                    ")\n"
                )
            else:

                new_content += (
                    INDENTATION * 2
                    + "self."
                    + column.name
                    + " = "
                    + sql_to_mimesis_map[type(column.type)]
                    + "\n"
                )

    sorted_generators += "]"

    new_content += "\n\n" + "sorted_generators = " + sorted_generators + "\n"

    return new_content
