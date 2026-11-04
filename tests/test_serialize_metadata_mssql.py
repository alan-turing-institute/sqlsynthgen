"""Tests for MS-SQL type support in datafaker.serialize_metadata."""
# pylint: disable=missing-function-docstring
import unittest

from sqlalchemy.dialects import mssql, postgresql
from sqlalchemy.sql import sqltypes

from datafaker.serialize_metadata import dict_to_metadata, type_parser
from datafaker.utils import unqualify_fk_target


def parse(type_str: str):
    """Shorthand: parse a type string and return the resulting SQLAlchemy type."""
    return type_parser.parse(type_str)


class TestMSSQLTypeParser(unittest.TestCase):
    """New MS-SQL-specific type strings are parsed correctly."""

    def test_uniqueidentifier(self) -> None:
        result = parse("UNIQUEIDENTIFIER")
        self.assertIs(result, mssql.UNIQUEIDENTIFIER)

    def test_datetimeoffset_bare(self) -> None:
        result = parse("DATETIMEOFFSET")
        self.assertIsInstance(result, mssql.DATETIMEOFFSET)

    def test_datetimeoffset_with_precision(self) -> None:
        result = parse("DATETIMEOFFSET(7)")
        self.assertIsInstance(result, mssql.DATETIMEOFFSET)
        self.assertEqual(result.precision, 7)

    def test_datetime2_bare(self) -> None:
        result = parse("DATETIME2")
        self.assertIsInstance(result, mssql.DATETIME2)

    def test_datetime2_with_precision(self) -> None:
        result = parse("DATETIME2(3)")
        self.assertIsInstance(result, mssql.DATETIME2)
        self.assertEqual(result.precision, 3)

    def test_varbinary_bare(self) -> None:
        result = parse("VARBINARY")
        self.assertIsInstance(result, mssql.VARBINARY)

    def test_varbinary_with_length(self) -> None:
        result = parse("VARBINARY(8000)")
        self.assertIsInstance(result, mssql.VARBINARY)
        self.assertEqual(result.length, 8000)

    def test_varbinary_max_lowercase(self) -> None:
        result = parse("VARBINARY(max)")
        self.assertIsInstance(result, mssql.VARBINARY)
        self.assertIsNone(result.length)

    def test_varbinary_max_uppercase(self) -> None:
        result = parse("VARBINARY(MAX)")
        self.assertIsInstance(result, mssql.VARBINARY)
        self.assertIsNone(result.length)

    def test_binary_bare(self) -> None:
        result = parse("BINARY")
        self.assertIsInstance(result, mssql.BINARY)

    def test_binary_with_length(self) -> None:
        result = parse("BINARY(16)")
        self.assertIsInstance(result, mssql.BINARY)
        self.assertEqual(result.length, 16)

    def test_money(self) -> None:
        self.assertIs(parse("MONEY"), mssql.MONEY)

    def test_smallmoney(self) -> None:
        self.assertIs(parse("SMALLMONEY"), mssql.SMALLMONEY)

    def test_image(self) -> None:
        self.assertIs(parse("IMAGE"), mssql.IMAGE)

    def test_tinyint(self) -> None:
        self.assertIs(parse("TINYINT"), mssql.TINYINT)

    def test_smalldatetime(self) -> None:
        self.assertIs(parse("SMALLDATETIME"), mssql.SMALLDATETIME)

    def test_ntext(self) -> None:
        self.assertIs(parse("NTEXT"), mssql.NTEXT)

    def test_sql_variant(self) -> None:
        self.assertIs(parse("SQL_VARIANT"), mssql.SQL_VARIANT)

    def test_rowversion(self) -> None:
        self.assertIs(parse("ROWVERSION"), mssql.ROWVERSION)


class TestPostgreSQLTypeDegradation(unittest.TestCase):
    """PostgreSQL-specific type strings degrade to cross-dialect equivalents."""

    def test_tsvector_maps_to_text(self) -> None:
        result = parse("TSVECTOR")
        self.assertIs(result, sqltypes.Text)

    def test_bytea_maps_to_largebinary(self) -> None:
        result = parse("BYTEA")
        self.assertIs(result, sqltypes.LargeBinary)

    def test_cidr_maps_to_string_43(self) -> None:
        result = parse("CIDR")
        self.assertIsInstance(result, sqltypes.String)
        self.assertEqual(result.length, 43)

    def test_serial_maps_to_integer(self) -> None:
        self.assertIs(parse("SERIAL"), sqltypes.INTEGER)

    def test_bigserial_maps_to_bigint(self) -> None:
        self.assertIs(parse("BIGSERIAL"), sqltypes.BIGINT)

    def test_smallserial_maps_to_smallint(self) -> None:
        self.assertIs(parse("SMALLSERIAL"), sqltypes.SMALLINT)


class TestExistingPostgreSQLTypesRoundTrip(unittest.TestCase):  # pylint: disable=R0904
    """Pre-existing PostgreSQL type strings still parse correctly (regression tests)."""

    def test_integer(self) -> None:
        self.assertIs(parse("INTEGER"), sqltypes.INTEGER)

    def test_bigint(self) -> None:
        self.assertIs(parse("BIGINT"), sqltypes.BIGINT)

    def test_smallint(self) -> None:
        self.assertIs(parse("SMALLINT"), sqltypes.SMALLINT)

    def test_boolean(self) -> None:
        self.assertIs(parse("BOOLEAN"), sqltypes.BOOLEAN)

    def test_float(self) -> None:
        self.assertIsInstance(parse("FLOAT"), sqltypes.FLOAT)

    def test_double_precision(self) -> None:
        self.assertIs(parse("DOUBLE PRECISION"), sqltypes.DOUBLE_PRECISION)

    def test_numeric_bare(self) -> None:
        result = parse("NUMERIC")
        self.assertIsInstance(result, sqltypes.NUMERIC)

    def test_numeric_with_args(self) -> None:
        result = parse("NUMERIC(10, 2)")
        self.assertIsInstance(result, sqltypes.NUMERIC)
        self.assertEqual(result.precision, 10)
        self.assertEqual(result.scale, 2)

    def test_varchar(self) -> None:
        result = parse("VARCHAR(255)")
        self.assertIsInstance(result, sqltypes.VARCHAR)
        self.assertEqual(result.length, 255)

    def test_varchar_with_mssql_collation(self) -> None:
        result = parse("VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS")
        self.assertIsInstance(result, sqltypes.VARCHAR)
        self.assertEqual(result.length, 50)
        self.assertEqual(result.collation, "SQL_Latin1_General_CP1_CI_AS")

    def test_nvarchar_with_mssql_collation(self) -> None:
        result = parse("NVARCHAR(100) COLLATE Latin1_General_CI_AS")
        self.assertIsInstance(result, sqltypes.NVARCHAR)
        self.assertEqual(result.length, 100)
        self.assertEqual(result.collation, "Latin1_General_CI_AS")

    def test_char_with_mssql_collation(self) -> None:
        result = parse("CHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS")
        self.assertIsInstance(result, sqltypes.CHAR)
        self.assertEqual(result.collation, "SQL_Latin1_General_CP1_CI_AS")

    def test_varchar_with_quoted_collation_still_works(self) -> None:
        result = parse('VARCHAR(255) COLLATE "fr"')
        self.assertIsInstance(result, sqltypes.VARCHAR)
        self.assertEqual(result.collation, "fr")

    def test_nvarchar(self) -> None:
        result = parse("NVARCHAR(100)")
        self.assertIsInstance(result, sqltypes.NVARCHAR)
        self.assertEqual(result.length, 100)

    def test_text(self) -> None:
        result = parse("TEXT")
        self.assertIsInstance(result, sqltypes.TEXT)

    def test_uuid(self) -> None:
        self.assertIs(parse("UUID"), sqltypes.UUID)

    def test_date(self) -> None:
        self.assertIs(parse("DATE"), sqltypes.DATE)

    def test_datetime(self) -> None:
        self.assertIs(parse("DATETIME"), sqltypes.DATETIME)

    def test_timestamp_bare(self) -> None:
        self.assertIs(parse("TIMESTAMP"), sqltypes.TIMESTAMP)

    def test_timestamp_with_timezone(self) -> None:
        result = parse("TIMESTAMP WITH TIME ZONE")
        self.assertIsInstance(result, postgresql.types.TIMESTAMP)
        self.assertTrue(result.timezone)

    def test_timestamp_with_precision_and_timezone(self) -> None:
        result = parse("TIMESTAMP(6) WITH TIME ZONE")
        self.assertIsInstance(result, postgresql.types.TIMESTAMP)
        self.assertEqual(result.precision, 6)
        self.assertTrue(result.timezone)

    def test_timestamp_without_timezone(self) -> None:
        # WITHOUT TIME ZONE means timezone=False; the parser returns the plain
        # sqltypes.TIMESTAMP class (not a pg-specific instance) in this case.
        result = parse("TIMESTAMP WITHOUT TIME ZONE")
        self.assertIs(result, sqltypes.TIMESTAMP)

    def test_time_bare(self) -> None:
        self.assertIs(parse("TIME"), sqltypes.TIME)

    def test_time_with_timezone(self) -> None:
        result = parse("TIME WITH TIME ZONE")
        self.assertIsInstance(result, postgresql.types.TIME)
        self.assertTrue(result.timezone)

    def test_bit_bare(self) -> None:
        result = parse("BIT")
        self.assertIsInstance(result, postgresql.BIT)

    def test_bit_with_length(self) -> None:
        result = parse("BIT(8)")
        self.assertIsInstance(result, postgresql.BIT)
        self.assertEqual(result.length, 8)

    def test_real_bare(self) -> None:
        result = parse("REAL")
        self.assertIsInstance(result, sqltypes.REAL)

    def test_blob(self) -> None:
        self.assertIs(parse("BLOB"), sqltypes.BLOB)

    def test_clob(self) -> None:
        self.assertIs(parse("CLOB"), sqltypes.CLOB)


class TestArrayType(unittest.TestCase):
    """Array types (PostgreSQL-specific) still parse correctly."""

    def test_integer_array(self) -> None:
        result = parse("INTEGER[]")
        self.assertIsInstance(result, postgresql.ARRAY)
        self.assertEqual(result.dimensions, 1)

    def test_text_array(self) -> None:
        result = parse("TEXT[]")
        self.assertIsInstance(result, postgresql.ARRAY)

    def test_multidimensional_array(self) -> None:
        result = parse("INTEGER[][]")
        self.assertIsInstance(result, postgresql.ARRAY)
        self.assertEqual(result.dimensions, 2)


class TestUnqualifyFkTarget(unittest.TestCase):
    """Schema prefix is stripped from 3-part FK targets."""

    def test_three_part_target_drops_schema(self) -> None:
        self.assertEqual(
            unqualify_fk_target("mimic100.concept.concept_id"), "concept.concept_id"
        )

    def test_two_part_target_unchanged(self) -> None:
        self.assertEqual(
            unqualify_fk_target("concept.concept_id"), "concept.concept_id"
        )

    def test_single_part_unchanged(self) -> None:
        self.assertEqual(unqualify_fk_target("concept_id"), "concept_id")


class TestSchemaQualifiedFKResolution(unittest.TestCase):
    """Schema-qualified FK targets resolve correctly when building MetaData."""

    def test_schema_qualified_fk_resolves_in_metadata(self) -> None:
        """Test that foreign keys resolve correctly in generated MetaData."""
        orm_dict = {
            "tables": {
                "concept": {
                    "columns": {
                        "concept_id": {
                            "type": "BIGINT",
                            "primary": True,
                            "nullable": False,
                        },
                    }
                },
                "person": {
                    "columns": {
                        "person_id": {
                            "type": "BIGINT",
                            "primary": True,
                            "nullable": False,
                        },
                        "gender_concept_id": {
                            "type": "BIGINT",
                            "primary": False,
                            "nullable": False,
                            "foreign_keys": ["myschema.concept.concept_id"],
                        },
                    }
                },
            }
        }
        meta = dict_to_metadata(orm_dict)
        person = meta.tables["person"]
        fks = list(person.c.gender_concept_id.foreign_keys)
        self.assertEqual(len(fks), 1)
        # FK target should resolve to the concept table without raising NoReferencedTableError
        self.assertEqual(fks[0].column.table.name, "concept")
