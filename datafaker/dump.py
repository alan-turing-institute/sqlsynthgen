"""Data dumping functions."""
import csv
import io
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import sqlalchemy
from sqlalchemy.schema import MetaData

from datafaker.db_utils import create_db_engine, get_sync_engine
from datafaker.utils import logger


class TableWriter(ABC):
    """Writes a table out to a file."""

    EXTENSION = ".csv"

    def __init__(self, metadata: MetaData, dsn: str, schema: str | None) -> None:
        """
        Initialize the TableWriter.

        :param metadata: The metadata for our database.
        :param dsn: The connection string for our database.
        :param schema: The schema name for our database, or None for the default.
        """
        self._metadata = metadata
        self._dsn = dsn
        self._schema = schema

    def connect(self) -> sqlalchemy.engine.Connection:
        """Connect to the database."""
        engine = get_sync_engine(create_db_engine(self._dsn, schema_name=self._schema))
        return engine.connect()

    @abstractmethod
    def write_file(self, table: sqlalchemy.Table, filepath: Path) -> bool:
        """
        Write the named table into the named file.

        :param table: The table to output
        :param dir: The directory to write into.
        :return: ``true`` on success, otherwise ``false``.
        """

    def write(self, table: sqlalchemy.Table, directory: Path) -> bool:
        """
        Write the table into a directory with a filename based on the table's name.

        :param table: The table to write out.
        :param directory: The directory to write the table into.
        :return: ``true`` on success, otherwise ``false``.
        """
        tn = table.name
        # DuckDB tables derived from files have confusing suffixes
        # that we should probably remove
        tn = tn.removesuffix(".csv")
        tn = tn.removesuffix(".parquet")
        return self.write_file(table, directory / f"{tn}{self.EXTENSION}")


class ParquetTableWriter(TableWriter):
    """Writes the table to a Parquet file."""

    EXTENSION = ".parquet"

    def write_file(self, table: sqlalchemy.Table, filepath: Path) -> bool:
        """
        Write the named table into the named file.

        :param table: The table to output
        :param filename: The filename of the file to write to.
        :return: ``true`` on success, otherwise ``false``.
        """
        with self.connect() as connection:
            dates = [
                str(name)
                for name, col in table.columns.items()
                if isinstance(
                    col.type,
                    (
                        sqlalchemy.types.DATE,
                        sqlalchemy.types.DATETIME,
                        sqlalchemy.types.TIMESTAMP,
                    ),
                )
            ]
            df = pd.read_sql(
                sql=f"SELECT * FROM {table.name}",
                con=connection,
                columns=[str(col.name) for col in table.columns.values()],
                parse_dates=dates,
            )
            df.to_parquet(filepath)
        return True


class DuckDbParquetTableWriter(ParquetTableWriter):
    """
    Writes the table to a Parquet file using DuckDB SQL.

    The Pandas method used by ParquetTableWriter currently
    does not work with DuckDB.
    """

    def write_file(self, table: sqlalchemy.Table, filepath: Path) -> bool:
        """
        Write the named table into the named file.

        :param table: The table to output
        :param filename: The filename of the file to write to.
        :return: ``true`` on success, otherwise ``false``.
        """
        with self.connect() as connection:
            result = connection.execute(
                sqlalchemy.text(
                    # We need the double quotes to get DuckDB to read the table not the file.
                    f"COPY \"{table.name}\" TO '{filepath}' (FORMAT PARQUET)"
                )
            )
            return result is not None


def get_parquet_table_writer(
    metadata: MetaData, dsn: str, schema: str | None
) -> TableWriter:
    """
    Get a ``TableWriter`` that writes parquet files.

    :param metadata: The database metadata containing the tables to be dumped to files.
    :param dsn: The database connection string.
    :param schema: The schema name, if required.
    :return: ``TableWriter`` to write a parquet file.
    """
    if dsn.startswith("duckdb:"):
        return DuckDbParquetTableWriter(metadata, dsn, schema)
    return ParquetTableWriter(metadata, dsn, schema)


class TableWriterIO(TableWriter):
    """Writes the table to an output object."""

    @abstractmethod
    def write_io(self, table: sqlalchemy.Table, out: io.TextIOBase) -> bool:
        """
        Write the named table into the named file.

        :param table: The table to output
        :param filename: The filename of the file to write to.
        :return: ``true`` on success, otherwise ``false``.
        """

    def write_file(self, table: sqlalchemy.Table, filepath: Path) -> bool:
        """
        Write the named table into the named file.

        :param table: The table to output
        :param filename: The filename of the file to write to.
        :return: ``true`` on success, otherwise ``false``.
        """
        with open(filepath, "wt", newline="", encoding="utf-8") as out:
            return self.write_io(table, out)


class CsvTableWriter(TableWriterIO):
    """Writes the table to a CSV file."""

    def write_io(self, table: sqlalchemy.Table, out: io.TextIOBase) -> bool:
        """
        Write the named table into the named file.

        :param table: The table to output
        :param filename: The filename of the file to write to.
        :return: ``True`` on success, otherwise ``False``.
        """
        if table.name not in self._metadata.tables:
            logger.error("%s is not a table described in the ORM file", table.name)
            return False
        table = self._metadata.tables[table.name]
        csv_out = csv.writer(out, quoting=csv.QUOTE_MINIMAL)
        csv_out.writerow(table.columns.keys())
        with self.connect() as connection:
            result = connection.execute(sqlalchemy.select(table))
            for row in result:
                csv_out.writerow(row)
        return True
