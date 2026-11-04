"""Add to ORM structure based on parquet files."""
import glob
import re
from abc import ABC
from collections.abc import Generator, Mapping
from pathlib import Path
from typing import Any

import numpy as np
from fastparquet import ParquetException, ParquetFile

from datafaker.utils import get_property, logger

PARQUET_SUFFIXES = {".parquet", ".parq"}

# The dtype recorded for a Hive partition key (such as the ``yr`` in ``yr=2023``).
# Partition keys are not stored in the files themselves.
PARTITION_KEY_DTYPE = "category"

KEY_TYPES = frozenset(["INTEGER", "UUID"])


class ParquetTable(ABC):
    """A table found in a parquet file or directory of parquet files."""

    def __init__(self, pf: ParquetFile, name: str) -> None:
        """Initialize a ParquetTable."""
        self.parquet_file = pf
        self.name = name

    def hive_files(self) -> list[Path]:
        """Get any files needing to be merged into a view."""
        return []


class ParquetHiveTable(ParquetTable):
    """A hive partitioned table formed from a directory."""

    def __init__(self, pf: ParquetFile, name: str, file_list: list[Path]):
        """Initialize a ParquetHiveTable."""
        super().__init__(pf, name)
        self.file_list = file_list

    def hive_files(self) -> list[Path]:
        """Return the list of files that make up this hive."""
        return self.file_list


def find_parquet_tables(directory: Path | str) -> Generator[ParquetTable, None, None]:
    """
    Find the tables represented by the parquet files in a directory.

    * A parquet file directly in ``directory`` is a table named after the file.
    * A subdirectory of ``directory`` containing parquet files at any depth
      is a single (partitioned) table named after the subdirectory.
    * If ``directory`` has no parquet files directly in it, and all the
      subdirectories containing parquet files are Hive partitions
      (named ``key=value``), then ``directory`` is itself a single table.

    :param directory: The directory to search for parquet files in.
    :return: The ParquetFiles found, sorted by name.
    """
    directory = Path(directory)
    for child in sorted(directory.iterdir()):
        try:
            if child.is_dir():
                files = glob.glob("**/*.parquet", root_dir=child, recursive=True)
                abs_files = [child / f for f in files]
                pf = ParquetFile(abs_files, root=child, verify=True)
                yield ParquetHiveTable(pf, child.name, abs_files)
            else:
                pf = ParquetFile(child, verify=True)
                yield ParquetTable(pf, child.name)
        except ParquetException as e:
            logger.error("Entry %s cannot be read as parquet", child.name)
            logger.warning(e)
        except ValueError as e:
            logger.error("Entry %s cannot be read as parquet", child.name)
            logger.warning(e)
        except IndexError as _e:
            logger.warning("Entry %s is empty", child.name)


def add_guessed_keys(tables_conf: dict[str, Any], table_names: set[str]):
    """
    Read the orm.yaml config "tables" portion, adding guesses for foreign keys and primary keys.

    :param tables_conf: On input, the tables portion of the configuration file; this dictionary will
      be changed to set the appropriate primary and foreign keys.
    :param table_names: The names of the tables to guess keys for.
    """
    guesser = _ColumnGuesser(tables_conf)
    for table_name in table_names:
        table_conf = tables_conf.get(table_name, {})
        tables_conf[table_name] = table_conf
        columns = get_property(table_conf, "columns", {})
        table_conf["columns"] = columns
        primaries = set()
        for column_name, column_conf in columns.items():
            ctype = get_property(column_conf, "type", "INTEGER")
            if ctype in KEY_TYPES:
                if guesser.is_likely_primary(table_name, column_name):
                    primaries.add(column_name)
                    column_conf["primary"] = True
                    column_conf["nullable"] = False
                elif (
                    guess := guesser.get_likely_foreign_key_target(column_name)
                ) and guess[0] != table_name:
                    column_conf["foreign_keys"] = [f"{guess[0]}.{guess[1]}"]
            if column_name == "id":
                column_conf["primary"] = True
                column_conf["nullable"] = False
        if len(primaries) == 0:
            logger.warning(
                "Found no likely primary keys for table %s",
                table_name,
            )
        elif 1 < len(primaries):
            logger.warning(
                "Found multiple likely primary keys for table %s: %s",
                table_name,
                ", ".join(sorted(list(primaries))),
            )


def get_initial_parquet_orm(directory: Path) -> dict[str, Any] | None:
    """
    Read the parquet files, guess a database structure from that.

    A directory of parquet files (possibly partitioned) is read as one table.

    :param directory: The directory to search for parquet files in.
    :return: The ORM dictionary on success, None on failure.
    :raises typer.Exit: If a directory holds files with differing schemas.
    """
    logger.debug("Examining directory %s", directory)
    if not directory.is_dir():
        logger.error("%s is not a directory", directory)
        return None
    parquets = find_parquet_tables(directory)
    return {p.name: _get_table_orm(p.parquet_file) for p in parquets}


_camel_case_re = re.compile(r"([a-z])([A-Z])")
_word_split_re = re.compile(r"[^A-Za-z0-9]+")


def _get_words(s: str) -> set[str]:
    """Get the words of a string."""
    decamel = _camel_case_re.sub(lambda m: f"{m.group(1)} {m.group(2)}", s)
    return {w.lower() for w in _word_split_re.split(decamel)}


class _ColumnGuesser:
    """Guesses foreign key targets."""

    def __init__(self, tables: Mapping[str, Any]) -> None:
        """
        Initialize the column guesser from table names to column names.

        :param tables: The "tables" key from the orm.yaml structure.
        """
        # List of triples (table_name, column_name, list_of_words_in_table_and_column_name)
        self.tn_cn_words: list[tuple[str, str, set[str]]] = []
        # Dict from table name to the words in that table name
        self.tn2words: dict[str, set[str]] = {}
        for name, table_conf in tables.items():
            table_words = _get_words(name)
            self.tn2words[name] = table_words
            columns = get_property(table_conf, "columns", {})
            for column in columns.keys():
                self.tn_cn_words.append(
                    (name, column, table_words | _get_words(column))
                )

    def get_likely_foreign_key_target(
        self,
        column_name: str,
    ) -> tuple[str, str] | None:
        """
        Get a foreign key target string if the name suggests it might be.

        :param column_name: The name of the column which might be a foreign key
        :return: A pair (table_name, column_name) or None if no column seems
          like a likely target.
        """
        our_words = _get_words(column_name)
        max_overlaps_so_far = (1, len(our_words) - 1, 0)
        best_so_far: tuple[str, str] | None = None
        for tn, cn, cws in self.tn_cn_words:
            # A triple: (overlap of column name with table name,
            # overlap of column name with column and table name,
            # negative how many words are left over)
            goodness = (
                len(our_words.intersection(self.tn2words[tn])),
                len(our_words.intersection(cws)),
                -len(cws.difference(our_words)),
            )
            # More overlap with table name is better.
            # Same overlap with table name but more overlap with column name is also better.
            if max_overlaps_so_far < goodness:
                max_overlaps_so_far = goodness
                best_so_far = (tn, cn)
        return best_so_far

    def is_likely_primary(self, table_name, column_name) -> bool:
        """Determine if the specified column name looks like a primary key."""
        words = _get_words(column_name) - {"id", "key"}
        return words <= self.tn2words[table_name]


def _get_table_orm(parquet_file: ParquetFile) -> dict[str, Any]:
    """
    Guess the ORM configuration of the table passed in.

    :param dtypes: The parquet file to make the table configuration for.
    :return: A reasonable ORM configuration for this table.
    """
    cols_orm = {
        column: {
            "type": _dtype_to_sql(dtype),
        }
        for column, dtype in parquet_file.dtypes.items()
    }
    return {
        "columns": cols_orm,
        "unique": [],
    }


_numpy_dtype_to_sql: dict[str, str | None] = {
    "?": "BOOLEAN",
    "b": "BOOLEAN",
    "B": "SMALLINT",
    "i": "INTEGER",
    "u": "INTEGER",
    "f": "DOUBLE",
    "M": "DATETIME",
    # O can be anything, but Pandas before version 3 makes text O
    "O": "TEXT",
    "U": "TEXT",
    "V": "BLOB",
}


def _dtype_to_sql(dtype: Any) -> str | None:
    """Convert a numpy or Pandas datatype into a SQL type."""
    # Pandas nullable types such as ``Int32`` wrap a numpy dtype
    dtype = getattr(dtype, "numpy_dtype", dtype)
    if isinstance(dtype, np.dtype):
        if dtype.shape != () or dtype.kind not in _numpy_dtype_to_sql:
            return None
        return _numpy_dtype_to_sql[dtype.kind]
    if isinstance(dtype, str):
        if dtype.startswith("datetime"):
            return "DATETIME"
        if dtype == PARTITION_KEY_DTYPE:
            return "TEXT"
    return None
