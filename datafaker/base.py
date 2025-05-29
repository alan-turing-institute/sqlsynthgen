"""Base table generator classes."""
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
import functools
import math
import os
from pathlib import Path
import random
from typing import Any

import yaml
import gzip
from sqlalchemy import Connection, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import Table

from datafaker.utils import (
    logger,
    stream_yaml,
    MAKE_VOCAB_PROGRESS_REPORT_EVERY,
    table_row_count,
)

@functools.cache
def zipf_weights(size):
    total = sum(map(lambda n: 1/n, range(1, size + 1)))
    return [
        1 / (n * total)
        for n in range(1, size + 1)
    ]


class DistributionGenerator:
    root3 = math.sqrt(3)

    def uniform(self, low, high) -> float:
        return random.uniform(float(low), float(high))

    def uniform_ms(self, mean, sd) -> float:
        m = float(mean)
        h = self.root3 * float(sd)
        return random.uniform(m - h, m + h)

    def normal(self, mean, sd) -> float:
        return random.normalvariate(float(mean), float(sd))

    def choice(self, a):
        c = random.choice(a)
        return c["value"] if type(c) is dict and "value" in c else c

    def zipf_choice(self, a, n=None):
        if n is None:
            n = len(a)
        c = random.choices(a, weights=zipf_weights(n))[0]
        return c["value"] if type(c) is dict and "value" in c else c

    def constant(self, value):
        return value


class TableGenerator(ABC):
    """Abstract base class for table generator classes."""

    num_rows_per_pass: int = 1

    @abstractmethod
    def __call__(self, dst_db_conn: Connection) -> dict[str, Any]:
        """Return, as a dictionary, a new row for the table that we are generating.

        The only argument, `dst_db_conn`, should be a database connection to the
        database to which the data is being written. Most generators won't use it, but
        some do, and thus it's required by the interface.

        The return value should be a dictionary with column names as strings for keys,
        and the values being the values for the new row.
        """


@dataclass
class FileUploader:
    """For uploading data files."""

    table: Table

    def _load_existing_file(self, connection: Connection, file_size: int, opener: Callable[[], Any]) -> None:
        count = 0
        with opener() as fh:
            rows = stream_yaml(fh)
            for row in rows:
                stmt = insert(self.table).values(row)
                connection.execute(stmt)
                connection.commit()
                count += 1
                if count % MAKE_VOCAB_PROGRESS_REPORT_EVERY == 0:
                    logger.info(
                        "inserted row %d of %s, %.1f%%",
                        count,
                        self.table.name,
                        100 * fh.tell() / file_size,
                    )

    def load(self, connection: Connection) -> None:
        """Load the data from file."""
        yaml_file = Path(self.table.fullname + ".yaml")
        if yaml_file.exists():
            opener = lambda: open(yaml_file, mode="r", encoding="utf-8")
        else:
            yaml_file = Path(self.table.fullname + ".yaml.gz")
            if yaml_file.exists():
                opener = lambda: gzip.open(yaml_file, mode="rt")
            else:
                logger.warning("File %s not found. Skipping...", yaml_file)
                return
        if 0 < table_row_count(self.table, connection):
            logger.warning("Table %s already contains data (consider running 'datafaker remove-vocab'), skipping...", self.table.name)
            return
        try:
            file_size = os.path.getsize(yaml_file)
            self._load_existing_file(connection, file_size, opener)
        except yaml.YAMLError as e:
            logger.warning("Error reading YAML file %s: %s", yaml_file, e)
            return
        except SQLAlchemyError as e:
            logger.warning(
                "Error inserting rows into table %s: %s", self.table.fullname, e
            )

class ColumnPresence:
    def sampled(self, patterns):
        total = 0
        for pattern in patterns:
            total += pattern.get("row_count", 0)
        s = random.randrange(total)
        for pattern in patterns:
            s -= pattern.get("row_count", 0)
            if s < 0:
                cs = set()
                for column, nullness in pattern.items():
                    if not nullness and column.endswith("__is_null"):
                        cs.add(column[:-9])
                return cs
        logger.error("failed to sample patterns")
        return set()
