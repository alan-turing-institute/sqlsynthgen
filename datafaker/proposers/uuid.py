"""Proposer for UUID identifiers, especially for UUID primary keys.

Some databases - notably Snowflake, which has no native UUID column type -
store UUIDs as a plain string column rather than a dedicated UUID type.
SQLAlchemy's own ``Uuid``/``postgresql.UUID``/``mssql.UNIQUEIDENTIFIER``
types (``get_column_type`` already normalises the latter two to ``Uuid`` via
``as_generic``) are recognised directly; a plain ``String`` *primary key*
column is checked against a sample of its real values instead, matching each
one against the canonical UUID pattern, with column length used only when
there is no data to sample.
"""

import re
import uuid as uuid_lib
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Column, Engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import String, Uuid

from datafaker.dialects import Random
from datafaker.proposers.base import Proposer, ProposerFactory, get_column_type

# Canonical textual UUID lengths: 36 characters with hyphens (the standard
# representation, e.g. "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"), or 32 without hyphen.
# Only used as a fallback when no real data is available to check directly.
UUID_STRING_LENGTHS = frozenset({32, 36})

# Matches a UUID's 8-4-4-4-12 hex groups, with or without hyphens.
UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?"
    r"[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$"
)

# How many real (non-null) values to sample before deciding.
SAMPLE_SIZE = 20

# Fraction of the sample that must match the UUID pattern. Not 100%: a real
# column can have a handful of legacy/malformed values without that meaning
# the column as a whole isn't a UUID column.
MIN_MATCHING_FRACTION = 0.9


def _sample_uuid_looking_fraction(column: Column, engine: Engine) -> float | None:
    """Sample real values from ``column`` and return the fraction that look like UUIDs.

    :return: The matching fraction in [0, 1], or None if no real (non-null)
        values could be sampled at all (e.g. an empty table), in which case
        the caller should fall back to a metadata-only heuristic instead.
    """
    try:
        stmt = (
            select(column)
            .where(column.isnot(None))
            .order_by(Random())
            .limit(SAMPLE_SIZE)
        )
        with engine.connect() as connection:
            values = [row[0] for row in connection.execute(stmt)]
    except SQLAlchemyError:
        return None
    if not values:
        return None
    matches = sum(1 for value in values if UUID_PATTERN.match(str(value)))
    return matches / len(values)


class UuidProposer(Proposer):
    """Generator of cryptographically random (version 4) UUIDs.

    Collisions between independently generated version 4 UUIDs are not
    structurally impossible the way a sequence continuing past the observed
    maximum is, but at 122 bits of randomness the probability is low enough
    that this is treated, for ranking purposes, as a fresh-uniqueness
    guarantee - the same convention under which ``generic.cryptographic.
    uuid`` is already used as the default generator for any native UUID
    column (see ``make.py``'s ``_COLUMN_TYPE_TO_GENERATOR_INFO``).
    """

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "generic.cryptographic.uuid"

    def guarantees_fresh_uniqueness(self) -> bool:
        """Treat version 4 UUIDs as fresh and unique for ranking purposes."""
        return True

    def nominal_kwargs(self) -> dict[str, str]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {}

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {}

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random UUID strings."""
        return [str(uuid_lib.uuid4()) for _ in range(count)]


class UuidProposerFactory(ProposerFactory):
    """Propose a UUID generator for columns that hold UUIDs.

    Applies to any column using a native UUID SQL type, and - because some
    databases have no such type and instead store UUIDs as a plain string
    primary key - any ``String`` *primary key* column whose real sampled
    values are mostly UUID-shaped (see ``_sample_uuid_looking_fraction``).
    The primary-key restriction keeps this from being offered for an
    unrelated string column that merely happens to hold some UUID-looking
    values. Falls back to a length-based guess only when the table has no
    data to sample at all.
    """

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        column_type = get_column_type(column)
        if isinstance(column_type, Uuid):
            return [UuidProposer()]
        if not (column.primary_key and isinstance(column_type, String)):
            return []
        matching_fraction = _sample_uuid_looking_fraction(column, engine)
        # Prefer sampled data over column length: an unrelated natural key can
        # coincidentally share a UUID's length, and a Snowflake VARCHAR/STRING
        # column declared with no explicit length can still hold real UUIDs.
        # Length is only a last resort, e.g. for an empty table.
        if matching_fraction is not None:
            looks_like_uuid = matching_fraction >= MIN_MATCHING_FRACTION
        else:
            looks_like_uuid = column_type.length in UUID_STRING_LENGTHS
        return [UuidProposer()] if looks_like_uuid else []
