"""Proposer for a monotonically-incrementing sequence past the observed max.

This is the only kind of candidate that can actually guarantee fresh, unique
values for a genuine integer primary key: it can never collide with the real
data (every value it emits is strictly greater than any observed value) and
it can never collide with itself (it only ever increments). It deliberately
reuses ``generic.column_value_provider.increment`` - the same mechanism
``make.py``'s own default generator assignment already uses for integer
primary keys (see ``_integer_generator``) - rather than inventing a new
runtime function, so a column left on this proposer's recommendation behaves
identically to the framework's own established default.
"""

from collections.abc import Sequence
from typing import Any

from sqlalchemy import Column, Engine, func, select
from sqlalchemy.types import Integer

from datafaker.proposers.base import Proposer, ProposerFactory, get_column_type


class IncrementProposer(Proposer):
    """Generator continuing a sequence past the real data's observed maximum."""

    def __init__(self, engine: Engine, column: Column):
        """Initialise an IncrementProposer."""
        super().__init__()
        self.engine = engine
        self.column = column
        self.table = column.table

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "generic.column_value_provider.increment"

    def guarantees_fresh_uniqueness(self) -> bool:
        """Monotonically increasing past the observed max can never collide."""
        return True

    def nominal_kwargs(self) -> dict[str, str]:
        """Get the arguments to be entered into ``config.yaml``.

        These match the expressions ``make.py``'s own default integer
        primary key generator emits (see ``_integer_generator``): not
        SRC_STATS references, but Python expressions the generation
        pipeline evaluates directly against the destination database and
        its metadata at generation time.
        """
        return {
            "db_connection": "dst_db_conn",
            "column": f'metadata.tables["{self.table.name}"].columns["{self.column.name}"]',
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {"column": f"{self.table.name}.{self.column.name}"}

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column.

        For interactive preview purposes only: continues past the *source*
        database's observed maximum. At actual generation time, the
        ``nominal_kwargs`` above instead continue past whatever's already in
        the *destination* database, via ``db_connection``/``column``.
        """
        with self.engine.connect() as connection:
            row = connection.execute(select(func.max(self.column))).first()
        start = 0 if row is None or row[0] is None else row[0]
        return [start + i + 1 for i in range(count)]


class IncrementProposerFactory(ProposerFactory):
    """Propose an incrementing-sequence generator for integer primary keys.

    Deliberately narrow: only a genuine integer primary key (and one that
    isn't also a foreign key - those should copy the referenced table's
    values, not synthesize their own, matching ``make.py``'s own default
    generator logic) gets this proposer. A non-key numeric column doesn't
    need guaranteed-fresh values, and a non-integer key (a string/UUID
    primary key) needs a different mechanism entirely - out of scope here.
    """

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        if not column.primary_key or column.foreign_keys:
            return []
        if not isinstance(get_column_type(column), Integer):
            return []
        return [IncrementProposer(engine, column)]
