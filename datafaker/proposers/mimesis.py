"""Generators using Mimesis."""

from collections.abc import Callable, Sequence
from typing import Any, Union

import mimesis
import mimesis.locales
from sqlalchemy import Column, Engine, cast, extract, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Date, DateTime, Integer, Numeric, String, Time

from datafaker.proposers.base import (
    Buckets,
    Proposer,
    ProposerError,
    ProposerFactory,
    get_column_type,
)
from datafaker.providers import DistributionProvider

NumericType = Union[int, float]

# How many distinct values can we have before we consider a
# choice distribution to be infeasible?
MAXIMUM_CHOICES = 500

dist_gen = DistributionProvider()
generic = mimesis.Generic(locale=mimesis.locales.Locale.EN_GB)


class MimesisProposerBase(Proposer):
    """Base class for a proposer using Mimesis."""

    def __init__(
        self,
        function_name: str,
    ):
        """
        Initialise a proposer that uses Mimesis.

        :param function_name: is relative to 'generic', for example 'person.name'.
        """
        super().__init__()
        f = generic
        for part in function_name.split("."):
            if not hasattr(f, part):
                raise ProposerError(
                    f"Mimesis does not have a function {function_name}: {part} not found"
                )
            f = getattr(f, part)
        if not callable(f):
            raise ProposerError(
                f"Mimesis object {function_name} is not a callable,"
                " so cannot be used as a generator"
            )
        self._name = "generic." + function_name
        self._generator_function: Callable[..., Any] = f

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return self._name

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [self._generator_function() for _ in range(count)]


class MimesisProposer(MimesisProposerBase):
    """A generator using Mimesis."""

    def __init__(
        self,
        function_name: str,
        value_fn: Callable[[Any], float] | None = None,
        buckets: Buckets | None = None,
    ):
        """
        Initialise a generator using Mimesis.

        :param function_name: is relative to 'generic', for example 'person.name'.
        :param value_fn: Function to convert generator output to floats, if needed. The values
        thus produced are compared against the buckets to estimate the fit.
        :param buckets: The distribution of string lengths in the real data. If this is None
        then the fit method will return None.
        """
        super().__init__(function_name)
        if buckets is None:
            self._fit = None
            return
        samples = self.generate_data(400)
        if value_fn:
            samples = [value_fn(s) for s in samples]
        self._fit = buckets.fit_from_values(samples)

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return self._name

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {}

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {}

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        return default if self._fit is None else self._fit


class MimesisGeneratorTruncated(MimesisProposer):
    """A string generator using Mimesis that must fit within a certain number of characters."""

    def __init__(
        self,
        function_name: str,
        length: int,
        value_fn: Callable[[Any], float] | None = None,
        buckets: Buckets | None = None,
    ):
        """Initialise a MimesisGeneratorTruncated."""
        self._length = length
        super().__init__(function_name, value_fn, buckets)

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "dist_gen.truncated_string"

    def name(self) -> str:
        """Get the name of the generator."""
        return f"{self._name} [truncated to {self._length}]"

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "subgen_fn": self._name,
            "params": {},
            "length": self._length,
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {
            "subgen_fn": self._name,
            "params": {},
            "length": self._length,
        }

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [self._generator_function()[: self._length] for _ in range(count)]


class MimesisDateTimeProposer(MimesisProposerBase):
    """DateTime proposer using Mimesis."""

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        column: Column,
        function_name: str,
        min_year: str,
        max_year: str,
        start: int,
        end: int,
    ) -> None:
        """
        Initialise a MimesisDateTimeProposer.

        :param column: The column to generate into
        :param function_name: The name of the mimesis function
        :param min_year: SQL expression extracting the minimum year
        :param min_year: SQL expression extracting the maximum year
        :param start: The actual first year found
        :param end: The actual last year found
        """
        super().__init__(function_name)
        self._column = column
        self._max_year = max_year
        self._min_year = min_year
        self._start = start
        self._end = end

    @classmethod
    def make_singleton(
        cls, column: Column, engine: Engine, function_name: str
    ) -> Sequence[Proposer]:
        """Make the appropriate generation configuration for this column."""
        year_expr = cast(extract("year", column), Integer())
        min_expr = func.min(year_expr)  # pylint: disable=E1111
        max_expr = func.max(year_expr)  # pylint: disable=E1111
        stmt = select(min_expr.label("start"), max_expr.label("end")).select_from(
            column.table
        )
        with engine.connect() as connection:
            result = connection.execute(stmt).first()
            if result is None or result.start is None or result.end is None:
                return []
        dialect = engine.dialect
        min_year = str(
            min_expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        )
        max_year = str(
            max_expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        )
        return [
            MimesisDateTimeProposer(
                column,
                function_name,
                min_year,
                max_year,
                int(result.start),
                int(result.end),
            )
        ]

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "start": (
                f'SRC_STATS["auto__{self._column.table.name}"]["results"]'
                f'[0]["{self._column.name}__start"]'
            ),
            "end": (
                f'SRC_STATS["auto__{self._column.table.name}"]["results"]'
                f'[0]["{self._column.name}__end"]'
            ),
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        return {
            "start": self._start,
            "end": self._end,
        }

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """Get the query fragments the generators need to call."""
        return {
            f"{self._column.name}__start": {
                "clause": self._min_year,
                "comment": (
                    f"Earliest year found for column {self._column.name}"
                    f" in table {self._column.table.name}"
                ),
            },
            f"{self._column.name}__end": {
                "clause": self._max_year,
                "comment": (
                    f"Latest year found for column {self._column.name}"
                    f" in table {self._column.table.name}"
                ),
            },
        }

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        return [
            self._generator_function(start=self._start, end=self._end)
            for _ in range(count)
        ]


class MimesisStringProposerFactory(ProposerFactory):
    """All Mimesis generators that return strings."""

    GENERATOR_NAMES = [
        "address.calling_code",
        "address.city",
        "address.continent",
        "address.country",
        "address.country_code",
        "address.postal_code",
        "address.province",
        "address.street_number",
        "address.street_name",
        "address.street_suffix",
        "person.blood_type",
        "person.email",
        "person.first_name",
        "person.last_name",
        "person.full_name",
        "person.gender",
        "person.language",
        "person.nationality",
        "person.occupation",
        "person.password",
        "person.title",
        "person.university",
        "person.username",
        "person.worldview",
        "text.answer",
        "text.color",
        "text.level",
        "text.quote",
        "text.sentence",
        "text.text",
        "text.word",
    ]

    def _get_generators_with(
        self, gen_class: Callable, **kwargs: Any
    ) -> list[Proposer]:
        gens: list[Proposer] = []
        for name in self.GENERATOR_NAMES:
            try:
                gens.append(gen_class(name, **kwargs))
            except:  # pylint: disable=bare-except
                pass
        return gens

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        column_type = get_column_type(column)
        if not isinstance(column_type, String):
            return []
        try:
            buckets = Buckets.make_buckets(
                engine,
                column.table,
                func.length(column),
            )
            fitness_fn = len
        except SQLAlchemyError:
            # Some column types that appear to be strings (such as enums)
            # cannot have their lengths measured. In this case we cannot
            # detect fitness using lengths.
            buckets = None
            fitness_fn = None
        length = column_type.length
        if length:
            return self._get_generators_with(
                MimesisGeneratorTruncated,
                length=length,
                value_fn=fitness_fn,
                buckets=buckets,
            )
        return self._get_generators_with(
            MimesisProposer,
            value_fn=fitness_fn,
            buckets=buckets,
        )


class MimesisFloatProposerFactory(ProposerFactory):
    """All Mimesis generators that return floating point numbers."""

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        if not isinstance(get_column_type(column), Numeric):
            return []
        return list(
            map(
                MimesisProposer,
                [
                    "person.height",
                ],
            )
        )


class MimesisDateProposerFactory(ProposerFactory):
    """All Mimesis generators that return dates."""

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, Date):
            return []
        return MimesisDateTimeProposer.make_singleton(column, engine, "datetime.date")


class MimesisDateTimeProposerFactory(ProposerFactory):
    """All Mimesis generators that return datetimes."""

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, DateTime):
            return []
        return MimesisDateTimeProposer.make_singleton(
            column, engine, "datetime.datetime"
        )


class MimesisTimeProposerFactory(ProposerFactory):
    """All Mimesis generators that return times."""

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, Time):
            return []
        return [MimesisProposer("datetime.time")]


class MimesisIntegerProposerFactory(ProposerFactory):
    """All Mimesis generators that return integers."""

    def get_proposers(
        self, columns: list[Column], engine: Engine
    ) -> Sequence[Proposer]:
        """Get the generators appropriate to these columns."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, Numeric) and not isinstance(ct, Integer):
            return []
        return [MimesisProposer("person.weight")]
