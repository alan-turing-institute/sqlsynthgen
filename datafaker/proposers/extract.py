"""Proposers for date member extraction."""
import datetime
from abc import abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import Column, Engine, MetaData, select
from sqlalchemy.types import Date, DateTime, Integer

from datafaker.dialects import Random
from datafaker.proposers.base import Proposer, ProposerFactory, get_column_type
from datafaker.providers import ExtractProvider


class DateComponentExtractProposer(Proposer):
    """Proposer that proposes the extraction of components from a datetime."""

    def __init__(
        self,
        column: Column,
        based_on: Column,
        engine: Engine,
    ):
        """
        Initialise a date component extractor proposer.

        :param column: The column to generate.
        :param based_on: The column containing the DateTime, which must be
         in the same table as column.
        """
        super().__init__()
        self._based_on = based_on
        self._column = column
        self._engine = engine

    def name(self) -> str:
        """Get the name of the generator."""
        fname = self.function_name()
        return f"{fname} [from column {self._based_on.name}]"

    def nominal_kwargs(self) -> dict[str, Any]:
        """Get the arguments to be entered into ``config.yaml``."""
        return {
            "extract_from": f'GENERATED_ROW["{self._based_on.name}"]',
        }

    def actual_kwargs(self) -> dict[str, Any]:
        """Get the kwargs (summary statistics) this generator was instantiated with."""
        # using a dummy value, sadly
        return {
            "extract_from": datetime.datetime(1970, 1, 1),
        }

    def select_aggregate_clauses(self) -> dict[str, dict[str, str]]:
        """Return that no data is required from the database."""
        return {}

    def custom_queries(self) -> dict[str, dict[str, Any]]:
        """Return that no data is required from the database."""
        return {}

    def fit(self, default: float = -1) -> float:
        """Get this generator's fit against the real data."""
        # What should the fit really be?
        return 0.0

    def get_based_on_data(self, count: int) -> list[datetime.datetime]:
        """Get the data from the based-on column in the source data."""
        with self._engine.connect() as conn:
            rows = conn.execute(select(self._based_on).order_by(Random())).fetchmany(
                count
            )
            return [row[0] for row in rows]

    def generate_data(self, count: int) -> list[Any]:
        """Generate ``count`` random data points for this column."""
        prov = ExtractProvider()
        return [self.do_extract(prov, dt) for dt in self.get_based_on_data(count)]

    @abstractmethod
    def do_extract(self, provider: ExtractProvider, dt: datetime.datetime) -> Any:
        """Generate a single value."""


class YearExtractProposer(DateComponentExtractProposer):
    """Proposer that proposes the extraction of year from a datetime."""

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "generic.extract_provider.year"

    def do_extract(self, provider: ExtractProvider, dt: datetime.datetime) -> Any:
        """Generate a single year value."""
        return provider.year(dt)


class MonthExtractProposer(DateComponentExtractProposer):
    """Proposer that proposes the extraction of year from a datetime."""

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "generic.extract_provider.month"

    def do_extract(self, provider: ExtractProvider, dt: datetime.datetime) -> Any:
        """Generate a single year value."""
        return provider.month(dt)


class DayExtractProposer(DateComponentExtractProposer):
    """Proposer that proposes the extraction of year from a datetime."""

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "generic.extract_provider.day"

    def do_extract(self, provider: ExtractProvider, dt: datetime.datetime) -> Any:
        """Generate a single year value."""
        return provider.day(dt)


class DateExtractProposer(DateComponentExtractProposer):
    """Proposer that proposes the extraction of date from a datetime."""

    def function_name(self) -> str:
        """Get the name of the generator function to call."""
        return "generic.extract_provider.date"

    def do_extract(self, provider: ExtractProvider, dt: datetime.datetime) -> Any:
        """Generate a single year value."""
        return provider.date(dt)


class DateComponentExtractProposerFactory(ProposerFactory):
    """Makes proposers for extracting a component from a datetime."""

    def __init__(self, config: Mapping, metadata: MetaData):
        """Initialize ``DateAfterProposerFactory``."""
        super().__init__()
        self._config = config
        self._metadata = metadata

    def get_proposers(
        self,
        columns: list[Column],
        engine: Engine,
    ) -> Sequence[Proposer]:
        """Get all proposers of dates or integers that might be extracted from a datetime."""
        if len(columns) != 1:
            return []
        column = columns[0]
        ct = get_column_type(column)
        if not isinstance(ct, (Date, Integer)):
            return []
        datetimes = [
            c for c in column.table.columns if isinstance(get_column_type(c), DateTime)
        ]
        if isinstance(ct, Date):
            return [DateExtractProposer(column, dt, engine) for dt in datetimes]
        return [
            prop
            for dt in datetimes
            for prop in [
                YearExtractProposer(column, dt, engine),
                MonthExtractProposer(column, dt, engine),
                DayExtractProposer(column, dt, engine),
            ]
        ]
