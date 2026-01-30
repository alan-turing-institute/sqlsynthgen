from __future__ import annotations

from typing import Callable, Generator, List, Optional, Union, cast
from typing import TypedDict, Callable, List, Optional, Dict, Union, Sequence
import datetime as dt
SqlValue = Union[float, int, str, bool, dt.datetime, dt.date, None]
SqlRow = dict[str, SqlValue]
SrcStatsResult = list[SqlRow]
SrcStats = dict[str, SrcStatsResult]

class MeasurementItem(TypedDict):
    unit_concept_id: int
    measurement_type_concept_id: int
    generator: Callable[[int|float], int|float]
    datetime_value: Dict[dt.datetime, Union[float, int, None]]

class GroupedMeasurements(TypedDict):
    person_id: int
    visit_occurrence_id: int
    measurements: Dict[int, MeasurementItem]

class MeasurementSeries(TypedDict):
    measurement_concept_id: int
    measurement_type_concept_id: int
    unit_concept_id: Optional[int]
    datetimes: Sequence[dt.datetime]
    values: Sequence[float]

class ProcedureSeries(TypedDict):
    procedure_concept_id: int
    procedure_type_concept_id: int
    datetimes: Sequence[dt.datetime]
