"""Generic measurement story used as a fallback for non-specialized concepts."""
from __future__ import annotations

import datetime as dt
from typing import Iterable, List, Sequence, cast

import numpy as np

import story_types as stypes
from event_story_utils import (
    build_stats_index,
    coerce_concept_id,
    sample_count_from_stats,
    sample_event_datetimes,
)
from measurement_registry import register_default_measurement_generator
from sqlsynthgen.utils_timeseries import generate_time_series

DEFAULT_MEASUREMENT_TYPE_CONCEPT_ID = 0  # EHR measurement
DEFAULT_UNIT_CONCEPT_ID = 0
_DEFAULT_MEAN = 1.0
_DEFAULT_STD = 0.25
_DEFAULT_EPSILON = 0.05
_DEFAULT_DRIFT = 0.0
_DEFAULT_MEASUREMENT_COUNT = 1
_DEFAULT_MEASUREMENT_COUNT_STD = 0.25


def sample_measurement_values(
    count: int,
    mean: float = _DEFAULT_MEAN,
    std: float = _DEFAULT_STD,
    epsilon_override: float | None = None,
    drift: float = _DEFAULT_DRIFT,
) -> List[float]:
    if count <= 0:
        return []
    epsilon_std = (
        epsilon_override
        if epsilon_override is not None
        else max(std * 0.2, _DEFAULT_EPSILON)
    )
    series = generate_time_series(
        count,
        "random_walk",
        {
            "mean": mean,
            "std": std,
            "epsilon_std": epsilon_std,
            "drift": drift,
        },
    )
    return np.asarray(series, dtype=float).tolist()


def build_measurement_rows(
    series_list: Iterable[stypes.MeasurementSeries],
    person_id: int,
    visit_occurrence_id: int,
) -> List[tuple[str, stypes.SqlRow]]:
    rows: List[tuple[str, stypes.SqlRow]] = []
    for series in series_list:
        datetimes = series["datetimes"]
        values = series["values"]
        for measurement_datetime, value in zip(datetimes, values):
            rows.append(
                (
                    "measurement",
                    {
                        "measurement_concept_id": series["measurement_concept_id"],
                        "person_id": person_id,
                        "visit_occurrence_id": visit_occurrence_id,
                        "measurement_datetime": measurement_datetime,
                        "measurement_date": measurement_datetime.date(),
                        "measurement_type_concept_id": series["measurement_type_concept_id"],
                        "unit_concept_id": series["unit_concept_id"],
                        "value_as_number": value,
                    },
                )
            )
    return rows


def _fallback_measurement_generator(
    tokens: Sequence[int | str],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
) -> List[tuple[str, stypes.SqlRow]]:
    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])

    series_list: list[stypes.MeasurementSeries] = []
    stats_index = build_stats_index(src_stats.get("measurement_stats", []), "measurement_concept_id")
    for token in tokens:
        concept_id = coerce_concept_id(token, "measurement")
        if concept_id is None:
            continue
        count = sample_count_from_stats(
            concept_id,
            stats_index,
            "measurements_per_visit_avg",
            "measurements_per_visit_std",
            _DEFAULT_MEASUREMENT_COUNT,
            _DEFAULT_MEASUREMENT_COUNT_STD,
        )
        stat_row = stats_index.get(concept_id)
        mean_value = (
            stat_row["measurement_mean"] if stat_row and stat_row.get("measurement_mean") is not None else _DEFAULT_MEAN
        )
        std_value = (
            stat_row["measurement_stddev"] if stat_row and stat_row.get("measurement_stddev") is not None else _DEFAULT_STD
        )
        values = sample_measurement_values(count, float(mean_value), float(std_value))
        unit_concept_id = DEFAULT_UNIT_CONCEPT_ID

        event_datetimes = sample_event_datetimes(count, visit_occurrence)

        series_list.append(
            {
                "measurement_concept_id": concept_id,
                "unit_concept_id": unit_concept_id,
                "measurement_type_concept_id": DEFAULT_MEASUREMENT_TYPE_CONCEPT_ID,
                "datetimes": event_datetimes,
                "values": values,
            }
        )
    return build_measurement_rows(series_list, person_id, visit_occurrence_id)


register_default_measurement_generator(_fallback_measurement_generator)
