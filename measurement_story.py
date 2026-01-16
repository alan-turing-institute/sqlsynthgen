"""Generic measurement story used as a fallback for non-specialized concepts."""
from __future__ import annotations

import datetime as dt
from collections import Counter
from typing import Iterable, List, Sequence, cast

import numpy as np

import story_types as stypes
from measurement_registry import register_default_measurement_generator
from sqlsynthgen.utils import logger
from sqlsynthgen.utils_timeseries import generate_time_series

DEFAULT_MEASUREMENT_TYPE_CONCEPT_ID = 0  # EHR measurement
DEFAULT_UNIT_CONCEPT_ID = 0
_DEFAULT_MEAN = 1.0
_DEFAULT_STD = 0.25
_DEFAULT_EPSILON = 0.05
_DEFAULT_DRIFT = 0.0


def _to_int(token: int | str) -> int | None:
    try:
        return int(token)
    except (TypeError, ValueError):
        try:
            return int(str(token).strip())
        except (TypeError, ValueError):
            logger.warning(
                "Unable to coerce measurement token '%s' to concept_id", token
            )
            return None


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


def sample_measurement_datetimes(
    count: int, visit_occurrence: stypes.SqlRow
) -> List[dt.datetime]:
    start = cast(dt.datetime, visit_occurrence["visit_start_datetime"])
    end = cast(dt.datetime, visit_occurrence["visit_end_datetime"])
    if count <= 0:
        return []
    if end <= start:
        return [start for _ in range(count)]
    period = end - start
    fractions = np.random.uniform(size=count)
    return sorted(start + period * float(fraction) for fraction in fractions)


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
                        "measurement_type_concept_id": series[
                            "measurement_type_concept_id"
                        ],
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
    visit_unique: bool = False,
) -> List[tuple[str, stypes.SqlRow]]:
    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])

    counts: Counter[int] = Counter()
    for raw_token in tokens:
        concept_id = _to_int(raw_token)
        if concept_id is None:
            continue
        counts[concept_id] += 1

    series_list: list[stypes.MeasurementSeries] = []
    for concept_id, count in counts.items():
        # add random noise to the count values
        if visit_unique and count > 1:
            noise = np.random.uniform(-0.2, 0.2)
            count = max(1, int(round(count * (1 + noise))))

        event_datetimes = sample_measurement_datetimes(count, visit_occurrence)

        measurement_values = [i for i in src_stats['measurement_stats'] if i['measurement_concept_id'] == concept_id]
        if len(measurement_values)> 0:
            mean_value = measurement_values[0]['measurement_mean']
            std_value = measurement_values[0]['measurement_stddev']
            values = sample_measurement_values(count, mean_value, std_value)
            measurement_type_concept_id = measurement_values[0]['measurement_type_concept_id']
            unit_concept_id = DEFAULT_UNIT_CONCEPT_ID
        else:
            values = sample_measurement_values(count)
            measurement_type_concept_id = DEFAULT_MEASUREMENT_TYPE_CONCEPT_ID
            unit_concept_id = DEFAULT_UNIT_CONCEPT_ID

        series_list.append(
            {
                "measurement_concept_id": concept_id,
                "measurement_type_concept_id": measurement_type_concept_id,
                "unit_concept_id": unit_concept_id,
                "datetimes": event_datetimes,
                "values": values,
            }
        )
    return build_measurement_rows(series_list, person_id, visit_occurrence_id)


register_default_measurement_generator(_fallback_measurement_generator)
