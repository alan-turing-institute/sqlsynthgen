"""Generic measurement story used as a fallback for non-specialized concepts."""
from __future__ import annotations

import datetime as dt
from collections import Counter
from typing import List, Sequence, cast

import numpy as np

import story_types as stypes
from measurement_registry import register_default_measurement_generator
from sqlsynthgen.utils import logger
from sqlsynthgen.utils_timeseries import generate_time_series

DEFAULT_MEASUREMENT_TYPE_CONCEPT_ID = 32817  # EHR measurement
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


def _sample_values(count: int, mean: float, std: float) -> List[float]:
    if count <= 0:
        return []
    series = generate_time_series(
        count,
        "random_walk",
        {
            "mean": mean,
            "std": std,
            "epsilon_std": max(std * 0.2, _DEFAULT_EPSILON),
            "drift": _DEFAULT_DRIFT,
        },
    )
    return np.asarray(series, dtype=float).tolist()


def _random_datetimes_for_count(
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


def _fallback_measurement_generator(
    tokens: Sequence[int | str],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
) -> List[tuple[str, stypes.SqlRow]]:
    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])

    counts: Counter[int] = Counter()
    for raw_token in tokens:
        concept_id = _to_int(raw_token)
        if concept_id is None:
            continue
        counts[concept_id] += 1

    rows: list[tuple[str, stypes.SqlRow]] = []
    for concept_id, count in counts.items():
        event_datetimes = _random_datetimes_for_count(count, visit_occurrence)
        values = _sample_values(count, _DEFAULT_MEAN, _DEFAULT_STD)
        for measurement_datetime, value in zip(event_datetimes, values):
            rows.append(
                (
                    "measurement",
                    {
                        "measurement_concept_id": concept_id,
                        "person_id": person_id,
                        "visit_occurrence_id": visit_occurrence_id,
                        "measurement_datetime": measurement_datetime,
                        "measurement_date": measurement_datetime.date(),
                        "measurement_type_concept_id": concept_id,
                        "unit_concept_id": None,
                        "value_as_number": value,
                    },
                )
            )
    return rows


register_default_measurement_generator(_fallback_measurement_generator)
