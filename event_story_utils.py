"""Shared helpers for measurement/procedure visit event stories."""
from __future__ import annotations

import datetime as dt
from typing import Mapping, Sequence

import numpy as np

import story_types as stypes
from sqlsynthgen.utils import logger


def coerce_concept_id(token: int | str, label: str) -> int | None:
    """Best-effort conversion of tokens to concept identifiers."""
    try:
        return int(token)
    except (TypeError, ValueError):
        try:
            return int(str(token).strip())
        except (TypeError, ValueError):
            logger.warning("Unable to coerce %s token '%s' to concept_id", label, token)
            return None


def sample_event_datetimes(
    count: int, visit_occurrence: stypes.SqlRow
) -> list[dt.datetime]:
    start = visit_occurrence["visit_start_datetime"]
    end = visit_occurrence["visit_end_datetime"]
    if not isinstance(start, dt.datetime) or not isinstance(end, dt.datetime):
        return []
    if count <= 0:
        return []
    if end <= start:
        return [start for _ in range(count)]
    period = end - start
    fractions = np.random.uniform(size=count)
    return sorted(start + period * float(fraction) for fraction in fractions)


def build_stats_index(
    stats: Sequence[stypes.SqlRow], key_name: str
) -> dict[int, stypes.SqlRow]:
    index: dict[int, stypes.SqlRow] = {}
    for entry in stats:
        concept_id = entry.get(key_name)
        if isinstance(concept_id, int):
            index[concept_id] = entry
    return index


def sample_count_from_stats(
    concept_id: int,
    stats_index: Mapping[int, stypes.SqlRow],
    avg_key: str,
    std_key: str,
    default_avg: float,
    default_std: float,
) -> int:
    stats_row = stats_index.get(concept_id)
    avg = (
        float(stats_row.get(avg_key, default_avg))
        if stats_row and stats_row.get(avg_key) is not None
        else default_avg
    )
    std = (
        float(stats_row.get(std_key, default_std))
        if stats_row and stats_row.get(std_key) is not None
        else default_std
    )
    draw = np.random.normal(avg, std)
    return max(int(round(draw)), 0)

