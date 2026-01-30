"""Generic procedure story mirroring measurement fallback behavior."""

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
from procedure_registry import register_default_procedure_generator

DEFAULT_PROCEDURE_TYPE_CONCEPT_ID = 0  # default OMOP EHR procedure type
_DEFAULT_PROCEDURE_COUNT = 1
_DEFAULT_PROCEDURE_COUNT_STD = 0.25


def build_procedure_rows(
    series_list: Iterable[stypes.ProcedureSeries],
    person_id: int,
    visit_occurrence_id: int,
) -> List[tuple[str, stypes.SqlRow]]:
    rows: List[tuple[str, stypes.SqlRow]] = []
    for series in series_list:
        for procedure_datetime in series["datetimes"]:
            rows.append(
                (
                    "procedure_occurrence",
                    {
                        "procedure_concept_id": series["procedure_concept_id"],
                        "person_id": person_id,
                        "visit_occurrence_id": visit_occurrence_id,
                        "procedure_datetime": procedure_datetime,
                        "procedure_date": procedure_datetime.date(),
                        "procedure_type_concept_id": series[
                            "procedure_type_concept_id"
                        ],
                    },
                )
            )
    return rows


def _fallback_procedure_generator(
    tokens: Sequence[int | str],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
) -> List[tuple[str, stypes.SqlRow]]:
    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])

    stats_index = build_stats_index(src_stats.get("procedure_stats", []), "procedure_concept_id")
    series_list: list[stypes.ProcedureSeries] = []
    for token in tokens:
        concept_id = coerce_concept_id(token, "procedure")
        if concept_id is None:
            continue
        count = sample_count_from_stats(
            concept_id,
            stats_index,
            "procedures_per_visit_avg",
            "procedures_per_visit_std",
            _DEFAULT_PROCEDURE_COUNT,
            _DEFAULT_PROCEDURE_COUNT_STD,
        )
        event_datetimes = sample_event_datetimes(count, visit_occurrence)

        series_list.append(
            {
                "procedure_concept_id": concept_id,
                "procedure_type_concept_id": DEFAULT_PROCEDURE_TYPE_CONCEPT_ID,
                "datetimes": event_datetimes,
            }
        )
    return build_procedure_rows(series_list, person_id, visit_occurrence_id)


register_default_procedure_generator(_fallback_procedure_generator)
