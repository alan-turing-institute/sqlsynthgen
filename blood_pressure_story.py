from typing import Sequence, cast
import datetime as dt
import numpy as np

import measurement_registry
import story_types as stypes
from event_story_utils import (
    build_stats_index,
    sample_count_from_stats,
    sample_event_datetimes,
)
from measurement_registry import register_measurement_generator
from measurement_story import (
    build_measurement_rows,
    sample_measurement_values,
)

Systolic_blood_pressure_by_Noninvasive = 21492239
Diastolic_blood_pressure_by_Noninvasive = 21492240
measurement_type_concept_id_bp = 32817  # EHR measurement
unit_concept_id_bp = 8876  # mmHg
_FALLBACK_EVENT_COUNT = 1
_FALLBACK_EVENT_COUNT_STD = 0.25


def _generate_bp_events(
    tokens: Sequence[measurement_registry.MeasurementKey],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
) -> list[tuple[str, stypes.SqlRow]]:
    """Generate blood pressure measurement rows using shared measurement helpers."""

    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])
    gender = cast(int, person["gender_concept_id"])
    age = (
        cast(dt.datetime, visit_occurrence["visit_start_datetime"])
        - cast(dt.datetime, person["birth_datetime"])
    ).days / 365.25

    stats_index = build_stats_index(
        src_stats.get("measurement_stats", []), "measurement_concept_id"
    )
    event_count = sample_count_from_stats(
        Systolic_blood_pressure_by_Noninvasive,
        stats_index,
        "measurements_per_visit_avg",
        "measurements_per_visit_std",
        _FALLBACK_EVENT_COUNT,
        _FALLBACK_EVENT_COUNT_STD,
    )

    event_datetimes = sample_event_datetimes(event_count, visit_occurrence)
    if not event_datetimes:
        return []
    value_count = len(event_datetimes)

    main_key = "bp_profile"
    relative_change_key = "bp_sys_relative_change_stats"
    under_sixty = age < 60
    key_mean = "average_under_60_systolic" if under_sixty else "average_over_60_systolic"
    key_std = "stddev_under_60_systolic" if under_sixty else "stddev_over_60_systolic"
    key_epsilon_mean = (
        "avg_under_60_systolic_rel_var"
        if under_sixty
        else "avg_over_60_systolic_rel_var"
    )
    key_epsilon_std = (
        "stddev_under_60_systolic_rel_var"
        if under_sixty
        else "stddev_over_60_systolic_rel_var"
    )

    gender_index = 0 if gender == 8507 else 1

    sample_epsilon = np.random.normal(
        src_stats[relative_change_key][gender_index][key_epsilon_mean],
        src_stats[relative_change_key][gender_index][key_epsilon_std],
    )

    systolic_values = np.asarray(
        sample_measurement_values(
            value_count,
            mean=src_stats[main_key][gender_index][key_mean],
            std=src_stats[main_key][gender_index][key_std],
            epsilon_override=float(abs(sample_epsilon)),
        )
    )
    diastolic_values = (
        systolic_values
        - src_stats[main_key][gender_index][
            "average_systolic_diastolic_difference"
        ]
    )

    series = [
        {
            "measurement_concept_id": Systolic_blood_pressure_by_Noninvasive,
            "measurement_type_concept_id": measurement_type_concept_id_bp,
            "unit_concept_id": unit_concept_id_bp,
            "datetimes": event_datetimes,
            "values": systolic_values.tolist(),
        },
        {
            "measurement_concept_id": Diastolic_blood_pressure_by_Noninvasive,
            "measurement_type_concept_id": measurement_type_concept_id_bp,
            "unit_concept_id": unit_concept_id_bp,
            "datetimes": event_datetimes,
            "values": diastolic_values.tolist(),
        },
    ]

    return build_measurement_rows(series, person_id, visit_occurrence_id)


def _generate_bp_diastolic(
    tokens: Sequence[measurement_registry.MeasurementKey],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
    visit_unique: bool = False,
) -> list[tuple[str, stypes.SqlRow]]:
    "Do not generate blood pressure diastolic measurement as is already generated in _generate_bp_events."
    return []


register_measurement_generator([Systolic_blood_pressure_by_Noninvasive, "blood_pressure_all"], _generate_bp_events)
register_measurement_generator([Diastolic_blood_pressure_by_Noninvasive, "blood_pressure_diastolic"], _generate_bp_diastolic)
