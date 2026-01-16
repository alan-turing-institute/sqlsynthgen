from typing import Sequence, cast
import datetime as dt
import numpy as np

import measurement_registry
import story_types as stypes
from measurement_registry import register_measurement_generator
from measurement_story import (
    build_measurement_rows,
    sample_measurement_datetimes,
    sample_measurement_values,
)

Systolic_blood_pressure_by_Noninvasive = 21492239
Diastolic_blood_pressure_by_Noninvasive = 21492240
measurement_type_concept_id_bp = 32817  # EHR measurement
unit_concept_id_bp = 8876  # mmHg


def _generate_bp_events(
    tokens: Sequence[measurement_registry.MeasurementKey],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
    visit_unique: bool = False,
) -> list[tuple[str, stypes.SqlRow]]:
    """Generate blood pressure measurement rows using shared measurement helpers."""

    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])
    gender = cast(int, person["gender_concept_id"])
    age = (
        cast(dt.datetime, visit_occurrence["visit_start_datetime"])
        - cast(dt.datetime, person["birth_datetime"])
    ).days / 365.25

    event_count = max(1, len(tokens))

    # add some randomness to the number of events
    if visit_unique and event_count > 1:
        event_count = max(1, int(event_count * (1 + np.random.uniform(-0.2, 0.2))))

    event_datetimes = sample_measurement_datetimes(event_count, visit_occurrence)
    if not event_datetimes:
        return []
    value_count = len(event_datetimes)

    main_key = "bp_profile"
    relative_change_key = "bp_sys_relative_change_stats"
    if age < 60:
        key_mean = "average_under_60_systolic"
        key_std = "stddev_under_60_systolic"
        key_epsilon_mean = "avg_under_60_systolic_rel_var"
        key_epsilon_std = "stddev_under_60_systolic_rel_var"
    else:
        key_mean = "average_over_60_systolic"
        key_std = "stddev_over_60_systolic"
        key_epsilon_mean = "avg_over_60_systolic_rel_var"
        key_epsilon_std = "stddev_over_60_systolic_rel_var"

    if gender == 8507:
        index_gender = 0
    else:
        index_gender = 1

    sample_epsilon = np.random.normal(
        src_stats[relative_change_key][index_gender][key_epsilon_mean],
        src_stats[relative_change_key][index_gender][key_epsilon_std],
    )

    systolic_values = np.asarray(
        sample_measurement_values(
            value_count,
            mean=src_stats[main_key][index_gender][key_mean],
            std=src_stats[main_key][index_gender][key_std],
            epsilon_override=float(abs(sample_epsilon)),
        )
    )
    diastolic_values = (
        systolic_values
        - src_stats[main_key][index_gender][
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

def _generate_bp_diastolic(tokens: Sequence[measurement_registry.MeasurementKey], person: stypes.SqlRow, visit_occurrence: stypes.SqlRow, src_stats: stypes.SrcStats, visit_unique: bool = False,
) -> list[tuple[str, stypes.SqlRow]]:
    "Do not generate blood pressure diastolic measurement as is already generated in _generate_bp_events."
    return []


register_measurement_generator([Systolic_blood_pressure_by_Noninvasive, "blood_pressure_all"], _generate_bp_events)
register_measurement_generator([Diastolic_blood_pressure_by_Noninvasive, "blood_pressure_diastolic"], _generate_bp_diastolic)
