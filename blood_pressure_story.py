import datetime as dt
from typing import Callable, List, Union, cast, TypedDict, Dict
from sqlsynthgen.utils import generate_time_series, logger

import numpy as np
import story_types as stypes

Systolic_blood_pressure_by_Noninvasive = 21492239
Diastolic_blood_pressure_by_Noninvasive = 21492240
measurement_type_concept_id_bp = 32817 # EHR measurement
avg_systolic = 114.236842
avg_diastolic = 74.447368
avg_difference = avg_systolic - avg_diastolic
unit_concept_id_bp = 8876  # mmHg

def toSqlRows(
        group: stypes.GroupedMeasurements,
    ) -> List[stypes.SqlRow]:
        
    """Generate SqlRows for the measurement table."""
    rows:List[stypes.SqlRow] = []
    visit_occurrence_id = group["visit_occurrence_id"]
    person_id = group["person_id"]
    for (concept_id, item) in group["measurements"].items():
        for (event_datetime, value) in item["datetime_value"].items():
            r: stypes.SqlRow = {
                "measurement_concept_id": concept_id,
                "person_id": person_id,
                "visit_occurrence_id": visit_occurrence_id,
                "measurement_datetime": event_datetime,
                "measurement_date": event_datetime.date(),
                "measurement_type_concept_id": item["measurement_type_concept_id"],
                "unit_concept_id": item["unit_concept_id"],
                "value_as_number": value,
            }
            rows.append(r)
    return rows

def get_diastolic_from_systolic(systolic:List[float], avg_difference: float) -> float:
    """Estimate diastolic value from systolic value."""
    return [s - avg_difference for s in systolic]

def generate_bp_rows_for_dates(
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    event_datetimes: List[dt.datetime],
    src_stats: stypes.SrcStats,
) -> List[tuple[str, stypes.SqlRow]]:
    """Generate events for a visit occurrence, at a given rate with a given generator.

    This is a utility function for generating multiple rows for one of the "event"
    tables (measurements, observation, etc.).
    """
    person_id = cast(int, person["person_id"])
    visit_occurrence_id = cast(int, visit_occurrence["visit_occurrence_id"])
    gender = cast(int, person["gender_concept_id"])
    age = (cast(dt.datetime, visit_occurrence["visit_start_datetime"]) - cast(dt.datetime,
                                                                            person["birth_datetime"])).days / 365.25

    main_key = 'bp_profile'
    relative_change_key = 'bp_sys_relative_change_stats'
    if age < 60:
        key_mean = 'average_under_60_systolic'
        key_std = 'stddev_under_60_systolic'
        key_epsilon_mean = 'avg_under_60_systolic_rel_var'
        key_epsilon_std = 'stddev_under_60_systolic_rel_var'
    else:
        key_mean = 'average_over_60_systolic'
        key_std = 'stddev_over_60_systolic'
        key_epsilon_mean = 'avg_over_60_systolic_rel_var'
        key_epsilon_std = 'stddev_over_60_systolic_rel_var'

    if gender == 8507:
        index_gender = 0
    else:
        index_gender = 1

    sample_epsilon = np.random.normal(src_stats[relative_change_key][index_gender][key_epsilon_mean],
                                    src_stats[relative_change_key][index_gender][key_epsilon_std], 1)[0]

    systolic_values = np.round(generate_time_series(len(event_datetimes), 'random_walk',
                                                {'mean': src_stats[main_key][index_gender][key_mean],
                                                    'std': src_stats[main_key][index_gender][key_std],
                                                    'epsilon_std': sample_epsilon, 'drift': 0}))
    diastolic_values = get_diastolic_from_systolic(systolic_values, src_stats[main_key][index_gender]["average_systolic_diastolic_difference"])

    bp_group: stypes.GroupedMeasurements = {
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "measurements": {
            Systolic_blood_pressure_by_Noninvasive: {
                "unit_concept_id": unit_concept_id_bp,
                "measurement_type_concept_id": measurement_type_concept_id_bp,
                "generator": lambda x: x,
                "datetime_value": {event_datetimes[i]: systolic_values[i] for i in range(len(event_datetimes))}
            },
            Diastolic_blood_pressure_by_Noninvasive: {
                "unit_concept_id": unit_concept_id_bp,
                "measurement_type_concept_id": measurement_type_concept_id_bp,
                "generator": lambda x: x,
                "datetime_value": {event_datetimes[i]: diastolic_values[i] for i in range(len(event_datetimes))}
            }
        }
    }
    
    bp_rows = toSqlRows(bp_group)
    # events: list[tuple[str, stypes.SqlRow]] = [("measurement", row) for row in bp_rows]
    # for idx, (table_name, row) in enumerate(events[:1]):
    #     logger.debug(f"\nSample {idx} of {len(events)}: {table_name} {row}")
    return [("measurement", bp_rows[0])]
