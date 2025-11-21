"""Story generators for the CC HIC OMOP schema."""
import datetime as dt
from typing import Callable, Generator, Optional, Union, cast
from sqlsynthgen.utils import generate_time_series
import numpy as np
from mimesis import Generic
import random

SqlValue = Union[float, int, str, bool, dt.datetime, dt.date, None]
SqlRow = dict[str, SqlValue]
SrcStatsResult = list[SqlRow]
SrcStats = dict[str, SrcStatsResult]


def random_normal(mean: float, std_dev: Optional[float] = None) -> float:
    """Return a normal distributed value with the given mean and standard deviation.

    If no standard devation is given, we assume it to be sqrt(abs(mean)).
    """
    return cast(
        float,
        np.random.normal(mean, std_dev if std_dev is not None else np.sqrt(abs(mean))),
    )


def gen_death(
        generic: Generic, person: SqlRow, src_stats: SrcStats
) -> Optional[tuple[str, SqlRow]]:
    """Generate a row for the death table."""

    def with_probability(p: float) -> bool:
        """Return True with probability p (0 ≤ p ≤ 1)."""
        return random.random() < p

    if with_probability(src_stats["proportion_alive"][0]["proportion_alive"]):
        return None
    else:
        avg_age_at_death_days = src_stats["age_at_death"][0]["average_age_years"] * 365
        std_dev_age_at_death_days = src_stats["age_at_death"][0]["stddev_age_years"] * 365
        age_at_death_days = abs(
            random_normal(cast(float, avg_age_at_death_days), cast(float, std_dev_age_at_death_days)))
        death_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
            days=age_at_death_days)
        return "death", {
            "person_id": person["person_id"],
            "death_datetime": death_datetime,
            "death_date": death_datetime.date(),
        }


def gen_visit_occurrence(
        person: SqlRow, death: Optional[SqlRow], src_stats: SrcStats
) -> tuple[str, SqlRow]:
    """Generate a row for the visit_occurrence table."""
    age_days_at_visit_start = abs(
        random_normal(
            cast(float, 63 * 365), cast(float, 13 * 365)
        )
    )
    if person["gender_concept_id"] == 8532:
        age_days_at_visit_start = abs(
            random_normal(
                cast(float, src_stats["age_first_admission"][0]["average_age_years"] * 365),
                cast(float, src_stats["age_first_admission"][0]["stddev_age_years"] * 365)
            )
        )
    if person["gender_concept_id"] == 8507:
        age_days_at_visit_start = abs(
            random_normal(
                cast(float, src_stats["age_first_admission"][1]["average_age_years"] * 365),
                cast(float, src_stats["age_first_admission"][1]["stddev_age_years"] * 365)
            )
        )
    visit_start_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
        days=age_days_at_visit_start
    )
    visit_length_hours = abs(
        random_normal(
            cast(float, src_stats["visit_duration"][0]["average_hours"]),
            cast(float, src_stats["visit_duration"][0]["stddev_hours"])
            # cast(float, 6), cast(float, 29*24)
        )
    )
    visit_end_datetime = visit_start_datetime + dt.timedelta(hours=visit_length_hours)
    if death:
        visit_start_datetime = min(
            visit_start_datetime, cast(dt.datetime, death["death_datetime"])
        )
        visit_end_datetime = min(
            visit_end_datetime, cast(dt.datetime, death["death_datetime"])
        )
    return (
        "visit_occurrence",
        {
            "person_id": person["person_id"],
            "visit_start_datetime": visit_start_datetime,
            "visit_start_date": visit_start_datetime.date(),
            "visit_end_datetime": visit_end_datetime,
            "visit_end_date": visit_end_datetime.date(),
        },
    )


def random_event_times(avg_rate: float, visit_occurrence: SqlRow) -> list[dt.datetime]:
    """Return random times during a visit, occurring roughly at the given rate."""
    start = cast(dt.datetime, visit_occurrence["visit_start_datetime"])
    end = cast(dt.datetime, visit_occurrence["visit_end_datetime"])
    period = end - start
    events_per_hour = abs(random_normal(avg_rate))
    period_hours = period.seconds / 3600
    num_events = int(round(events_per_hour * period_hours))
    datetimes = [
        start + period * cast(float, fraction)
        for fraction in np.random.uniform(size=num_events)
    ]
    return datetimes


def gen_events(  # pylint: disable=too-many-arguments
        generic: Generic,
        avg_rate: float,
        visit_occurrence: SqlRow,
        person: SqlRow,
        generator_function: Callable[
            [Generic, int, int, dt.datetime, SrcStats], Optional[SqlRow]
        ],
        table_name: str,
        src_stats: SrcStats,
) -> list[tuple[str, SqlRow]]:
    """Generate events for a visit occurrence, at a given rate with a given generator.

    This is a utility function for generating multiple rows for one of the "event"
    tables (measurements, observation, etc.).
    """
    event_datetimes = random_event_times(avg_rate, visit_occurrence)
    events: list[tuple[str, SqlRow]] = []
    for event_datetime in sorted(event_datetimes):
        event = generator_function(
            generic,
            cast(int, person["person_id"]),
            cast(int, visit_occurrence["visit_occurrence_id"]),
            event_datetime,
            src_stats,
        )
        if event is not None:
            events.append((table_name, event))
    return events


def gen_blood_pressure_events(  # pylint: disable=too-many-arguments
        avg_rate: float,
        visit_occurrence: SqlRow,
        person: SqlRow,
        src_stats: SrcStats,
) -> list[tuple[str, SqlRow]]:
    """Generate events for a visit occurrence, at a given rate with a given generator.

    This is a utility function for generating multiple rows for one of the "event"
    tables (measurements, observation, etc.).
    """

    def generate_paired_measurement(
            person_id: int,
            visit_occurrence_id: int,
            event_datetime: dt.datetime,
            values: tuple[float, float],
            measurement_concept_id: tuple[int, int],
            measurement_type_concept_ids: int,
            unit_concept_id: int,
            unit_source_value: str,
    ) -> tuple[SqlRow, SqlRow]:

        ### This can be abastracted to generate any number of set of measurements
        """Generate two rows for the measurement table."""
        measurement1: SqlRow = {
            "measurement_concept_id": cast(int, measurement_concept_id[0]),
            "person_id": person_id,
            "visit_occurrence_id": visit_occurrence_id,
            "measurement_datetime": event_datetime,
            "measurement_date": event_datetime.date(),
            "measurement_type_concept_id": measurement_type_concept_ids,
            "unit_concept_id": unit_concept_id,
            "unit_source_value": unit_source_value,
            "value_as_number": values[0],
        }

        measurement2: SqlRow = {
            "measurement_concept_id": cast(int, measurement_concept_id[1]),
            "person_id": person_id,
            "visit_occurrence_id": visit_occurrence_id,
            "measurement_datetime": event_datetime,
            "measurement_date": event_datetime.date(),
            "measurement_type_concept_id": measurement_type_concept_ids,
            "unit_concept_id": unit_concept_id,
            "unit_source_value": unit_source_value,
            "value_as_number": values[1],
        }
        return measurement1, measurement2

    event_datetimes = random_event_times(avg_rate, visit_occurrence)

    if len(event_datetimes) == 0:
        return []

    # can we get this from the data?
    sys_bp_non_invasive_concept_id = 21492239
    dias_bp_non_invasive_concept_id = 21492240
    measurement_type_concept_id = 32817  # EHR measurement
    unit_source_value = "mmHg"
    unit_concept_id = 8876  # mmHg

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
                                      src_stats[relative_change_key][index_gender][key_epsilon_std], 1)

    systolic_value = np.round(generate_time_series(len(event_datetimes), 'random_walk',
                                                   {'mean': src_stats[main_key][index_gender][key_mean],
                                                    'std': src_stats[main_key][0][key_std],
                                                    'epsilon_std': sample_epsilon, 'drift': 0},
                                                   random_state=42))

    # diastolic value is calculated based on systolic value plus the average difference extrated from data
    # we add some variation to the difference between systolic and diastolic
    diastolic_value = np.round(systolic_value - random_normal(src_stats[main_key][index_gender]['average_systolic_diastolic_difference'],
                                             src_stats[main_key][index_gender][
                                                 "average_systolic_diastolic_difference"] * 0.1) )

    events: list[tuple[str, SqlRow]] = []
    for index, event_datetime in enumerate(sorted(event_datetimes)):
        systolic_dict, diastolic_dict = generate_paired_measurement(cast(int, person["person_id"]),
                                                                    cast(int, visit_occurrence["visit_occurrence_id"]),
                                                                    event_datetime,
                                                                    (systolic_value[index], diastolic_value[index]),
                                                                    (sys_bp_non_invasive_concept_id,
                                                                     dias_bp_non_invasive_concept_id),
                                                                    measurement_type_concept_id, unit_concept_id,
                                                                    unit_source_value)
        events.append(("measurement", systolic_dict)),
        events.append(("measurement", diastolic_dict))
    return events


def generate(
        generic: Generic,
        src_stats: SrcStats,
) -> Generator[tuple[str, SqlRow], SqlRow, None]:
    """Yield all the data related to a single patient.

    This includes, in order
    * a row for the `person` table
    * possibly a row for `death`, if the patient has died
    * rows for `visit_occurence` and `observation_period`
    * possibly multiple rows, depending on the length of the hospital stay, for
        * `condition_occurrence`
        * `measurement`
        * `device_exposure`
        * `observation`
        * `procedure_occurrence`
        * `specimen`
        * `drug_exposure`
    """
    person = yield "person", {}
    death = gen_death(generic, person, src_stats)
    death_row = (yield death) if death else None
    visit_occurrence = yield gen_visit_occurrence(person, death_row, src_stats)

    # abs to avoid negative rates due to random normal variation
    # abs to avoid negative rates due to random normal variation
    avg_rate = abs(random_normal(
        src_stats["avg_measurements_per_visit_hour"][0]['avg_measurements_per_hour'],
        src_stats["avg_measurements_per_visit_hour"][0]['stddev_measurements_per_hour'])
    )

    print(f"Generating blood pressure events at an average rate of {avg_rate} per hour.")
    for event in gen_blood_pressure_events(
            avg_rate,
            visit_occurrence,
            person,
            src_stats,
    ):
        # Yield each measurement event if is not empty dictionary
        if len(event) > 0:
            yield event
