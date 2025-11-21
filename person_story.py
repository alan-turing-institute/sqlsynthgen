"""Story generators for the CC HIC OMOP schema."""
import datetime as dt
from typing import Callable, Generator, List, Optional, Union, cast

import numpy as np
from mimesis import Generic
import random

SqlValue = Union[float, int, str, bool, dt.datetime, dt.date, None]
SqlRow = dict[str, SqlValue]
SrcStatsResult = list[SqlRow]
SrcStats = dict[str, SrcStatsResult]
from typing import TypedDict, Callable, List, Optional, Dict, Union

class SingularMeasurement(TypedDict):
    values: list[float] | list[int]

class GroupedMeasurements(TypedDict):
    datetime: dt.datetime
    person_id: int
    visit_occurrence_id: int
    concepts: Dict[int, tuple[int, int]]
    values: Dict[int, List[SingularMeasurement]]
    generators: Dict[int, Callable[[int|float], int|float]]


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
        age_at_death_days = abs(random_normal(cast(float,avg_age_at_death_days), cast(float,std_dev_age_at_death_days)))
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
            cast(float, 63*365), cast(float, 13*365)
        )
    )
    if person["gender_concept_id"] == 8532:
        age_days_at_visit_start = abs(
            random_normal(
                cast(float, src_stats["age_first_admission"][0]["average_age_years"]*365), 
                cast(float, src_stats["age_first_admission"][0]["stddev_age_years"]*365)
            )
        )
    if person["gender_concept_id"] == 8507:
        age_days_at_visit_start = abs(
            random_normal(
                cast(float, src_stats["age_first_admission"][1]["average_age_years"]*365), 
                cast(float, src_stats["age_first_admission"][1]["stddev_age_years"]*365)
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

    def populate_blood_pressure_values(
        person_id: int,
        visit_occurrence_id: int,
        event_datetime: dt.datetime,
    ) -> tuple[SqlRow, SqlRow]:
        
        Systolic_blood_pressure_by_Noninvasive = 21492239
        Diastolic_blood_pressure_by_Noninvasive = 21492240
        measurement_type_concept_id = 32817 # EHR measurement
        avg_systolic = 114.236842
        avg_diastolic = 74.447368
        avg_difference = avg_systolic - avg_diastolic
        unit_concept_id = 8876  # mmHg

        gender = cast(int, person["gender_concept_id"])
        if gender == 8507:
            systolic_value = random_normal(src_stats["bp_profile"][0]["average_under_60_systolic"],src_stats["bp_profile"][0]["stddev_under_60_systolic"])
            diastolic_value = src_stats["bp_profile"][0]["average_systolic_diastolic_difference"] + systolic_value
        elif gender == 8532:
            systolic_value = random_normal(src_stats["bp_profile"][1]["average_under_60_systolic"],src_stats["bp_profile"][1]["stddev_under_60_systolic"])
            diastolic_value = src_stats["bp_profile"][1]["average_systolic_diastolic_difference"] + systolic_value
        else:
            systolic_value = avg_systolic
            diastolic_value = avg_diastolic

        """Generate two rows for the measurement table."""
        systolic: SqlRow = {
            "measurement_concept_id": cast(int, Systolic_blood_pressure_by_Noninvasive),
            "person_id": person_id,
            "visit_occurrence_id": visit_occurrence_id,
            "measurement_datetime": event_datetime,
            "measurement_date": event_datetime.date(),
            "measurement_type_concept_id": measurement_type_concept_id,
            "unit_concept_id": unit_concept_id,
            "unit_source_value": "mmHg",
            "value_as_number": systolic_value,
        }

        diastolic: SqlRow = {
            "measurement_concept_id": cast(int, Diastolic_blood_pressure_by_Noninvasive),
            "person_id": person_id,
            "visit_occurrence_id": visit_occurrence_id,
            "measurement_datetime": event_datetime,
            "measurement_date": event_datetime.date(),
            "measurement_type_concept_id": measurement_type_concept_id,
            "unit_concept_id": unit_concept_id,
            "unit_source_value": "mmHg",
            "value_as_number": diastolic_value,
        }
        return systolic, diastolic
    
    event_datetimes = random_event_times(avg_rate, visit_occurrence)
    events: list[tuple[str, SqlRow]] = []
    for event_datetime in sorted(event_datetimes):
        systolic, diastolic = populate_blood_pressure_values(cast(int, person["person_id"]),
            cast(int, visit_occurrence["visit_occurrence_id"]),
            event_datetime)
        events.append(("measurement", systolic))
        events.append(("measurement", diastolic))
    return events

def populate_group_measurement(
    person: SqlRow,
    visit_occurrence: SqlRow,
    src_stats: SrcStats,
) -> List[tuple[str, SqlRow]]:
    """Generate events for a visit occurrence, at a given rate with a given generator.

    This is a utility function for generating multiple rows for one of the "event"
    tables (measurements, observation, etc.).
    """

    Systolic_blood_pressure_by_Noninvasive = 21492239
    Diastolic_blood_pressure_by_Noninvasive = 21492240
    measurement_type_concept_id = 32817 # EHR measurement
    avg_systolic = 114.236842
    avg_diastolic = 74.447368
    avg_difference = avg_systolic - avg_diastolic
    unit_concept_id = 8876  # mmHg
    
    def get_diastolic_from_systolic(systolic: List[float]) -> float:
        """Estimate diastolic value from systolic value."""
        return [s - avg_difference for s in systolic]

    def timeseries(length: int) -> float:
        """Estimate diastolic value from systolic value."""
        return [0] * length

    generators: dict[str, Callable[[int|float], int|float]] = {
        "timeseries": timeseries,
        "diastolic": get_diastolic_from_systolic
    }

    m: GroupedMeasurements = {
        "concepts": {Systolic_blood_pressure_by_Noninvasive: (measurement_type_concept_id, unit_concept_id),
                     Diastolic_blood_pressure_by_Noninvasive: (measurement_type_concept_id, unit_concept_id)},
        "values": {Systolic_blood_pressure_by_Noninvasive: [], 
                   Diastolic_blood_pressure_by_Noninvasive: []},
        "generators": {Systolic_blood_pressure_by_Noninvasive: generators["timeseries"],
                       Diastolic_blood_pressure_by_Noninvasive: generators["diastolic"]},
        "datetime": dt.datetime.now(),
        "person_id": cast(int, person["person_id"]),
        "visit_occurrence_id": cast(int, visit_occurrence["visit_occurrence_id"]),
    }

    m["values"][Systolic_blood_pressure_by_Noninvasive] = generators["timeseries"](10)
    m["values"][Diastolic_blood_pressure_by_Noninvasive] = generators["diastolic"](m["values"][Systolic_blood_pressure_by_Noninvasive])

    def populate_values(
        event_datetime: dt.datetime,
    ) -> dict[int, SqlRow]:
        
        """Generate two rows for the measurement table."""
        r: SqlRow = {
            "measurement_concept_id": m.concept_id,
            "person_id": m.person_id,
            "visit_occurrence_id": visit_occurrence_id,
            "measurement_datetime": event_datetime,
            "measurement_date": event_datetime.date(),
            "measurement_type_concept_id": m.type_concept_id,
            "unit_concept_id": m.unit_concept_id,
            "value_as_number": abs(random_normal(m.properties["average_value"], m.properties["stddev_value"])),
        }

        return r
    
    event_datetimes = random_event_times(10.0, visit_occurrence)
    events: list[tuple[str, SqlRow]] = []
    for event_datetime in sorted(event_datetimes):
        systolic, diastolic = populate_values(cast(int, person["person_id"]),
            cast(int, visit_occurrence["visit_occurrence_id"]),
            event_datetime)
        events.append(("measurement", systolic))
        events.append(("measurement", diastolic))
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

    for event in gen_blood_pressure_events(
        10.0,
        visit_occurrence,
        person,
        src_stats,
    ):
        yield event
