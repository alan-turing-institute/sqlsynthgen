"""Story generators for the CC HIC OMOP schema."""
import datetime as dt
from typing import Callable, Generator, Optional, Union, cast

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
        avg_age_at_death_days = 70*365
        std_dev_age_at_death_days = 11*365
        age_at_death_days = abs(random_normal(cast(float,avg_age_at_death_days), cast(float,std_dev_age_at_death_days)))
        death_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
            days=age_at_death_days)
        #     death_datetime = dt.datetime(1960, 1, 1)
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
    visit_start_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
        days=age_days_at_visit_start
    )
    visit_length_hours = abs(
        random_normal(
            cast(float, 6), cast(float, 29*24)
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


def assign_categoricals(
    generic: Generic,
    row: SqlRow,
    categoricals_result: SrcStatsResult,
    categorical_columns: list[str],
    filter_dict: dict[str, SqlValue],
) -> SqlRow:
    """Add to a row categorical variables sampled from a query result.

    This is a utility function for sampling from a group by query and assigning the
    results to a row dictionary, used by the event generators.
    """
    result = cast(
        tuple[SqlValue, ...],
        generic.sql_group_by_provider.sample(
            categoricals_result,
            weights_column="num",
            value_columns=categorical_columns,
            filter_dict=filter_dict,
        ),
    )
    for column_name, value in zip(categorical_columns, result):
        row[column_name] = value
    return row

def gen_measurement(
    generic: Generic,
    person_id: int,
    visit_occurrence_id: int,
    event_datetime: dt.datetime,
    src_stats: SrcStats,
) -> Optional[SqlRow]:
    """Generate a row for the measurement table."""
    concept_id = cast(
        int,
        generic.sql_group_by_provider.sample(
            src_stats["count_measurements"],
            weights_column="num",
            value_columns="measurement_concept_id",
        ),
    )
    row: SqlRow = {
        "measurement_concept_id": concept_id,
        "person_id": person_id,
        "visit_occurrence_id": visit_occurrence_id,
        "measurement_datetime": event_datetime,
        "measurement_date": event_datetime.date(),
    }

    categorical_columns = [
        "measurement_type_concept_id",
        "operator_concept_id",
        "value_as_concept_id",
        "unit_concept_id",
        "value_as_number_sign",
        "range_low_sign",
        "range_high_sign",
        "provider_id",
        "visit_detail_id",
        "measurement_source_value",
        "measurement_source_concept_id",
        "unit_source_value",
    ]
    try:
        assign_categoricals(
            generic,
            row,
            src_stats["measurement_categoricals"],
            categorical_columns,
            filter_dict={"measurement_concept_id": concept_id},
        )
    except ValueError:
        print(f"No data for measurement of id {concept_id}")
        return None

    for key in ("value_as_number", "range_low", "range_high"):
        key_sign = key + "_sign"
        sign = cast(str, row[key_sign])
        if sign == "NULL":
            row[key] = None
        else:
            try:
                avg = next(
                    cast(float, row["avg_value"])
                    for row in src_stats["avg_measurement_value_as_number"]
                    if row["measurement_concept_id"] == concept_id
                )
            except StopIteration:
                print(f"No mean value for measurement of id {concept_id}")
                return None
            # To fix: Improve generating negative values. This method produces too few
            # negative values for variables that can be negative.
            value = random_normal(avg)
            if sign == ">=0":
                value = abs(value)
            row[key] = value
        del row[key_sign]
    return row

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
    
    def gen_events_for_patient(
        rate_query_name: str,
        gen_func: Callable[
            [Generic, int, int, dt.datetime, SrcStats], Optional[SqlRow]
        ],
        table_name: str,
    ) -> list[tuple[str, SqlRow]]:
        return gen_events(
            generic,
            cast(float, src_stats[rate_query_name][0]["avg_frequency_per_hour"]),
            visit_occurrence,
            person,
            gen_func,
            table_name,
            src_stats,
        )

    for event in gen_events_for_patient(
        "bp_measurements",
        gen_measurement,
        "measurement",
    ):
        yield event
