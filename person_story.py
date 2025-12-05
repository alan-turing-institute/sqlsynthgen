"""Story generators for the CC HIC OMOP schema."""
import datetime as dt
from typing import Generator,Optional, cast
from sqlsynthgen.utils import generate_time_series
import numpy as np
from mimesis import Generic
import random
from blood_pressure_story import generate_bp_rows_for_dates
import story_types as stypes

def random_normal(mean: float, std_dev: Optional[float] = None) -> float:
    """Return a normal distributed value with the given mean and standard deviation.

    If no standard devation is given, we assume it to be sqrt(abs(mean)).
    """
    return cast(
        float,
        np.random.normal(mean, std_dev if std_dev is not None else np.sqrt(abs(mean))),
    )


def gen_death(
        generic: Generic, person: stypes.SqlRow, src_stats: stypes.SrcStats
) -> Optional[tuple[str, stypes.SqlRow]]:
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
        person: stypes.SqlRow, death: Optional[stypes.SqlRow], src_stats: stypes.SrcStats
) -> tuple[str, stypes.SqlRow]:
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


def random_event_times(avg_rate: float, visit_occurrence: stypes.SqlRow) -> list[dt.datetime]:
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
    return sorted(datetimes)


def generate(
        generic: Generic,
        src_stats: stypes.SrcStats,
) -> Generator[tuple[str, stypes.SqlRow], stypes.SqlRow, None]:
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
    avg_rate = abs(random_normal(
        src_stats["avg_measurements_per_visit_hour"][0]['avg_measurements_per_hour'],
        src_stats["avg_measurements_per_visit_hour"][0]['stddev_measurements_per_hour'])
    )

    print(f"Generating blood pressure events at an average rate of {avg_rate} per hour. Using IID sampling.")
    bp_rows = generate_bp_rows_for_dates(
        person=person,  
        visit_occurrence=visit_occurrence,
        event_datetimes=random_event_times(avg_rate, visit_occurrence),
        src_stats=src_stats,
    )
    yield [("measurement", row) for row in bp_rows]
