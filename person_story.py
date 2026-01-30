"""Story generators for the CC HIC OMOP schema."""
from asyncio import events
import datetime as dt
from typing import Generator, Optional, cast
from sqlsynthgen.utils_values import random_normal, random_event_times
from mimesis import Generic
import random
from measurement_registry import dispatch_measurement_generators
from procedure_registry import dispatch_procedure_generators
import story_types as stypes


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
        std_dev_age_at_death_days = (
            src_stats["age_at_death"][0]["stddev_age_years"] * 365
        )
        age_at_death_days = abs(
            random_normal(
                cast(float, avg_age_at_death_days),
                cast(float, std_dev_age_at_death_days),
            )
        )
        death_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
            days=age_at_death_days
        )
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
        random_normal(cast(float, 63 * 365), cast(float, 13 * 365))
    )
    if person["gender_concept_id"] == 8532:
        age_days_at_visit_start = abs(
            random_normal(
                src_stats["age_first_admission"][0]["average_age_years"] * 365,
                src_stats["age_first_admission"][0]["stddev_age_years"] * 365,
            )
        )
    if person["gender_concept_id"] == 8507:
        age_days_at_visit_start = abs(
            random_normal(
                src_stats["age_first_admission"][1]["average_age_years"] * 365,
                src_stats["age_first_admission"][1]["stddev_age_years"] * 365,
            )
        )
    visit_start_datetime = cast(dt.datetime, person["birth_datetime"]) + dt.timedelta(
        days=age_days_at_visit_start
    )
    visit_length_hours = abs(
        random_normal(
            src_stats["visit_duration"][0]["average_hours"],
            src_stats["visit_duration"][0]["stddev_hours"]
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

    # generate measurements that occur during the visit
    choice_idx = random.choices(
        range(len(src_stats["unique_measurements_in_visits"])),
        weights=[
            src_stats["unique_measurements_in_visits"][i]["percent_of_total"]
            for i in range(len(src_stats["unique_measurements_in_visits"]))
        ],
    )[0]
    measurement_tokens = [
        token for token in src_stats["unique_measurements_in_visits"][choice_idx][
            "measurement_type_ids"
        ].split(",")
        if token
    ]

    for event in dispatch_measurement_generators(
        measurement_tokens, person, visit_occurrence, src_stats
    ):
        yield event

    # generate procedures that occur during the visit, these keep a relationship to the measurements
    procedure_tokens = [
        token
        for token in src_stats["unique_measurements_in_visits"][choice_idx][
            "procedure_type_ids"
        ].split(",")
        if token
    ]

    print(procedure_tokens)

    #TODO: UNDERSTAND WHY PROCEDURES ARE NOT BEING RECORDED
    # not all visits have procedures, but if they do, generate them
    if procedure_tokens:
        for event in dispatch_procedure_generators(
            procedure_tokens, person, visit_occurrence, src_stats
        ):
            yield event
