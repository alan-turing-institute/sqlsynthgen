import datetime as dt
import random
from typing import Any, Dict, Generator, Tuple
import sqlalchemy as sqla


def short_story(
    generic: Any,
) -> Generator[Tuple[str, Dict[str, Any]], Dict[str, Any], None]:
    yield ("person", {"name": generic.person.first_name()})


def long_story(
    dst_db_conn: Any, generic: Any, count_opt_outs: list
) -> Generator[Tuple[str, Dict[str, Any]], Dict[str, Any], None]:
    # Find out whether this person opts out
    count_false = next(s[0] for s in count_opt_outs if s[1] is False)
    count_true = next(s[0] for s in count_opt_outs if s[1] is True)
    opt_out_rate = count_true / (count_true + count_false)
    opt_out = generic.weighted_boolean_provider.bool(opt_out_rate)

    # Find the primary key ID of the concept "another concept"
    concept_query = sqla.text(
        "SELECT concept_id FROM concept WHERE concept_name = 'another concept'"
    )
    concept_id = dst_db_conn.execute(concept_query).first()[0]

    # Make a story where we first create a person, and then give that person two visits.
    person = yield ("person", {"research_opt_out": opt_out})
    visit1 = yield (
        "hospital_visit",
        {
            "person_id": person["person_id"],
            "visit_type_concept_id": concept_id,
        },
    )

    visit1_start = visit1["visit_start"]
    visit1_duration_seconds = visit1["visit_duration_seconds"]

    visit2_start = visit1_start + dt.timedelta(days=365)
    visit2_duration_seconds = visit1_duration_seconds * 2
    visit2_end = visit2_start + dt.timedelta(seconds=visit2_duration_seconds)
    visit2 = {
        "person_id": person["person_id"],
        "visit_type_concept_id": concept_id,
        "visit_start": visit2_start,
        "visit_end": visit2_end,
        "visit_duration_seconds": visit2_duration_seconds,
    }
    yield ("hospital_visit", visit2)
