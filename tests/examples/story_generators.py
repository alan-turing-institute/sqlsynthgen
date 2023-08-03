import datetime as dt
import random
from typing import Any, Dict, Generator, Tuple
import sqlalchemy as sqla


def short_story(
    generic: Any,
) -> Generator[Tuple[str, Dict[str, Any]], Dict[str, Any], None]:
    # Create one row in the person table and override the default name
    yield "person", {"name": generic.person.first_name()}


def full_row_story(
    generic: Any,
) -> Generator[Tuple[str, Dict[str, Any]], Dict[str, Any], None]:
    # Create a row in the person table, providing values for all columns. This used to
    # crash, and is kept to prevent regression.
    yield "person", {
        # The randint thing could in principle cause a collision, but eh, we are not
        # going to be _that_ unlucky are we? There should only ever be a handful of rows
        # in the dst db.
        "person_id": random.randint(666, 666666666),
        "name": generic.person.first_name(),
        "research_opt_out": False,
        "stored_from": dt.datetime(year=1970, month=1, day=1),
    }


def long_story(
    dst_db_conn: Any, generic: Any, count_opt_outs: list
) -> Generator[Tuple[str, Dict[str, Any]], Dict[str, Any], None]:
    # Find out whether this person opts out
    count_false = int(
        next(row["num"] for row in count_opt_outs if row["research_opt_out"] is False)
    )
    count_true = int(
        next(row["num"] for row in count_opt_outs if row["research_opt_out"] is True)
    )
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
