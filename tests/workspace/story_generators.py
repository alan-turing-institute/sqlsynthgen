import datetime as dt
import random
from typing import Any, Dict
import sqlalchemy as sqla


def short_story(generic: Any) -> Dict[str, list]:
    story = {}
    story["person"] = [{"name": generic.person.first_name()}]
    return story


def long_story(dst_db_conn: Any, generic: Any, count_opt_outs: list) -> Dict[str, list]:
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
    story = {}
    story["person"] = [{"research_opt_out": opt_out}]
    story["hospital_visit"] = [
        {
            "person_id": lambda: story["person"][0]["person_id"],
            "visit_type_concept_id": concept_id,
        }
    ]

    # Make the second visit be exactly a year after the first, and be twice as long.
    def second_visit():
        visit1_start = story["hospital_visit"][0]["visit_start"]
        visit1_duration_seconds = story["hospital_visit"][0]["visit_duration_seconds"]

        visit2_start = visit1_start + dt.timedelta(days=365)
        visit2_duration_seconds = visit1_duration_seconds * 2
        visit2_end = visit2_start + dt.timedelta(seconds=visit2_duration_seconds)
        return {
            "person_id": story["person"][0]["person_id"],
            "visit_type_concept_id": concept_id,
            "visit_start": visit2_start,
            "visit_end": visit2_end,
            "visit_duration_seconds": visit2_duration_seconds,
        }

    story["hospital_visit"].append(second_visit)
    return story
