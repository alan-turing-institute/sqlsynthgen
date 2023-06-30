"""This file was auto-generated by sqlsynthgen but can be edited manually."""
from mimesis import Generic
from mimesis.locales import Locale
from sqlsynthgen.base import FileUploader
from sqlsynthgen.unique_generator import UniqueGenerator

generic = Generic(locale=Locale.EN_GB)

from sqlsynthgen.providers import BytesProvider

generic.add_provider(BytesProvider)
from sqlsynthgen.providers import ColumnValueProvider

generic.add_provider(ColumnValueProvider)
from sqlsynthgen.providers import TimedeltaProvider

generic.add_provider(TimedeltaProvider)
from sqlsynthgen.providers import TimespanProvider

generic.add_provider(TimespanProvider)
from sqlsynthgen.providers import WeightedBooleanProvider

generic.add_provider(WeightedBooleanProvider)

import tests.examples.example_orm
import row_generators
import story_generators

import yaml

with open("example_stats.yaml", "r", encoding="utf-8") as f:
    SRC_STATS = yaml.unsafe_load(f)

concept_vocab = FileUploader(tests.examples.example_orm.Concept.__table__)


class entityGenerator:
    num_rows_per_pass = 1

    def __init__(self):
        pass

    def __call__(self, dst_db_conn):
        result = {}
        return result


class personGenerator:
    num_rows_per_pass = 2

    def __init__(self):
        pass
        self.unique_nhs_number_uniq = UniqueGenerator(["nhs_number"], "person")

    def __call__(self, dst_db_conn):
        result = {}
        result["name"] = generic.person.full_name()
        result["stored_from"] = generic.datetime.datetime(2022, 2022)
        result["research_opt_out"] = row_generators.boolean_from_src_stats_generator(
            generic=generic, src_stats=SRC_STATS["count_opt_outs"]
        )
        result["nhs_number"] = self.unique_nhs_number_uniq(
            dst_db_conn, None, generic.text.color
        )
        result["source_system"] = generic.text.color()
        return result


class test_entityGenerator:
    num_rows_per_pass = 1

    def __init__(self):
        pass

    def __call__(self, dst_db_conn):
        result = {}
        result["single_letter_column"] = generic.person.password(1)
        return result


class hospital_visitGenerator:
    num_rows_per_pass = 3

    def __init__(self):
        pass

    def __call__(self, dst_db_conn):
        result = {}
        (
            result["visit_start"],
            result["visit_end"],
            result["visit_duration_seconds"],
        ) = row_generators.timespan_generator(
            generic, 2021, 2022, min_dt_days=1, max_dt_days=30
        )
        result["person_id"] = generic.column_value_provider.column_value(
            dst_db_conn, tests.examples.example_orm.Person, "person_id"
        )
        result["visit_image"] = generic.bytes_provider.bytes()
        return result


table_generator_dict = {
    "entity": entityGenerator(),
    "person": personGenerator(),
    "test_entity": test_entityGenerator(),
    "hospital_visit": hospital_visitGenerator(),
}


vocab_dict = {
    "concept": concept_vocab,
}


def run_story_generators_short_story(dst_db_conn):
    return story_generators.short_story(generic)


def run_story_generators_full_row_story(dst_db_conn):
    return story_generators.full_row_story(generic)


def run_story_generators_long_story(dst_db_conn):
    return story_generators.long_story(
        dst_db_conn=dst_db_conn,
        generic=generic,
        count_opt_outs=SRC_STATS["count_opt_outs"],
    )


story_generator_list = [
    {
        "name": run_story_generators_short_story,
        "num_stories_per_pass": 3,
    },
    {
        "name": run_story_generators_full_row_story,
        "num_stories_per_pass": 1,
    },
    {
        "name": run_story_generators_long_story,
        "num_stories_per_pass": 2,
    },
]
