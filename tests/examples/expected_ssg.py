"""This file was auto-generated by sqlsynthgen but can be edited manually."""
from mimesis import Generic
from mimesis.locales import Locale
from sqlsynthgen.base import FileUploader

generic = Generic(locale=Locale.EN)

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
import custom_generators
import yaml
with open("example_stats.yaml", "r", encoding="utf-8") as f:
    SRC_STATS = yaml.load(f, Loader=yaml.FullLoader)

concept_vocab = FileUploader(tests.examples.example_orm.Concept.__table__)

class entityGenerator:
    num_rows_per_pass = 1

    def __init__(self, src_db_conn, dst_db_conn):
        pass


class personGenerator:
    num_rows_per_pass = 2

    def __init__(self, src_db_conn, dst_db_conn):
        self.name = generic.person.full_name()
        self.stored_from = generic.datetime.datetime(start=2022, end=2022)
        self.research_opt_out = custom_generators.boolean_from_src_stats_generator(generic=generic, src_stats=SRC_STATS["count_opt_outs"])
        pass
        self.nhs_number = generic.text.color()
        self.source_system = generic.text.color()


class hospital_visitGenerator:
    num_rows_per_pass = 3

    def __init__(self, src_db_conn, dst_db_conn):
        self.visit_start, self.visit_end, self.visit_duration_seconds = custom_generators.timespan_generator(generic=generic, earliest_start_year=2021, last_start_year=2022, min_dt_days=1, max_dt_days=30)
        pass
        self.person_id = generic.column_value_provider.column_value(dst_db_conn, tests.examples.example_orm.Person, "person_id")
        self.visit_image = generic.bytes_provider.bytes()


sorted_generators = [
    entityGenerator,
    personGenerator,
    hospital_visitGenerator,
]


sorted_vocab = [
    concept_vocab,
]
