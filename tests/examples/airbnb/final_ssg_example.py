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

import orm
import airbnb_generators
import airbnb_generators

import yaml

with open("src-stats.yaml", "r", encoding="utf-8") as f:
    SRC_STATS = yaml.unsafe_load(f)

countries_vocab = FileUploader(orm.Countries.__table__)


class age_gender_bktsGenerator:
    num_rows_per_pass = 1

    def __init__(self):
        pass

    def __call__(self, dst_db_conn):
        result = {}
        result["gender"] = generic.person.password()
        result["age_bucket"] = generic.person.password()
        result["country_destination"] = generic.column_value_provider.column_value(
            dst_db_conn, orm.Countries, "country_destination"
        )
        result["population_in_thousands"] = generic.numeric.integer_number()
        result["year"] = generic.numeric.integer_number()
        return result


class usersGenerator:
    num_rows_per_pass = 0

    def __init__(self):
        pass

    def __call__(self, dst_db_conn):
        result = {}
        result["id"] = generic.person.password()
        result["id"] = generic.person.identifier(mask="@@##@@@@")
        (
            result["date_account_created"],
            result["date_first_booking"],
        ) = airbnb_generators.user_dates_provider(generic=generic)
        result["age"] = airbnb_generators.user_age_provider(
            query_results=SRC_STATS["age_stats"]
        )
        result["timestamp_first_active"] = generic.datetime.datetime()
        result["gender"] = generic.text.color()
        result["signup_method"] = generic.text.color()
        result["signup_flow"] = generic.numeric.integer_number()
        result["language"] = generic.text.color()
        result["affiliate_channel"] = generic.text.color()
        result["affiliate_provider"] = generic.text.color()
        result["first_affiliate_tracked"] = generic.text.color()
        result["signup_app"] = generic.text.color()
        result["first_device_type"] = generic.text.color()
        result["first_browser"] = generic.text.color()
        result["country_destination"] = generic.column_value_provider.column_value(
            dst_db_conn, orm.Countries, "country_destination"
        )
        return result


class sessionsGenerator:
    num_rows_per_pass = 0

    def __init__(self):
        pass

    def __call__(self, dst_db_conn):
        result = {}
        result["secs_elapsed"] = generic.numeric.integer_number(start=0, end=3600)
        result["action"] = generic.choice(items=["show", "index", "personalize"])
        result["user_id"] = generic.column_value_provider.column_value(
            dst_db_conn, orm.Users, "id"
        )
        result["action_type"] = generic.text.color()
        result["action_detail"] = generic.text.color()
        result["device_type"] = generic.text.color()
        return result


table_generator_dict = {
    "age_gender_bkts": age_gender_bktsGenerator(),
    "users": usersGenerator(),
    "sessions": sessionsGenerator(),
}


vocab_dict = {
    "countries": countries_vocab,
}


def run_airbnb_generators_sessions_story(dst_db_conn):
    return airbnb_generators.sessions_story()


story_generator_list = [
    {
        "name": run_airbnb_generators_sessions_story,
        "num_stories_per_pass": 30,
    },
]
