"""This file was auto-generated by sqlsynthgen but can be edited manually."""
from mimesis import Generic
from mimesis.locales import Locale
from sqlsynthgen.providers import BinaryProvider, ForeignKeyProvider

generic = Generic(locale=Locale.EN)
generic.add_provider(ForeignKeyProvider)
generic.add_provider(BinaryProvider)


class entityGenerator:
    def __init__(self, db_connection):
        pass


class personGenerator:
    def __init__(self, db_connection):
        pass
        self.name = generic.text.color()
        self.nhs_number = generic.text.color()
        self.research_opt_out = generic.development.boolean()
        self.source_system = generic.text.color()
        self.stored_from = generic.datetime.datetime()


class hospital_visitGenerator:
    def __init__(self, db_connection):
        pass
        self.person_id = generic.foreign_key_provider.key(db_connection, "myschema", "person", "person_id")
        self.visit_start = generic.datetime.datetime()
        self.visit_end = generic.datetime.date()
        self.visit_duration_seconds = generic.numeric.float_number()
        self.visit_image = generic.binary_provider.bytes()


sorted_generators = [
    entityGenerator,
    personGenerator,
    hospital_visitGenerator,
]
