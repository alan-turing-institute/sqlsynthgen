# coding: utf-8

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    DateTime,
    Text,
    Date,
    Float,
    LargeBinary,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Person(Base):
    __tablename__ = "person"
    __table_args__ = (UniqueConstraint("nhs_number", name="nhs_number_uniq"),)

    person_id = Column(
        Integer,
        primary_key=True,
    )
    name = Column(Text)
    nhs_number = Column(Text)
    research_opt_out = Column(Boolean)
    source_system = Column(Text)
    stored_from = Column(DateTime(True))


class HopsitalVisit(Base):
    __tablename__ = "hospital_visit"

    hospital_visit_id = Column(
        BigInteger,
        primary_key=True,
    )
    person_id = Column(ForeignKey("person.person_id"))
    visit_start = Column(DateTime(True))
    visit_end = Column(Date)
    visit_duration_seconds = Column(Float)
    visit_image = Column(LargeBinary)


class Entity(Base):
    __tablename__ = "entity"

    # NB Do not add any more columns to this table as
    # we use it to test what happens in the one-column case
    entity_id = Column(
        Integer,
        primary_key=True,
    )


class TestEntity(Base):
    __tablename__ = "test_entity"

    test_entity_id = Column(
        Integer,
        primary_key=True,
    )

    single_letter_attribute = Column("single_letter_column", Text(length=1))


class Concept(Base):
    __tablename__ = "concept"

    concept_id = Column(
        Integer,
        primary_key=True,
    )
    concept_name = Column(Text)
