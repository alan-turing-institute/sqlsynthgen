# coding: utf-8

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, Integer, DateTime, Text, Date, Float, LargeBinary
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Person(Base):
    __tablename__ = "person"
    __table_args__ = {"schema": "myschema"}

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
    __table_args__ = {"schema": "myschema"}

    hospital_visit_id = Column(
        BigInteger,
        primary_key=True,
    )
    person_id = Column(ForeignKey("myschema.person.person_id"))
    visit_start = Column(DateTime(True))
    visit_end = Column(Date)
    visit_duration_seconds = Column(Float)
    visit_image = Column(LargeBinary)
