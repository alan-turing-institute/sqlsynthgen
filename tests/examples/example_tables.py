# coding: utf-8

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, Integer, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class Mrn(Base):
    __tablename__ = "mrn"
    __table_args__ = {"schema": "star"}

    mrn_id = Column(
        Integer,
        primary_key=True,
        # server_default=text("nextval('star.mrn_mrn_id_seq'::regclass)"),
    )
    mrn = Column(Text)
    nhs_number = Column(Text)
    research_opt_out = Column(Boolean)
    source_system = Column(Text)
    stored_from = Column(DateTime(True))


class LabSample(Base):
    __tablename__ = "lab_sample"
    __table_args__ = {"schema": "star"}

    lab_sample_id = Column(
        Integer,
        primary_key=True,
        # server_default=text("nextval('star.lab_sample_lab_sample_id_seq'::regclass)"),
    )
    mrn_id = Column(ForeignKey("star.mrn.mrn_id"))
    external_lab_number = Column(Text)
    receipt_at_lab_datetime = Column(DateTime(True))
    sample_collection_datetime = Column(DateTime(True))
    specimen_type = Column(Text)
    sample_site = Column(Text)
    collection_method = Column(Text)
    valid_from = Column(DateTime(True))
    stored_from = Column(DateTime(True))

    mrn = relationship("Mrn")


class AdvanceDecision(Base):
    __tablename__ = "advance_decision"
    __table_args__ = {"schema": "star"}

    advance_decision_id = Column(
        Integer,
        primary_key=True,
        # server_default=text(
        # "nextval('star.advance_decision_advance_decision_id_seq'::regclass)"
        # ),
    )
    advance_decision_type_id = Column(BigInteger)
    # hospital_visit_id = Column(BigInteger)
    # internal_id = Column(BigInteger)
    status_change_datetime = Column(DateTime(True))
    # requested_datetime = Column(DateTime(True))
    # valid_from = Column(DateTime(True))
    # stored_from = Column(DateTime(True))
