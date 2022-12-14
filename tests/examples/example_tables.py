# coding: utf-8

from sqlalchemy import BigInteger, Column, Integer,  DateTime #,; text,
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata

# text = lambda x: None


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
