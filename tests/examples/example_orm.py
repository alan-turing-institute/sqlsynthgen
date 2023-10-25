from typing import Any, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import CIDR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime


class Base(DeclarativeBase):
    pass


t_data_type_test = Table(
    "data_type_test", Base.metadata, Column("myuuid", Uuid, nullable=False)
)


class EmptyVocabulary(Base):
    __tablename__ = "empty_vocabulary"
    __table_args__ = (PrimaryKeyConstraint("entry_id", name="empty_vocabulary_pkey"),)

    entry_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entry_name: Mapped[str] = mapped_column(Text)


class MitigationType(Base):
    __tablename__ = "mitigation_type"
    __table_args__ = (PrimaryKeyConstraint("id", name="mitigation_type_pkey"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)

    concept_type: Mapped[List["ConceptType"]] = relationship(
        "ConceptType", back_populates="mitigation_type"
    )


t_no_pk_test = Table(
    "no_pk_test", Base.metadata, Column("not_an_id", Integer, nullable=False)
)


class Person(Base):
    __tablename__ = "person"
    __table_args__ = (PrimaryKeyConstraint("person_id", name="person_pkey"),)

    person_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    research_opt_out: Mapped[bool] = mapped_column(Boolean)
    stored_from: Mapped[datetime.datetime] = mapped_column(DateTime(True))

    hospital_visit: Mapped[List["HospitalVisit"]] = relationship(
        "HospitalVisit", back_populates="person"
    )


class StrangeTypeTable(Base):
    __tablename__ = "strange_type_table"
    __table_args__ = (PrimaryKeyConstraint("id", name="strange_type_table_pkey"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    column_with_unusual_type: Mapped[Optional[Any]] = mapped_column(CIDR)


class UnignorableTable(Base):
    __tablename__ = "unignorable_table"
    __table_args__ = (PrimaryKeyConstraint("id", name="unignorable_table_pkey"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    ref_to_unignorable_table: Mapped[List["RefToUnignorableTable"]] = relationship(
        "RefToUnignorableTable", back_populates="unignorable_table"
    )


class UniqueConstraintTest(Base):
    __tablename__ = "unique_constraint_test"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="unique_constraint_test_pkey"),
        UniqueConstraint("a", "b", name="ab_uniq"),
        UniqueConstraint("c", name="c_uniq"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    a: Mapped[bool] = mapped_column(Boolean)
    b: Mapped[bool] = mapped_column(Boolean)
    c: Mapped[str] = mapped_column(Text)


class UniqueConstraintTest2(Base):
    __tablename__ = "unique_constraint_test2"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="unique_constraint_test_pkey2"),
        UniqueConstraint("a", "b", "c", name="abc_uniq2"),
        UniqueConstraint("a", name="a_uniq2"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    a: Mapped[str] = mapped_column(Text)
    b: Mapped[str] = mapped_column(Text)
    c: Mapped[str] = mapped_column(Text)


class ConceptType(Base):
    __tablename__ = "concept_type"
    __table_args__ = (
        ForeignKeyConstraint(
            ["mitigation_type_id"],
            ["mitigation_type.id"],
            name="concept_type_mitigation_type_id_fkey",
        ),
        PrimaryKeyConstraint("id", name="concept_type_pkey"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    mitigation_type_id: Mapped[Optional[int]] = mapped_column(Integer)
    lucky_number: Mapped[Optional[int]] = mapped_column(Integer)

    mitigation_type: Mapped["MitigationType"] = relationship(
        "MitigationType", back_populates="concept_type"
    )
    concept: Mapped[List["Concept"]] = relationship(
        "Concept", back_populates="concept_type"
    )


class RefToUnignorableTable(Base):
    __tablename__ = "ref_to_unignorable_table"
    __table_args__ = (
        ForeignKeyConstraint(
            ["ref"],
            ["unignorable_table.id"],
            name="ref_to_unignorable_table_ref_unignorable_table_id_fkey",
        ),
        PrimaryKeyConstraint("id", name="ref_to_unignorable_table_pkey"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ref: Mapped[int] = mapped_column(Integer)

    unignorable_table: Mapped["UnignorableTable"] = relationship(
        "UnignorableTable", back_populates="ref_to_unignorable_table"
    )


t_test_entity = Table(
    "test_entity",
    Base.metadata,
    Column("single_letter_column", String(1)),
    Column("vocabulary_entry_id", Integer),
    ForeignKeyConstraint(
        ["vocabulary_entry_id"],
        ["empty_vocabulary.entry_id"],
        name="test_entity_vocabulary_entry_id_fkey",
    ),
)


class Concept(Base):
    __tablename__ = "concept"
    __table_args__ = (
        ForeignKeyConstraint(
            ["concept_type_id"],
            ["concept_type.id"],
            name="concept_concept_type_id_fkey",
        ),
        PrimaryKeyConstraint("concept_id", name="concept_pkey"),
        UniqueConstraint("concept_name", name="concept_name_uniq"),
        Index("fki_concept_concept_type_id_fkey", "concept_type_id"),
    )

    concept_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    concept_name: Mapped[str] = mapped_column(Text)
    concept_valid_from: Mapped[datetime.datetime] = mapped_column(DateTime(True))
    concept_type_id: Mapped[Optional[int]] = mapped_column(Integer)

    concept_type: Mapped["ConceptType"] = relationship(
        "ConceptType", back_populates="concept"
    )
    hospital_visit: Mapped[List["HospitalVisit"]] = relationship(
        "HospitalVisit", back_populates="visit_type_concept"
    )


class HospitalVisit(Base):
    __tablename__ = "hospital_visit"
    __table_args__ = (
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="hospital_visit_person_id_fkey"
        ),
        ForeignKeyConstraint(
            ["visit_type_concept_id"],
            ["concept.concept_id"],
            name="hospital_visit_visit_type_concept_id_fkey",
        ),
        PrimaryKeyConstraint("hospital_visit_id", name="hospital_visit_pkey"),
    )

    hospital_visit_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(Integer)
    visit_start: Mapped[datetime.date] = mapped_column(Date)
    visit_end: Mapped[datetime.date] = mapped_column(Date)
    visit_duration_seconds: Mapped[float] = mapped_column(Float)
    visit_image: Mapped[bytes] = mapped_column(LargeBinary)
    visit_type_concept_id: Mapped[int] = mapped_column(Integer)

    person: Mapped["Person"] = relationship("Person", back_populates="hospital_visit")
    visit_type_concept: Mapped["Concept"] = relationship(
        "Concept", back_populates="hospital_visit"
    )
