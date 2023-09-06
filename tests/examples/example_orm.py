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
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()
metadata = Base.metadata


t_data_type_test = Table(
    "data_type_test", metadata, Column("myuuid", UUID, nullable=False)
)


class EmptyVocabulary(Base):
    __tablename__ = "empty_vocabulary"
    __table_args__ = (PrimaryKeyConstraint("entry_id", name="empty_vocabulary_pkey"),)

    entry_id = Column(Integer)
    entry_name = Column(Text, nullable=False)


class MitigationType(Base):
    __tablename__ = "mitigation_type"
    __table_args__ = (PrimaryKeyConstraint("id", name="mitigation_type_pkey"),)

    id = Column(Integer)
    name = Column(Text)
    description = Column(Text)

    concept_type = relationship("ConceptType", back_populates="mitigation_type")


t_no_pk_test = Table(
    "no_pk_test", metadata, Column("not_an_id", Integer, nullable=False)
)


class Person(Base):
    __tablename__ = "person"
    __table_args__ = (PrimaryKeyConstraint("person_id", name="person_pkey"),)

    person_id = Column(Integer)
    name = Column(Text, nullable=False)
    research_opt_out = Column(Boolean, nullable=False)
    stored_from = Column(DateTime(True), nullable=False)

    hospital_visit = relationship("HospitalVisit", back_populates="person")


class UniqueConstraintTest(Base):
    __tablename__ = "unique_constraint_test"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="unique_constraint_test_pkey"),
        UniqueConstraint("a", "b", name="ab_uniq"),
        UniqueConstraint("c", name="c_uniq"),
    )

    id = Column(Integer)
    a = Column(Boolean, nullable=False)
    b = Column(Boolean, nullable=False)
    c = Column(Text, nullable=False)


class UniqueConstraintTest2(Base):
    __tablename__ = "unique_constraint_test2"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="unique_constraint_test_pkey2"),
        UniqueConstraint("a", "b", "c", name="abc_uniq2"),
        UniqueConstraint("a", name="a_uniq2"),
    )

    id = Column(Integer)
    a = Column(Text, nullable=False)
    b = Column(Text, nullable=False)
    c = Column(Text, nullable=False)


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

    id = Column(Integer)
    name = Column(Text, nullable=False)
    mitigation_type_id = Column(Integer)
    lucky_number = Column(Integer)

    mitigation_type = relationship("MitigationType", back_populates="concept_type")
    concept = relationship("Concept", back_populates="concept_type")


t_test_entity = Table(
    "test_entity",
    metadata,
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
    )

    concept_id = Column(Integer)
    concept_name = Column(Text, nullable=False)
    concept_valid_from = Column(DateTime(True), nullable=False)
    concept_type_id = Column(Integer, index=True)

    concept_type = relationship("ConceptType", back_populates="concept")
    hospital_visit = relationship("HospitalVisit", back_populates="visit_type_concept")


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

    hospital_visit_id = Column(BigInteger)
    person_id = Column(Integer, nullable=False)
    visit_start = Column(Date, nullable=False)
    visit_end = Column(Date, nullable=False)
    visit_duration_seconds = Column(Float, nullable=False)
    visit_image = Column(LargeBinary, nullable=False)
    visit_type_concept_id = Column(Integer, nullable=False)

    person = relationship("Person", back_populates="hospital_visit")
    visit_type_concept = relationship("Concept", back_populates="hospital_visit")
