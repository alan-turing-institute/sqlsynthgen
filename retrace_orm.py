from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import decimal


class Base(DeclarativeBase):
    pass


t_cdm_source = Table(
    "cdm_source",
    Base.metadata,
    Column("cdm_source_name", String(255), nullable=False),
    Column("cdm_source_abbreviation", String(25), nullable=False),
    Column("cdm_holder", String(255), nullable=False),
    Column("source_description", Text),
    Column("source_documentation_reference", String(255)),
    Column("cdm_etl_reference", String(255)),
    Column("source_release_date", Date, nullable=False),
    Column("cdm_release_date", Date, nullable=False),
    Column("cdm_version", String(10)),
    Column("vocabulary_version", String(20), nullable=False),
)


t_cohort = Table(
    "cohort",
    Base.metadata,
    Column("cohort_definition_id", BigInteger, nullable=False),
    Column("subject_id", BigInteger, nullable=False),
    Column("cohort_start_date", Date, nullable=False),
    Column("cohort_end_date", Date, nullable=False),
)


class Concept(Base):
    __tablename__ = "concept"
    __table_args__ = (
        ForeignKeyConstraint(
            ["concept_class_id"],
            ["concept_class.concept_class_id"],
            name="fpk_concept_concept_class_id",
        ),
        ForeignKeyConstraint(
            ["domain_id"], ["domain.domain_id"], name="fpk_concept_domain_id"
        ),
        ForeignKeyConstraint(
            ["vocabulary_id"],
            ["vocabulary.vocabulary_id"],
            name="fpk_concept_vocabulary_id",
        ),
        PrimaryKeyConstraint("concept_id", name="xpk_concept"),
    )

    concept_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    concept_name: Mapped[str] = mapped_column(String(255))
    domain_id: Mapped[str] = mapped_column(String(20))
    vocabulary_id: Mapped[str] = mapped_column(String(50))
    concept_class_id: Mapped[str] = mapped_column(String(20))
    concept_code: Mapped[str] = mapped_column(String(255))
    valid_start_date: Mapped[datetime.date] = mapped_column(Date)
    valid_end_date: Mapped[datetime.date] = mapped_column(Date)
    standard_concept: Mapped[Optional[str]] = mapped_column(String(1))
    invalid_reason: Mapped[Optional[str]] = mapped_column(String(1))

    concept_class: Mapped["ConceptClass"] = relationship(
        "ConceptClass", foreign_keys=[concept_class_id], back_populates="concept"
    )
    domain: Mapped["Domain"] = relationship(
        "Domain", foreign_keys=[domain_id], back_populates="concept"
    )
    vocabulary: Mapped["Vocabulary"] = relationship(
        "Vocabulary", foreign_keys=[vocabulary_id], back_populates="concept"
    )
    concept_class_: Mapped[List["ConceptClass"]] = relationship(
        "ConceptClass",
        foreign_keys="[ConceptClass.concept_class_concept_id]",
        back_populates="concept_class_concept",
    )
    domain_: Mapped[List["Domain"]] = relationship(
        "Domain",
        foreign_keys="[Domain.domain_concept_id]",
        back_populates="domain_concept",
    )
    vocabulary_: Mapped[List["Vocabulary"]] = relationship(
        "Vocabulary",
        foreign_keys="[Vocabulary.vocabulary_concept_id]",
        back_populates="vocabulary_concept",
    )
    care_site: Mapped[List["CareSite"]] = relationship(
        "CareSite", back_populates="place_of_service_concept"
    )
    cost: Mapped[List["Cost"]] = relationship(
        "Cost",
        foreign_keys="[Cost.cost_type_concept_id]",
        back_populates="cost_type_concept",
    )
    cost_: Mapped[List["Cost"]] = relationship(
        "Cost",
        foreign_keys="[Cost.currency_concept_id]",
        back_populates="currency_concept",
    )
    cost1: Mapped[List["Cost"]] = relationship(
        "Cost", foreign_keys="[Cost.drg_concept_id]", back_populates="drg_concept"
    )
    cost2: Mapped[List["Cost"]] = relationship(
        "Cost",
        foreign_keys="[Cost.revenue_code_concept_id]",
        back_populates="revenue_code_concept",
    )
    metadata_: Mapped[List["Metadata"]] = relationship(
        "Metadata",
        foreign_keys="[Metadata.metadata_concept_id]",
        back_populates="metadata_concept",
    )
    metadata__: Mapped[List["Metadata"]] = relationship(
        "Metadata",
        foreign_keys="[Metadata.metadata_type_concept_id]",
        back_populates="metadata_type_concept",
    )
    metadata_1: Mapped[List["Metadata"]] = relationship(
        "Metadata",
        foreign_keys="[Metadata.value_as_concept_id]",
        back_populates="value_as_concept",
    )
    note_nlp: Mapped[List["NoteNlp"]] = relationship(
        "NoteNlp",
        foreign_keys="[NoteNlp.note_nlp_concept_id]",
        back_populates="note_nlp_concept",
    )
    note_nlp_: Mapped[List["NoteNlp"]] = relationship(
        "NoteNlp",
        foreign_keys="[NoteNlp.note_nlp_source_concept_id]",
        back_populates="note_nlp_source_concept",
    )
    note_nlp1: Mapped[List["NoteNlp"]] = relationship(
        "NoteNlp",
        foreign_keys="[NoteNlp.section_concept_id]",
        back_populates="section_concept",
    )
    relationship_: Mapped[List["Relationship"]] = relationship(
        "Relationship", back_populates="relationship_concept"
    )
    provider: Mapped[List["Provider"]] = relationship(
        "Provider",
        foreign_keys="[Provider.gender_concept_id]",
        back_populates="gender_concept",
    )
    provider_: Mapped[List["Provider"]] = relationship(
        "Provider",
        foreign_keys="[Provider.gender_source_concept_id]",
        back_populates="gender_source_concept",
    )
    provider1: Mapped[List["Provider"]] = relationship(
        "Provider",
        foreign_keys="[Provider.specialty_concept_id]",
        back_populates="specialty_concept",
    )
    provider2: Mapped[List["Provider"]] = relationship(
        "Provider",
        foreign_keys="[Provider.specialty_source_concept_id]",
        back_populates="specialty_source_concept",
    )
    person: Mapped[List["Person"]] = relationship(
        "Person",
        foreign_keys="[Person.ethnicity_concept_id]",
        back_populates="ethnicity_concept",
    )
    person_: Mapped[List["Person"]] = relationship(
        "Person",
        foreign_keys="[Person.ethnicity_source_concept_id]",
        back_populates="ethnicity_source_concept",
    )
    person1: Mapped[List["Person"]] = relationship(
        "Person",
        foreign_keys="[Person.gender_concept_id]",
        back_populates="gender_concept",
    )
    person2: Mapped[List["Person"]] = relationship(
        "Person",
        foreign_keys="[Person.gender_source_concept_id]",
        back_populates="gender_source_concept",
    )
    person3: Mapped[List["Person"]] = relationship(
        "Person", foreign_keys="[Person.race_concept_id]", back_populates="race_concept"
    )
    person4: Mapped[List["Person"]] = relationship(
        "Person",
        foreign_keys="[Person.race_source_concept_id]",
        back_populates="race_source_concept",
    )
    condition_era: Mapped[List["ConditionEra"]] = relationship(
        "ConditionEra", back_populates="condition_concept"
    )
    dose_era: Mapped[List["DoseEra"]] = relationship(
        "DoseEra",
        foreign_keys="[DoseEra.drug_concept_id]",
        back_populates="drug_concept",
    )
    dose_era_: Mapped[List["DoseEra"]] = relationship(
        "DoseEra",
        foreign_keys="[DoseEra.unit_concept_id]",
        back_populates="unit_concept",
    )
    drug_era: Mapped[List["DrugEra"]] = relationship(
        "DrugEra", back_populates="drug_concept"
    )
    episode: Mapped[List["Episode"]] = relationship(
        "Episode",
        foreign_keys="[Episode.episode_concept_id]",
        back_populates="episode_concept",
    )
    episode_: Mapped[List["Episode"]] = relationship(
        "Episode",
        foreign_keys="[Episode.episode_object_concept_id]",
        back_populates="episode_object_concept",
    )
    episode1: Mapped[List["Episode"]] = relationship(
        "Episode",
        foreign_keys="[Episode.episode_source_concept_id]",
        back_populates="episode_source_concept",
    )
    episode2: Mapped[List["Episode"]] = relationship(
        "Episode",
        foreign_keys="[Episode.episode_type_concept_id]",
        back_populates="episode_type_concept",
    )
    observation_period: Mapped[List["ObservationPeriod"]] = relationship(
        "ObservationPeriod", back_populates="period_type_concept"
    )
    payer_plan_period: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.payer_concept_id]",
        back_populates="payer_concept",
    )
    payer_plan_period_: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.payer_source_concept_id]",
        back_populates="payer_source_concept",
    )
    payer_plan_period1: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.plan_concept_id]",
        back_populates="plan_concept",
    )
    payer_plan_period2: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.plan_source_concept_id]",
        back_populates="plan_source_concept",
    )
    payer_plan_period3: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.sponsor_concept_id]",
        back_populates="sponsor_concept",
    )
    payer_plan_period4: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.sponsor_source_concept_id]",
        back_populates="sponsor_source_concept",
    )
    payer_plan_period5: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.stop_reason_concept_id]",
        back_populates="stop_reason_concept",
    )
    payer_plan_period6: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod",
        foreign_keys="[PayerPlanPeriod.stop_reason_source_concept_id]",
        back_populates="stop_reason_source_concept",
    )
    specimen: Mapped[List["Specimen"]] = relationship(
        "Specimen",
        foreign_keys="[Specimen.anatomic_site_concept_id]",
        back_populates="anatomic_site_concept",
    )
    specimen_: Mapped[List["Specimen"]] = relationship(
        "Specimen",
        foreign_keys="[Specimen.disease_status_concept_id]",
        back_populates="disease_status_concept",
    )
    specimen1: Mapped[List["Specimen"]] = relationship(
        "Specimen",
        foreign_keys="[Specimen.specimen_concept_id]",
        back_populates="specimen_concept",
    )
    specimen2: Mapped[List["Specimen"]] = relationship(
        "Specimen",
        foreign_keys="[Specimen.specimen_type_concept_id]",
        back_populates="specimen_type_concept",
    )
    specimen3: Mapped[List["Specimen"]] = relationship(
        "Specimen",
        foreign_keys="[Specimen.unit_concept_id]",
        back_populates="unit_concept",
    )
    visit_occurrence: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence",
        foreign_keys="[VisitOccurrence.admitted_from_concept_id]",
        back_populates="admitted_from_concept",
    )
    visit_occurrence_: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence",
        foreign_keys="[VisitOccurrence.discharged_to_concept_id]",
        back_populates="discharged_to_concept",
    )
    visit_occurrence1: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence",
        foreign_keys="[VisitOccurrence.visit_concept_id]",
        back_populates="visit_concept",
    )
    visit_occurrence2: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence",
        foreign_keys="[VisitOccurrence.visit_source_concept_id]",
        back_populates="visit_source_concept",
    )
    visit_occurrence3: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence",
        foreign_keys="[VisitOccurrence.visit_type_concept_id]",
        back_populates="visit_type_concept",
    )
    visit_detail: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail",
        foreign_keys="[VisitDetail.visit_detail_concept_id]",
        back_populates="visit_detail_concept",
    )
    visit_detail_: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail",
        foreign_keys="[VisitDetail.visit_detail_type_concept_id]",
        back_populates="visit_detail_type_concept",
    )
    condition_occurrence: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence",
        foreign_keys="[ConditionOccurrence.condition_concept_id]",
        back_populates="condition_concept",
    )
    condition_occurrence_: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence",
        foreign_keys="[ConditionOccurrence.condition_source_concept_id]",
        back_populates="condition_source_concept",
    )
    condition_occurrence1: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence",
        foreign_keys="[ConditionOccurrence.condition_type_concept_id]",
        back_populates="condition_type_concept",
    )
    device_exposure: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure",
        foreign_keys="[DeviceExposure.device_concept_id]",
        back_populates="device_concept",
    )
    device_exposure_: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure",
        foreign_keys="[DeviceExposure.device_source_concept_id]",
        back_populates="device_source_concept",
    )
    device_exposure1: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure",
        foreign_keys="[DeviceExposure.device_type_concept_id]",
        back_populates="device_type_concept",
    )
    drug_exposure: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure",
        foreign_keys="[DrugExposure.drug_concept_id]",
        back_populates="drug_concept",
    )
    drug_exposure_: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure",
        foreign_keys="[DrugExposure.drug_source_concept_id]",
        back_populates="drug_source_concept",
    )
    drug_exposure1: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure",
        foreign_keys="[DrugExposure.drug_type_concept_id]",
        back_populates="drug_type_concept",
    )
    drug_exposure2: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure",
        foreign_keys="[DrugExposure.route_concept_id]",
        back_populates="route_concept",
    )
    measurement: Mapped[List["Measurement"]] = relationship(
        "Measurement",
        foreign_keys="[Measurement.measurement_concept_id]",
        back_populates="measurement_concept",
    )
    measurement_: Mapped[List["Measurement"]] = relationship(
        "Measurement",
        foreign_keys="[Measurement.measurement_source_concept_id]",
        back_populates="measurement_source_concept",
    )
    measurement1: Mapped[List["Measurement"]] = relationship(
        "Measurement",
        foreign_keys="[Measurement.measurement_type_concept_id]",
        back_populates="measurement_type_concept",
    )
    measurement2: Mapped[List["Measurement"]] = relationship(
        "Measurement",
        foreign_keys="[Measurement.operator_concept_id]",
        back_populates="operator_concept",
    )
    measurement3: Mapped[List["Measurement"]] = relationship(
        "Measurement",
        foreign_keys="[Measurement.unit_concept_id]",
        back_populates="unit_concept",
    )
    measurement4: Mapped[List["Measurement"]] = relationship(
        "Measurement",
        foreign_keys="[Measurement.value_as_concept_id]",
        back_populates="value_as_concept",
    )
    note: Mapped[List["Note"]] = relationship(
        "Note",
        foreign_keys="[Note.encoding_concept_id]",
        back_populates="encoding_concept",
    )
    note_: Mapped[List["Note"]] = relationship(
        "Note",
        foreign_keys="[Note.language_concept_id]",
        back_populates="language_concept",
    )
    note1: Mapped[List["Note"]] = relationship(
        "Note",
        foreign_keys="[Note.note_class_concept_id]",
        back_populates="note_class_concept",
    )
    note2: Mapped[List["Note"]] = relationship(
        "Note",
        foreign_keys="[Note.note_event_field_concept_id]",
        back_populates="note_event_field_concept",
    )
    note3: Mapped[List["Note"]] = relationship(
        "Note",
        foreign_keys="[Note.note_type_concept_id]",
        back_populates="note_type_concept",
    )
    observation: Mapped[List["Observation"]] = relationship(
        "Observation",
        foreign_keys="[Observation.observation_concept_id]",
        back_populates="observation_concept",
    )
    observation_: Mapped[List["Observation"]] = relationship(
        "Observation",
        foreign_keys="[Observation.observation_source_concept_id]",
        back_populates="observation_source_concept",
    )
    observation1: Mapped[List["Observation"]] = relationship(
        "Observation",
        foreign_keys="[Observation.observation_type_concept_id]",
        back_populates="observation_type_concept",
    )
    observation2: Mapped[List["Observation"]] = relationship(
        "Observation",
        foreign_keys="[Observation.qualifier_concept_id]",
        back_populates="qualifier_concept",
    )
    observation3: Mapped[List["Observation"]] = relationship(
        "Observation",
        foreign_keys="[Observation.unit_concept_id]",
        back_populates="unit_concept",
    )
    procedure_occurrence: Mapped[List["ProcedureOccurrence"]] = relationship(
        "ProcedureOccurrence",
        foreign_keys="[ProcedureOccurrence.modifier_concept_id]",
        back_populates="modifier_concept",
    )
    procedure_occurrence_: Mapped[List["ProcedureOccurrence"]] = relationship(
        "ProcedureOccurrence",
        foreign_keys="[ProcedureOccurrence.procedure_type_concept_id]",
        back_populates="procedure_type_concept",
    )


t_concept_ancestor = Table(
    "concept_ancestor",
    Base.metadata,
    Column("ancestor_concept_id", BigInteger, nullable=False),
    Column("descendant_concept_id", BigInteger, nullable=False),
    Column("min_levels_of_separation", BigInteger, nullable=False),
    Column("max_levels_of_separation", BigInteger, nullable=False),
)


class ConceptClass(Base):
    __tablename__ = "concept_class"
    __table_args__ = (
        ForeignKeyConstraint(
            ["concept_class_concept_id"],
            ["concept.concept_id"],
            name="fpk_concept_class_concept_class_concept_id",
        ),
        PrimaryKeyConstraint("concept_class_id", name="xpk_concept_class"),
    )

    concept_class_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    concept_class_name: Mapped[str] = mapped_column(String(255))
    concept_class_concept_id: Mapped[int] = mapped_column(BigInteger)

    concept: Mapped[List["Concept"]] = relationship(
        "Concept",
        foreign_keys="[Concept.concept_class_id]",
        back_populates="concept_class",
    )
    concept_class_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[concept_class_concept_id],
        back_populates="concept_class_",
    )


class Domain(Base):
    __tablename__ = "domain"
    __table_args__ = (
        ForeignKeyConstraint(
            ["domain_concept_id"],
            ["concept.concept_id"],
            name="fpk_domain_domain_concept_id",
        ),
        PrimaryKeyConstraint("domain_id", name="xpk_domain"),
    )

    domain_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    domain_name: Mapped[str] = mapped_column(String(255))
    domain_concept_id: Mapped[int] = mapped_column(BigInteger)

    concept: Mapped[List["Concept"]] = relationship(
        "Concept", foreign_keys="[Concept.domain_id]", back_populates="domain"
    )
    domain_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[domain_concept_id], back_populates="domain_"
    )
    cost: Mapped[List["Cost"]] = relationship("Cost", back_populates="cost_domain")


class Location(Base):
    __tablename__ = "location"
    __table_args__ = (PrimaryKeyConstraint("location_id", name="xpk_location"),)

    location_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    address_1: Mapped[Optional[str]] = mapped_column(String(50))
    address_2: Mapped[Optional[str]] = mapped_column(String(50))
    city: Mapped[Optional[str]] = mapped_column(String(50))
    state: Mapped[Optional[str]] = mapped_column(String(2))
    zip: Mapped[Optional[str]] = mapped_column(String(9))
    county: Mapped[Optional[str]] = mapped_column(String(20))
    location_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    care_site: Mapped[List["CareSite"]] = relationship(
        "CareSite", back_populates="location"
    )
    person: Mapped[List["Person"]] = relationship("Person", back_populates="location")


class Vocabulary(Base):
    __tablename__ = "vocabulary"
    __table_args__ = (
        ForeignKeyConstraint(
            ["vocabulary_concept_id"],
            ["concept.concept_id"],
            name="fpk_vocabulary_vocabulary_concept_id",
        ),
        PrimaryKeyConstraint("vocabulary_id", name="xpk_vocabulary"),
    )

    vocabulary_id: Mapped[str] = mapped_column(String(30), primary_key=True)
    vocabulary_name: Mapped[str] = mapped_column(String(255))
    vocabulary_concept_id: Mapped[int] = mapped_column(BigInteger)
    vocabulary_reference: Mapped[Optional[str]] = mapped_column(String(255))
    vocabulary_version: Mapped[Optional[str]] = mapped_column(String(255))

    concept: Mapped[List["Concept"]] = relationship(
        "Concept", foreign_keys="[Concept.vocabulary_id]", back_populates="vocabulary"
    )
    vocabulary_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[vocabulary_concept_id], back_populates="vocabulary_"
    )


class CareSite(Base):
    __tablename__ = "care_site"
    __table_args__ = (
        ForeignKeyConstraint(
            ["location_id"], ["location.location_id"], name="fpk_care_site_location_id"
        ),
        ForeignKeyConstraint(
            ["place_of_service_concept_id"],
            ["concept.concept_id"],
            name="fpk_care_site_place_of_service_concept_id",
        ),
        PrimaryKeyConstraint("care_site_id", name="xpk_care_site"),
    )

    care_site_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    care_site_name: Mapped[Optional[str]] = mapped_column(String(255))
    place_of_service_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    location_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    care_site_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    place_of_service_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    location: Mapped["Location"] = relationship("Location", back_populates="care_site")
    place_of_service_concept: Mapped["Concept"] = relationship(
        "Concept", back_populates="care_site"
    )
    provider: Mapped[List["Provider"]] = relationship(
        "Provider", back_populates="care_site"
    )
    person: Mapped[List["Person"]] = relationship("Person", back_populates="care_site")
    visit_occurrence: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence", back_populates="care_site"
    )
    visit_detail: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail", back_populates="care_site"
    )


t_cohort_definition = Table(
    "cohort_definition",
    Base.metadata,
    Column("cohort_definition_id", BigInteger, nullable=False),
    Column("cohort_definition_name", String(255), nullable=False),
    Column("cohort_definition_description", Text),
    Column("definition_type_concept_id", BigInteger, nullable=False),
    Column("cohort_definition_syntax", Text),
    Column("subject_concept_id", BigInteger, nullable=False),
    Column("cohort_initiation_date", Date),
    ForeignKeyConstraint(
        ["definition_type_concept_id"],
        ["concept.concept_id"],
        name="fpk_cohort_definition_definition_type_concept_id",
    ),
    ForeignKeyConstraint(
        ["subject_concept_id"],
        ["concept.concept_id"],
        name="fpk_cohort_definition_subject_concept_id",
    ),
)


t_concept_synonym = Table(
    "concept_synonym",
    Base.metadata,
    Column("concept_id", BigInteger, nullable=False),
    Column("concept_synonym_name", String(1000), nullable=False),
    Column("language_concept_id", BigInteger, nullable=False),
    ForeignKeyConstraint(
        ["language_concept_id"],
        ["concept.concept_id"],
        name="fpk_concept_synonym_language_concept_id",
    ),
)


class Cost(Base):
    __tablename__ = "cost"
    __table_args__ = (
        ForeignKeyConstraint(
            ["cost_domain_id"], ["domain.domain_id"], name="fpk_cost_cost_domain_id"
        ),
        ForeignKeyConstraint(
            ["cost_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_cost_cost_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["currency_concept_id"],
            ["concept.concept_id"],
            name="fpk_cost_currency_concept_id",
        ),
        ForeignKeyConstraint(
            ["drg_concept_id"], ["concept.concept_id"], name="fpk_cost_drg_concept_id"
        ),
        ForeignKeyConstraint(
            ["revenue_code_concept_id"],
            ["concept.concept_id"],
            name="fpk_cost_revenue_code_concept_id",
        ),
        PrimaryKeyConstraint("cost_id", name="xpk_cost"),
    )

    cost_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cost_event_id: Mapped[int] = mapped_column(BigInteger)
    cost_domain_id: Mapped[str] = mapped_column(String(20))
    cost_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    currency_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    total_charge: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    total_cost: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    total_paid: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_by_payer: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_by_patient: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_patient_copay: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_patient_coinsurance: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_patient_deductible: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_by_primary: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_ingredient_cost: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    paid_dispensing_fee: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    payer_plan_period_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    amount_allowed: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    revenue_code_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    revenue_code_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    drg_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    drg_source_value: Mapped[Optional[str]] = mapped_column(String(3))

    cost_domain: Mapped["Domain"] = relationship("Domain", back_populates="cost")
    cost_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[cost_type_concept_id], back_populates="cost"
    )
    currency_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[currency_concept_id], back_populates="cost_"
    )
    drg_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[drg_concept_id], back_populates="cost1"
    )
    revenue_code_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[revenue_code_concept_id], back_populates="cost2"
    )


t_drug_strength = Table(
    "drug_strength",
    Base.metadata,
    Column("drug_concept_id", BigInteger, nullable=False),
    Column("ingredient_concept_id", BigInteger, nullable=False),
    Column("amount_value", Numeric),
    Column("amount_unit_concept_id", BigInteger),
    Column("numerator_value", Numeric),
    Column("numerator_unit_concept_id", BigInteger),
    Column("denominator_value", Numeric),
    Column("denominator_unit_concept_id", BigInteger),
    Column("box_size", BigInteger),
    Column("valid_start_date", Date, nullable=False),
    Column("valid_end_date", Date, nullable=False),
    Column("invalid_reason", String(1)),
    ForeignKeyConstraint(
        ["amount_unit_concept_id"],
        ["concept.concept_id"],
        name="fpk_drug_strength_amount_unit_concept_id",
    ),
    ForeignKeyConstraint(
        ["denominator_unit_concept_id"],
        ["concept.concept_id"],
        name="fpk_drug_strength_denominator_unit_concept_id",
    ),
    ForeignKeyConstraint(
        ["drug_concept_id"],
        ["concept.concept_id"],
        name="fpk_drug_strength_drug_concept_id",
    ),
    ForeignKeyConstraint(
        ["ingredient_concept_id"],
        ["concept.concept_id"],
        name="fpk_drug_strength_ingredient_concept_id",
    ),
    ForeignKeyConstraint(
        ["numerator_unit_concept_id"],
        ["concept.concept_id"],
        name="fpk_drug_strength_numerator_unit_concept_id",
    ),
)


t_fact_relationship = Table(
    "fact_relationship",
    Base.metadata,
    Column("domain_concept_id_1", BigInteger, nullable=False),
    Column("fact_id_1", BigInteger, nullable=False),
    Column("domain_concept_id_2", BigInteger, nullable=False),
    Column("fact_id_2", BigInteger, nullable=False),
    Column("relationship_concept_id", BigInteger, nullable=False),
    ForeignKeyConstraint(
        ["domain_concept_id_1"],
        ["concept.concept_id"],
        name="fpk_fact_relationship_domain_concept_id_1",
    ),
    ForeignKeyConstraint(
        ["domain_concept_id_2"],
        ["concept.concept_id"],
        name="fpk_fact_relationship_domain_concept_id_2",
    ),
    ForeignKeyConstraint(
        ["relationship_concept_id"],
        ["concept.concept_id"],
        name="fpk_fact_relationship_relationship_concept_id",
    ),
)


class Metadata(Base):
    __tablename__ = "metadata"
    __table_args__ = (
        ForeignKeyConstraint(
            ["metadata_concept_id"],
            ["concept.concept_id"],
            name="fpk_metadata_metadata_concept_id",
        ),
        ForeignKeyConstraint(
            ["metadata_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_metadata_metadata_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["value_as_concept_id"],
            ["concept.concept_id"],
            name="fpk_metadata_value_as_concept_id",
        ),
        PrimaryKeyConstraint("metadata_id", name="xpk_metadata"),
    )

    metadata_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    metadata_concept_id: Mapped[int] = mapped_column(BigInteger)
    metadata_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    name: Mapped[str] = mapped_column(String(250))
    value_as_string: Mapped[Optional[str]] = mapped_column(String(250))
    value_as_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    value_as_number: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    metadata_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    metadata_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)

    metadata_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[metadata_concept_id], back_populates="metadata_"
    )
    metadata_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[metadata_type_concept_id], back_populates="metadata__"
    )
    value_as_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[value_as_concept_id], back_populates="metadata_1"
    )


class NoteNlp(Base):
    __tablename__ = "note_nlp"
    __table_args__ = (
        ForeignKeyConstraint(
            ["note_nlp_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_nlp_note_nlp_concept_id",
        ),
        ForeignKeyConstraint(
            ["note_nlp_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_nlp_note_nlp_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["section_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_nlp_section_concept_id",
        ),
        PrimaryKeyConstraint("note_nlp_id", name="xpk_note_nlp"),
    )

    note_nlp_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    note_id: Mapped[int] = mapped_column(BigInteger)
    lexical_variant: Mapped[str] = mapped_column(String(250))
    nlp_date: Mapped[datetime.date] = mapped_column(Date)
    section_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    snippet: Mapped[Optional[str]] = mapped_column(String(250))
    offset: Mapped[Optional[str]] = mapped_column(String(50))
    note_nlp_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    note_nlp_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    nlp_system: Mapped[Optional[str]] = mapped_column(String(250))
    nlp_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    term_exists: Mapped[Optional[str]] = mapped_column(String(1))
    term_temporal: Mapped[Optional[str]] = mapped_column(String(50))
    term_modifiers: Mapped[Optional[str]] = mapped_column(String(2000))

    note_nlp_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[note_nlp_concept_id], back_populates="note_nlp"
    )
    note_nlp_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[note_nlp_source_concept_id], back_populates="note_nlp_"
    )
    section_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[section_concept_id], back_populates="note_nlp1"
    )


class Relationship(Base):
    __tablename__ = "relationship"
    __table_args__ = (
        ForeignKeyConstraint(
            ["relationship_concept_id"],
            ["concept.concept_id"],
            name="fpk_relationship_relationship_concept_id",
        ),
        PrimaryKeyConstraint("relationship_id", name="xpk_relationship"),
    )

    relationship_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    relationship_name: Mapped[str] = mapped_column(String(255))
    is_hierarchical: Mapped[str] = mapped_column(String(1))
    defines_ancestry: Mapped[str] = mapped_column(String(1))
    reverse_relationship_id: Mapped[str] = mapped_column(String(20))
    relationship_concept_id: Mapped[int] = mapped_column(BigInteger)

    relationship_concept: Mapped["Concept"] = relationship(
        "Concept", back_populates="relationship_"
    )


t_source_to_concept_map = Table(
    "source_to_concept_map",
    Base.metadata,
    Column("source_code", String(50), nullable=False),
    Column("source_concept_id", BigInteger, nullable=False),
    Column("source_vocabulary_id", String(20), nullable=False),
    Column("source_code_description", String(255)),
    Column("target_concept_id", BigInteger, nullable=False),
    Column("target_vocabulary_id", String(20), nullable=False),
    Column("valid_start_date", Date, nullable=False),
    Column("valid_end_date", Date, nullable=False),
    Column("invalid_reason", String(1)),
    ForeignKeyConstraint(
        ["source_concept_id"],
        ["concept.concept_id"],
        name="fpk_source_to_concept_map_source_concept_id",
    ),
    ForeignKeyConstraint(
        ["target_concept_id"],
        ["concept.concept_id"],
        name="fpk_source_to_concept_map_target_concept_id",
    ),
    ForeignKeyConstraint(
        ["target_vocabulary_id"],
        ["vocabulary.vocabulary_id"],
        name="fpk_source_to_concept_map_target_vocabulary_id",
    ),
)


t_concept_relationship = Table(
    "concept_relationship",
    Base.metadata,
    Column("concept_id_1", BigInteger, nullable=False),
    Column("concept_id_2", BigInteger, nullable=False),
    Column("relationship_id", String(20), nullable=False),
    Column("valid_start_date", Date, nullable=False),
    Column("valid_end_date", Date, nullable=False),
    Column("invalid_reason", String(1)),
    ForeignKeyConstraint(
        ["relationship_id"],
        ["relationship.relationship_id"],
        name="fpk_concept_relationship_relationship_id",
    ),
)


class Provider(Base):
    __tablename__ = "provider"
    __table_args__ = (
        ForeignKeyConstraint(
            ["care_site_id"],
            ["care_site.care_site_id"],
            name="fpk_provider_care_site_id",
        ),
        ForeignKeyConstraint(
            ["gender_concept_id"],
            ["concept.concept_id"],
            name="fpk_provider_gender_concept_id",
        ),
        ForeignKeyConstraint(
            ["gender_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_provider_gender_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["specialty_concept_id"],
            ["concept.concept_id"],
            name="fpk_provider_specialty_concept_id",
        ),
        ForeignKeyConstraint(
            ["specialty_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_provider_specialty_source_concept_id",
        ),
        PrimaryKeyConstraint("provider_id", name="xpk_provider"),
    )

    provider_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    provider_name: Mapped[Optional[str]] = mapped_column(String(255))
    npi: Mapped[Optional[str]] = mapped_column(String(20))
    dea: Mapped[Optional[str]] = mapped_column(String(20))
    specialty_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    care_site_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    year_of_birth: Mapped[Optional[int]] = mapped_column(BigInteger)
    gender_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    provider_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    specialty_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    specialty_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    gender_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    gender_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    care_site: Mapped["CareSite"] = relationship("CareSite", back_populates="provider")
    gender_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[gender_concept_id], back_populates="provider"
    )
    gender_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[gender_source_concept_id], back_populates="provider_"
    )
    specialty_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[specialty_concept_id], back_populates="provider1"
    )
    specialty_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[specialty_source_concept_id],
        back_populates="provider2",
    )
    person: Mapped[List["Person"]] = relationship("Person", back_populates="provider")
    visit_occurrence: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence", back_populates="provider"
    )
    visit_detail: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail", back_populates="provider"
    )
    condition_occurrence: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence", back_populates="provider"
    )
    device_exposure: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure", back_populates="provider"
    )
    drug_exposure: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure", back_populates="provider"
    )
    measurement: Mapped[List["Measurement"]] = relationship(
        "Measurement", back_populates="provider"
    )
    note: Mapped[List["Note"]] = relationship("Note", back_populates="provider")
    observation: Mapped[List["Observation"]] = relationship(
        "Observation", back_populates="provider"
    )
    procedure_occurrence: Mapped[List["ProcedureOccurrence"]] = relationship(
        "ProcedureOccurrence", back_populates="provider"
    )


class Person(Base):
    __tablename__ = "person"
    __table_args__ = (
        ForeignKeyConstraint(
            ["care_site_id"], ["care_site.care_site_id"], name="fpk_person_care_site_id"
        ),
        ForeignKeyConstraint(
            ["ethnicity_concept_id"],
            ["concept.concept_id"],
            name="fpk_person_ethnicity_concept_id",
        ),
        ForeignKeyConstraint(
            ["ethnicity_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_person_ethnicity_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["gender_concept_id"],
            ["concept.concept_id"],
            name="fpk_person_gender_concept_id",
        ),
        ForeignKeyConstraint(
            ["gender_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_person_gender_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["location_id"], ["location.location_id"], name="fpk_person_location_id"
        ),
        ForeignKeyConstraint(
            ["provider_id"], ["provider.provider_id"], name="fpk_person_provider_id"
        ),
        ForeignKeyConstraint(
            ["race_concept_id"],
            ["concept.concept_id"],
            name="fpk_person_race_concept_id",
        ),
        ForeignKeyConstraint(
            ["race_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_person_race_source_concept_id",
        ),
        PrimaryKeyConstraint("person_id", name="xpk_person"),
    )

    person_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    gender_concept_id: Mapped[int] = mapped_column(BigInteger)
    year_of_birth: Mapped[int] = mapped_column(BigInteger)
    race_concept_id: Mapped[int] = mapped_column(BigInteger)
    ethnicity_concept_id: Mapped[int] = mapped_column(BigInteger)
    month_of_birth: Mapped[Optional[int]] = mapped_column(BigInteger)
    day_of_birth: Mapped[Optional[int]] = mapped_column(BigInteger)
    birth_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    location_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    care_site_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    person_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    gender_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    gender_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    race_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    race_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    ethnicity_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    ethnicity_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    care_site: Mapped["CareSite"] = relationship("CareSite", back_populates="person")
    ethnicity_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[ethnicity_concept_id], back_populates="person"
    )
    ethnicity_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[ethnicity_source_concept_id], back_populates="person_"
    )
    gender_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[gender_concept_id], back_populates="person1"
    )
    gender_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[gender_source_concept_id], back_populates="person2"
    )
    location: Mapped["Location"] = relationship("Location", back_populates="person")
    provider: Mapped["Provider"] = relationship("Provider", back_populates="person")
    race_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[race_concept_id], back_populates="person3"
    )
    race_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[race_source_concept_id], back_populates="person4"
    )
    condition_era: Mapped[List["ConditionEra"]] = relationship(
        "ConditionEra", back_populates="person"
    )
    dose_era: Mapped[List["DoseEra"]] = relationship("DoseEra", back_populates="person")
    drug_era: Mapped[List["DrugEra"]] = relationship("DrugEra", back_populates="person")
    episode: Mapped[List["Episode"]] = relationship("Episode", back_populates="person")
    observation_period: Mapped[List["ObservationPeriod"]] = relationship(
        "ObservationPeriod", back_populates="person"
    )
    payer_plan_period: Mapped[List["PayerPlanPeriod"]] = relationship(
        "PayerPlanPeriod", back_populates="person"
    )
    specimen: Mapped[List["Specimen"]] = relationship(
        "Specimen", back_populates="person"
    )
    visit_occurrence: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence", back_populates="person"
    )
    visit_detail: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail", back_populates="person"
    )
    condition_occurrence: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence", back_populates="person"
    )
    device_exposure: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure", back_populates="person"
    )
    drug_exposure: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure", back_populates="person"
    )
    measurement: Mapped[List["Measurement"]] = relationship(
        "Measurement", back_populates="person"
    )
    note: Mapped[List["Note"]] = relationship("Note", back_populates="person")
    observation: Mapped[List["Observation"]] = relationship(
        "Observation", back_populates="person"
    )
    procedure_occurrence: Mapped[List["ProcedureOccurrence"]] = relationship(
        "ProcedureOccurrence", back_populates="person"
    )


class ConditionEra(Base):
    __tablename__ = "condition_era"
    __table_args__ = (
        ForeignKeyConstraint(
            ["condition_concept_id"],
            ["concept.concept_id"],
            name="fpk_condition_era_condition_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_condition_era_person_id"
        ),
        PrimaryKeyConstraint("condition_era_id", name="xpk_condition_era"),
    )

    condition_era_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    condition_concept_id: Mapped[int] = mapped_column(BigInteger)
    condition_era_start_date: Mapped[datetime.date] = mapped_column(Date)
    condition_era_end_date: Mapped[datetime.date] = mapped_column(Date)
    condition_occurrence_count: Mapped[Optional[int]] = mapped_column(BigInteger)

    condition_concept: Mapped["Concept"] = relationship(
        "Concept", back_populates="condition_era"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="condition_era")


t_death = Table(
    "death",
    Base.metadata,
    Column("person_id", BigInteger, nullable=False),
    Column("death_date", Date, nullable=False),
    Column("death_datetime", DateTime),
    Column("death_type_concept_id", BigInteger),
    Column("cause_concept_id", BigInteger),
    Column("cause_source_value", String(50)),
    Column("cause_source_concept_id", BigInteger),
    ForeignKeyConstraint(
        ["cause_concept_id"], ["concept.concept_id"], name="fpk_death_cause_concept_id"
    ),
    ForeignKeyConstraint(
        ["cause_source_concept_id"],
        ["concept.concept_id"],
        name="fpk_death_cause_source_concept_id",
    ),
    ForeignKeyConstraint(
        ["death_type_concept_id"],
        ["concept.concept_id"],
        name="fpk_death_death_type_concept_id",
    ),
    ForeignKeyConstraint(
        ["person_id"], ["person.person_id"], name="fpk_death_person_id"
    ),
)


class DoseEra(Base):
    __tablename__ = "dose_era"
    __table_args__ = (
        ForeignKeyConstraint(
            ["drug_concept_id"],
            ["concept.concept_id"],
            name="fpk_dose_era_drug_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_dose_era_person_id"
        ),
        ForeignKeyConstraint(
            ["unit_concept_id"],
            ["concept.concept_id"],
            name="fpk_dose_era_unit_concept_id",
        ),
        PrimaryKeyConstraint("dose_era_id", name="xpk_dose_era"),
    )

    dose_era_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    drug_concept_id: Mapped[int] = mapped_column(BigInteger)
    unit_concept_id: Mapped[int] = mapped_column(BigInteger)
    dose_value: Mapped[decimal.Decimal] = mapped_column(Numeric)
    dose_era_start_date: Mapped[datetime.date] = mapped_column(Date)
    dose_era_end_date: Mapped[datetime.date] = mapped_column(Date)

    drug_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[drug_concept_id], back_populates="dose_era"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="dose_era")
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[unit_concept_id], back_populates="dose_era_"
    )


class DrugEra(Base):
    __tablename__ = "drug_era"
    __table_args__ = (
        ForeignKeyConstraint(
            ["drug_concept_id"],
            ["concept.concept_id"],
            name="fpk_drug_era_drug_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_drug_era_person_id"
        ),
        PrimaryKeyConstraint("drug_era_id", name="xpk_drug_era"),
    )

    drug_era_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    drug_concept_id: Mapped[int] = mapped_column(BigInteger)
    drug_era_start_date: Mapped[datetime.date] = mapped_column(Date)
    drug_era_end_date: Mapped[datetime.date] = mapped_column(Date)
    drug_exposure_count: Mapped[Optional[int]] = mapped_column(BigInteger)
    gap_days: Mapped[Optional[int]] = mapped_column(BigInteger)

    drug_concept: Mapped["Concept"] = relationship("Concept", back_populates="drug_era")
    person: Mapped["Person"] = relationship("Person", back_populates="drug_era")


class Episode(Base):
    __tablename__ = "episode"
    __table_args__ = (
        ForeignKeyConstraint(
            ["episode_concept_id"],
            ["concept.concept_id"],
            name="fpk_episode_episode_concept_id",
        ),
        ForeignKeyConstraint(
            ["episode_object_concept_id"],
            ["concept.concept_id"],
            name="fpk_episode_episode_object_concept_id",
        ),
        ForeignKeyConstraint(
            ["episode_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_episode_episode_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["episode_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_episode_episode_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_episode_person_id"
        ),
        PrimaryKeyConstraint("episode_id", name="xpk_episode"),
    )

    episode_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    episode_concept_id: Mapped[int] = mapped_column(BigInteger)
    episode_start_date: Mapped[datetime.date] = mapped_column(Date)
    episode_object_concept_id: Mapped[int] = mapped_column(BigInteger)
    episode_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    episode_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    episode_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    episode_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    episode_parent_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    episode_number: Mapped[Optional[int]] = mapped_column(BigInteger)
    episode_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    episode_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    episode_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[episode_concept_id], back_populates="episode"
    )
    episode_object_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[episode_object_concept_id], back_populates="episode_"
    )
    episode_source_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[episode_source_concept_id], back_populates="episode1"
    )
    episode_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[episode_type_concept_id], back_populates="episode2"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="episode")


class ObservationPeriod(Base):
    __tablename__ = "observation_period"
    __table_args__ = (
        ForeignKeyConstraint(
            ["period_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_observation_period_period_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_observation_period_person_id"
        ),
        PrimaryKeyConstraint("observation_period_id", name="xpk_observation_period"),
    )

    observation_period_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    observation_period_start_date: Mapped[datetime.date] = mapped_column(Date)
    observation_period_end_date: Mapped[datetime.date] = mapped_column(Date)
    period_type_concept_id: Mapped[int] = mapped_column(BigInteger)

    period_type_concept: Mapped["Concept"] = relationship(
        "Concept", back_populates="observation_period"
    )
    person: Mapped["Person"] = relationship(
        "Person", back_populates="observation_period"
    )


class PayerPlanPeriod(Base):
    __tablename__ = "payer_plan_period"
    __table_args__ = (
        ForeignKeyConstraint(
            ["payer_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_payer_concept_id",
        ),
        ForeignKeyConstraint(
            ["payer_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_payer_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_payer_plan_period_person_id"
        ),
        ForeignKeyConstraint(
            ["plan_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_plan_concept_id",
        ),
        ForeignKeyConstraint(
            ["plan_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_plan_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["sponsor_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_sponsor_concept_id",
        ),
        ForeignKeyConstraint(
            ["sponsor_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_sponsor_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["stop_reason_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_stop_reason_concept_id",
        ),
        ForeignKeyConstraint(
            ["stop_reason_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_payer_plan_period_stop_reason_source_concept_id",
        ),
        PrimaryKeyConstraint("payer_plan_period_id", name="xpk_payer_plan_period"),
    )

    payer_plan_period_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    payer_plan_period_start_date: Mapped[datetime.date] = mapped_column(Date)
    payer_plan_period_end_date: Mapped[datetime.date] = mapped_column(Date)
    payer_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    payer_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    payer_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    plan_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    plan_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    plan_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    sponsor_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    sponsor_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    sponsor_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    family_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    stop_reason_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    stop_reason_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    stop_reason_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    payer_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[payer_concept_id], back_populates="payer_plan_period"
    )
    payer_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[payer_source_concept_id],
        back_populates="payer_plan_period_",
    )
    person: Mapped["Person"] = relationship(
        "Person", back_populates="payer_plan_period"
    )
    plan_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[plan_concept_id], back_populates="payer_plan_period1"
    )
    plan_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[plan_source_concept_id],
        back_populates="payer_plan_period2",
    )
    sponsor_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[sponsor_concept_id],
        back_populates="payer_plan_period3",
    )
    sponsor_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[sponsor_source_concept_id],
        back_populates="payer_plan_period4",
    )
    stop_reason_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[stop_reason_concept_id],
        back_populates="payer_plan_period5",
    )
    stop_reason_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[stop_reason_source_concept_id],
        back_populates="payer_plan_period6",
    )


class Specimen(Base):
    __tablename__ = "specimen"
    __table_args__ = (
        ForeignKeyConstraint(
            ["anatomic_site_concept_id"],
            ["concept.concept_id"],
            name="fpk_specimen_anatomic_site_concept_id",
        ),
        ForeignKeyConstraint(
            ["disease_status_concept_id"],
            ["concept.concept_id"],
            name="fpk_specimen_disease_status_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_specimen_person_id"
        ),
        ForeignKeyConstraint(
            ["specimen_concept_id"],
            ["concept.concept_id"],
            name="fpk_specimen_specimen_concept_id",
        ),
        ForeignKeyConstraint(
            ["specimen_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_specimen_specimen_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["unit_concept_id"],
            ["concept.concept_id"],
            name="fpk_specimen_unit_concept_id",
        ),
        PrimaryKeyConstraint("specimen_id", name="xpk_specimen"),
    )

    specimen_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    specimen_concept_id: Mapped[int] = mapped_column(BigInteger)
    specimen_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    specimen_date: Mapped[datetime.date] = mapped_column(Date)
    specimen_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    quantity: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    unit_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    anatomic_site_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    disease_status_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    specimen_source_id: Mapped[Optional[str]] = mapped_column(String(255))
    specimen_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    anatomic_site_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    disease_status_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    anatomic_site_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[anatomic_site_concept_id], back_populates="specimen"
    )
    disease_status_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[disease_status_concept_id], back_populates="specimen_"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="specimen")
    specimen_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[specimen_concept_id], back_populates="specimen1"
    )
    specimen_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[specimen_type_concept_id], back_populates="specimen2"
    )
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[unit_concept_id], back_populates="specimen3"
    )


class VisitOccurrence(Base):
    __tablename__ = "visit_occurrence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["admitted_from_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_occurrence_admitted_from_concept_id",
        ),
        ForeignKeyConstraint(
            ["care_site_id"],
            ["care_site.care_site_id"],
            name="fpk_visit_occurrence_care_site_id",
        ),
        ForeignKeyConstraint(
            ["discharged_to_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_occurrence_discharged_to_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_visit_occurrence_person_id"
        ),
        ForeignKeyConstraint(
            ["preceding_visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_visit_occurrence_preceding_visit_occurrence_id",
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_visit_occurrence_provider_id",
        ),
        ForeignKeyConstraint(
            ["visit_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_occurrence_visit_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_occurrence_visit_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_occurrence_visit_type_concept_id",
        ),
        PrimaryKeyConstraint("visit_occurrence_id", name="xpk_visit_occurrence"),
    )

    visit_occurrence_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    visit_concept_id: Mapped[int] = mapped_column(BigInteger)
    visit_start_date: Mapped[datetime.date] = mapped_column(Date)
    visit_end_date: Mapped[datetime.date] = mapped_column(Date)
    visit_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    visit_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    visit_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    care_site_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    visit_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    admitted_from_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    admitted_from_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    discharged_to_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    discharged_to_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    preceding_visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    admitted_from_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[admitted_from_concept_id],
        back_populates="visit_occurrence",
    )
    care_site: Mapped["CareSite"] = relationship(
        "CareSite", back_populates="visit_occurrence"
    )
    discharged_to_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[discharged_to_concept_id],
        back_populates="visit_occurrence_",
    )
    person: Mapped["Person"] = relationship("Person", back_populates="visit_occurrence")
    preceding_visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence",
        remote_side=[visit_occurrence_id],
        back_populates="preceding_visit_occurrence_reverse",
    )
    preceding_visit_occurrence_reverse: Mapped[List["VisitOccurrence"]] = relationship(
        "VisitOccurrence",
        remote_side=[preceding_visit_occurrence_id],
        back_populates="preceding_visit_occurrence",
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="visit_occurrence"
    )
    visit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[visit_concept_id], back_populates="visit_occurrence1"
    )
    visit_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[visit_source_concept_id],
        back_populates="visit_occurrence2",
    )
    visit_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[visit_type_concept_id],
        back_populates="visit_occurrence3",
    )
    visit_detail: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail", back_populates="visit_occurrence"
    )
    condition_occurrence: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence", back_populates="visit_occurrence"
    )
    device_exposure: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure", back_populates="visit_occurrence"
    )
    drug_exposure: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure", back_populates="visit_occurrence"
    )
    measurement: Mapped[List["Measurement"]] = relationship(
        "Measurement", back_populates="visit_occurrence"
    )
    note: Mapped[List["Note"]] = relationship("Note", back_populates="visit_occurrence")
    observation: Mapped[List["Observation"]] = relationship(
        "Observation", back_populates="visit_occurrence"
    )
    procedure_occurrence: Mapped[List["ProcedureOccurrence"]] = relationship(
        "ProcedureOccurrence", back_populates="visit_occurrence"
    )


t_episode_event = Table(
    "episode_event",
    Base.metadata,
    Column("episode_id", BigInteger, nullable=False),
    Column("event_id", BigInteger, nullable=False),
    Column("episode_event_field_concept_id", BigInteger, nullable=False),
    ForeignKeyConstraint(
        ["episode_event_field_concept_id"],
        ["concept.concept_id"],
        name="fpk_episode_event_episode_event_field_concept_id",
    ),
    ForeignKeyConstraint(
        ["episode_id"], ["episode.episode_id"], name="fpk_episode_event_episode_id"
    ),
)


class VisitDetail(Base):
    __tablename__ = "visit_detail"
    __table_args__ = (
        ForeignKeyConstraint(
            ["care_site_id"],
            ["care_site.care_site_id"],
            name="fpk_visit_detail_care_site_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_visit_detail_person_id"
        ),
        ForeignKeyConstraint(
            ["preceding_visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_visit_detail_preceding_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_visit_detail_provider_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_detail_visit_detail_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_visit_detail_visit_detail_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_visit_detail_visit_occurrence_id",
        ),
        PrimaryKeyConstraint("visit_detail_id", name="xpk_visit_detail"),
    )

    visit_detail_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    visit_detail_concept_id: Mapped[int] = mapped_column(BigInteger)
    visit_detail_start_date: Mapped[datetime.date] = mapped_column(Date)
    visit_detail_end_date: Mapped[datetime.date] = mapped_column(Date)
    visit_detail_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[int] = mapped_column(BigInteger)
    visit_detail_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    visit_detail_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    care_site_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    admitting_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    discharge_to_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    preceding_visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_source_value: Mapped[Optional[str]] = mapped_column(String(120))
    visit_detail_source_concept_id: Mapped[Optional[str]] = mapped_column(String(50))
    admitting_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    discharge_to_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    visit_detail_parent_id: Mapped[Optional[str]] = mapped_column(String(50))

    care_site: Mapped["CareSite"] = relationship(
        "CareSite", back_populates="visit_detail"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="visit_detail")
    preceding_visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail",
        remote_side=[visit_detail_id],
        back_populates="preceding_visit_detail_reverse",
    )
    preceding_visit_detail_reverse: Mapped[List["VisitDetail"]] = relationship(
        "VisitDetail",
        remote_side=[preceding_visit_detail_id],
        back_populates="preceding_visit_detail",
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="visit_detail"
    )
    visit_detail_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[visit_detail_concept_id], back_populates="visit_detail"
    )
    visit_detail_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[visit_detail_type_concept_id],
        back_populates="visit_detail_",
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="visit_detail"
    )
    condition_occurrence: Mapped[List["ConditionOccurrence"]] = relationship(
        "ConditionOccurrence", back_populates="visit_detail"
    )
    device_exposure: Mapped[List["DeviceExposure"]] = relationship(
        "DeviceExposure", back_populates="visit_detail"
    )
    drug_exposure: Mapped[List["DrugExposure"]] = relationship(
        "DrugExposure", back_populates="visit_detail"
    )
    measurement: Mapped[List["Measurement"]] = relationship(
        "Measurement", back_populates="visit_detail"
    )
    note: Mapped[List["Note"]] = relationship("Note", back_populates="visit_detail")
    observation: Mapped[List["Observation"]] = relationship(
        "Observation", back_populates="visit_detail"
    )
    procedure_occurrence: Mapped[List["ProcedureOccurrence"]] = relationship(
        "ProcedureOccurrence", back_populates="visit_detail"
    )


class ConditionOccurrence(Base):
    __tablename__ = "condition_occurrence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["condition_concept_id"],
            ["concept.concept_id"],
            name="fpk_condition_occurrence_condition_concept_id",
        ),
        ForeignKeyConstraint(
            ["condition_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_condition_occurrence_condition_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["condition_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_condition_occurrence_condition_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"],
            ["person.person_id"],
            name="fpk_condition_occurrence_person_id",
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_condition_occurrence_provider_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_condition_occurrence_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_condition_occurrence_visit_occurrence_id",
        ),
        PrimaryKeyConstraint(
            "condition_occurrence_id", name="xpk_condition_occurrence"
        ),
    )

    condition_occurrence_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    condition_concept_id: Mapped[int] = mapped_column(BigInteger)
    condition_start_date: Mapped[datetime.date] = mapped_column(Date)
    condition_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    condition_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    condition_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    condition_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    stop_reason: Mapped[Optional[str]] = mapped_column(String(20))
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    condition_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    condition_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    condition_status_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    condition_status_concept_id: Mapped[Optional[str]] = mapped_column(String(50))

    condition_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[condition_concept_id],
        back_populates="condition_occurrence",
    )
    condition_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[condition_source_concept_id],
        back_populates="condition_occurrence_",
    )
    condition_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[condition_type_concept_id],
        back_populates="condition_occurrence1",
    )
    person: Mapped["Person"] = relationship(
        "Person", back_populates="condition_occurrence"
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="condition_occurrence"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="condition_occurrence"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="condition_occurrence"
    )


class DeviceExposure(Base):
    __tablename__ = "device_exposure"
    __table_args__ = (
        ForeignKeyConstraint(
            ["device_concept_id"],
            ["concept.concept_id"],
            name="fpk_device_exposure_device_concept_id",
        ),
        ForeignKeyConstraint(
            ["device_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_device_exposure_device_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["device_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_device_exposure_device_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_device_exposure_person_id"
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_device_exposure_provider_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_device_exposure_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_device_exposure_visit_occurrence_id",
        ),
        PrimaryKeyConstraint("device_exposure_id", name="xpk_device_exposure"),
    )

    device_exposure_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    device_concept_id: Mapped[int] = mapped_column(BigInteger)
    device_exposure_start_date: Mapped[datetime.date] = mapped_column(Date)
    device_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    device_exposure_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    device_exposure_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    device_exposure_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    unique_device_id: Mapped[Optional[str]] = mapped_column(String(255))
    quantity: Mapped[Optional[int]] = mapped_column(BigInteger)
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    device_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    device_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    device_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[device_concept_id], back_populates="device_exposure"
    )
    device_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[device_source_concept_id],
        back_populates="device_exposure_",
    )
    device_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[device_type_concept_id],
        back_populates="device_exposure1",
    )
    person: Mapped["Person"] = relationship("Person", back_populates="device_exposure")
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="device_exposure"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="device_exposure"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="device_exposure"
    )


class DrugExposure(Base):
    __tablename__ = "drug_exposure"
    __table_args__ = (
        ForeignKeyConstraint(
            ["drug_concept_id"],
            ["concept.concept_id"],
            name="fpk_drug_exposure_drug_concept_id",
        ),
        ForeignKeyConstraint(
            ["drug_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_drug_exposure_drug_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["drug_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_drug_exposure_drug_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_drug_exposure_person_id"
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_drug_exposure_provider_id",
        ),
        ForeignKeyConstraint(
            ["route_concept_id"],
            ["concept.concept_id"],
            name="fpk_drug_exposure_route_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_drug_exposure_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_drug_exposure_visit_occurrence_id",
        ),
        PrimaryKeyConstraint("drug_exposure_id", name="xpk_drug_exposure"),
    )

    drug_exposure_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    drug_concept_id: Mapped[int] = mapped_column(BigInteger)
    drug_exposure_start_date: Mapped[datetime.date] = mapped_column(Date)
    drug_exposure_end_date: Mapped[datetime.date] = mapped_column(Date)
    drug_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    drug_exposure_start_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    drug_exposure_end_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime
    )
    verbatim_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    stop_reason: Mapped[Optional[str]] = mapped_column(String(20))
    refills: Mapped[Optional[int]] = mapped_column(BigInteger)
    quantity: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    days_supply: Mapped[Optional[int]] = mapped_column(BigInteger)
    sig: Mapped[Optional[str]] = mapped_column(Text)
    route_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    lot_number: Mapped[Optional[str]] = mapped_column(String(50))
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    drug_source_value: Mapped[Optional[str]] = mapped_column(String(255))
    drug_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    route_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    dose_unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    drug_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[drug_concept_id], back_populates="drug_exposure"
    )
    drug_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[drug_source_concept_id],
        back_populates="drug_exposure_",
    )
    drug_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[drug_type_concept_id], back_populates="drug_exposure1"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="drug_exposure")
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="drug_exposure"
    )
    route_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[route_concept_id], back_populates="drug_exposure2"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="drug_exposure"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="drug_exposure"
    )


class Measurement(Base):
    __tablename__ = "measurement"
    __table_args__ = (
        ForeignKeyConstraint(
            ["measurement_concept_id"],
            ["concept.concept_id"],
            name="fpk_measurement_measurement_concept_id",
        ),
        ForeignKeyConstraint(
            ["measurement_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_measurement_measurement_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["measurement_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_measurement_measurement_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["operator_concept_id"],
            ["concept.concept_id"],
            name="fpk_measurement_operator_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_measurement_person_id"
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_measurement_provider_id",
        ),
        ForeignKeyConstraint(
            ["unit_concept_id"],
            ["concept.concept_id"],
            name="fpk_measurement_unit_concept_id",
        ),
        ForeignKeyConstraint(
            ["value_as_concept_id"],
            ["concept.concept_id"],
            name="fpk_measurement_value_as_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_measurement_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_measurement_visit_occurrence_id",
        ),
        PrimaryKeyConstraint("measurement_id", name="xpk_measurement"),
    )

    measurement_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    measurement_concept_id: Mapped[int] = mapped_column(BigInteger)
    measurement_date: Mapped[datetime.date] = mapped_column(Date)
    measurement_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    measurement_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    measurement_time: Mapped[Optional[str]] = mapped_column(String(10))
    operator_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    value_as_number: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    value_as_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    unit_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    range_low: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    range_high: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    measurement_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    measurement_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    value_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    measurement_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[measurement_concept_id], back_populates="measurement"
    )
    measurement_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[measurement_source_concept_id],
        back_populates="measurement_",
    )
    measurement_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[measurement_type_concept_id],
        back_populates="measurement1",
    )
    operator_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[operator_concept_id], back_populates="measurement2"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="measurement")
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="measurement"
    )
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[unit_concept_id], back_populates="measurement3"
    )
    value_as_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[value_as_concept_id], back_populates="measurement4"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="measurement"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="measurement"
    )


class Note(Base):
    __tablename__ = "note"
    __table_args__ = (
        ForeignKeyConstraint(
            ["encoding_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_encoding_concept_id",
        ),
        ForeignKeyConstraint(
            ["language_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_language_concept_id",
        ),
        ForeignKeyConstraint(
            ["note_class_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_note_class_concept_id",
        ),
        ForeignKeyConstraint(
            ["note_event_field_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_note_event_field_concept_id",
        ),
        ForeignKeyConstraint(
            ["note_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_note_note_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_note_person_id"
        ),
        ForeignKeyConstraint(
            ["provider_id"], ["provider.provider_id"], name="fpk_note_provider_id"
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_note_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_note_visit_occurrence_id",
        ),
        PrimaryKeyConstraint("note_id", name="xpk_note"),
    )

    note_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    note_date: Mapped[datetime.date] = mapped_column(Date)
    note_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    note_class_concept_id: Mapped[int] = mapped_column(BigInteger)
    note_text: Mapped[str] = mapped_column(Text)
    encoding_concept_id: Mapped[int] = mapped_column(BigInteger)
    language_concept_id: Mapped[int] = mapped_column(BigInteger)
    note_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    note_title: Mapped[Optional[str]] = mapped_column(String(250))
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    note_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    note_event_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    note_event_field_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    encoding_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[encoding_concept_id], back_populates="note"
    )
    language_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[language_concept_id], back_populates="note_"
    )
    note_class_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[note_class_concept_id], back_populates="note1"
    )
    note_event_field_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[note_event_field_concept_id], back_populates="note2"
    )
    note_type_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[note_type_concept_id], back_populates="note3"
    )
    person: Mapped["Person"] = relationship("Person", back_populates="note")
    provider: Mapped["Provider"] = relationship("Provider", back_populates="note")
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="note"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="note"
    )


class Observation(Base):
    __tablename__ = "observation"
    __table_args__ = (
        ForeignKeyConstraint(
            ["observation_concept_id"],
            ["concept.concept_id"],
            name="fpk_observation_observation_concept_id",
        ),
        ForeignKeyConstraint(
            ["observation_source_concept_id"],
            ["concept.concept_id"],
            name="fpk_observation_observation_source_concept_id",
        ),
        ForeignKeyConstraint(
            ["observation_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_observation_observation_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"], ["person.person_id"], name="fpk_observation_person_id"
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_observation_provider_id",
        ),
        ForeignKeyConstraint(
            ["qualifier_concept_id"],
            ["concept.concept_id"],
            name="fpk_observation_qualifier_concept_id",
        ),
        ForeignKeyConstraint(
            ["unit_concept_id"],
            ["concept.concept_id"],
            name="fpk_observation_unit_concept_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_observation_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_observation_visit_occurrence_id",
        ),
        PrimaryKeyConstraint("observation_id", name="xpk_observation"),
    )

    observation_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    observation_concept_id: Mapped[int] = mapped_column(BigInteger)
    observation_date: Mapped[datetime.date] = mapped_column(Date)
    observation_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    observation_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    value_as_number: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    value_as_string: Mapped[Optional[str]] = mapped_column(String(120))
    value_as_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    qualifier_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    unit_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    observation_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    observation_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    unit_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    qualifier_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    observation_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[observation_concept_id], back_populates="observation"
    )
    observation_source_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[observation_source_concept_id],
        back_populates="observation_",
    )
    observation_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[observation_type_concept_id],
        back_populates="observation1",
    )
    person: Mapped["Person"] = relationship("Person", back_populates="observation")
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="observation"
    )
    qualifier_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[qualifier_concept_id], back_populates="observation2"
    )
    unit_concept: Mapped["Concept"] = relationship(
        "Concept", foreign_keys=[unit_concept_id], back_populates="observation3"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="observation"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="observation"
    )


class ProcedureOccurrence(Base):
    __tablename__ = "procedure_occurrence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["modifier_concept_id"],
            ["concept.concept_id"],
            name="fpk_procedure_occurrence_modifier_concept_id",
        ),
        ForeignKeyConstraint(
            ["person_id"],
            ["person.person_id"],
            name="fpk_procedure_occurrence_person_id",
        ),
        ForeignKeyConstraint(
            ["procedure_type_concept_id"],
            ["concept.concept_id"],
            name="fpk_procedure_occurrence_procedure_type_concept_id",
        ),
        ForeignKeyConstraint(
            ["provider_id"],
            ["provider.provider_id"],
            name="fpk_procedure_occurrence_provider_id",
        ),
        ForeignKeyConstraint(
            ["visit_detail_id"],
            ["visit_detail.visit_detail_id"],
            name="fpk_procedure_occurrence_visit_detail_id",
        ),
        ForeignKeyConstraint(
            ["visit_occurrence_id"],
            ["visit_occurrence.visit_occurrence_id"],
            name="fpk_procedure_occurrence_visit_occurrence_id",
        ),
        PrimaryKeyConstraint(
            "procedure_occurrence_id", name="xpk_procedure_occurrence"
        ),
    )

    procedure_occurrence_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    person_id: Mapped[int] = mapped_column(BigInteger)
    procedure_concept_id: Mapped[int] = mapped_column(BigInteger)
    procedure_date: Mapped[datetime.date] = mapped_column(Date)
    procedure_type_concept_id: Mapped[int] = mapped_column(BigInteger)
    procedure_datetime: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    modifier_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    quantity: Mapped[Optional[int]] = mapped_column(BigInteger)
    provider_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_occurrence_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    visit_detail_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    procedure_source_value: Mapped[Optional[str]] = mapped_column(String(50))
    procedure_source_concept_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    modifier_source_value: Mapped[Optional[str]] = mapped_column(String(50))

    modifier_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[modifier_concept_id],
        back_populates="procedure_occurrence",
    )
    person: Mapped["Person"] = relationship(
        "Person", back_populates="procedure_occurrence"
    )
    procedure_type_concept: Mapped["Concept"] = relationship(
        "Concept",
        foreign_keys=[procedure_type_concept_id],
        back_populates="procedure_occurrence_",
    )
    provider: Mapped["Provider"] = relationship(
        "Provider", back_populates="procedure_occurrence"
    )
    visit_detail: Mapped["VisitDetail"] = relationship(
        "VisitDetail", back_populates="procedure_occurrence"
    )
    visit_occurrence: Mapped["VisitOccurrence"] = relationship(
        "VisitOccurrence", back_populates="procedure_occurrence"
    )
