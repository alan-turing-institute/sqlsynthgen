from typing import Any, Callable, Dict, List, Optional, Type
import numpy as np
import pandas as pd
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

columns: List[str] = [
    "age",
    "workclass",
    "fnlwgt",
    "education",
    "education-num",
    "marital-status",
    "occupation",
    "relationship",
    "race",
    "sex",
    "capital-gain",
    "capital-loss",
    "hours-per-week",
    "native-country",
    "income",
]


Base = declarative_base()


class Adult(Base):
    __tablename__ = "adult"

    adult_id = Column(Integer, primary_key=True)
    age = Column(Integer)
    fnlwgt = Column(Integer)
    education_num = Column(Integer)
    capital_gain = Column(Integer)
    capital_loss = Column(Integer)
    hours_per_week = Column(Integer)

    income_id = Column(Integer, ForeignKey("income.income_id"))
    work_class_id = Column(Integer, ForeignKey("workclass.work_class_id"))
    education_id = Column(Integer, ForeignKey("education.education_id"))
    marital_status_id = Column(Integer, ForeignKey("marital_status.marital_status_id"))
    occupation_id = Column(Integer, ForeignKey("occupation.occupation_id"))
    relationship_id = Column(Integer, ForeignKey("relationship.relationship_id"))
    race_id = Column(Integer, ForeignKey("race.race_id"))
    sex_id = Column(Integer, ForeignKey("sex.sex_id"))
    native_country_id = Column(Integer, ForeignKey("native_country.native_country_id"))


class WorkClass(Base):
    __tablename__ = "workclass"

    work_class_id = Column(Integer, primary_key=True)
    work_class_name = Column(String, unique=True)


class Education(Base):
    __tablename__ = "education"

    education_id = Column(Integer, primary_key=True)
    education_name = Column(String, unique=True)


class MaritalStatus(Base):
    __tablename__ = "marital_status"

    marital_status_id = Column(Integer, primary_key=True)
    marital_status_name = Column(String, unique=True)


class Occupation(Base):
    __tablename__ = "occupation"

    occupation_id = Column(Integer, primary_key=True)
    occupation_name = Column(String, unique=True)


class Relationship(Base):
    __tablename__ = "relationship"

    relationship_id = Column(Integer, primary_key=True)
    relationship_name = Column(String, unique=True)


class Race(Base):
    __tablename__ = "race"

    race_id = Column(Integer, primary_key=True)
    race_name = Column(String, unique=True)


class Sex(Base):
    __tablename__ = "sex"

    sex_id = Column(Integer, primary_key=True)
    sex_name = Column(String, unique=True)


class NativeCountry(Base):
    __tablename__ = "native_country"

    native_country_id = Column(Integer, primary_key=True)
    native_country_name = Column(String, unique=True)


class Income(Base):
    __tablename__ = "income"

    income_id = Column(Integer, primary_key=True)
    income_name = Column(String, unique=True)


def load_vocabulary_table(
    session: Session, series: pd.Series, object_creator: Callable
):
    all_names: np.ndarray = series.unique()
    all_values = [object_creator(name) for name in all_names]
    session.add_all(all_values)


def get_reference_by_value(
    session: Session, entity_class: Type, filter: Dict[str, Any]
) -> Any:
    query_results: Optional[entity_class] = (
        session.query(entity_class).filter_by(**filter).first()
    )

    return query_results


def load_all_vocabulary_tables(session: Session, adult_data_frame: pd.DataFrame):
    load_vocabulary_table(
        session,
        adult_data_frame["workclass"].str.strip(),
        lambda name: WorkClass(work_class_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["education"].str.strip(),
        lambda name: Education(education_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["marital-status"].str.strip(),
        lambda name: MaritalStatus(marital_status_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["occupation"].str.strip(),
        lambda name: Occupation(occupation_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["relationship"].str.strip(),
        lambda name: Relationship(relationship_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["race"].str.strip(),
        lambda name: Race(race_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["sex"].str.strip(),
        lambda name: Sex(sex_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["native-country"].str.strip(),
        lambda name: NativeCountry(native_country_name=name),
    )

    load_vocabulary_table(
        session,
        adult_data_frame["income"].str.strip(),
        lambda name: Income(income_name=name),
    )


def get_adult_from_series(session: Session, data_series: pd.Series) -> Adult:
    adult_work_class: WorkClass = get_reference_by_value(
        session, WorkClass, {"work_class_name": data_series["workclass"]}
    )

    adult_education: Education = get_reference_by_value(
        session, Education, {"education_name": data_series["education"]}
    )

    adult_marital_status: MaritalStatus = get_reference_by_value(
        session,
        MaritalStatus,
        {"marital_status_name": data_series["marital-status"]},
    )

    adult_occupation: Occupation = get_reference_by_value(
        session,
        Occupation,
        {"occupation_name": data_series["occupation"]},
    )

    adult_relationship: Relationship = get_reference_by_value(
        session,
        Relationship,
        {"relationship_name": data_series["relationship"]},
    )

    adult_race: Race = get_reference_by_value(
        session,
        Race,
        {"race_name": data_series["race"]},
    )

    adult_income: Income = get_reference_by_value(
        session,
        Income,
        {"income_name": data_series["income"]},
    )

    adult_sex: Sex = get_reference_by_value(
        session,
        Sex,
        {"sex_name": data_series["sex"]},
    )

    adult_native_country: NativeCountry = get_reference_by_value(
        session,
        NativeCountry,
        {"native_country_name": data_series["native-country"]},
    )

    return Adult(
        age=data_series["age"],
        fnlwgt=data_series["fnlwgt"],
        education_num=data_series["education-num"],
        capital_gain=data_series["capital-gain"],
        capital_loss=data_series["capital-loss"],
        hours_per_week=data_series["hours-per-week"],
        work_class_id=adult_work_class.work_class_id,
        education_id=adult_education.education_id,
        marital_status_id=adult_marital_status.marital_status_id,
        income_id=adult_income.income_id,
        occupation_id=adult_occupation.occupation_id,
        relationship_id=adult_relationship.relationship_id,
        race_id=adult_race.race_id,
        sex_id=adult_sex.sex_id,
        native_country_id=adult_native_country.native_country_id,
    )


def main():
    database_url: str = "postgresql://postgres:password@localhost:5432/adults"
    adult_dataset_file: str = "adult.data"

    engine = create_engine(database_url)
    metadata = Base.metadata

    metadata.drop_all(engine)
    metadata.create_all(engine)

    adult_data_frame: pd.DataFrame = pd.read_csv(adult_dataset_file, header=None)
    adult_data_frame.columns = columns

    with Session(engine) as session:
        load_all_vocabulary_tables(session, adult_data_frame)

        for _, data_series in adult_data_frame.iterrows():
            session.add(get_adult_from_series(session, data_series))

        session.commit()


if __name__ == "__main__":
    main()
