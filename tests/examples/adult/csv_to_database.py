from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, Mapped

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
    __tablename__ = "adults"

    adult_id = Column(Integer, primary_key=True)
    age = Column(Integer)
    fnlwgt = Column(Integer)
    education_num = Column(Integer)
    capital_gain = Column(Integer)
    capital_loss = Column(Integer)
    hours_per_week = Column(Integer)

    work_class_id = Column(Integer, ForeignKey("workclasses.work_class_id"))


class WorkClass(Base):
    __tablename__ = "workclasses"

    work_class_id = Column(Integer, primary_key=True)
    work_class_name = Column(String)


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
        all_work_classes: np.ndarray = adult_data_frame["workclass"].unique()

        for work_class_name in all_work_classes:
            work_class: WorkClass = WorkClass(work_class_name=work_class_name)
            session.add(work_class)

        for _, data_series in adult_data_frame.iterrows():
            adult_work_class: Optional[WorkClass] = (
                session.query(WorkClass)
                .filter_by(work_class_name=data_series["workclass"])
                .first()
            )

            adult: Adult = Adult(
                age=data_series["age"],
                fnlwgt=data_series["fnlwgt"],
                work_class_id=adult_work_class.work_class_id if adult_work_class else None,
            )
            session.add(adult)

        session.commit()


if __name__ == "__main__":
    main()
