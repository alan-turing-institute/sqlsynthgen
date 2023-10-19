from typing import Any, Callable, List, Type
import numpy as np
import pandas as pd
from tqdm import tqdm
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()


class Country(Base):
    __tablename__ = "countries"

    country_destination = Column(String, primary_key=True)
    lat_destination = Column(Float)
    lng_destination = Column(Float)
    distance_km = Column(Float)
    destination_km2 = Column(Integer)
    destination_language = Column(String)
    language_levenshtein_distance = Column(Float)


class AgeGenderBucket(Base):
    __tablename__ = "age_gender_bkts"

    age_bucket = Column(String, primary_key=True)
    country_destination = Column(
        String, ForeignKey("countries.country_destination"), primary_key=True
    )
    gender = Column(String, primary_key=True)
    population_in_thousands = Column(Integer)
    year = Column(Integer)


class UserSession(Base):
    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"))
    action = Column(String)
    action_type = Column(String)
    action_detail = Column(String)
    device_type = Column(String)
    secs_elapsed = Column(Float)


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True)
    date_account_created = Column(Date)
    timestamp_first_active = Column(DateTime)
    date_first_booking = Column(Date)
    gender = Column(String)
    age = Column(Integer)
    signup_method = Column(String)
    signup_flow = Column(Integer)
    language = Column(String)
    affiliate_channel = Column(String)
    affiliate_provider = Column(String)
    first_affiliate_tracked = Column(String)
    signup_app = Column(String)
    first_device_type = Column(String)
    first_browser = Column(String)
    country_destination = Column(String, ForeignKey("countries.country_destination"))


def csv_to_dataframe(filename: str) -> pd.DataFrame:
    dataframe: pd.DataFrame = pd.read_csv(filename, header=0)
    dataframe.columns = [column.strip() for column in dataframe.columns]

    return dataframe


def user_csv_to_dataframe(filename: str) -> pd.DataFrame:
    dataframe: pd.DataFrame = csv_to_dataframe(filename)
    dataframe["timestamp_first_active"] = pd.to_datetime(
        dataframe["timestamp_first_active"], format="%Y%m%d%H%M%S"
    )
    dataframe["country_destination"] = dataframe["country_destination"].replace(
        ["NDF", "other"], None
    )

    return dataframe


def always_insert(object: Any, db_session: Session) -> bool:
    return True


def upload_csv_to_database(
    filename: str,
    mapped_class: Type,
    session: Session,
    dataframe_function: Callable[[str], pd.DataFrame] = csv_to_dataframe,
    filter_function: Callable[[Any, Session], bool] = always_insert,
) -> None:
    print(f"Loading {filename}")

    dataframe: pd.DataFrame = dataframe_function(filename)
    dataframe = dataframe.replace({np.nan: None})

    num_rows = len(dataframe)
    for _, data_as_series in tqdm(dataframe.iterrows(), total=num_rows):
        model_instance = mapped_class(**data_as_series)
        if filter_function(model_instance, session):
            session.add(model_instance)
        else:
            print(f"Skipping: {model_instance=}")
    session.commit()


def main():
    database_url: str = "postgresql://postgres:password@localhost:5432/airbnb"
    countries_data_file: str = "countries.csv"
    buckets_data_file: str = "age_gender_bkts.csv"
    users_data_file: str = "train_users_2.csv"
    sessions_data_file: str = "sessions.csv"

    engine = create_engine(database_url)
    metadata = Base.metadata

    metadata.drop_all(engine)
    metadata.create_all(engine)

    with Session(engine) as session:
        upload_csv_to_database(countries_data_file, Country, session)
        upload_csv_to_database(buckets_data_file, AgeGenderBucket, session)
        upload_csv_to_database(users_data_file, User, session, user_csv_to_dataframe)

        all_user_ids = {t[0] for t in session.query(User.id).all()}

        def insert_if_user_present(user_session: UserSession, _: Session) -> bool:
            return user_session.user_id in all_user_ids

        upload_csv_to_database(
            sessions_data_file,
            UserSession,
            session,
            filter_function=insert_if_user_present,
        )


if __name__ == "__main__":
    main()
