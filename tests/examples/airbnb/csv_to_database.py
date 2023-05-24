from typing import Any, Callable, List, Type
import numpy as np
import pandas as pd
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

    dataframe = dataframe.replace({np.nan: None})

    return dataframe


def upload_csv_to_database(
    filename: str,
    mapped_class: Type,
    session: Session,
    dataframe_function: Callable[[str], pd.DataFrame] = csv_to_dataframe,
) -> None:
    print(f"Loading {filename}")

    dataframe: pd.DataFrame = dataframe_function(filename)
    session.add_all(
        [mapped_class(**data_as_series) for _, data_as_series in dataframe.iterrows()]
    )
    session.commit()


def main():
    database_url: str = "postgresql://postgres:password@localhost:5432/airbnb"
    countries_data_file: str = "countries.csv"
    buckets_data_file: str = "age_gender_bkts.csv"
    users_data_file: str = "train_users_2.csv"

    engine = create_engine(database_url)
    metadata = Base.metadata

    metadata.drop_all(engine)
    metadata.create_all(engine)

    with Session(engine) as session:
        upload_csv_to_database(countries_data_file, Country, session)
        upload_csv_to_database(buckets_data_file, AgeGenderBucket, session)
        upload_csv_to_database(users_data_file, User, session, user_csv_to_dataframe)


if __name__ == "__main__":
    main()
