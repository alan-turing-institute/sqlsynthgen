from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base


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
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    main()
