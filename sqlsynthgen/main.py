from sqlalchemy import create_engine

from sqlsynthgen.settings import Settings
from sqlsynthgen.star import metadata


def main():
    settings = Settings()
    engine = create_engine(settings.postgres_dsn)
    metadata.create_all(bind=engine)


if __name__ == "__main__":
    main()
