"""Utils for reading settings from environment variables.

See module pydantic for enforcing type hints at runtime.
See module functools.lru_cache to save time and memory
in case of repeated calls.
See module typing for type hinting.

Classes:

    Settings

Functions:

    get_settings() -> Settings

"""
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional

# pylint: disable=no-self-argument
from pydantic import BaseSettings, PostgresDsn, validator


class Settings(BaseSettings):
    """A Pydantic settings class with optional and mandatory settings.

    Settings class attributes describe two database connection. The source database connection is
    the database schema from which the object relational model is discovered. The destination
    database connection is the location where tables based on the ORM is created
    and synthetic values inserted.

    Attributes:
        src_host_name (str):
            An element (host-name) of connection parameter
        src_port (int):
            Connection port eg. 5432
        src_user_name (str) :
            Connection username e.g. `postgres` or `myuser@mydb`
        src_password (str) :
            Connection password
        src_db_name (str) :
            Connection database e.g. "postgres"
        src_ssl_required (bool) :
            Flag `True` if db requires SSL

        dst_host_name (str):
            Connection host-name to destination db
        dst_port (int) :
            Connection port eg. 5432
        dst_user_name (str) :
            Connection username e.g. `postgres` or `myuser@mydb`
        dst_password (str) :
            Connection password
        dst_db_name (str) :
            Connection database e.g. `postgres`
        dst_ssl_required (bool) :
            Flag `True` if db requires SSL
    """

    # Connection parameters for the source PostgreSQL database. See also
    # https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    src_host_name: Optional[str]  # e.g. "mydb.mydomain.com" or "0.0.0.0"
    src_port: int = 5432
    src_user_name: Optional[str]  # e.g. "postgres" or "myuser@mydb"
    src_password: Optional[str]
    src_db_name: Optional[str]
    src_ssl_required: bool = False  # whether the db requires SSL
    src_schema: Optional[str]

    # Connection parameters for the destination PostgreSQL database.
    dst_host_name: Optional[
        str
    ]  # Connection parameter e.g. "mydb.mydomain.com" or "0.0.0.0"
    dst_port: int = 5432
    dst_user_name: Optional[str]  # e.g. "postgres" or "myuser@mydb"
    dst_password: Optional[str]
    dst_db_name: Optional[str]
    dst_schema: Optional[str]
    dst_ssl_required: bool = False  # whether the db requires SSL

    # These are calculated so do not provide them explicitly
    src_postgres_dsn: Optional[PostgresDsn]
    dst_postgres_dsn: Optional[PostgresDsn]

    @validator("src_postgres_dsn", pre=True)
    def validate_src_postgres_dsn(
        cls, _: Optional[PostgresDsn], values: Any
    ) -> Optional[str]:
        """Create and validate the source db data source name."""
        return cls.check_postgres_dsn(_, values, "src")

    @validator("dst_postgres_dsn", pre=True)
    def validate_dst_postgres_dsn(
        cls, _: Optional[PostgresDsn], values: Any
    ) -> Optional[str]:
        """Create and validate the destination db data source name."""
        return cls.check_postgres_dsn(_, values, "dst")

    @staticmethod
    def check_postgres_dsn(
        _: Optional[PostgresDsn], values: Any, prefix: str
    ) -> Optional[str]:
        """Build a DSN string from the host, db name, port, username and password."""
        # We want to build the Data Source Name ourselves so none should be provided
        if _:
            raise ValueError("postgres_dsn should not be provided")

        user = values[f"{prefix}_user_name"]
        password = values[f"{prefix}_password"]
        host = values[f"{prefix}_host_name"]
        port = values[f"{prefix}_port"]
        db_name = values[f"{prefix}_db_name"]

        if user and password and host and port and db_name:
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

            if values[f"{prefix}_ssl_required"]:
                return dsn + "?sslmode=require"

            return dsn

        return None

    @dataclass
    class Config:
        """Meta-settings for the Settings class."""

        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache(1)
def get_settings() -> Settings:
    """Return the same Settings object every call."""
    return Settings()
