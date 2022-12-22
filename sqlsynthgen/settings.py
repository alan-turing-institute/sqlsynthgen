"""Utils for reading settings from environment variables."""
from functools import lru_cache
from typing import Any, Optional

# pylint: disable=no-name-in-module
# pylint: disable=no-self-argument
from pydantic import BaseSettings, PostgresDsn, validator


class Settings(BaseSettings):
    """A Pydantic settings class with optional and mandatory settings."""

    # Connection parameters for a PostgreSQL database. See also,
    # https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    db_host_name: str  # e.g. "mydb.mydomain.com" or "0.0.0.0"
    db_port: int = 5432
    db_user_name: str  # e.g. "postgres" or "myuser@mydb"
    db_password: str
    db_name: str = ""  # leave empty to get the user's default db
    ssl_required: bool = False  # whether the db requires SSL

    # postgres_dsn is calculated so do not provide it explicitly
    postgres_dsn: Optional[PostgresDsn]

    @validator("postgres_dsn", pre=True)
    def validate_postgres_dsn(cls, _: Optional[PostgresDsn], values: Any) -> str:
        """Build a DSN string from the host, db name, port, username and password."""

        # We want to build the Data Source Name ourselves so none should be provided
        if _:
            raise ValueError("postgres_dsn should not be provided")

        user = values["db_user_name"]
        password = values["db_password"]
        host = values["db_host_name"]
        port = values["db_port"]
        db_name = values["db_name"]

        dsn = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

        if values["ssl_required"]:
            return dsn + "?sslmode=require"

        return dsn

    class Config:
        """Meta-settings for the Settings class."""

        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache(1)
def get_settings() -> Settings:
    """Return the same Settings object every call."""
    return Settings()
