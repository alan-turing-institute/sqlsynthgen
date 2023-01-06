"""Utils for reading settings from environment variables."""
from functools import lru_cache
from typing import Any, Optional

# pylint: disable=no-name-in-module
# pylint: disable=no-self-argument
from pydantic import BaseSettings, PostgresDsn, validator


class Settings(BaseSettings):
    """A Pydantic settings class with optional and mandatory settings."""

    # Connection parameters for the source PostgreSQL database. See also
    # https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    src_host_name: str  # e.g. "mydb.mydomain.com" or "0.0.0.0"
    src_port: int = 5432
    src_user_name: str  # e.g. "postgres" or "myuser@mydb"
    src_password: str
    src_db_name: str
    src_ssl_required: bool = False  # whether the db requires SSL
    src_schema: Optional[str]

    # Connection parameters for the destination PostgreSQL database.
    dst_host_name: str  # e.g. "mydb.mydomain.com" or "0.0.0.0"
    dst_port: int = 5432
    dst_user_name: str  # e.g. "postgres" or "myuser@mydb"
    dst_password: str
    dst_db_name: str
    dst_ssl_required: bool = False  # whether the db requires SSL

    # These are calculated so do not provide them explicitly
    src_postgres_dsn: Optional[PostgresDsn]
    dst_postgres_dsn: Optional[PostgresDsn]

    @validator("src_postgres_dsn", pre=True)
    def validate_src_postgres_dsn(cls, _: Optional[PostgresDsn], values: Any) -> str:
        """Create and validate the source database DSN."""
        return cls.check_postgres_dsn(_, values, "src")

    @validator("dst_postgres_dsn", pre=True)
    def validate_dst_postgres_dsn(cls, _: Optional[PostgresDsn], values: Any) -> str:
        """Create and validate the destination database DSN."""
        return cls.check_postgres_dsn(_, values, "dst")

    @staticmethod
    def check_postgres_dsn(_: Optional[PostgresDsn], values: Any, prefix: str) -> str:
        """Build a DSN string from the host, db name, port, username and password."""

        # We want to build the Data Source Name ourselves so none should be provided
        if _:
            raise ValueError("postgres_dsn should not be provided")

        user = values[f"{prefix}_user_name"]
        password = values[f"{prefix}_password"]
        host = values[f"{prefix}_host_name"]
        port = values[f"{prefix}_port"]
        db_name = values[f"{prefix}_db_name"]

        dsn = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

        if values[f"{prefix}_ssl_required"]:
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
