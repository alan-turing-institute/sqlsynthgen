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
from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    """A Pydantic settings class with optional and mandatory settings.

    Settings class attributes describe two database connection. The source database connection is
    the database schema from which the object relational model is discovered. The destination
    database connection is the location where tables based on the ORM is created
    and synthetic values inserted.

    Attributes:
        src_dsn (str) :
            A DSN for connecting to the source database.

        src_schema (str) :
            The source database schema to use, if applicable.

        dst_dsn (str) :
            A DSN for connecting to the destination database.

        dst_schema (str) :
            The destination database schema to use, if applicable.
    """

    src_schema: Optional[str]
    dst_schema: Optional[str]

    # DSNs for the databases. For example:
    # postgresql://user:secret@localhost:6789/dbname
    # See also
    # https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    src_dsn: Optional[str]
    dst_dsn: Optional[str]

    @validator("src_dsn")
    def validate_src_dsn(cls, dsn: Optional[str], values: Any) -> Optional[str]:
        """Create and validate the source DB DSN."""
        if dsn and dsn.startswith("mariadb"):
            assert values.get("src_schema") is None
        return dsn

    @validator("dst_dsn")
    def validate_dst_dsn(cls, dsn: Optional[str], values: Any) -> Optional[str]:
        """Create and validate the destination DB DSN."""
        if dsn and dsn.startswith("mariadb"):
            assert values.get("dst_schema") is None
        return dsn

    @dataclass
    class Config:
        """Meta-settings for the Settings class."""

        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache(1)
def get_settings() -> Settings:
    """Return the same Settings object every call."""
    return Settings()
