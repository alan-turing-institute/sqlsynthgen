"""Utility functions."""
import json
import logging
import os
import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Mapping, Optional, Union, Literal, Dict
import numpy as np
import yaml
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from sqlalchemy import Engine, create_engine, event, select
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.schema import MetaData, Table

# Define some types used repeatedly in the code base
MaybeAsyncEngine = Union[Engine, AsyncEngine]


CONFIG_SCHEMA_PATH: Final[Path] = (
    Path(__file__).parent / "json_schemas/config_schema.json"
)


def read_config_file(path: str) -> dict:
    """Read a config file, warning if it is invalid.

    Args:
        path: The path to a YAML-format config file.

    Returns:
        The config file as a dictionary.
    """
    with open(path, "r", encoding="utf8") as f:
        config = yaml.safe_load(f)

    assert isinstance(config, dict)

    schema_config = json.loads(CONFIG_SCHEMA_PATH.read_text(encoding="UTF-8"))
    try:
        validate(config, schema_config)
    except ValidationError as e:
        logger.error("The config file is invalid: %s", e.message)

    return config


def import_file(file_path: str) -> ModuleType:
    """Import a file.

    This utility function returns file_path imported as a module.

    Args:
        file_path (str): The path of a file to import.

    Returns:
        ModuleType
    """
    module_name = os.path.splitext(os.path.basename(file_path))[0]

    sys.path.append(os.path.dirname(os.path.abspath(file_path)))

    try:
        module = import_module(module_name)
    finally:
        sys.path.pop()

    return module


def download_table(
    table: Table, engine: Engine, yaml_file_name: Union[str, Path]
) -> None:
    """Download a Table and store it as a .yaml file."""
    stmt = select(table)
    with engine.connect() as conn:
        result = [dict(row) for row in conn.execute(stmt).mappings()]

    with Path(yaml_file_name).open("w", newline="", encoding="utf-8") as yamlfile:
        yamlfile.write(yaml.dump(result))


def get_sync_engine(engine: MaybeAsyncEngine) -> Engine:
    """Given an SQLAlchemy engine that may or may not be async return one that isn't."""
    if isinstance(engine, AsyncEngine):
        return engine.sync_engine
    return engine


def create_db_engine(
    db_dsn: str,
    schema_name: Optional[str] = None,
    use_asyncio: bool = False,
    **kwargs: Any,
) -> MaybeAsyncEngine:
    """Create a SQLAlchemy Engine."""
    if use_asyncio:
        async_dsn = db_dsn.replace("postgresql://", "postgresql+asyncpg://")
        engine: MaybeAsyncEngine = create_async_engine(async_dsn, **kwargs)
    else:
        engine = create_engine(db_dsn, **kwargs)

    if schema_name is not None:
        event_engine = get_sync_engine(engine)

        @event.listens_for(event_engine, "connect", insert=True)
        def connect(dbapi_connection: DBAPIConnection, _: Any) -> None:
            set_search_path(dbapi_connection, schema_name)

    return engine


def set_search_path(connection: DBAPIConnection, schema: str) -> None:
    """Set the SEARCH_PATH for a PostgreSQL connection."""
    # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#remote-schema-table-introspection-and-postgresql-search-path
    existing_autocommit = connection.autocommit
    connection.autocommit = True

    cursor = connection.cursor()
    # Parametrised queries don't work with asyncpg, hence the f-string.
    cursor.execute(f"SET search_path TO {schema};")
    cursor.close()

    connection.autocommit = existing_autocommit


def get_orm_metadata(
    orm_module: ModuleType, tables_config: Mapping[str, Any]
) -> MetaData:
    """Get the SQLAlchemy Metadata object from an ORM module.

    Drop all tables from the metadata that are marked with `ignore` in `tables_config`.
    """
    metadata: MetaData = orm_module.Base.metadata
    # The call to tuple makes a copy of the iterable, allowing us to mutate the original
    # within the loop.
    for table_name, table in tuple(metadata.tables.items()):
        ignore = tables_config.get(table_name, {}).get("ignore", False)
        if ignore:
            metadata.remove(table)
    return metadata


# This is the main logger that the other modules of sqlsynthgen should use for output.
# conf_logger() should be called once, as early as possible, to configure this logger.
logger = logging.getLogger(__name__)


def info_or_lower(record: logging.LogRecord) -> bool:
    """Allow records with level of INFO or lower."""
    return record.levelno in (logging.DEBUG, logging.INFO)


def warning_or_higher(record: logging.LogRecord) -> bool:
    """Allow records with level of WARNING or higher."""
    return record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL)


def conf_logger(verbose: bool) -> None:
    """Configure the logger."""
    # Note that this function modifies the global `logger`.
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    log_format = "%(message)s"

    # info will always be printed to stdout
    # debug will be printed to stdout only if verbose=True
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter(log_format))
    stdout_handler.addFilter(info_or_lower)

    # warning and error will always be printed to stderr
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter(log_format))
    stderr_handler.addFilter(warning_or_higher)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)


def generate_time_series(
    N: int,
    model_option: Literal["iid", "random_walk", "ar1"],
    model_params: Dict[str, Any],
) -> np.ndarray:
    """
    Generate a synthetic time series using one of three simple models.

    Parameters
    ----------
    N : int
        Number of time steps.
    model_option : {"iid", "random_walk", "ar1"}
        Which model to use.
    model_params : dict
        Dictionary of parameters. Expected keys:

        For all models:
            - "mean": float
            - "std": float

        For random_walk:
            - "drift": float
            - "epsilon_std": float

        For ar1:
            - "mu": float
            - "phi": float
            - "epsilon_std": float

    random_state : int or None
        Optional random seed.

    Returns
    -------
    np.ndarray
        Synthetic time series of length N.
    """

    rng = np.random.default_rng(None)

    # ----------------------------
    # MODEL 1: IID Gaussian
    # ----------------------------
    if model_option == "iid":
        return sample_iid_gaussian(
            N=N,
            mu=model_params["mean"],
            sigma=model_params["std"],
            rng=rng,
        )

    # ----------------------------
    # MODEL 2: Random Walk
    # ----------------------------

    x0: float = rng.normal(
        loc=model_params["mean"],
        scale=model_params["std"]
    )
    if model_option == "random_walk":
        required = ["drift", "epsilon_std"]
        for key in required:
            if key not in model_params:
                raise KeyError(f"src_stats must contain '{key}' for random_walk")

        return random_walk_with_drift(
            N=N,
            x0=x0,
            drift=model_params["drift"],
            sigma_eps=model_params["epsilon_std"],
            rng=rng,
        )

    # ----------------------------
    # MODEL 3: AR(1)
    # ----------------------------
    if model_option == "ar1":
        required = ["mu", "phi", "epsilon_std"]
        for key in required:
            if key not in model_params:
                raise KeyError(f"src_stats must contain '{key}' for ar1")

        return ar1_process(
            N=N,
            x0=x0,
            mu=model_params["mu"],
            phi=model_params["phi"],
            sigma_eps=model_params["epsilon_std"],
            rng=rng,
        )

    # ----------------------------
    raise ValueError(f"Unknown model_option: {model_option!r}")


def sample_iid_gaussian(
    N: int,
    mu: float,
    sigma: float,
    rng: np.random.Generator
) -> np.ndarray:
    """"
    Generate an IID Gaussian time series.

    Parameters
    ----------
    N : int
        Length of the time series.
    mu : float
        Mean of the Gaussian.
    sigma : float
        Standard deviation of the Gaussian.
    rng : np.random.Generator
        Random number generator.
    Returns
    -------
    np.ndarray
        Generated IID Gaussian time series of length N
    """
    return rng.normal(loc=mu, scale=sigma, size=N)



def random_walk_with_drift(
    N: int,
    x0: float,
    drift: float,
    sigma_eps: float,
    rng: np.random.Generator
) -> np.ndarray:
    """
    Generate a random walk time series with drift.

    Parameters
    ----------
    N : int
        Length of the time series.
    x0 : float
        Initial value of the time series.
    drift : float
        Drift term added at each time step.
    sigma_eps : float
        Standard deviation of the white noise.
    rng : np.random.Generator
        Random number generator.
    Returns
    -------
    np.ndarray
        Generated random walk time series of length N
    """
    x = np.empty(N)
    x[0] = x0
    for t in range(1, N):
        x[t] = x[t-1] + drift + rng.normal(0.0, sigma_eps)*100
    return x


def ar1_process(
    N: int,
    x0: float,
    mu: float,
    phi: float,
    sigma_eps: float,
    rng: np.random.Generator
) -> np.ndarray:
    """
    Generate an AR(1) time series.
    An AR(1) process is defined by the equation:
        x[t] = mu + phi * (x[t-1] - mu) + eps[t]
    where eps[t] ~ N(0, sigma_eps^2)

    Parameters
    ----------
    N : int
        Length of the time series.
    x0 : float
        Initial value of the time series.
    mu : float
        Mean of the AR(1) process.
    phi : float
        Autoregressive coefficient.
    sigma_eps : float
        Standard deviation of the white noise.
    rng : np.random.Generator
        Random number generator.
    Returns
    -------
    np.ndarray
        Generated AR(1) time series of length N
    """
    x = np.empty(N)
    x[0] = x0
    for t in range(1, N):
        eps = rng.normal(0.0, sigma_eps)
        x[t] = mu + phi * (x[t-1] - mu) + eps
    return x