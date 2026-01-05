import numpy as np
from typing import Any, Dict, Literal

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

    if sigma_eps < 0 or not np.isfinite(sigma_eps):
       sigma_eps = 0.0
    
    x = np.empty(N)
    x[0] = x0
    for t in range(1, N):
        x[t] = x[t-1] + drift + rng.normal(0.0, sigma_eps)
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