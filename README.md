# sqlsynthgen

Synthetic data for SQL databases

## Documentation

Welcome to `sqlsynthgen`.
Our full documentation is available on <https://sqlsynthgen.readthedocs.io/>.

## Development

### Setup

### Poetry

1. Install [Poetry](https://python-poetry.org/docs/#installation).
1. Create a Poetry environment and install dependencies with `poetry install`.
1. Activate a Poetry shell with `poetry shell`.
   We will assume that this shell is active for the remainder of the instructions.

### Pre-Commit

1. Install [Pre-commit](https://pre-commit.com/#install).
1. Install Pre-commit hooks with `pre-commit install --install-hooks`.
   The hooks will run whenever you perform a Git commit.

### Testing

You can execute most unit tests by running `python -m unittest discover --verbose tests/` from the root directory.
However, the functional tests require:

1. A local PostgreSQL server to be running with:
    1. A `postgres` user whose password is `password`.
    1. Databases named `src` and `dst`.
    1. See [tests.yml](.github/workflows/tests.yml) and [test_functional.py](tests/test_functional.py) for more.
1. The `FUNCTIONAL_TESTS` environment variable to be set to `1`.
   For example, you could run `FUNCTIONAL_TESTS=1 python -m unittest discover --verbose tests/`.
