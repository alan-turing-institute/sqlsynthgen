# How to Develop for SQLSYNTHGEN

The following instructions has been tested on MacOS Ventura, but they *should* work on other Unix-based OS.

## Pre-requisites

Please install the following software on your workstation:

1. [Poetry](https://python-poetry.org/docs/#installation).
1. [Pre-commit](https://pre-commit.com/#install).
1. [PostgreSQL](https://postgresapp.com).

## Setting up your development environment

1. Clone the GitHub repository:

    ```bash
    git clone https://github.com/alan-turing-institute/sqlsynthgen.git
    ```

1. In the directory of your local copy, create a virtual environment with all `sqlsynthgen` dependencies:

    ```bash
    cd sqlsynthgen
    poetry install --all-extras
    ```

   If Poetry errors when installing PyYaml, you will need to manually specify the Cython version and manually install PyYaml (this is a temporary workaround for a PyYaml v5 conflict with Cython v3, see [here](https://github.com/yaml/pyyaml/issues/601) for full details):

    ```bash
    poetry run pip install "cython<3"
    poetry run pip install wheel
    poetry run pip install --no-build-isolation "pyyaml==5.4.1"
    poetry install --all-extras
    ```

    *If you don't need to [build the project documentation](#building-documentation-locally), run instead just `poetry install`.*

1. Install the git hook scripts. They will run whenever you perform a commit:

    ```bash
    pre-commit install --install-hooks
    ```

    *To execute the hooks before a commit, run `pre-commit run --all-files`.*

1. Finally, activate the Poetry shell. Now you're ready to play with the code:

    ```bash
    poetry shell
    ```

## Running unit tests

Executing unit tests is straightward:

```bash
cd sqlsynthgen
python -m unittest discover --verbose tests/
```

## Running functional tests

Functional tests require a PostgreSQL service running. Perform the following steps on your local server:

1. Set the password of user `postgres` to `password`:

   ```bash
    psql -p5432 "postgres"
    postgres=# \password postgres
    Enter new password: <password>
    postgres=# \q
    ```

1. From the shell, create and load the `src` database:

    ```bash
    createdb src
    cd sqlsynthgen
    PGPASSWORD=password psql --host=localhost --username=postgres --file=tests/examples/src.dump
    ```

    *WARNING: Some MacOS systems [do not recognise the 'en_US.utf8' locale](https://apple.stackexchange.com/questions/206495/load-a-locale-from-usr-local-share-locale-in-os-x). As a workaround, replace `en_US.utf8` with `en_US.UTF-8` on every `*.dump` file.*

1. Also, create `dst` database:

    ```bash
    createdb dst
    ```

1. Finally, run the functional tests. You will need the environment variable `REQUIRES_DB` with a value of `1`.

    ```bash
    REQUIRES_DB=1 poetry run python -m unittest discover --verbose tests
    ```

## Building documentation locally

```bash
cd docs
make html
```

*WARNING: Some systems [won't be able to import the `sphinxcontrib.napoleon` extension](https://github.com/sphinx-doc/sphinx/issues/10378). In that case,
please replace `sphinxcontrib.napoleon` with `sphinx.ext.napoleon` in `docs/source/conf.py`.*
