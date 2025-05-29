# How to Develop for DATAFAKER

## Pre-requisites

Please install the following software on your workstation:

1. [Poetry](https://python-poetry.org/docs/#installation).
1. [Pre-commit](https://pre-commit.com/#install).
1. [PostgreSQL](https://postgresapp.com).

## Setting up your development environment

1. Clone the GitHub repository:

    ```bash
    git clone https://github.com/SAFEHR-data/datafaker
    ```

1. In the directory of your local copy, create a virtual environment with all `datafaker` dependencies:

    ```bash
    cd datafaker
    poetry install --all-extras
    ```

    *If you don't need to [build the project documentation](#building-documentation-locally), the `--all-extras` option can be omitted.*

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

Executing unit tests is straightforward:

```bash
python -m unittest discover --verbose tests/
```

Although currently not many tests actually work, so try:

```bash
python -m unittest -k Config -k Dump -k Remove
```

for tests that are currently maintained.

## Running functional tests

These tests do not currently work, and will be replaced by unit tests.

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
    cd datafaker
    PGPASSWORD=password psql --host=localhost --username=postgres --file=tests/examples/src.dump
    ```

    *WARNING: Some MacOS systems [do not recognise the 'en_US.utf8' locale](https://apple.stackexchange.com/questions/206495/load-a-locale-from-usr-local-share-locale-in-os-x). As a workaround, replace `en_US.utf8` with `en_US.UTF-8` on every `*.dump` file.*

1. Also, create `dst` database:

    ```bash
    createdb dst
    PGPASSWORD=password psql --host=localhost --username=postgres --file=tests/examples/dst.dump
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
