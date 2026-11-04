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

Executing unit tests is straightforward. For example inside a Poetry shell you can run:

```bash
pytest -rs
```

or outside:

```bash
poetry run pytest -rs
```

## Building documentation locally

```bash
cd docs
make html
```

*WARNING: Some systems [won't be able to import the `sphinxcontrib.napoleon` extension](https://github.com/sphinx-doc/sphinx/issues/10378). In that case,
please replace `sphinxcontrib.napoleon` with `sphinx.ext.napoleon` in `docs/source/conf.py`.*
