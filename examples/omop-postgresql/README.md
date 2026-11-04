# How to run datafaker process on omop schema

## Make a YAML file representing the tables in the schema

`poetry run datafaker make-tables --orm-file ./orm.yaml --config-file ./config.yaml`

## Interactively set generators for column data.

`poetry run datafaker configure-generators --orm-file ./orm.yaml --config-file ./config.yaml`

## Compute summary statistics from the source database.

`poetry run datafaker make-stats --orm-file ./orm.yaml --config-file ./config.yaml --stats-file ./src-stats.yaml`

## Create schema from the ORM YAML file

`poetry run datafaker create-tables --orm-file ./orm.yaml --config-file ./config.yaml`

## Create generator table

`poetry run datafaker create-generators --orm-file ./orm.yaml --config-file ./config.yaml --df-file ./df.py`

## Create data

`poetry run datafaker create-data --orm-file ./orm.yaml --config-file ./config.yaml --df-file ./df.py`

## Remove data

`poetry run datafaker remove-data --orm-file ./orm.yaml --config-file ./config.yaml`

Plan: /Users/myong/.claude/plans/cached-rolling-snowglobe.md
