1. Make a YAML file representing the tables in the schema

`poetry run datafaker make-tables --orm-file ./examples/mimic_omop/orm.yaml`

2. Create schema from the ORM YAML file

`poetry run datafaker create-tables --orm-file ./examples/mimic_omop/orm.yaml --config-file ./examples/mimic_omop/config.yaml`

3. Create generator table

`poetry run datafaker create-generators --orm-file ./examples/mimic_omop/orm.yaml --config-file ./examples/mimic_omop/config.yaml --df-file ./examples/mimic_omop/df.py`

3. Create data

`poetry run datafaker create-data --orm-file ./examples/mimic_omop/orm.yaml --config-file ./examples/mimic_omop/config.yaml --df-file .\examples\pollution\df.py`

4. Remove data

`poetry run datafaker remove-data --orm-file ./examples/mimic_omop/orm.yaml --config-file ./examples/mimic_omop/config.yaml`