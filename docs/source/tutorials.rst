Tutorials
=========

OMOP CDM
--------

ToDo Explain why you might want to use sqlsynthgen and not simply use the .sql files provided by ohdsi

If you have an OMOP CDM style database, there are a couple of things to bear in mind when using `sqlsynthgen`:

1. At least one version of the OMOP CDM contains a circular foreign key, between the `vocabulary` and `concept` tables.
2. There are several standardized vocabulary tables (`concept`, `concept_relationship`, etc).
   These should be marked as such in the sqlsynthgen config file.
   The tables will be exported to .csv files during the `make-tables` step.
   However, you should check the license agreement of each standardized vocabulary before sharing any of the .csv files.

Steps
~~~~~

Remove the circular foreign key
+++++++++++++++++++++++++++++++

In the source database, remove the circular foreign key between `concept` and `vocabulary` tables with, for example:

.. code-block:: sql

  alter table concept drop constraint concept.concept_vocabulary_id_fkey

Create a config file
++++++++++++++++++++

Make a config file called `omop.yaml`.
At the very least, our config file will need to specify the tables that need to be copied over in their entirety:

.. code-block:: yaml

  tables:
    # Standardized Vocabularies
    concept:
      vocabulary_table: true
    concept_class
      vocabulary_table: true
    concept_relationship:
      vocabulary_table: true
    concept_synonym:
      vocabulary_table: true
    domain:
      vocabulary_table: true
    drug_strength:
      vocabulary_table: true
    cohort_definition:
      vocabulary_table: true
    attribute_definition:
      vocabulary_table: true
    relationship:
      vocabulary_table: true
    source_to_concept_map
      vocabulary_table: true
    vocabulary:
      vocabulary_table: true
    # Standardized meta-data
    cdm_source:
      vocabulary_table: true
    # Standardized health system data
    location:
      vocabulary_table: true
    care_site:
      vocabulary_table: true
    provider:
      vocabulary_table: true

Make SQLAlchemy file
++++++++++++++++++++

We can make a SQLAlchemy file called `orm.py` by setting the connection parameters in environment variables or in a `.env` file and running

.. code-block:: shell

  sqlsynthgen make-tables

Make SQLSynthGen file
+++++++++++++++++++++

We can make a file of SQLSynthGen data generators called `ssg.py` by running

.. code-block:: shell

  sqlsynthgen make-generators --config-file omop.yaml

This will also create one `.csv` file for each of the vocabulary tables listed in the config file.


Restore the circular foreign key
+++++++++++++++++++++++++++++++

If we deleted a foreign key constraint in the first step, we can now restore it

.. code-block:: sql

  --alter table concept drop constraint concept.concept_vocabulary_id_fkey
  ToDo
