Tutorials
=========

OMOP CDM
--------

Even though the OMOP CDM schema is publicly available, there are sometimes variations with certain implementations (at schema and data levels).
`sqlsynthgen` allows you to generate data irrespective of your schema peculiarities.

If you have an OMOP CDM style database, there are a couple of things to bear in mind when using `sqlsynthgen`:

1. At least one version of the OMOP CDM contains a circular foreign key, between the `vocabulary` and `concept` tables.
2. There are several standardized vocabulary tables (`concept`, `concept_relationship`, etc).
   These should be marked as such in the sqlsynthgen config file.
   The tables will be exported to .yaml files during the `make-tables` step.
   However, you should check the license agreement of each standardized vocabulary before sharing any of the .yaml files.

Steps
~~~~~

Remove the circular foreign key
+++++++++++++++++++++++++++++++

In the source database, remove the circular foreign key between `concept` and `vocabulary` tables with, for example:

.. code-block:: sql

  alter table concept drop constraint concept.concept_vocabulary_id_fkey

and between `concept` and `domain` with, for example:

.. code-block:: sql

  alter table concept drop constraint concept.concept_domain_id_fkey

Create a config file
++++++++++++++++++++

Make a config file called `omop.yaml`.
At the very least, our config file will need to specify the tables that need to be copied over in their entirety:

.. literalinclude:: ../../tests/examples/omop/config.yaml
   :language: yaml

Create a custom generators file
+++++++++++++++++++++++++++++++

Make a python file called `custom_generators.py`.
We will define a generator which produces a maximum of one row in the `death` table per row in the `person` table.

.. literalinclude:: ../../tests/examples/omop/custom_generators.py
   :language: python

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

This will also create one `.yaml` file for each of the vocabulary tables listed in the config file.

Create Synthetic Data
+++++++++++++++++++++

We can now create an empty schema with

.. code-block:: shell

  sqlsynthgen create-tables

this will use SQLAlchemy and the `orm.py` to make an empty copy of our source database.

Next, we upload the vocabulary tables with

.. code-block:: shell

  sqlsynthgen create-vocab

To create data for all of the other (non-vocabulary) tables, we run

.. code-block:: shell

  sqlsynthgen create-data

There will now be data in each of our tables.

Restore the circular foreign key
++++++++++++++++++++++++++++++++

If we deleted foreign key constraints in the first step, we can now restore them

.. code-block:: sql

  ALTER TABLE concept ADD CONSTRAINT concept_vocabulary_id_fkey FOREIGN KEY (vocabulary_id) REFERENCES vocabulary(vocabulary_id);
  ALTER TABLE concept ADD CONSTRAINT concept_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES domain(domain_id);
