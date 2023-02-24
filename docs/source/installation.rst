Get Started
###########

Sqlsynthgen is a Python package that references a database schema and generates synthetic data accordingly. The generators are configurable. The output is a database schema populated with synthetic values.

Installation
**********

To use Sqlsynthgen, first install it using poetry:

.. code-block:: bash

   (<your_poetry_shell>) $ poetry add sqlsynthgen


Overview
*********

Sqlsynthgen provides a CLI interface. The commands are as below:

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py --help

   Commands:
   create-data      Populate schema with synthetic data.
   create-tables    Create schema from Python classes.
   create-vocab     Create tables using the SQLAlchemy file.
   make-generators  Make a SQLSynthGen file of generator classes.
   make-tables      Make a SQLAlchemy file of Table classes.

The ordering of the steps from end to end may be as follows:

#. Make python classes based on source database schema tables. The console printout contains metadata about the source schema. The example below shows the output when the source schema ``public`` comprises of a table ``icu_admission_date`` with a column ``observation_date``.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-tables

which outputs the following:

.. code-block:: python

   from sqlalchemy import BigInteger, Boolean, Column, Date, Float, Integer, MetaData, SmallInteger, Table, Text
   from sqlalchemy.dialects.postgresql import OID

   metadata = MetaData()

   t__icu_admission_date = Table(
      '_icu_admission_date', metadata,
      Column('observation_date', Date),
      schema='public'
   )

This printout from stdout is the referred to as the `object-relational-model` value.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-tables >> sqlsynthgen/public_orm.py

In this example, it is piped as input for the next step and named `sqlsynthgen/public_orm.py`

#. Make a set of default generators for generating values in reference to Table classes above.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-generators public_orm.py
