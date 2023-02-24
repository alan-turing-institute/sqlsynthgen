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

#. Make python classes based on source database schema tables. The console printout contains metadata about the source schema. The example below shows the output when the source schema comprises of a table ``Person`` with three columns.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-tables

which outputs the following:

.. code-block:: python

   from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, Integer, DateTime, Text, Date, Float, LargeBinary
   from sqlalchemy.ext.declarative import declarative_base

   Base = declarative_base()
   metadata = Base.metadata

   class Person(Base):
      __tablename__ = "person"
      __table_args__ = {"schema": "myschema"}

      person_id = Column(
         Integer,
         primary_key=True,
      )
      name = Column(Text)
      nhs_number = Column(Text)
      research_opt_out = Column(Boolean)

This printout from stdout is the referred to as the `object-relational-model` value.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-tables >> sqlsynthgen/person_orm.py

In this example, it is piped as input for the next step and named `sqlsynthgen/person_orm.py`

#. Make a set of default generators for generating values in reference to Table classes above.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-generators person_orm.py
