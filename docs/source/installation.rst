Get Started
###########

Sqlsynthgen is a Python package that references a database schema and generates synthetic data accordingly. The generators are configurable. The output is a database schema populated with synthetic values.

Installation
************

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

#. Make python classes based on source database schema tables. Classes are output as stdout and is referred to as the `object-relational-model` (orm) value. Tables in schema must have primary key constraints in order for the orm to be generated.

The example below shows the output when the source schema comprises of a table ``Person`` with three columns. The stdout is piped into a python file eg. person_orm.py

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-tables >> sqlsynthgen/person_orm.py

The orm value (snippet) is as follows:

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

#. Make a set of default generators for generating values in reference to Table classes above.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-generators person_orm.py >> person_generator.py

A snippet of the generator code is as below:

.. code-block:: python

   class personGenerator:
      def __init__(self, src_db_conn, dst_db_conn):
         pass
         self.name = generic.text.color()
         self.nhs_number = generic.text.color()
         self.research_opt_out = generic.development.boolean()
         self.source_system = generic.text.color()
         self.stored_from = generic.datetime.datetime()
