Tutorial
########

To start
========

Sqlsynthgen provides a CLI interface. The commands are as below:

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py --help

   Commands:
   create-data      Populate schema with synthetic data.
   create-tables    Create schema from Python classes.
   create-vocab     Create tables using the SQLAlchemy file.
   make-generators  Make a SQLSynthGen file of generator classes.
   make-tables      Make a SQLAlchemy file of Table classes.

The `--help` argument for each command will show an example of how the command should be called, the input arguments, as well as their default values. Eg.

.. code-block:: bash

   (<your_poetry_shell>) $ python sqlsynthgen/main.py make-tables --help
   Usage: main.py make-tables [OPTIONS]

   Make a SQLAlchemy file of Table classes.

   This CLI command deploys sqlacodegen to discover a schema structure, and
   generates an object relational model declared as Python classes.

   Example:     $ sqlsynthgen make_tables

   Args:     orm_file (str): Path to write the Python ORM file.

   Options:
   --orm-file TEXT  [default: orm.py]
   --help           Show this message and exit.

Example 1 - Default values
==========================

In this example, our task is to synthesise values for a database schema containing a single `Person` table described as follows:

.. code-block:: sql
   CREATE TABLE [IF NOT EXISTS] Person (
   id serial PRIMARY KEY,
   name VARCHAR ( 50 ) NOT NULL,
   nhs_number VARCHAR ( 50 ) UNIQUE NOT NULL,
   research_opt_out BOOLEAN NOT NULL,
);

We will provide a database schema connection to identify database schema for which to generate synthetic values (referred here as the `source` database schema) and for which to populate with synthetic values (`destination` schema).

Connection properties to both schemas should be stored in an ``.env`` file as follows:

.. code-block:: bash
   src_host_name="address_of_host_to_source_database"
   src_user_name="user_name"
   src_password="password"
   src_db_name="source_database_name"
   src_schema="source_database_schema"
   dst_host_name="dhost"
   dst_user_name="duser"
   dst_password="dpassword"
   dst_db_name="d_db_name"

#. Make python classes
======================

First we generate Python classes based on source database schema tables. Create a `Person` table in `src_schema.src_db_name`, using the database connection values in the `.env` file. Note: Tables in `src_db_name` must have primary key constraints in order for the orm to be generated.

`make-tables` uses the database connection provided in `.env` and discovers the tables within the schema. It creates Python classes mapped to the tables and outputs them in the path specified by `--orm-file`.

.. code-block:: bash

   $ python sqlsynthgen/main.py make-tables --orm-file sqlsynthgen/person_orm.py

A code snippet from the file specified as argument to `--orm-file` is as follows:

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

#. Make generators
==================

Default generators are made to synthesise values in reference to table classes above. In this case, a generator `personGenerator` has been created to synthesise values for the `Person` table. These can be also be manually configured and customised with domain knowledge.

We create the generator using the `make-generators` command. The inputs comprise of the orm file, and a path which will store the output containing the generators.

.. code-block:: bash

   $ python sqlsynthgen/main.py make-generators --orm-file person_orm.py --ssg-file person_generator.py

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

#. Create new tables
====================

This step creates empty tables in `src_schema.dst_db_name` under a default schema `myschema`. The tables structures are identical to tables from  `src_schema.src_db_name`. In this case an empty `Person` table will be created in `src_schema.dst_db_name` and will be used to store the data synthesised by the generators above.

.. code-block:: bash
   $ python sqlsynthgen/main.py create-tables --orm-file person_orm.py

#. Synthesise data
===================

This step brings together the orm outputs, the generators to synthesise values and populates the empty tables in `myschema` with these values.

.. code-block:: bash
   $ python sqlsynthgen/main.py create-data --orm-file person_orm.py --ssg-file person_generator.py --num-passes 3
