Quick Start
===========

After :ref:`Installation <enduser>`, we can run sqlsynthgen with the `--help` option to see the available commands:

.. code-block:: console

   $ sqlsynthgen --help
   Usage: sqlsynthgen [OPTIONS] COMMAND [ARGS]...

   Options:
     --help           Show this message and exit.

   Commands:
     create-data      Populate schema with synthetic data.
     create-tables    Create schema from Python classes.
     create-vocab     Create tables using the SQLAlchemy file.
     make-generators  Make a SQLSynthGen file of generator classes.
     make-stats       Compute summary statistics from the source database,...
     make-tables      Make a SQLAlchemy file of Table classes.

For the simplest case, we will need `make-tables`, `make-generators`, `create-tables` and `create-data` but, first,
we need to set environment variables to tell sqlsynthgen how to access our source database (where the real data resides now) and destination database (where the synthetic data will go).
We can do that in the terminal with the `export` keyword, as shown below, or in a file called `.env`.
The source and destination may be on the same database server, as long as the database or schema names differ.

.. code-block:: console

   $ export SRC_HOST_NAME='myserver@mydomain.com'
   $ export SRC_USER_NAME='someuser'
   $ export SRC_PASSWORD='secretpassword'
   $ export SRC_SCHEMA='myschema'
   $ export SRC_DB_NAME='source_db'

   $ export DST_HOST_NAME='myserver@mydomain.com'
   $ export DST_USER_NAME='someuser'
   $ export DST_PASSWORD='secretpassword'
   $ export DST_SCHEMA='myschema'
   $ export DST_DB_NAME='destination_db'


Next, we make a SQLAlchemy file that defines the structure of your database using the `make-tables` command:

.. code-block:: console

   $ sqlsynthgen make-tables

This will have created a file called `orm.py` in the current directory, with a SQLAlchemy class for each of your tables.

The next step is to make a sqlsynthgen file that defines one data generator per table in the source database:

.. code-block:: console

   $ sqlsynthgen make-generators

This will have created a file called `ssg.py` in the current directory.

We can use the `create-table` command to read the `orm.py` file, create our destination schema (if it doesn't already exist) and to create empty copies of all the tables that in the source database.

.. code-block:: console

   $ sqlsynthgen create-tables

Now that we have created the schema that will hold synthetic data, we can use the `create-data` command to read `orm.py` & `ssg.py` and generate data:

.. code-block:: console

   $ sqlsynthgen create-data

By default, `create-data` will have inserted one row per table and will have used the column data types to decide how to randomly generate data.
To create more data each time we call `create-data`, we can provide an integer argument:

.. code-block:: console

   $ sqlsynthgen create-data 10

We will have inserted 11 rows per table, with the last two commands.
