Working with CSV and Parquet files with DuckDB
==============================================

DuckDB is supported for both the source and the destination database.
DuckDB is famous for being able to work with ``.parquet`` and ``.csv`` files without first pulling the data into its own database.
(A Parquet file is a data format like CSV but much more efficient for storage and searching, and not human readable).

Using DuckDB without using Parquet or CSV files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

DuckDB will work as source and/or destination databases like this (on Mac or Linux):

.. code-block:: shell

   export SRC_DSN=duckdb:////path/to/file/duck.db
   export DST_DSN=duckdb:////path/to/file/fake.db

Or in Windows:

.. code-block::

   set SRC_DSN=duckdb:///C:/path/to/file/duck.db
   set DST_DSN=duckdb:///C:/path/to/file/fake.db

Or in Windows PowerShell:

.. code-block::

   $env:SRC_DSN='duckdb:///C:/path/to/file/duck.db'
   $env:DST_DSN='duckdb:///C:/path/to/file/fake.db'

This will use the DuckDB database in the file ``/path/to/file/duck.db`` and output to the file ``/path/to/file/fake.db``.

Using Datafaker's ``create-tables`` command will create the new database file ``/path/to/file/fake.db``.

Using DuckDB to source from Parquet or CSV files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You do not need to set up a real DuckDB database if all your source data is in Parquet or CSV files.
Simply set your source connection string to an in-memory database:

.. code-block:: shell

   export SRC_DSN=duckdb:///:memory:

If you do this, `datafaker make-tables` will not work as there is nothing in this database.
You can write your ``orm.yaml`` file by hand, but you can try
`datafaker make-tables --parquet-dir /path/to/directory/containing/parquet/files`
to give you a head start; the ``orm.yaml`` file produced will be valid, but it
might be wrong in many ways. Look out for the warnings given, and take
special care checking that foreign keys are specified correctly.

Let us look at an example of how to write an ``orm.yaml`` file.
Assume you have two Parquet files, ``artist.parquet`` and ``artwork.parquet``.

Our ``orm.yaml`` file must specify these files and the columns within them.
You must specify each table (named after its parquet file), the SQL type of each table,
whether each column is nullable (default is ``true``) or a primary key (default is ``false``)
and the target for any column that represents a foreign key.

For example:

.. code-block:: yaml

   tables:
      artist.parquet:  # this is the name of the parquet file
         columns:
            artist_id:
               type: INTEGER
               primary: true  # mark artist_id as the primary key
               nullable: false  # columns are nullable by default, so set this if not.
            name:
               type: TEXT
            gender:
               type: TEXT
            nationality:
               type: TEXT
            birth_date:
               type: DATE
            end_date:
               type: DATE
      artwork.parquet:  # The other parquet file
         columns:
            artwork_id:
               type: INTEGER
               primary: true
               nullable: false
            artist_id:
               foreign_keys:
               - artist.parquet.artist_id  # Maps to the artist_id column of the artist.parquet file
            name:
               type: TEXT
            date:
               type: DATE
            medium:
               type: TEXT

Now you can run ``datafaker configure-tables`` and similar to begin configuring datafaker.

Using DuckDB to write fake Parquet or CSV files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You cannot use an in-memory DuckDB for the destination database because it needs to survive multiple calls to ``datafaker``,
but Datafaker will create the DuckDB file for you if you set the `DST_DSN` environment variable appropriately.

After using ``datafaker create-tables`` and ``datafaker create-data``,
you now have a database file containing the fake data. If you want CSV or parquet files you can use the following commands:

.. code-block:: shell

   export DST_DSN=duckdb:////path/to/file/fake.db
   mkdir fake_csv
   datafaker dump-data --output fake_csv/
   mkdir fake_parquet
   datafaker dump-data --parquet --output fake_parquet/

.. warning::

   Setting ``SRC_SCHEMA`` or ``DST_SCHEMA`` can expose a `DuckDB bug`_ that produces very confusing error messages.
   If you must use a schema, you must prefix it with the basename of the database file.
   For example, if ``DST_DSN`` is set to ``duckdb:////path/to/file.db`` then ``DST_SCHEMA`` could be set to ``file.myschema``.

.. _duckdb bug: https://github.com/duckdb/duckdb/issues/20530

You now have a directory of fake data in CSV format and anther directory of the same fake data in parquet format.

Parquet end-to-end recipe:
^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose you have some sensitive parquet files and want to make fake versions of them.
You have some files that are vocabulary files and you don't want new versions of those.
Here is a step-by-step guide to achieving this.

Firstly, ensure that you have Datafaker and DuckDB installed.

Now set the environment variables:

.. code-block:: shell

   export SRC_DSN=duckdb:///:memory:
   export DST_DSN=duckdb:///./fake.db

for Mac or Linux, or on Windows use:

.. code-block:: shell

   set SRC_DSN=duckdb:///:memory:
   set DST_DSN=duckdb:///./fake.db

Now generate the ``orm.yaml``. Here we are assuming the parquet files are
in a directory named ``inputdir``. This directory can be ``.`` but it is better
to put these files in a different directory so that we can be sure that the faked
output files don't get mixed up with these source files:

.. code-block:: shell

   datafaker make-tables --parquet-dir inputdir

... and edit the ``orm.yaml`` file as detailed above.

For example, you might see a warning like:
``No likely primary keys found for table artwork.parquet`` if the artwork
file does not have a column called something like ``id`` or ``artwork_id``.
In this case you would look for the primary key in the data and update the
``orm.yaml`` file appropriately:

.. code-block:: yaml

  tables:
    #...
    artwork.parquet:
      columns:
        #...
        object_id:
          primary: true  # add this line
          #nullable: true  # remove this line; data does not have nulls here
          type: INTEGER
        #...

We should also check the foreign keys; I notice here that the join table has
not been marked as having a foreign key to the Artwork table, again
because the name was ``object_id`` not ``artwork_id`` so ``make-tables``
did not guess it correctly:

.. code-block:: yaml

  tables:
    #...
    artist_artwork.parquet:
      columns:
        artist_id:
          foreign_keys:
          - artist.parquet.artist_id
          nullable: false
          primary: true
          type: INTEGER
        object_id:
          foreign_keys:  # Add this line
          - artwork.parquet.object_id  # and this line
          nullable: true
          type: INTEGER
      unique: []

Also check the types and nullabilities, these are not guaranteed to be correct.

Once we have the ``orm.yaml`` file correct, we configure the tables and
generators, and summary statistics:

.. code-block:: shell

   datafaker configure-tables
   datafaker configure-generators
   datafaker configure-missingness
   datafaker make-stats

Now you have your three files: ``orm.yaml``, ``config.yaml`` and ``src-stats.yaml``,
and can create the fake data parquet files in a new directory called ``fake``:

.. code-block:: shell

   datafaker create-tables
   datafaker create-data --num-passes 100
   mkdir fake
   datafaker dump-data --parquet --output fake
