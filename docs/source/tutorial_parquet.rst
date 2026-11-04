Tutorial: Generate Synthetic Data from CSV and Parquet Files
==============================================================

This tutorial demonstrates how to use Datafaker with CSV and Parquet datasets
through DuckDB. By the end of the tutorial, you will be able to generate
synthetic data from existing files and export the results in CSV or Parquet
format.

Overview
^^^^^^^^

Datafaker can work directly with CSV and Parquet files through DuckDB.
You do not need to import these files into a separate database before
generating synthetic data.

The workflow is:

1. Configure source and destination DSNs.
2. Generate the default schema definition (``orm.yaml`` file)
3. Review and refine the schema definition.
4. Generate synthetic data.
5. Export the results.

Using a DuckDB Database
^^^^^^^^^^^^^^^^^^^^^^^

If your source data already resides in a DuckDB database, configure the
source and destination databases using DSNs.

macOS / Linux:

.. code-block:: shell

   export SRC_DSN=duckdb:////path/to/source.db
   export DST_DSN=duckdb:////path/to/fake.db

Windows Command Prompt:

.. code-block:: shell

   set SRC_DSN=duckdb:///C:/path/to/source.db
   set DST_DSN=duckdb:///C:/path/to/fake.db

Windows PowerShell:

.. code-block:: shell

   $env:SRC_DSN='duckdb:///C:/path/to/source.db'
   $env:DST_DSN='duckdb:///C:/path/to/fake.db'

Create the destination schema:

.. code-block:: shell

   datafaker create-tables

Using CSV and Parquet Files as Input
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If your source data consists of CSV or Parquet files, use an in-memory
DuckDB instance as the source database.

macOS / Linux:

.. code-block:: shell

   export SRC_DSN=duckdb:///:memory:
   export DST_DSN=duckdb:///./fake.db

Windows:

.. code-block:: shell

   set SRC_DSN=duckdb:///:memory:
   set DST_DSN=duckdb:///./fake.db

Example directory structure:

.. code-block:: text

   input_data/
   ├── artist.parquet
   └── artwork.parquet

Partitioned Parquet Datasets
""""""""""""""""""""""""""""

Large tables are often saved as a directory of Parquet files, partitioned
(for example by year) and split into several part files:

.. code-block:: text

   input_data/
   ├── artist.parquet           # a single file: the table ``artist.parquet``
   └── artwork/                 # a directory: the table ``artwork``
       ├── yr=2023/
       │   ├── p00001.parquet
       │   └── p00002.parquet
       └── yr=__MISSING__/
           └── p00001.parquet

* A ``.parquet`` file directly inside the input directory is one table,
  named after the file, as before.
* A subdirectory is one table, named after the directory, made from all
  the Parquet files inside it at any depth.
* Partition directories named ``key=value`` become a ``TEXT`` column
  (``yr`` above), because partition keys are not stored in the files.
* If ``--parquet-dir`` points directly at a partitioned dataset (a directory
  containing only ``key=value`` subdirectories) the whole directory is one
  table, named after that directory.

All the files of one partitioned table must have the same column names
and the same partition keys. If they do not, ``make-tables`` stops with an
error naming the differing file. This usually means the subdirectory holds
several different tables; move each table into its own directory directly
inside the input directory and run ``make-tables`` again.

Column types may differ between files (for example when one partition has no
values for a column and so is stored with a different type). This is logged as
a warning and the type from the first file is used. Differences in case only,
such as ``int32`` and ``Int32``, are ignored.

The synthetic data is written as one file per table (for example
``artwork.parquet``) by ``dump-data --parquet``; the partitioning of the
input is not reproduced.

Building the ORM Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Datafaker needs an ``orm.yaml`` file describing tables,
columns, keys, and relationships.

Generating an Initial ORM
"""""""""""""""""""""""""

Generate a first draft of the ORM from your Parquet files:

.. code-block:: shell

   datafaker make-tables --parquet-dir ./input_data

This creates:

.. code-block:: text

   orm.yaml

This generates an initial ``orm.yaml`` file based on the Parquet files found in
the specified directory. The generated ``orm.yaml`` provides a useful starting point, but it may not
be entirely correct. Always review warnings carefully and verify primary keys,
foreign keys, column types, and nullability before proceeding.

Suppose your input directory contains two files:

* ``artist.parquet``
* ``artwork.parquet``

The ``orm.yaml`` file describes the tables, columns, data types, primary keys,
foreign keys, and nullability rules used by Datafaker.

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

Reviewing Primary Keys
""""""""""""""""""""""

Suppose ``make-tables`` produces:

.. code-block:: text

   WARNING: No likely primary keys found for table artwork.parquet

Update the ORM manually:

.. code-block:: yaml

   artwork.parquet:
     columns:
       object_id:
         type: INTEGER
         primary: true
         nullable: false

Reviewing Foreign Keys
""""""""""""""""""""""

Verify all table relationships.

For example:

.. code-block:: yaml

   artist_artwork.parquet:
     columns:
       artist_id:
         foreign_keys:
         - artist.parquet.artist_id

       object_id:
         foreign_keys:
         - artwork.parquet.object_id

Reviewing Data Types and Nullability
""""""""""""""""""""""""""""""""""""

Check inferred column types:

.. code-block:: yaml

   birth_date:
     type: DATE
     nullable: true

   artwork_id:
     type: INTEGER
     nullable: false

Ensure these definitions match the source data.

Generating Synthetic Data
^^^^^^^^^^^^^^^^^^^^^^^^^

Once the ORM has been reviewed, generate the Datafaker configuration
and source statistics:

.. code-block:: shell

   datafaker configure-tables
   datafaker configure-generators
   datafaker configure-missing
   datafaker make-stats

This creates:

.. code-block:: text

   config.yaml
   src-stats.yaml

Create the destination schema:

.. code-block:: shell

   datafaker create-tables

Generate synthetic data:

.. code-block:: shell

   datafaker create-data --num-passes 10

Each pass produces roughly one row per table, so
``--num-passes 10`` will generate about 10 rows in each table:

Exporting Synthetic Data
^^^^^^^^^^^^^^^^^^^^^^^^

After generation, the synthetic data resides in the destination DuckDB
database.

Exporting to CSV
""""""""""""""""

Create a CSV output directory:

.. code-block:: shell

   mkdir fake_csv

Export all tables:

.. code-block:: shell

   datafaker dump-data --output ./fake_csv/

Result:

.. code-block:: text

   fake_csv/
   ├── artist.csv
   └── artwork.csv

Exporting to Parquet
""""""""""""""""""""

Create a Parquet output directory:

.. code-block:: shell

   mkdir fake_parquet

Export all tables:

.. code-block:: shell

   datafaker dump-data --parquet --output ./fake_parquet/

Result:

.. code-block:: text

   fake_parquet/
   ├── artist.parquet
   └── artwork.parquet

End-to-End Example
^^^^^^^^^^^^^^^^^^

Assume you have a directory containing sensitive Parquet files:

.. code-block:: text

   input_parquet/
   ├── artist.parquet
   └── artwork.parquet

Configure DSNs:

.. code-block:: shell

   export SRC_DSN=duckdb:///:memory:
   export DST_DSN=duckdb:///./fake.db

Generate the ORM:

.. code-block:: shell

   datafaker make-tables --parquet-dir ./input_parquet

Review and update ``orm.yaml`` as necessary.

Generate configuration:

.. code-block:: shell

   datafaker configure-tables
   datafaker configure-generators
   datafaker configure-missing
   datafaker make-stats

Create schema and generate data:

.. code-block:: shell

   datafaker create-tables
   datafaker create-data --num-passes 10

Export synthetic Parquet files:

.. code-block:: shell

   mkdir fake
   datafaker dump-data --parquet --output ./fake

Quick Recipe: Parquet to CSV
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a minimal end-to-end workflow:

.. code-block:: shell

   export SRC_DSN=duckdb:///:memory:
   export DST_DSN=duckdb:///./fake.db

   datafaker make-tables --parquet-dir ./input_parquet
   datafaker configure-tables
   datafaker configure-generators
   datafaker configure-missing
   datafaker make-stats
   datafaker create-tables
   datafaker create-data --num-passes 10

   mkdir fake_csv

   datafaker dump-data --output ./fake_csv/

Troubleshooting
^^^^^^^^^^^^^^^

* If you see a command not found error when running ``datafaker``, check it's
  installed and on your ``PATH`` — see :ref:`Installation <page-installation>`.
  If you installed with ``pipx``, try ``pipx ensurepath`` and open a new shell.
  If you're working from a development checkout, use ``poetry run datafaker``
  instead.
* If ``make-tables`` logs warnings like "Could not determine type of column ...",
  inspect and fix the Parquet schema or edit ``orm.yaml`` (nested/struct or
  mixed-type columns often need flattening or manual typing).
* Setting ``SRC_SCHEMA`` or ``DST_SCHEMA`` can expose a `DuckDB bug`_ that produces very confusing error messages.
  If you must use a schema, you must prefix it with the basename of the database file.
  For example, if ``DST_DSN`` is set to ``duckdb:////path/to/file.db`` then ``DST_SCHEMA`` could be set to ``file.myschema``.

.. _duckdb bug: https://github.com/duckdb/duckdb/issues/20530
