.. _page-quickstart:

Quick Start
===========

Overview
--------

After :ref:`Installation <page-installation>`, we can run ``sqlsynthgen`` to see the available commands:

.. code-block:: console

   $ sqlsynthgen
   Usage: sqlsynthgen [OPTIONS] COMMAND [ARGS]...

   Options:
     --help                          Show this message and exit.

   Commands:
      configure-generators  Interactively set generators for column data.
      configure-missing     Interactively set the missingness of the...
      configure-tables      Interactively set tables to ignored, vocabulary...
      create-data           Populate the schema in the target directory with...
      create-generators     Make a SQLSynthGen file of generator classes.
      create-tables         Create schema from the ORM YAML file.
      create-vocab          Import vocabulary data into the target database.
      list-tables           List the names of tables
      make-stats            Compute summary statistics from the source database.
      make-tables           Make a YAML file representing the tables in the...
      make-vocab            Make files of vocabulary tables.
      remove-data           Truncate non-vocabulary tables in the destination...
      remove-tables         Drop all tables in the destination schema.
      remove-vocab          Truncate vocabulary tables in the destination...
      validate-config       Validate the format of a config file.
      version               Display version information.

sqlsynthgen is designed to be run connected to either the private source database or the more public destination database. It never needs to be connected to both.
So you can install sqlsynthgen on a machine with access to the private source database that will do the reading, and again on another machine that will do the creation.

In this guide we will walk through configuring column generators. We will not discuss stories here.

Connecting to the source database
---------------------------------

To connect to the source database, set the ``SRC_DSN`` and ``SRC_SCHEMA`` environment variables.
You can leave ``SRC_SCHEMA`` unset if you are using the default schema (or a database that does not use schema names such as MariaDB):

MacOS or Linux:

.. code-block:: console

   $ export SRC_DSN="postgresql://someuser:somepassword@myserver.mydomain.com"
   $ export SRC_SCHEMA='myschema'

Windows Command Shell:

.. code-block:: console

   $ set SRC_DSN "postgresql://someuser:somepassword@myserver.mydomain.com"
   $ set SRC_SCHEMA "myschema"

Running from the ready-built Docker container, make an output directory then use that as the data volume like so (please use WSL for this on Windows):

.. code-block:: console

   $ mkdir output
   $ docker run --rm --user $(id -u):$(id -g) --network host -e SRC_SCHEMA=myschema -e DST_DSN=postgresql://someuser:somepassword@myserver.mydomain.com -itv ./output:data --pull always timband/ssg

Now you can use the commands that use the source database (the ones beginning ``configure-`` and ``make-`` but not the ones beginning ``create-`` and ``remove-``).

Initial configuration
---------------------

The first job is to read the structure of the source database:

.. code-block:: console

   $ sqlsynthgen make-tables

This will create a file called ``orm.yaml``. You should not need to edit this file.

Configuring table types
-----------------------

Next you can use the ``configure-tables`` command categorize each of your source tables into one of five types:

* ``private`` for tables that are Primary Private, that is the tables containing the subjects of privacy (the table of hospital patients,  for example). Not every table containing sensitive data needs to be marked private, only the table directly referring to the individuals (or families) that need to be protected.
* ``ignore`` for tables that should not be present in the destination database
* ``empty`` for tables that should contain no data, but be present (also for tables that should be populated entirely from stories, see later)
* ``vocabulary`` for tables that should be reproduced exactly in the destination database
* ``normal`` for everything else

This command will start an interactive command shell. Don't be intimidated, just type ``?`` (and press return) to get help:

.. code-block:: console

   $ sqlsynthgen configure-tables
   Interactive table configuration (ignore, vocabulary, private, normal or empty). Type ? for help.

   (table: myfirsttable) ?

   Use the commands 'ignore', 'vocabulary',
   'private', 'empty' or 'normal' to set the table's type. Use 'next' or
   'previous' to change table. Use 'tables' and 'columns' for
   information about the database. Use 'data', 'peek', 'select' or
   'count' to see some data contained in the current table. Use 'quit'
   to exit this program.
   Documented commands (type help <topic>):
   ========================================
   columns  data   help    next    peek      private  select  vocabulary
   counts   empty  ignore  normal  previous  quit     tables

   (table: myfirsttable) 

You can also get help for any of the commands listed; for example to see help for the ``vocabulary`` command type ``? vocabulary`` or ``help vocabulary``:

.. code-block:: console

   (table: myfirsttable) help vocabulary
   Set the current table as a vocabulary table, and go to the next table
   (table: myfirsttable)

Note that the prompt here is ``(table: myfirsttable)``. This will be different on your database; it will show the name of the table that is currently under consideration.

Tab completion
^^^^^^^^^^^^^^

You can use the Tab key on your keyboard to shorten these commands. Try typing h-tab-space-v-tab-return, and you will get ``help vocabulary`` again.
Some commands require a little more. Try typing h-tab-p-tab and you will see that the ``p`` does not get expanded to ``private`` because there is more than one possibility (it could be ``peek`` or ``previous``).
Press the Tab key again to see these options:

.. code-block:: console

   (table: actor) help p
   peek      previous  private   
   (table: actor) help p

Now you can continue with r-i-tab to get ``private``, r-e-tab to get ``previous`` or e-tab to get ``peek``. This can be very useful; try pressing Tab twice on an empty line to see quickly all the possible commands, for example!

Navigating the database
^^^^^^^^^^^^^^^^^^^^^^^

Use ``next`` and ``previous`` to go forwards and backwards through the list of tables.
You can use ``next tablename`` to go to the table ``tablename`` (tab completion works here too!)
You can use ``tables`` to list all the tables and any configuration you have already done.

Setting the type of the table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``private``, ``ignore``, ``empty``, ``vocabulary`` or ``normal`` to set the type of the table. Any you don't set will be ``normal``.
If you have previously run ``configure-tables`` (or edited the ``config.yaml`` file yourself!) the previously set types will be preserved unless you change them.

Examining the data
^^^^^^^^^^^^^^^^^^

But how do you know which type to choose? You can sample the data in the table to help here:

* ``data`` is the easiest: it shows a sample of ten complete rows from the database.
* ``data 20`` if you want more (or fewer) than ten lines, add how many lines you want.
* ``data 20 columnname`` if you want to see just one column, use this formulation with the name of the column you want to examine.
* ``data 20 columnname 30`` adding one extra number here restricts the sampling to only entries at least as long as this number of characters. You can use this to find odd descriptions that people have put into strange places in the database.
* ``columns`` shows structural data about the table
* ``counts`` tells you how many NULLs are in each column (not so useful here, perhaps)
* ``peek column1 column2`` is like ``data`` but restricted to the columns you specified (and it will not show fully NULL rows, so use this to see data in sparse columns)
* and if none of that satisfies you, type any SQL query beginning with ``select`` to get the first 50 results from an arbitrary query.

Repeat last command
^^^^^^^^^^^^^^^^^^^

Entering an empty command will repeat the last command.
So if you want more data than ``data`` gives you, you can type d-a-t-a-return-return-return,
or if you want to step through tables without altering them, you can type n-e-x-t-return-return-return-...

When you are finished
^^^^^^^^^^^^^^^^^^^^^

Use the command ``quit``. It will then ask you if you want to save the results.
You must type ``yes`` to save, ``no`` to exit without saving or ``cancel`` to return to the ``configure-tables`` command prompt.
You must type one of these three options in full but tab completion is available, so y-tab-return, n-tab-return or c-tab-return will do!

Configuring column generators
-----------------------------

The ``configure-generators`` command is similar to ``configure-tables``, but here you are configuring each column in ``normal`` and  ``private`` tables.

The ``next``, ``previous``, ``peek``, ``columns``, ``select``, ``tables``, ``counts``, ``help`` and ``quit`` work as before, but ``next`` allows you to visit not just a different table but also any column with the ``next table.column`` syntax.

``info`` gives you simple information on the current column. Use this while you are getting used to configuring generators.

Configuring a column generator has three steps:

1. ``propose`` shows you a list of built-in generators that would be appropriate for this column
2. ``compare`` allows you to see the output from these generators together with the data each generator requires from the database.
3. ``set`` allows you to set the generator from the proposal list (or ``unset`` removes any previously set generator)

Propose
^^^^^^^

``propose`` will provide a list of suitable generators, attempting to list them by relevance (might not do a fantastic job):

.. code-block:: console

   (film.length) propose
   Sample of actual source data: 173,73,172,81,86...
   1. dist_gen.uniform_ms: (fit: 1.19e-05) 107.55835091131807, 108.68424131615669, 76.18479907993151, 124.02617636581346, 142.3863993456911 ...
   2. dist_gen.normal: (fit: 0.000109) 94.49927930013584, 69.6024952777228, 101.74949693935817, 22.45166839395958, 76.40908811297868 ...
   3. dist_gen.choice: (fit: 0.0346) 155, 86, 89, 178, 166 ...
   4. dist_gen.zipf_choice: (fit: 2) 75, 53, 179, 179, 135 ...
   5. generic.person.weight: (no fit) 85, 73, 69, 58, 81 ...
   6. dist_gen.constant: (no fit) None, None, None, None, None ...
   (film.length)

Here we can see the first line is a small sample of data from the real column in the source database.
The other lines have four elements:

* ``3.`` is the number of the generator, we will need that later!
* ``dist_gen.choice`` is the name of the generator
* ``(fit: 0.0346)`` is a measure of how good sqlsynthgen thinks the generator is (not necessarily a very good measure)
* ``155, 86, 89, 178, 166 ...`` is a sample of data from this generator

For more information, we need the next command, ``compare``.

Compare
^^^^^^^

In the previous example, we might consider that ``1``, ``2``, ``3`` and ``4`` are worth investigating further, so we try:

.. code-block:: console

   (film.length) compare 1 2 3 4
   Not private
   1. dist_gen.uniform_ms requires the following data from the source database:
   SELECT AVG(length) AS mean__length, STDDEV(length) AS stddev__length FROM film; providing the following values: [Decimal('115.2720000000000000'), Decimal('40.4263318185598470')]
   2. dist_gen.normal requires the following data from the source database:
   SELECT AVG(length) AS mean__length, STDDEV(length) AS stddev__length FROM film; providing the following values: [Decimal('115.2720000000000000'), Decimal('40.4263318185598470')]
   3. dist_gen.choice requires the following data from the source database:
   SELECT length AS value FROM film GROUP BY value ORDER BY COUNT(length) DESC; providing the following values: [[85, 179, 112, 84, 74, 100, 73, 102, 48, 122, 92, 139, 114, 61, 107, 75, 181, 176, 178, 80, 185, 135, 63, 50, 137, 136, 59, 53, 152, 110, 103, 161, 126, 64, 153, 147, 120, 172, 121, 144, 150, 67, 60, 184, 93, 132, 98, 99, 118, 171, 113, 58, 71, 51, 70, 52, 101, 180, 115, 65, 173, 82, 125, 57, 151, 163, 167, 109, 111, 123, 128, 142, 141, 154, 47, 76, 145, 148, 129, 143, 157, 79, 182, 54, 83, 91, 130, 69, 87, 169, 78, 159, 158, 155, 119, 160, 106, 62, 177, 104, 174, 105, 89, 149, 175, 138, 77, 134, 133, 162, 146, 117, 166, 68, 46, 127, 183, 108, 140, 49, 56, 165, 131, 90, 86, 97, 164, 170, 94, 116, 72, 156, 124, 88, 168, 81, 95, 96, 55, 66]]
   4. dist_gen.zipf_choice requires the following data from the source database:
   SELECT length AS value FROM film GROUP BY value ORDER BY COUNT(length) DESC; providing the following values: [[85, 179, 112, 84, 74, 100, 73, 102, 48, 122, 92, 139, 114, 61, 107, 75, 181, 176, 178, 80, 185, 135, 63, 50, 137, 136, 59, 53, 152, 110, 103, 161, 126, 64, 153, 147, 120, 172, 121, 144, 150, 67, 60, 184, 93, 132, 98, 99, 118, 171, 113, 58, 71, 51, 70, 52, 101, 180, 115, 65, 173, 82, 125, 57, 151, 163, 167, 109, 111, 123, 128, 142, 141, 154, 47, 76, 145, 148, 129, 143, 157, 79, 182, 54, 83, 91, 130, 69, 87, 169, 78, 159, 158, 155, 119, 160, 106, 62, 177, 104, 174, 105, 89, 149, 175, 138, 77, 134, 133, 162, 146, 117, 166, 68, 46, 127, 183, 108, 140, 49, 56, 165, 131, 90, 86, 97, 164, 170, 94, 116, 72, 156, 124, 88, 168, 81, 95, 96, 55, 66]]
   +--------+------------------------+--------------------+--------------------+-------------------------+
   | source | 1. dist_gen.uniform_ms | 2. dist_gen.normal | 3. dist_gen.choice | 4. dist_gen.zipf_choice |
   +--------+------------------------+--------------------+--------------------+-------------------------+
   |   60   |   46.632794372002664   | 87.89991176975211  |         96         |            59           |
   |   56   |   96.17573671882317    | 143.27403823693294 |        145         |            67           |
   |  167   |   158.2777826396661    | 69.60827255211873  |         99         |           107           |
   |  160   |   48.91052171988566    | 101.08450212269153 |        108         |            85           |
   |   64   |   151.7534973807259    | 46.65796712446469  |        106         |           136           |
   |  138   |   92.64980389758904    | 129.6901021567232  |        109         |           122           |
   |  109   |   62.851359423566414   | 96.26116817758401  |        158         |            85           |
   |   74   |   68.29348043746441    | 33.58822018478509  |         85         |            84           |
   |   75   |   123.84806734660017   |  91.6033632909829  |         53         |            61           |
   |  143   |   59.016661941662406   | 175.02921918869674 |         62         |           181           |
   |   62   |    77.0672702141529    | 153.55365499492189 |        185         |           147           |
   |   75   |   126.53040995684793   | 137.32698597697157 |        102         |           179           |
   |  162   |   125.58699420416819   | 113.8898812686725  |         94         |            85           |
   |  157   |   96.93359267654796    | 61.654471841517044 |         97         |           180           |
   |  117   |   181.0134365019266    | 91.93492164429024  |         57         |            85           |
   |   61   |   75.68573964087891    | 115.79796856358605 |        141         |           102           |
   |   73   |   85.37110501852806    | 141.1104329209363  |         51         |           137           |
   |  110   |   136.56146532743944   | 112.04603094742818 |        127         |           139           |
   |   67   |   152.49478264537873   | 146.82247056721147 |         51         |            74           |
   |  109   |   129.69326718355967   | 111.24264422243346 |         61         |            85           |
   +--------+------------------------+--------------------+--------------------+-------------------------+
   (film.length)

The first line is telling us whether the table is Primary Private (``private`` in ``configure-tables``), Secondary Private (refers to a Primary Private table) or Not Private.
The next lines tell us, for each generator we chose, the query it needs running on the database and what data that results in.
The table below that is a sample from the source database and each generator.

Set and unset
^^^^^^^^^^^^^

Say we decide on generator 2, we can set this with ``set 2``.
``unset`` removes any previously set generator.

Configuring missingness
-----------------------

The ``configure-missing`` command is also similar to ``configure-tables``, but here you are configuring the patterns of NULLs within tables.

This configuration can only really cope with MCAR (Missing Completely at Random) data.
This means we cannot specify that certain patterns of NULLs are more or less likely depending on the generated values for certain fields. Something for future development.

At the moment there are only two missingness generators.
Use command ``none`` to set that no NULLs will be generated (unless the generator itelf generates them).
Use the command ``sampled`` to set that the NULLs are generated according to a sample of rows from the database.
The  ``sampled`` missingness generator samples 1000 rows from the table, and generates missingness patterns present in these rows in proportion to how common they are in this sample.
This gives a reasonable approximation to the missingness patterns in the original data.

The other commands ``counts``, ``help``, ``next``, ``peek``, ``previous``, ``quit``, ``select`` and ``tables`` work the same as before.

Generating the data
-------------------

Now you have files ``orm.yaml`` (generated with ``make-tables``) and ``config.yaml`` (generated from the ``generate-`` commands).
You also need two more. Run the following commands:

.. code-block:: console

   $ sqlsynthgen make-stats
   $ sqlsynthgen make-vocab --compress --no-force

The first of these generates a files ``src-stats.yaml`` containing summary statistics from the database that the generators need.
The second generates files ``tablename.yaml.gz`` containing data from the vocabulary tables. WARNING: this can take many hours depending on how big they are!
``--compress`` compresses the files with gzip, which might be necessary if the machine sqlsynthgen is running on risks running out of disk space.
``-no-force`` is necessary if you have had to interrupt the process previously and want to keep your existing files; it will generate only files that do not already exist.
If you had to stop ``make-vocab`` (or it got stopped for some other reason) you will need to check which of your ``.gz`` files are complete. You can use ``gzip -t filename.gz`` for this.

Taking files out of the private network
---------------------------------------

You now have ``orm.yaml``, ``config.yaml``, ``src-stats.yaml`` and all the ``tablename.yaml.gz`` files.
These can all be checked for compliance with any privacy checks you are using then sent out of the private network.

Connecting to the destination database
--------------------------------------

Just like connecting to the source database, we will use environment variables, either in Bash, Windows Command Shell or docker:

MacOS or Linux:

.. code-block:: console

   $ export DST_DSN="postgresql://someuser:somepassword@myserver.mydomain.com/dst_db"
   $ export DST_SCHEMA='myschema'

Windows Command Shell:

.. code-block:: console

   $ set DST_DSN "postgresql://someuser:somepassword@myserver.mydomain.com/dst_db"
   $ set DST_SCHEMA "myschema"

Running from the ready-built Docker container, from within a directory holding only your ``.yaml`` and ``.yaml.gz`` files (please use WSL for this on Windows):

.. code-block:: console

   $ docker run --rm --user $(id -u):$(id -g) --network host -e DST_SCHEMA=myschema -e DST_DSN=postgresql://someuser:somepassword@myserver.mydomain.com/dst_db -itv .:data --pull always timband/ssg

Whichever we chose, now we can create the generators Python file:

.. code-block:: console

   $ sqlsynthgen create-tables
   $ sqlsynthgen create-vocab
   $ sqlsynthgen create-generators --stats-file src-stats.yaml
   $ sqlsynthgen create-data --num-passes 10

The first of these uses ``orm.yaml`` to create the destination database.
The second uses all the ``.yaml.gz`` (or ``.yaml``) files representing the vocabulary tables (this can take hours, too).
The third uses ``config.yaml`` to create a file ``ssg.py`` file containing code to call the generators as configured.
The last one actually generates the data. ``--num-passes`` controls how many rows are generated.
At present the only ways to generate different numbers of rows for different tables is to configure ``num_rows_per_pass`` in ``config.yaml``:

.. code-block:: yaml

   observation:
      num_rows_per_pass: 50

This makes every call to ``create-data`` produce 50 rows in the ``observation`` table (each time you change ``config.yaml` you need to re-run ``create-generators``).
If you call ``create-data`` multiple times you get more data added to whatever already exists. Call ``remove-data`` to remove all rows from all non-vocabulary tables.

You can call ``remove-vocab`` to remove all rows from all vocabulary tables, and you can call ``remove-tables`` to empty the database completely.
