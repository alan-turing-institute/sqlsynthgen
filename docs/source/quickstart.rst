.. _page-quickstart:

Command-Line Interface (CLI) Guide
===============================================

Overview
--------

After :ref:`Installation <page-installation>`, we can run ``datafaker`` to see the available commands:

.. code-block:: console

   $ datafaker
   Usage: datafaker [OPTIONS] COMMAND [ARGS]...

   Options:
     --help                          Show this message and exit.

   Commands:
      configure-generators  Interactively set generators for column data.
      configure-missing     Interactively set the missingness of the...
      configure-tables      Interactively set tables to ignored, vocabulary...
      create-data           Populate the schema in the target directory with...
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

datafaker is designed to be run connected to either the private source database or the more public destination database. It never needs to be connected to both.
So you can install datafaker on a machine with access to the private source database that will do the reading, and again on another machine that will do the creation.

In this guide we will walk through configuring column generators. We will not discuss stories here.

Connecting to the source database
---------------------------------

To connect to the source database, set the ``SRC_DSN`` and ``SRC_SCHEMA`` environment variables.
You can leave ``SRC_SCHEMA`` unset if you are using the default schema (or a database that does not use schema names such as MariaDB):

MacOS or Linux:

.. code-block:: console

   $ export SRC_DSN="postgresql://someuser:somepassword@myserver.mydomain.com:5432/db_name"
   $ export SRC_SCHEMA='myschema'

Windows Command Shell:

.. code-block:: console

   $ set SRC_DSN=postgresql://someuser:somepassword@myserver.mydomain.com:5432/db_name
   $ set SRC_SCHEMA=myschema

Running from the ready-built Docker container, make an output directory then use that as the data volume like so (please use WSL for this on Windows):

.. code-block:: console

   $ mkdir output
   $ docker run --rm --user $(id -u):$(id -g) --network host -e SRC_SCHEMA=myschema -e DST_DSN=postgresql://someuser:somepassword@myserver.mydomain.com:5432/db_name -itv ./output:data --pull always timband/datafaker

Now you can use the commands that use the source database (the ones beginning ``configure-`` and ``make-`` but not the ones beginning ``create-`` and ``remove-``).

Initial configuration
---------------------

The first job is to read the structure of the source database:

.. code-block:: console

   $ datafaker make-tables

This will create a file called ``orm.yaml``. You should not need to edit this file.

Troubleshooting: What if my tables do not appear in the orm.yaml file?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the tables you expected to see do not all appear in the ``orm.yaml`` file,
perhaps these are not actual tables, but views.

DataFaker can copy views as though they are tables. If you want this behaviour
(for example, if you have no access to the underlying tables) you can use
the ``--views-to-tables`` option:

.. code-block:: console

   $ datafaker make-tables --views-to-tables

This will produce definitions of each view as well as each table.
This is not a perfect system; DataFaker will guess which columns represent
primary or foreign keys as this information is not present in views.
So, after running ``make-tables`` with the ``--view-to-tables`` option,
please check the ``orm.yaml`` file produced:

* make sure that each table has its correct primary key (DataFaker will have warned you if DataFaker found no or multiple primary keys for any table)
* make sure that all foreign key columns are correctly marked
* delete any tables you don't want and have no foreign keys pointing to them (or you could mark them as ignored in the following ``configure-tables`` stage)

Configuring table types
-----------------------

Next you can use the ``configure-tables`` command categorize each of your source tables into one of five types:

* ``private`` for tables that are Primary Private, that is the tables containing the subjects of privacy (the table of hospital patients,  for example). Not every table containing sensitive data needs to be marked private, only the table directly referring to the individuals (or families) that need to be protected.
* ``ignore`` for tables that should not be present in the destination database
* ``empty`` for tables that should contain no data, but be present (also for tables that should be populated entirely from stories, see later)
* ``vocabulary`` for tables that should be reproduced exactly in the destination database
* ``generate`` for everything else

This command will start an interactive command shell. Don't be intimidated, just type ``?`` (and press return) to get help:

.. code-block:: console

   $ datafaker configure-tables
   Interactive table configuration (ignore, vocabulary, private, generate or empty). Type ? for help.

   (table: myfirsttable) ?

   Use the commands 'ignore', 'vocabulary',
   'private', 'empty' or 'generate' to set the table's type. Use 'next' or
   'previous' to change table. Use 'tables' and 'columns' for
   information about the database. Use 'data', 'peek', 'select' or
   'count' to see some data contained in the current table. Use 'quit'
   to exit this program.
   Documented commands (type help <topic>):
   ========================================
   columns  data   help    next    peek      private  select  vocabulary
   counts   empty  ignore  generate  previous  quit     tables

   (table: myfirsttable)

You can also get help for any of the commands listed; for example to see help for the ``vocabulary`` command type ``? vocabulary`` or ``help vocabulary``:

.. code-block:: console

   (table: myfirsttable) help vocabulary
   Set the current table as a vocabulary table, and go to the next table
   (table: myfirsttable)

Note that the prompt here is ``(table: myfirsttable)``. This will be different on your database; it will show the name of the table that is currently under consideration.

Tab completion
^^^^^^^^^^^^^^

For the following examples we will be using the `Pagila <https://github.com/devrimgunduz/pagila>`_ database.

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

Use ``private``, ``ignore``, ``empty``, ``vocabulary`` or ``generate`` to set the type of the table. Any you don't set will be ``generate``.
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

The ``configure-generators`` command is similar to ``configure-tables``, but here you are configuring each column in ``generate`` and  ``private`` tables.

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
* ``(fit: 0.0346)`` is a measure of how good datafaker thinks the generator is (not necessarily a very good measure)
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

Short commands
^^^^^^^^^^^^^^

In case you couldn't get tab completion working on your favourite terminal on your machine, there are single letter commands for the most common operations of ``configure-generators``:
``n`` and ``b`` are synonymns for ``next`` and ``previous`` ("back"), and ``p``, ``c`` and ``s`` are synonymns for ``propose``, ``compare`` and ``set`` respectively.

Multivariate generators
^^^^^^^^^^^^^^^^^^^^^^^

So far we have been talking about generators that generate one column in isolation.
If we want columns that are not independent we will need to merge generators.
Let us merge the width and height of the artwork so that we can generate larger widths when we have larger heights.

For this example we will use the `database of artworks from the Museam of Modern Art in New York <https://github.com/MuseumofModernArt/collection>`_.

.. code-block:: console

   (artist.artist_bio) next artwork.width_cm
   (artwork.width_cm) merge height_cm
   (artwork.width_cm,height_cm) propose
   Sample of actual source data: Decimal('19.5'),Decimal('24.2'); Decimal('17.8'),Decimal('22.3'); Decimal('60.3251206502'),Decimal('73.3426466853'); Decimal('29.5'),Decimal('18.8'); Decimal('33.5'),Decimal('24.4')...
   1. dist_gen.multivariate_normal: (no fit) [156.09031589469922, 26.69778970196757]; [-4.090845423753819, -8.18194806335324]; [57.91680668094389, -3.1887356250744077]; [-26.06615585218517, 9.165796782425087]; [123.06615368995867, 73.76191916586535] ...
   2. dist_gen.multivariate_lognormal: (no fit) [13.111796803254672, 36.29819014891643]; [8.243881226804909, 12.183080281415767]; [34.74746356276248, 40.259883403242]; [16.71081622360879, 13.136174935970404]; [28.625189220743074, 25.134767045686736] ...
   3. null-partitioned grouped_multivariate_normal: (no fit) [18.832731713525874, -20.218237295889274]; [9.22970763980161, 69.63305466303493]; [159.45614650560498, 52.46062596299266]; [91.30038782576139, 64.3899269396307]; [230.3406736168121, 20.91434294652789] ...
   4. null-partitioned grouped_multivariate_normal [sampled and suppressed]: (no fit) [-168.25567806040397, -12.805359915935114]; [104.58903478494895, 41.38105009883969]; [-108.643833670611, -25.594600883707706]; [None, None]; [-0.20287791770687846, 69.88744644001237] ...
   5. null-partitioned grouped_multivariate_lognormal: (no fit) [17.96925731188804, 8.41346587248666]; [None, None]; [18.05960425078999, 18.472900483777277]; [25.244847272832885, 21.32227474728793]; [22.271880531808534, 27.509500509472115] ...
   6. null-partitioned grouped_multivariate_lognormal [sampled and suppressed]: (no fit) [None, None]; [None, None]; [6.189055115396227, 6.661888913381631]; [29.99920965125257, 17.858917208234534]; [None, None] ...
   (artwork.width_cm,height_cm) compare 1 2
   Not private
   1. dist_gen.multivariate_normal requires the following data from the source database:
   SELECT q.m0, q.m1, (q.s0_0 - q.count * q.m0 * q.m0)/NULLIF(q.count - 1, 0) AS c0_0, (q.s0_1 - q.count * q.m0 * q.m1)/NULLIF(q.count - 1, 0) AS c0_1, (q.s1_1 - q.count * q.m1 * q.m1)/NULLIF(q.count - 1, 0) AS c1_1, q.count AS count, 2 AS rank FROM (SELECT COUNT(*) AS count, SUM(width_cm * width_cm) AS s0_0, SUM(width_cm * height_cm) AS s0_1, SUM(height_cm * height_cm) AS s1_1, AVG(width_cm) AS m0, AVG(height_cm) AS m1 FROM artwork WHERE width_cm IS NOT NULL AND height_cm IS NOT NULL) AS q WHERE 1 < q.count; providing the following values: [{'m0': Decimal('39.8408881695503020'), 'm1': Decimal('37.8942602940080112'), 'c0_0': Decimal('8566.13996292591607014145908725748824'), 'c0_1': Decimal('1771.51432112064062574103021748260967'), 'c1_1': Decimal('2434.30506961622772921636781436857116'), 'count': 127808, 'rank': 2}]
   2. dist_gen.multivariate_lognormal requires the following data from the source database:
   SELECT q.m0, q.m1, (q.s0_0 - q.count * q.m0 * q.m0)/NULLIF(q.count - 1, 0) AS c0_0, (q.s0_1 - q.count * q.m0 * q.m1)/NULLIF(q.count - 1, 0) AS c0_1, (q.s1_1 - q.count * q.m1 * q.m1)/NULLIF(q.count - 1, 0) AS c1_1, q.count AS count, 2 AS rank FROM (SELECT COUNT(*) AS count, SUM(LN(width_cm) * LN(width_cm)) AS s0_0, SUM(LN(width_cm) * LN(height_cm)) AS s0_1, SUM(LN(height_cm) * LN(height_cm)) AS s1_1, AVG(LN(width_cm)) AS m0, AVG(LN(height_cm)) AS m1 FROM artwork WHERE width_cm IS NOT NULL AND 0 < width_cm AND height_cm IS NOT NULL AND 0 < height_cm) AS q WHERE 1 < q.count; providing the following values: [{'m0': Decimal('3.328012379626851426'), 'm1': Decimal('3.353020870725566034'), 'c0_0': Decimal('0.588886424217914574880579068386344791'), 'c0_1': Decimal('0.482342791051859102539757116685206243'), 'c1_1': Decimal('0.576396368516107766179437511660540672'), 'count': 125300, 'rank': 2}]
   +------------------------------+-------------------------------------------+------------------------------------------+
   |            source            |      1. dist_gen.multivariate_normal      |    2. dist_gen.multivariate_lognormal    |
   +------------------------------+-------------------------------------------+------------------------------------------+
   |          22.4, 29.9          |  [-30.30197300226458, 125.32226985736872] | [59.152930502697714, 56.40653542376871]  |
   |          51.9, 41.6          |  [-12.853806535442509, 49.52394347123934] | [62.80967227803588, 27.610574449276893]  |
   |          23.6, 27.7          |  [-76.54701202747279, 95.28076769991027]  | [120.22207976396665, 67.99534689550484]  |
   |          19.1, 25.3          |  [-94.73357751277317, -40.53065124214407] |  [10.256731266417718, 9.92401948516523]  |
   |          56.4, 76.2          |   [5.783164682381688, 75.31682595299304]  |  [33.56480847165019, 32.31307126699535]  |
   |          45.1, 52.8          |  [13.239585293742458, 14.95156014276887]  | [42.772989425028385, 43.146644322968754] |
   | 14.2875285751, 20.9550419101 |   [67.13380379777615, 68.38226051217048]  | [19.072875407542448, 8.705480286190836]  |
   |          22.5, 16.7          | [-11.246221416602566, -52.48693945380513] | [39.977277575041306, 24.472887821531405] |
   | 79.3751587503, 59.6901193802 |  [-43.54109084064334, 23.635761625126122] | [17.591732388768918, 12.468330774072989] |
   |          27.6, 28.1          |   [39.1207176171055, 50.930461148452565]  |  [24.24087885150377, 29.01327658224259]  |
   |          50.2, 39.8          | [-147.21637042993717, 29.937545279847257] | [20.598207180125364, 18.586852513581082] |
   |          23.2, 30.8          |   [54.8823235542188, 35.32302587446014]   | [15.312141023476611, 18.439038524603294] |
   |       70.4851, 58.7376       |  [12.201614091657802, 23.37266118274506]  | [14.842880257906147, 28.060559922386517] |
   |     32.5438150876, 42.7      |  [36.36776373020976, 3.9374033292046207]  |  [46.87929388618674, 27.50376839671559]  |
   |          49.5, 35.0          |   [38.4553214014625, 44.05597131161997]   | [19.934782162790142, 29.393521224229563] |
   |          16.4, 33.5          |  [-53.22130937119619, 13.891352066160252] |  [88.36839133560514, 69.24444774268322]  |
   |          62.5, 76.2          |  [120.61263891147385, 81.78495411418078]  | [39.880687407969226, 30.60721063876066]  |
   |          36.7, 24.4          |   [125.801983433586, 59.14111658833157]   |  [53.733946922347656, 33.0511244388031]  |
   |  12.382524765, 7.937515875   |   [50.56656447948824, 80.36754342712976]  | [17.354428334282115, 27.234368131874156] |
   |           0.0, 0.0           |  [120.63922185844007, 30.084733739945715] | [20.476178451775507, 18.79872306014655]  |
   +------------------------------+-------------------------------------------+------------------------------------------+
   (artwork.width_cm,height_cm)

We can see six generators; normal and lognormal distributions, and some are "null-partitioned grouped" and some are "sampled and suppressed".
We can see in the ``compare`` table that the normal distribution produces negative heights and widths, whereas the lognormal distribution produces much saner results.

To describe "null-partitioned grouped", let us make the generator much more complicated by adding ``depth_cm`` and ``medium`` columns and peeking at the data there:

.. code-block:: console

   (artwork.width_cm,height_cm) merge depth_cm medium
   (artwork.depth_cm,width_cm,height_cm,medium) peek
   +----------+---------------+---------------+------------------------------------------------------------------------------------------------+
   | depth_cm |    width_cm   |   height_cm   |                                             medium                                             |
   +----------+---------------+---------------+------------------------------------------------------------------------------------------------+
   |   None   |      26.7     |      38.1     |                               One from a set of four lithographs                               |
   |   None   |     24.13     |    30.4801    |                              Page with chromogenic print and text                              |
   |   2.9    |     150.0     |      42.0     |     Pencil on paper, gelatin silver print, metallic paper, colored paper, and graph paper      |
   |   0.0    |      40.0     |      40.3     |                                         Alkyd on board                                         |
   |   None   |    22.3838    |    28.5751    |                                        Pencil on paper                                         |
   |   None   | 70.8026416053 | 82.5501651003 |                                             Poster                                             |
   |   None   |      19.8     |      26.0     |                             Lift ground aquatint, printed in color                             |
   |   None   |      45.8     |      32.5     |                                           Lithograph                                           |
   |   None   |      86.4     |      None     |                                           Polyester                                            |
   |   None   |      None     |      None     |                                      Video (color, sound)                                      |
   |   None   |      18.4     |      13.6     |                                      Gelatin silver print                                      |
   |   None   |      None     |      None     |                                      Albumen silver print                                      |
   |   None   |      13.0     |      18.0     |                                            Drypoint                                            |
   |   None   | 17.7800355601 | 24.4475488951 |                                      Watercolor on paper                                       |
   |   None   |      33.4     |      25.8     |                                      Gelatin silver print                                      |
   |   None   |      24.0     |      18.0     |                                      Gelatin silver print                                      |
   |   None   |      7.2      |      4.0      | Wood engraving from an illustrated book with 323 wood engravings and one etching and engraving |
   |   None   |      91.1     |      71.8     |     Cut-and-pasted printed and painted papers, wood veneer, gouache, oil, and ink on board     |
   |   None   |      33.3     |      24.3     |                              Illustrated book with one lithograph                              |
   |   None   |      15.1     |      16.9     |                       One from an artist's book of twenty-four die-cuts                        |
   |   None   |      23.2     |      30.8     |                                           Periodical                                           |
   |   None   |      None     |      None     |                                   Matte albumen silver print                                   |
   |   None   |      14.8     |      12.2     |                                     Drypoint and engraving                                     |
   |   None   |      None     |      None     |                                    Pencil on tracing paper                                     |
   |   None   |      18.5     |      24.3     |               Lithograph from an illustrated book of poems and four lithographs                |
   +----------+---------------+---------------+------------------------------------------------------------------------------------------------+
   (artwork.depth_cm,width_cm,height_cm,medium)

Here we can see that Moma understandably does not record depths for 2D artworks so we have many NULLs in that column.
If we try to apply the standard normal or lognormal to data with many NULLs, it will ignore those rows with any NULLs.
So while we could use missingness generators (see later) to give us the sense of 2D as well as 3D artworks,
if we did this the dimensions of the 2D artworks would be based entirely on the dimensions of the 3D artworks!
The other obvious problem is that normal and lognormal distributions can't help you with the ``medium``  column!

Both of these problems can be addressed with the null-partitioned grouped generators.

Null-partitioned generators deal in three different types of value: NULL, numeric (while not a foreign key) and category.
As long as the columns contain these three types of data the Null-partitioned grouped generators are appropriate.
"Null-partitioned" essentially means that you will get a different generator for each pattern of NULLs in a row (not very different!).
"Grouped" means that you will get a different set of covariates for the numeric columns for the different patterns of choice values within each partition.

This is a little confusing, so let's talk about how this works with the Moma example above:

.. code-block:: console

   (artwork.depth_cm,width_cm,height_cm,medium) propose
   Sample of actual source data: Decimal('0.0'),Decimal('50.8001016002'),Decimal('40.9575819152'),'Colored pencil and pencil on cut-and-pasted paper on oil on paper'; Decimal('0.0'),Decimal('21.9'),Decimal('31.7500635001'),'Gelatin silver print'; Decimal('0.0'),Decimal('66.0401320803'),Decimal('101.6002032004'),'Solvent transfer drawing with gouache, pencil, colored pencil, and lithograph on paper'; Decimal('0.0'),Decimal('0.0'),Decimal('0.0'),'Offset'; Decimal('9.5'),Decimal('43.0'),Decimal('32.0'),'Vinyl-covered attaché case with screenprint'...
   1. null-partitioned grouped_multivariate_normal: (no fit) [None, 39.973634541811435, 31.93624913182064, 'Gelatin silver print']; [None, 14.147337233160641, 34.97976525453238, 'Woodcut and wood engraving, printed in color']; [None, 10.092882610994772, 14.249800582490234, 'Gelatin silver print']; [None, -6.391051500993143, -2.403385248222161, 'Gelatin silver print']; [None, 29.390544388814472, 30.162571070278858, 'Gelatin silver print'] ...
   2. null-partitioned grouped_multivariate_normal [sampled and suppressed]: (no fit) [None, 36.06686932038727, 29.41013826875849, 'Gelatin silver print']; [None, 17.514537089293558, 17.891332542467747, None]; [None, 47.774761208268984, 45.61942518797726, 'Lithograph']; [None, 33.65613203530817, 61.84008585634961, 'Lithograph']; [None, -31.874726507172454, -22.155743053736508, 'Lithograph'] ...
   3. null-partitioned grouped_multivariate_lognormal: (no fit) [None, None, None, 'Pencil on tracing']; [None, None, None, 'Albumen silver print']; [None, None, None, 'Ink on note paper']; [None, 29.770314274108912, 36.92932081226337, 'Gelatin silver print']; [None, 28.09314226304076, 19.23404715223526, 'Lithograph'] ...
   4. null-partitioned grouped_multivariate_lognormal [sampled and suppressed]: (no fit) [None, None, None, 'Albumen silver print']; [None, None, None, 'Pencil on paper']; [None, None, None, 'Pencil on tracing paper']; [None, 19.117201243763237, 15.87224244805247, 'Chromogenic print']; [None, 73.59589035622726, 36.004739858192416, 'Lithograph'] ...
   (artwork.depth_cm,width_cm,height_cm,medium)

The ``medium`` column is preventing the ``dist_gen.multivariate_normal``  and ``dist_gen.multivariate_lognormal`` from appearing,
so we just have the null-partitioned generators.
In the data above we can see three patterns of missingness: All four columns are non-null, only ``depth_cm`` is null, and only ``medium`` is non-null.
The first thing these generators will do is partition the data into these three missingness patterns (and more if there are others present).
The second thing it will do is get data for each medium listed in each partition;
a query will be run for each partition finding the covariates between the width, height and depth (if present) for each medium
("grouped by medium" in the language of SQL, hence the name null-partitioned "grouped" generators).
When it comes to generating the data, it will choose a partition (weighted by how popular these partitions are in the source data),
then it will choose a medium (again weighted by the source data), then it will generate values for the numeric columns based on the covariates associated with that medium.
In this way, the dimensions of the artwork will be dependent on the materials it is made out of!
So these generators can cope with the missingness, category choices and numeric values, all related to one another:

.. code-block:: console

   (artwork.depth_cm,width_cm,height_cm,medium) compare 3
   ...
   16.8000000000000000'), 'm2': Decimal('7.2500000000000000'), 'c0_0': Decimal('0.00500000000000000000000000000000'), 'c0_1': Decimal('0E-32'), 'c1_1': Decimal('0E-32'), 'c0_2': Decimal('-0.00500000000000000000000000000000'),
   'c1_2': Decimal('0E-32'), 'c2_2': Decimal('0.00500000000000000000000000000000'), 'count': 2, 'rank': 3, 'k3': 'Wood chessboard with offset label, containing thirty-two grinder-attachment pieces'},
   {'m0': Decimal('0.18142893428571428571'), 'm1': Decimal('148.6275989694857143'), 'm2': Decimal('86.7962190210142857'), 'c0_0': Decimal('0.2304152073723502285732428607714285714286'),
   'c0_1': Decimal('-26.486443141750314132709249005152571429'), 'c1_1': Decimal('50474.07439826790816042306146292190476'), 'c0_2': Decimal('-16.221361167705473902113631904894928572'),
   'c1_2': Decimal('17016.51237059149724684191157056976191'), 'c2_2': Decimal('6161.43161737166644044758872927190476'), 'count': 7, 'rank': 3, 'k3': 'Woodcut'},
   {'m0': Decimal('14.2875285750500000'), 'm1': Decimal('45.4025908051500000'), 'm2': Decimal('78.7401574803000000'), 'c0_0': Decimal('193.75038750119350238700500000000000'),
   'c0_1': Decimal('256.25051250126100252200500000000000'), 'c1_1': Decimal('338.91196814640914765700500000000000'), 'c0_2': Decimal('-350.00070000234650469301000000000000'),
   'c1_2': Decimal('-462.90415161543311796301000000000000'), 'c2_2': Decimal('632.25932903684104142402000000000000'), 'count': 2, 'rank': 3, 'k3': 'Wood, metal, and plastic'},
   {'m0': Decimal('34.9250698501000000'), 'm1': Decimal('34.9250698501000000'), 'm2': Decimal('132.8210989755166667'), 'c0_0': Decimal('0E-32'), 'c0_1': Decimal('0E-32'), 'c1_1': Decimal('0E-32'),
   'c0_2': Decimal('-1.39700279400400000E-15'), 'c1_2': Decimal('-1.39700279400400000E-15'), 'c2_2': Decimal('2346.52351025098558274334974862533333'), 'count': 6, 'rank': 3, 'k3': 'Wood, plastic and acrylic paint'},
   {'m0': Decimal('3.8000000000000000'), 'm1': Decimal('21.3000000000000000'), 'm2': Decimal('21.3000000000000000'), 'c0_0': Decimal('0E-32'), 'c0_1': Decimal('0E-32'), 'c1_1': Decimal('0E-32'),
   'c0_2': Decimal('0E-32'), 'c1_2': Decimal('0E-32'), 'c2_2': Decimal('0E-32'), 'count': 5, 'rank': 3, 'k3': 'Wood, plastic, and graphite on paper on plywood'}, {'m0': Decimal('65.8588500000000000'),
   'm1': Decimal('36.7606500000000000'), 'm2': Decimal('63.4000000000000000'), 'c0_0': Decimal('1091.27174664500000000000000000000000'), 'c0_1': Decimal('292.48316850500000000000000000000000'),
   'c1_1': Decimal('78.39147684500000000000000000000000'), 'c0_2': Decimal('-953.04108000000000000000000000000000'), 'c1_2': Decimal('-255.43452000000000000000000000000000'),
   'c2_2': Decimal('832.32000000000000000000000000000000'), 'count': 2, 'rank': 3, 'k3': 'Wood, plastic and metal'}, {'m0': Decimal('29.2100584201000000'), 'm1': Decimal('127.0002540005000000'),
   'm2': Decimal('167.6403352807000000'), 'c0_0': Decimal('1706.45502581130981616802000000000000'), 'c0_1': Decimal('-890.32436129565130098002000000000000'),
   'c1_1': Decimal('464.51705806875407299202000000000000'), 'c0_2': Decimal('4451.62180646657248153206000000000000'), 'c1_2': Decimal('-2322.58529033767435276806000000000000')
    'c2_2': Decimal('11612.92645165789170288018000000000000'), 'count': 2, 'rank': 3, 'k3': 'Wood, plexiglass and painted metal'},
    {'m0': Decimal('45.7201000000000000'), 'm1': Decimal('78.9765181818181818'), 'm2': Decimal('49.6455545454545455'), 'c0_0': Decimal('0E-32'), 'c0_1': Decimal('9.1440200000000000E-16'),
    'c1_1': Decimal('2128.48295510363636679542436363636364'), 'c0_2': Decimal('-2.28600500000000000E-15'), 'c1_2': Decimal('-399.02437570909091204682390909090909'),
    'c2_2': Decimal('114.01736727272726776271727272727273'), 'count': 11, 'rank': 3, 'k3': 'Wood with Honduras mahogany veneer'}, {'m0': Decimal('5.0006350012500000'), 'm1': Decimal('48.4982219964000000'),
    'm2': Decimal('105.0133350266500000'), 'c0_0': Decimal('50.01270083145317500312500000000000'), 'c0_1': Decimal('115.10859715225737104700000000000000'), 'c1_1': Decimal('264.93248551031473030688000000000000'),
    'c0_2': Decimal('450.11430748507882902862500000000000'), 'c1_2': Decimal('1035.97737437492009863052000000000000'), 'c2_2': Decimal('4051.02876738371174726220500000000000'), 'count': 2, 'rank': 3, 'k3': 'Wool'},
    {'m0': Decimal('71.1201000000000000'), 'm1': Decimal('140.0178000000000000'), 'm2': Decimal('127.3178000000000000'), 'c0_0': Decimal('0E-32'), 'c0_1': Decimal('0E-32'), 'c1_1': Decimal('0E-32'), 'c0_2': Decimal('0E-32'),
    'c1_2': Decimal('0E-32'), 'c2_2': Decimal('0E-32'), 'count': 3, 'rank': 3, 'k3': 'Wool felt and polyester resin\r\n'},
    {'m0': Decimal('32.7900040640000000'), 'm1': Decimal('95.6151614681200000'), 'm2': Decimal('99.3051305461000000'), 'c0_0': Decimal('491.80807007928258048000000000000000'),
    'c0_1': Decimal('138.88226464069653219840000000000000'), 'c1_1': Decimal('4898.08109161279111888167200000000000'), 'c0_2': Decimal('764.37647839261694675200000000000000'),
    'c1_2': Decimal('3490.06731087702316920166000000000000'), 'c2_2': Decimal('3690.17446446333032112605000000000000'), 'count': 5, 'rank': 3, 'k3': 'Wrought iron'},
    {'m0': Decimal('2.7883953175371784'), 'm1': Decimal('14.7226723153384232'), 'm2': Decimal('18.6838369725005809'), 'c0_0': Decimal('227.28021114171587616480741876139618'),
    'c0_1': Decimal('121.47245807562292099861695800674074'), 'c1_1': Decimal('528.93747093614426159615049398166995'), 'c0_2': Decimal('44.67974187605438985600073640111816'),
    'c1_2': Decimal('472.61993813801730220734491759261259'), 'c2_2': Decimal('1975.74652217638248323643763765853738'), 'count': 1205, 'rank': 3, 'k3': None}]]
   +--------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   |                                           source                                           |                                                                             1. null-partitioned grouped_multivariate_normal                                                                             |
   +--------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   |               0.0, 35.6, 43.8, Spiral-bound sketchbook with pencil on paper                |                                                                                  [None, None, None, 'Pencil on paper']                                                                                  |
   |               20.3200406401, 20.3200406401, 231.1404622809, Acrylic on wood                |                                     [None, 12.47756100823895, 8.392584765777505, 'Linoleum cut from an illustrated book with 51 linoleum cuts (including wrapper)']                                     |
   |    24.13, 19.0, 21.59, Enameled aluminum, plastic, enameled steel, aluminum, and glass     |                                                                   [None, -25.017489688446325, -17.11990764028542, 'Chromogenic print']                                                                  |
   |          0.0, 270.0, 147.6377952756, Textile, plastic, glass, ceramic, and metal           |                                                   [None, 25.123206640930412, 31.3603335841623, 'Drypoint, with selective wiping, and hand additions']                                                   |
   |             0.0, 9.8, 14.9, Colored pencil, pencil, and ballpoint pen on board             |                                                                                [None, None, None, 'Gelatin silver print']                                                                               |
   |                     0.0, 289.5605791212, 139.1922783846, Inkjet print                      |                                                                           [None, 5.396357927615062, 21.705187032410645, None]                                                                           |
   |                                  0.0, 0.0, 0.0, (confirm)                                  |                                                                [None, 85.92337359469983, 56.44070634304563, 'Gouache on paper on board']                                                                |
   |         0.0, 27.6, 34.0, Page from a spiral-bound sketchbook with pencil on paper          | [None, 26.733426637007618, 31.200065658361655, 'Engraving from an illustrated book with twenty engravings, ten aquatints (one with drypoint), one drypoint, and one etching (including wrapper front)'] |
   |        0.0, 33.3375666751, 38.4175768352, black, blue, red marker (faded) on paper         |                                   [None, 22.136869416798323, 25.274412343341794, 'Lithograph with watercolor and gouache additions and lithographed manuscript text']                                   |
   | 4.127508255, 119.3802387605, 83.8201676403, Inkjet print with hand engraving on lava stone |                                                                   [None, 33.206742942716964, 47.71103655986387, 'Watercolor on paper']                                                                  |
   |                    2.0, 12.0, 19.0, Artist's book with offset bookplate                    |                                   [None, 17.0, 27.0, 'Page from an illustrated book with forty-three in-text prints and one supplementary ink drawing (frontispiece)']                                  |
   |          34.2900685801, 31.3, 31.5, 12-inch vinyl record with screenprinted cover          |                                                               [None, 82.0805969523993, 136.27700248250204, 'Etching, with hand additions']                                                              |
   |                                 58.7, 109.5, 74.9, Bronze                                  |                                                                                         [None, None, None, None]                                                                                        |
   |     0.0, 18.4, 27.6, Spiral-bound sketchbook with pencil, ink, and watercolor on paper     |                                                                  [None, 8.131937888918507, -5.780436152200849, 'Gelatin silver print']                                                                  |
   |                       0.0, 70.8026416053, 106.6802133604, Lithograph                       |                                                                           [None, -18.456152268697725, 2.330663099603008, None]                                                                          |
   |           81.2802, 543.5611, 424.1808, Painted cast iron, glazed lava, and glass           |                                                                                  [None, None, None, 'Pencil on vellum']                                                                                 |
   |                          12.3825, 41.2751, 41.2751, Crumpled map                           |                                                                   [None, 24.543223703363182, 20.0212206104858, 'Gelatin silver print']                                                                  |
   |                 1.1, 14.0, 18.0, Plastic box containing nine offset cards                  |                                                                      [None, None, None, 'Gelatin silver printing-out-paper print']                                                                      |
   |                  0.0, 38.1000762002, 45.7200914402, Etching with aquatint                  |                                                                                [None, None, None, 'Gelatin silver print']                                                                               |
   |          0.0, 25.6, 25.9, One from a set of six records with lithographic sleeves          |                                                                         [None, 95.70986740244891, 72.83297376657666, 'Etching']                                                                         |
   +--------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
   (artwork.depth_cm,width_cm,height_cm,medium)

In the listing above I have had to cut the amount of source statistics reported from the database because datafaker actually produces far more than this!
As we have five columns, this implies up to 32 missingness patterns.
Currently, none of our generators suppress the generation of data from patterns that are not represented in the source database at time of configuration;
therefore data for all 32 possible are pulled into the source stats file.
This works well if we want future missingness patters to be accounted for,
but it does mean that manually checking the actual ``src-stats.yaml`` output
(if that is what your Information Governance requires) means looking at a huge amount of summary statistics;
even if most of those are just ``count: 0``, this is still a burden and certainly a scary-looking wall of text!.

Still, we can see some nice, faithful data being reproduced.

If we have too many rows in any one partition, this could result in too much data in the source stats file, so the generator will not be proposed.
However, we have the "sampled and suppressed" generators that get around that problem.
These generators pull a sample of rows for each partition and operate on those.
They also suppress any groups with fewer than five members to improve anonymity.
Currently there are no "sampled but not suppressed" or "suppressed but not sampled" generators, though both would be useful.

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

   $ datafaker make-stats
   $ datafaker make-vocab --compress --no-force

The first of these generates a files ``src-stats.yaml`` containing summary statistics from the database that the generators need.
The second generates files ``tablename.yaml.gz`` containing data from the vocabulary tables. WARNING: this can take many hours depending on how big they are!
``--compress`` compresses the files with gzip, which might be necessary if the machine datafaker is running on risks running out of disk space.
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

   $ set DST_DSN=postgresql://someuser:somepassword@myserver.mydomain.com:5432/dst_db
   $ set DST_SCHEMA=myschema

Running from the ready-built Docker container, from within a directory holding only your ``.yaml`` and ``.yaml.gz`` files (please use WSL for this on Windows):

.. code-block:: console

   $ docker run --rm --user $(id -u):$(id -g) --network host -e DST_SCHEMA=myschema -e DST_DSN=postgresql://someuser:somepassword@myserver.mydomain.com:5432/dst_db -itv .:data --pull always timband/datafaker

(Windows users will need to modify this docker command, perhaps removing the `--user` option and its argument?)

Whichever we chose, now we can create the generators Python file and generate the data:

.. code-block:: console

   $ datafaker create-tables
   $ datafaker create-vocab
   $ datafaker create-data --num-passes 10

The first of these uses ``orm.yaml`` to create the destination database.
The second uses all the ``.yaml.gz`` (or ``.yaml``) files representing the vocabulary tables (this can take hours, too).
The third actually generates the data. ``--num-passes`` controls how many rows are generated.
At present the only ways to generate different numbers of rows for different tables is to configure ``num_rows_per_pass`` in ``config.yaml``:

.. code-block:: yaml

   observation:
      num_rows_per_pass: 50

This makes every call to ``create-data`` produce 50 rows in the ``observation`` table.
If you call ``create-data`` multiple times you get more data added to whatever already exists. Call ``remove-data`` to remove all rows from all non-vocabulary tables.

You can call ``remove-vocab`` to remove all rows from all vocabulary tables, and you can call ``remove-tables`` to empty the database completely.
