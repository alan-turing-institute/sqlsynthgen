.. _page-example-loan-data:

Example: Loan Data
===================

Intro
+++++

There are many potential applications of synthetic data in banking and finance where the nature of the data, being both personally and commercially sensitive, may rule out sharing real, identifiable data.

Here, we show how to use SqlSynthGen to generate a simple (uniformly random) synthetic version of the freely-available `PKDD'99 <https://relational.fit.cvut.cz/dataset/Financial>`_ dataset.
This dataset contains 606 successful and 76 not successful loan applications.

Setup
+++++

The PKDD'99 dataset is stored on a MariaDB database, which means that we need a local MariaDB database to store the synthetic data.
MariaDB installation instructions can be found `here <https://mariadb.org/download/?t=mariadb&p=mariadb&r=11.2.0#entry-header>`__.
We presume that you have a local server running on port 3306, with a user called ``myuser``, a password ``mypassword`` and a database called ``financial``.

.. code-block:: console

    $ mysql
    MariaDB > create user 'myuser'@'localhost' identified by 'mypassword';
    MariaDB > create database financial;
    MariaDB > grant all privileges on financial.* to 'myuser'@'localhost';
    MariaDB > \q

After :ref:`installing SqlSynthGen <page-installation>`, we create a `.env` file to set some environment variables to define the source database as the one linked at the bottom of the PKDD'99 page, and the destination database as the local one:

**.env**

.. code-block:: console

    SRC_DSN="mariadb+pymysql://guest:relational@relational.fit.cvut.cz:3306/Financial_ijs"
    DST_DSN="mariadb+pymysql://myuser:mypassword@localhost:3306/financial"

Generating Data Uniformly at Random
+++++++++++++++++++++++++++++++++++

We run SqlSynthGen's ``make-tables`` command to create a file called ``orm.py`` that contains the schema of the source database.

.. code-block:: console

    $ sqlsynthgen make-tables

Inspecting the ``orm.py`` file, we see that the ``tkeys`` table has column called ``goodClient``, which is a ``TINYINT``.
SqlSynthGen doesn't know what to do with ``TINYINT`` columns, so we need to create a config file to tell it how to handle them.
This isn't necessary for normal ``Integer`` columns.

Looking at the ``goodClient`` values:

.. list-table:: tkeys
   :header-rows: 1

   * - id
     - goodClient
   * - 0
     - 1
   * - 1
     - 0
   * - 2
     - 0
   * - 3
     - 0

we see that they are always 0 or 1 so we will pick randomly from 0 and 1 for our synthetic value:

**config.yaml**

.. literalinclude:: ../../examples/loans/config1.yaml
   :language: yaml

We run SqlSynthGen's ``create-generators`` command to create ``datafaker.py``, which contains a generator class for each table in the source database:

.. code-block:: console

    $ sqlsynthgen create-generators --config config.yaml

We then run SqlSynthGen's ``create-tables`` command to create the tables in the destination database:

.. code-block:: console

    $ sqlsynthgen create-tables

Note that, alternatively, you could use another tool, such as ``mysqldump`` to create the tables in the destination database.

Finally, we run SqlSynthGen's ``create-data`` command to populate the tables with synthetic data:

.. code-block:: console

    $ sqlsynthgen create-data --num-passes 100

This will make 100 rows in each of the nine tables with uniformly random data.

Improving Data Generation with Vocabularies
+++++++++++++++++++++++++++++++++++++++++++

We can do better than generating data randomly and uniformly, however.
We notice that the ``districts`` table doesn't contain any sensitive data so we choose to copy it whole to the destination database:

**config.yaml**

.. literalinclude:: ../../examples/loans/config2.yaml
   :language: yaml

We can export the vocabularies to ``.yaml`` files, delete the old synthetic data, import the vocabularies and create new synthetic data with:

.. code-block:: console

    $ sqlsynthgen create-generators
    $ sqlsynthgen remove-data
    $ sqlsynthgen create-vocab
    $ sqlsynthgen create-data --num-passes 100

This will give us an exact copy of the ``districts`` table:

.. list-table:: districts
   :header-rows: 1

   * - id
     - A2
     - A3
     - A4
     - A5
   * - 1
     - Hl.m. Praha
     - Prague
     - 1204953
     - 0
   * - 2
     - Benesov
     - central Bohemia
     - 88884
     - 80
   * - 3
     - Beroun
     - central Bohemia
     - 75232
     - 55

Adding a Foreign Key
++++++++++++++++++++

We notice that the source database does not have a foreign key constraint between the ``clients.tkey_id`` column and the ``tkeys.id`` column, even though it looks like there ought to be one.

We add it manually to the orm.py file:

**orm.py**:

.. code-block:: python3
   :linenos:

   class Clients(Base):
       __tablename__ = 'clients'
       __table_args__ = (
           ForeignKeyConstraint(['district_id'], ['districts.id'], ondelete='CASCADE', onupdate='CASCADE', name='clients_ibfk_1'),
           # Added manually
           ForeignKeyConstraint(['tkey_id'], ['tkeys.id'], ondelete='CASCADE', onupdate='CASCADE', name='clients_tkey_id'),
       )
       ...

We'll need to recreate the ``datafaker.py`` file, the destination database and the data:

.. code-block:: console

    $ sqlsynthgen create-generators --config-file config.yaml --force
    $ sqlsynthgen remove-tables --yes
    $ sqlsynthgen create-tables
    $ sqlsynthgen create-vocab
    $ sqlsynthgen create-data --num-passes 100

We now have a FK relationship and all synthetic values of ``clients.tkey_id`` exist in the synthetic ``tkeys.id`` column.

Marginal Distributions with Differential Privacy
++++++++++++++++++++++++++++++++++++++++++++++++

For many of the remaining categorical columns, such as ``cards.type``, we may decide that we want to use the real values in the right proportions.
Here is a sample of the ``cards`` table:

.. list-table:: cards
   :header-rows: 1

   * - id
     - disp_id
     - type
     - issued
   * - 1
     - 9
     - gold
     - 1998-10-16
   * - 2
     - 19
     - classic
     - 1998-03-13
   * - 3
     - 41
     - gold
     - 1995-09-03

We can take the real values in the right proportions, and even add noise to make them differentially private by using the :ref:`source-statistics <source_statistics>` and SmartNoise SQL features:

**config.yaml**

.. literalinclude:: ../../examples/loans/config3.yaml
   :language: yaml

We define a custom row-generator to use the source statistics and Python's ``random.choices()`` function to choose a value:

**my_row_generators.py**

.. literalinclude:: ../../examples/loans/my_row_generators.py
   :language: python

As before, we will need to re-create ``datafaker.py`` and the data.

.. code-block:: console

    $ sqlsynthgen create-generators --config-file config.yaml --force
    $ sqlsynthgen make-stats --config-file config.yaml --force
    $ sqlsynthgen remove-data --yes
    $ sqlsynthgen create-vocab
    $ sqlsynthgen create-data --num-passes 100

For further refinement, you can use "story generators" to create inter-table correlations so that, for example, the number of loan applications depends on the number of cards they have or the average amount of a bank transfer depends on the home city of a client.
See the :ref:`introduction <page-introduction>` for more.
