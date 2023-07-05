Tutorial: Loan Data
===================

There are many potential applications of synthetic data in banking and finance where the nature of the data, being both personally and commercially sensitive, may rule out sharing real, identifiable data.

Here, we show how to use SqlSynthGen to generate a simple (uniformly random) synthetic version of the freely-available `PKDD'99 <https://relational.fit.cvut.cz/dataset/Financial>`_ dataset.
This dataset contains 606 successful and 76 not successful loan applications.

The PKDD'99 dataset is stored on a MariaDB database, which means that we need a local MariaDB database to store the synthetic data.
MariaDB installation instructions can be found `here <https://mariadb.org/download/?t=mariadb&p=mariadb&r=11.2.0#entry-header>`_.
We presume that you have a local server running on port 3306, with a user called ``myuser``, a password ``mypassword`` and a database called ``financial``.

.. code-block:: console

    $ mysql
    MariaDB > create user 'myuser'@'localhost' identified by 'mypassword';
    MariaDB > create database financial;
    MariaDB > grant all privileges on financial.* to 'myuser'@'localhost';
    MariaDB > \q

After :ref:`installing SqlSynthGen <enduser>`, we create a `.env` file to set some environment variables to define the source database as the one linked at the bottom of the PKDD'99 page, and the destination database as the local one:

**.env**

.. code-block:: console

    SRC_DSN="mariadb+pymysql://guest:relational@relational.fit.cvut.cz:3306/Financial_ijs"
    DST_DSN="mariadb+pymysql://myuser:mypassword@localhost:3306/financial"

We run SqlSynthGen's ``make-tables`` command to create a file called ``orm.py`` that contains the schema of the source database.

.. code-block:: console

    $ sqlsynthgen make-tables

Inspecting the ``orm.py`` file, we see that the ``tkeys`` table has column called ``goodClient``, which is a ``TINYINT``.
SqlSynthGen doesn't know what to do with ``TINYINT`` columns, so we need to create a config file to tell it how to handle them. This isn't necessary for normal ``Integer`` columns.

**config.yaml**

.. literalinclude:: ../../../tests/examples/loans/config.yaml
   :language: yaml

We run SqlSynthGen's ``make-generators`` command to create ``ssg.py``, which contains a generator class for each table in the source database:

.. code-block:: console

    $ sqlsynthgen make-generators --config config.yaml

We then run SqlSynthGen's ``create-tables`` command to create the tables in the destination database:

.. code-block:: console

    $ sqlsynthgen create-tables

Note that, alternatively, you could use another tool, such as ``mysqldump`` to create the tables in the destination database.

Finally, we run SqlSynthGen's ``create-data`` command to populate the tables with synthetic data:

.. code-block:: console

    $ sqlsynthgen create-data --num-passes 100

This will make 100 rows in each of the nine tables.
The data will be entirely random so you may wish to fine tune it using the source-statistics, custom generators or "story generators" explained in the longer :ref:`introduction <introduction>`.
