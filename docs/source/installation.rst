.. _page-installation:

Installation
============

To use datafaker, first install it.

Make sure you have pipx installed. To do this on Windows:

.. code-block:: console

   $ python -m pip install pipx
   $ python -m pipx ensurepath

Windows users should also install `pyreadline3` so that tab completion works in the interactive commands:

.. code-block:: console

   $ python -m pip install pyreadline3

Then close your command shell and open another. Now you can use ``pipx``.

.. code-block:: console

   $ pipx install git+https://github.com/safehr-data/datafaker

Check that you can view the help message with:

.. code-block:: console

   $ datafaker --help

If you need to use MS SQL (such as SQL Server) you need to install and register an ODBC driver.

Install and register the ODBC driver
------------------------------------

Mac OS
^^^^^^

If you do not already have the Microsoft ODBC driver installed:

.. code-block:: console

   $ brew tap microsoft/mssql-release
   $ brew install unixodbc msodbcsql18 mssql-tools18

Then verify the driver is registered:

.. code-block:: console

   $ odbcinst -q -d

If the output is empty, register it manually:

.. code-block:: console

   $ cat >> /opt/homebrew/etc/odbcinst.ini <<'EOF'
   [ODBC Driver 18 for SQL Server]
   Description=Microsoft ODBC Driver 18 for SQL Server
   Driver=/opt/homebrew/lib/libmsodbcsql.18.dylib
   UsageCount=1
   EOF

Ubuntu
^^^^^^

Install and check the MS SQL tools:

.. code-block:: console

   $ sudo apt install mssql-tools18
   $ odbcinst -q -d
   [ODBC Driver 18 for SQL Server]


Use in a docker container
-------------------------

It can also be used directly within a Docker container by downloading image ``timband/datafaker``.
See the :ref:`quickstart guide <page-quickstart>` for more information.
