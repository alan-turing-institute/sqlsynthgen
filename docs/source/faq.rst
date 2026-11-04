FAQ
===

Can DataFaker work with two different schemas?
**********************************************

DataFaker can only work with a single source schema or a single destination schema at a time.
It is possible to use the same database for source and destination by choosing a different schema for source and destination,
for example by setting ``DST_SCHEMA=fake``.

Which DBMSs does datafaker support?
***********************************

PostgreSQL
^^^^^^^^^^

Fully supported.

Microsoft SQL Server
^^^^^^^^^^^^^^^^^^^^

Supported. Microsoft Transact SQL probably works as well.
You need to install an  `ODBC driver <https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16>`_.

The DSN format is similar to:

``mssql+pyodbc://<username>:<password>@127.0.0.1:1433/<database>?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes``

DuckDB
^^^^^^

Supported.
If you want to produce Parquet or CSV files as your output format, the easiest way is via a DuckDB database.
You do not need to install DuckDB to do this, simply use a destination DSN like ``duckdb:///./fake.db``,
then use ``datafaker dump-data --output my-csv-folder`` or ``datafaker dump-data --parquet --output my-parquet-folder``.

An in-memory DuckDB database can be used as an empty database that you can augment with parquet files:

``datafaker make-tables --parquet-dir my-parquet-input-folder``

Snowflake
^^^^^^^^^

Currently experimental support. If the input database includes the type ``TIMEZONE_LTZ`` (or equivalent)
then the output database must also be a Snowflake database.

Other
^^^^^

In principle DataFaker can support any database that SqlAlchemy supports.
However, each new database supported so far has required significant effort,
so no guarantees can be made for, for example, MySQL or MariaDB.
