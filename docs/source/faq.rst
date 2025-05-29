FAQ
===

Can datafaker work with two different schemas?
**********************************************

datafaker can only work with a single source schema and a single destination schema at a time.
However, you can choose for the destination schema to have a different name to the source schema by setting the ``DST_SCHEMA`` environment variable.

Which DBMSs does datafaker support?
***********************************

* datafaker most fully supports **PostgresSQL**, which it uses for its end-to-end functional tests.
* datafaker also supports **MariaDB**, as long as you don't set ``use-asyncio: true`` in your config.
* datafaker *might* work with **SQLite** but this is largely untested.
* datafaker may also work with SQL Server.
  To connect to SQL Server, you will need to install `pyodbc <https://pypi.org/project/pyodbc/>`_ and an `ODBC driver <https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16>`_, after which you should be able to use a DSN setting similar to ``SRC_DSN="mssql+pyodbc://username:password@hostname/dbname?driver=ODBC Driver 18 for SQL Server"``.

Please open a GitHub issue if you would like to see support for another DBMS.
