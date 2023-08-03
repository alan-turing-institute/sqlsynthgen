FAQ
===

Can SqlSynthGen work with two different schemas?
************************************************

SqlSynthGen can only work with a single source schema and a single destination schema at a time.
However, you can choose for the destination schema to have a different name to the source schema by setting the DST_SCHEMA environment variable.

Which DBMSs does SqlSynthGen support?
*************************************

* SqlSynthGen most fully supports **PostgresSQL**, which it uses for its end-to-end functional tests.
* SqlSynthGen also supports **MariaDB**, as long as you don't set ``use-asyncio: true`` in your config.
* SqlSynthGen *might*, work with **SQLite** but this is largely untested.

Please open a GitHub issue if you would like to see support for another DBMS.
