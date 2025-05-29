Glossary
========

.. list-table::
   :widths: 25 25
   :header-rows: 1

   * - Term
     - Definition
   * - Row generator
     - A user-defined Python function which will provide one or more random column values for a single table when called.
   * - Story generator
     - A user-defined Python generator function that ``yields`` rows, possibly multiple rows for multiple tables.
   * - Destination database and destination schema
     - A database and a schema within that database where `datafaker` creates the synthetic data tables and inserts the synthetic data it generates.
   * - Source database and source schema
     - A database and a schema within that database that `datafaker` will create a copy of and mimic when creating synthetic data.
   * - Vocabulary Table
     - A table that can be copied in its entirety to the destination database.
