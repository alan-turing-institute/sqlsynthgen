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
   * - Generator (function)
     - A callable, referenced by name in ``config.yaml`` (e.g. ``dist_gen.normal``), that produces one random value (or tuple of values) per call. See :doc:`builtin_generators` for the ones built into `datafaker`, or :doc:`custom_generators` to write your own.
   * - Proposer
     - An internal `datafaker` object, used by the ``propose`` command of ``configure-generators``, that suggests a generator function and arguments for a column (or group of columns) and estimates how well it fits the real data. See :doc:`builtin_generators`.
   * - Role
     - A tag (``start`` or ``source``) set on a column, via the ``role`` command in ``configure-generators`` or by hand in ``config.yaml``, that some proposers use to find related columns -- for example, the ``start`` role marks an anchor date that other dates in the same table can be generated as an offset from. See :doc:`builtin_generators`.
   * - Destination database and destination schema
     - A database and a schema within that database where `datafaker` creates the synthetic data tables and inserts the synthetic data it generates.
   * - Source database and source schema
     - A database and a schema within that database that `datafaker` will create a copy of and mimic when creating synthetic data.
   * - Vocabulary Table
     - A table that can be copied in its entirety to the destination database.
