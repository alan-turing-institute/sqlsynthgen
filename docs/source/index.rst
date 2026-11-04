.. _page-index:

Datafaker
---------

**Datafaker** is a package for generating synthetic versions of relational and tabular datasets.
It can work with relational databases as well as Parquet files through DuckDB integration.

If you are new to Datafaker, we recommend following the documentation in order: start with the :ref:`Installation <page-installation>` guide and learn the basic commands that Datafaker uses from the :ref:`Command-Line Interface (CLI) Guide <page-quickstart>`.
Then, work through the :ref:`Tutorials <page-introduction>` before exploring the example use cases and reference documentation.


.. note::

   New features are regularly added. See the GitHub repository for the latest updates.

Contents:
---------

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   Overview <overview>
   installation
   docker
   Command-Line Interface (CLI) Guide <quickstart>
   Tutorial: Generate Synthetic Data from PostgreSQL <introduction>
   Tutorial: Generate Synthetic Data from CSV and Parquet <tutorial_parquet>

.. toctree::
   :glob:
   :maxdepth: 1
   :caption: Advanced

   orm
   configuration
   health_data
   Choosing a Generator <choosing_a_generator>
   Custom Generators <custom_generators>
   Built-in Generators and Proposers <builtin_generators>
   api

.. toctree::
   :glob:
   :maxdepth: 1
   :caption: Reference

   faq
   glossary


Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
