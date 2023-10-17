.. _page-index:

sqlsynthgen's Documentation
---------------------------

**sqlsynthgen** is a package for making copies of relational databases and populating them with random data.

If you are new to sqlsynthgen (SSG for short), we recommend going through the pages of this documentation roughly in order:
After :ref:`installing <page-installation>` SSG and learning the basic commands that it uses from the :ref:`quick start guide <page-quickstart>`, the :ref:`introductory tutorial <page-introduction>` will walk you through a relatively simple use case in detail.
You can then look at one of our other two example use cases, one for :ref:`financial data <page-example-loan-data>` and one for :ref:`health data <page-example-health-data>`.
The latter also goes through some more advanced features of SSG and how to use them, that are relevant beyond health data use cases.

.. note::

   This project will be under active development from Jan - Oct 2023


.. note::

   We do not currently support tables without primary keys.
   If you have tables without primary keys, some sqlsynthgen functionality
   may work but vocabulary tables will not.

Contents:
---------

.. toctree::
   :glob:
   :maxdepth: 2

   installation
   quickstart
   introduction
   loan_data
   health_data
   configuration
   api
   faq
   glossary


Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
