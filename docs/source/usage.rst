Usage
=====

Installation
------------

To use sqlsynthgen, first install it using poetry:

.. code-block:: console

   (<your_poetry_shell>) $ poetry add sqlsynthgen

Test print integer function
---------------------------

To test print integer function,
you can use the ``sqlsynthgen.main.print_int`` function:

.. autofunction:: sqlsynthgen.main.print_int

The ``phone`` argument should be of type integer. Otherwise, :py:func:`sqlsynthgen.main.print_int`
will raise an exception.

.. py:exception:: TypeError

   Raised if the ``phone`` argument is invalid.
