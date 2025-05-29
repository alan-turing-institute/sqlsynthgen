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

   $ pipx install git+https://github.com/tim-band/sqlsynthgen

Check that you can view the help message with:

.. code-block:: console

   $ datafaker --help

It can also be used directly within a Docker container by downloading image ``timband/datafaker``.
See the :ref:`quickstart guide <page-quickstart>` for more information.
