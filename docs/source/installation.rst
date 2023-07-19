Installation
============

.. _enduser:

End User
--------

To use SqlSynthGen, first install it:

.. code-block:: console

   $ pip install sqlsynthgen


If Pip errors when installing PyYaml, you will need to manually specify the Cython version and manually install PyYaml (this is a temporary workaround for a PyYaml v5 conflict with Cython v3, see `here <https://github.com/yaml/pyyaml/issues/601>`_ for full details):

.. code-block:: console

    pip install "cython<3"
    pip install wheel
    pip install --no-build-isolation "pyyaml==5.4.1"
    pip install sqlsynthgen

Check that you can view the help message with:

.. code-block:: console

   $ sqlsynthgen --help
