Configuration reference
=======================

SqlSynthGen is configured using a YAML file, which is passed to several commands with the ``--config`` option.
Throughout the docs, we will refer to this file as ``config.yaml`` but it can be called anything (the exception being that there will be a naming conflict if you have a vocabulary table called ``config``).

Below, we see the schema for the configuration file.
Note that our config file format includes a section of SmartNoise SQL metadata, which is explained more fully `here <https://docs.smartnoise.org/sql/metadata.html#yaml-format>`_.

.. raw:: html
   :file: _static/config_schema.html
