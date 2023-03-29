FAQ
===

How can you choose a different destination schema name
******************************************************

If you want the destination schema to have a different name to the source schema, you will need to open the SQLAlchemy ORM file you created with the `make-tables` command and replace all instances of

.. code-block:: python

    __table_args__ = {"schema": "source-schema-name"}

with

.. code-block:: python

    __table_args__ = {"schema": "destination-schema-name"}
