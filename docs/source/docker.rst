Using Docker
============

datafaker can be run in a docker container. You can build it locally or run it directly from Docker Hub.

Building Docker locally
-----------------------

This will build a Docker image locally called ``datafaker``:

.. code-block:: shell

   docker build -t datafaker .

Running datafaker in Docker
-----------------------------

Let us run the image built above in a way that can access a source
database on the local machine (with DSN
``postgresql://tim:tim@localhost:5432/pagila`` and schema ``public``),
and stores the files produced in a directory called ``output``:

.. code-block:: shell

   mkdir output
   docker run --rm --user $(id -u):$(id -g) --network host -e SRC_SCHEMA=public -e SRC_DSN=postgresql://tim:tim@localhost:5432/pagila -itv ./output:data datafaker

You do need to create the output folder first.

You don't need ``--network host`` if the source database is not on the local
computer.

Running the image in this way will give you a command prompt from which
datafaker can be called. Tab completion can be used. For example, if
you type ``sq<TAB> ma<TAB>t<TAB>`` you will see
``datafaker make-tables``; although you might have to wait a second
or two after some of the ``<TAB>`` key presses for the completed text
to appear. Tab completion can also be used for command options such
as ``--force``. Press ``<TAB>`` twice to see a list of possible completions.
