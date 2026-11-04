Including your own generators
=============================

There are two types of generators; row generators (possibly a misnomer) and story generators.
While there are many row generators included with datafaker (such as ``dist_gen.normal`` or ``generic.text.first_name``),
there are no story generators.

However, you can make your own!

Row generators
^^^^^^^^^^^^^^

The `configure-generators` command allows you to set the included row generators for single columns.
It is possible to have row generators that set more than one column at a time, but none of the built-in generators do this;
to add your own Python functions that write any number of values at once, put them into a file and copy it to the directory you are running datafaker from.
Now set the name of this file (minus the ``.py``) as the ``row_generators_module`` in your ``config.yaml``, for example:

.. code-block:: yaml

   row_generators_module: my_row_gens
   tables:
   person:
      row_generators:
         - name: my_row_gens.generate_ifespan
           kwargs:
             current_year: 2025
           columns_assigned: birthdate, deathdate

This will call the function ``lifespan`` in your ``my_row_gens.py`` file with argument ``current_year=2025``;
it must produce a tuple of two values, which will be put into the ``birthdate`` and ``deathdate`` columns of the ``person`` table.

You might prefer to instantiate one or more classes at the start and call methods on them for each value or tuple of values generated.
This is achieved with ``object_instantiation`` in the following way:

.. code-block:: yaml

   row_generators_module: my_row_gens
   object_instantiation:
   uct:
      class: my_row_gens.Lifespan
      kwargs:
         current_year: 2025
   tables:
   person:
      row_generators:
         - name: uct.generate_ifespan
           columns_assigned: birthdate, deathdate

Here we are instantiating a ``Lifespan`` class defined in your ``my_row_gens.py`` file.
We are instantiating this class as an object called ``uct`` which is then referenced in the method call ``uct.generate_lifespan``.
We have defined this class to accept the ``current_year`` parameter in the constructor of the ``Lifespan`` class so it doesn't have to be passed for each ``generate_lifespan`` call;
we can see how the ``config.yaml`` file has changed to pass this value into the constructor instead of the method.
You don't need to do it this way, it is fine to pass extra parameters to methods of classes if you want.

Story generators
^^^^^^^^^^^^^^^^

Story generators allow you to generate lines for any table in any order and create links between them.
Again, you must define your own; ``datafaker`` provides no built-in story generators.

You can put your story generators in their own Python file, or you can re-use your row generators file if you like.

A story generator is a Python Generator function (a function that calls ``yield`` to return multiple values rather than ``return`` a single one).
