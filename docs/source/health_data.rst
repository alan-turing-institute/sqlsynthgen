.. _page-example-health-data:

Advanced Example: OMOP Health Data
==================================

The OMOP common data model (CDM) is a widely used format for storing health data.
Here we will show how datafaker can be configured to generate data for OMOP.

Before getting into the config itself, we need to discuss a few peculiarities of the OMOP CDM that need to be taken into account:

1. Some versions of OMOP contain a circular foreign key, for instance between the ``vocabulary``, ``concept``, and ``domain`` tables.
2. There are several standardized vocabulary tables (``concept``, ``concept_relationship``, etc).
   These should be marked as such using ``configure-tables``, and will be exported to ``.yaml`` files during the ``make-vocab`` step.
   However, some of these vocabulary tables may be too large to practically be writable to ``.yaml`` files, and will need to be dealt with manually.
   You should also check the license agreement of each standardized vocabulary before sharing any of the ``.yaml`` files.

Dealing with Circular Foreign Keys
++++++++++++++++++++++++++++++++++

Datafaker will warn if schemas have circular foreign keys as these can indicate an error in the schema
(by circular references we mean a table has a foreign key to itself, or to another table that has a foreign key back to the first table, or some longer loop).
Datafaker cannot cope with circular keys between generated tables because it needs to generate data in referenced tables before it can generate references to them,
and a circular dependency loop implies that there is no sensible order in which to do this.
However, datafaker can cope with circular foreign keys between vocabulary (or ignored) tables without user intervention.

Vocabulary Tables
+++++++++++++++++++++

The OMOP schema has many vocabulary tables.
Using the ``configure-tables`` command, tables can be marked as ``vocabulary`` tables;
this means that the destination table will not have any generators run,
but will instead get all its data copied directly from the source database's table via a ``yaml`` file.

This mechanism can cope with tables of tens or hundreds of thousands of rows without too much trouble.
Datafaker can be make to cope with even more by passing the ``--compress`` flag to the ``make-vocab`` command,
which will produce compressed ``.yaml.gz`` files instead of plain text ``.yaml`` files.
More stability might be achieved by limiting ``make-vocab`` to one table at a time via the ``--only TABLENAME`` option.
Even still, a ``make-vocab`` call can take many hours to complete.

If all of these mechanisms fail, the offending tables will need to be marked (using ``configure-tables``) as ``ignore``.
Such tables can be downloaded manually via the database's own software
(for example ``psql`` for Postgres), or you can just leave them ignored.
For example, if you do not have permission to publish the OMOP concepts table, you might have to leave it as ignored.
If you do leave a concept table ignored, the default generator for all foreign keys to it will be a plain integer genenerator, which is probably not what you want.
Instead, use one of the choice generators for such foreign keys. The null-partitioned grouped generators will also work.

The standard OMOP vocabulary tables are as follows:

===================== ================
Table name            Approximate size
===================== ================
concept               7 million rows
concept_ancestor      80 million rows
concept_class         small
concept_relationship  50 million rows
concept_synonym       2 million rows
domain                small
drug_strength         3 million rows
relationship          small
source_to_concept_map small
vocabulary            small
===================== ================

You might also want to treat other tables such as ``care_site``, ``cdm_source`` and ``location`` as vocabulary.

So, ``concept``, ``concept_ancestor``, ``concept_relationship``, ``concept_synonym`` and ``drug_strength``
might need to be ignored and dealt with manually depending on the source database's speed
and the capabilities of the machine running datafaker.

Entity Attribute Values
+++++++++++++++++++++++

We can get reasonable fidelity in most OMOP tables with simple generators that are suggested
by the ``propose`` command in ``configure-generators`` for each individual column.
However, there are two tables that are very different. These are ``observation`` and ``measurement``.

These tables have a ``concept_id`` column which indicates what some of the rest of the columns mean.
Depending on the value in the ``concept_id`` column, you will have a different pattern of nulls in the rest of the table,
a different spread of values for numeric colums, and a different set of choices for other columns.

For example, in the ``measurement`` table, if the ``measurement_concept_id`` is ``4152194``
(which references ``Systolic blood pressure`` in the ``concept`` table),
then ``unit_concept_id`` is ``8876`` (which references the unit of pressure ``mmHg``),
``unit_source_value`` is null and ``value_as_number`` has values somewhere near 160.
This means that measurements of the systolic blood pressure are numeric and around 160mmHg.
Other types of measurements will have their own ranges of values.

Having all different kinds of measurements in the one table presents a challenge for generating fake data.
Datafaker has "null-partitioned grouped" generators for this sort of difficulty.

Datafaker's null-partitioned grouped generators are multi-column genenerators.
The amount of data captured in the ``src-stats.yaml`` file for this sort of generator is, sadly, prodigious.
"Null-partitioning" refers to dividing the table into different "partitions" based on which columns are null,
then gathering data for each partition separately (and also gathering data on how big each partition is).
Each partition will have multiple results, one for each combination of values in the non-numeric columns.

Even if the source data has no examples of a row with a particular pattern of columns being null,
datafaler will still generate a query for that partition (which will return no results),
just in case a later run of ``make-stats`` does find some matching data.
Therefore the amount of queries appearing in ``src-stats.yaml`` grows exponentially with the number of columns being generated for.
For example, with eight columns generated, 257 query results will appear in ``src-stats.yaml``!
That's a doubling for every column added plus one for the sizes of the partitions.

Therefore, depending on the user's patience for generating such data and
your information governance process' capacity for looking through it,
restraint might be called for in merging too many columns!

But which columns should we merge for ``observation`` and ``measurement`` tables?

The essential columns for the ``observation`` table would seem to be: ``observation_concept_id``,
``value_as_number``, ``value_as_string``, ``value_as_concept_id``, and ``unit_concept_id``.
But note that these columns are essential to be in the generator output only if they need to be present in the destination database at all.
For example, if the destination database is producing the minimal data required to be processed by the
`UK HRA's Cohort Discovery Tool <https://ukhealthdata.org/wp-content/uploads/2024/11/2024-OHDSI-UK-Cohort-Discovery-Poster-v0.2-B-Kirby.pdf>`_,
then the ``unit_concept_id`` column will not be produced and so does not need to be in any generator.

Another group of columns (depending on what the source data looks like) that could be nice:
``observation_type_concept_id``, ``qualifier_concept_id``, ``observation_source_value``,
``observation_source_concept_id``, ``unit_source_value`` and ``qualifier_source_value``.
Check if the source database puts meaningful values into these columns before adding.

If your ``provider`` table is a vocabulary table and there are not too many different values,
``provider_id`` can be added usefully.

``observation_date`` and ``observation_datetime`` should not be added because these generators do not know how to handle dates,
and so will just treat them as a set of choices on the same level as ``observation_concept_id``.
This would destroy any correlation in the data, so please don't do it.
A later update of datafaker might allow dates and datetimes to be numeric values,
which would then piggyback onto the correlation and so keep it intact.

Similarly ``observation_id`` and ``visit_occurrence_id`` would destroy all correlation and ``person_id`` would destroy
all correlation except that of any single individual with multiple observations of the same type.
Do not add these.

The ``measurement`` table is very similar. Essential columns are ``measurement_concept_id``,
``value_as_number``, ``value_as_concept_id`` and ``unit_concept_id``
(with the same caveat as for the ``observation`` table). Useful columns are
``measurement_type_concept_id``, ``operator_concept_id``, ``range_low``, ``range_high``,
``measurement_source_value``, ``measurement_source_concept_id``, ``unit_source_value`` and ``value_source_value``.
``provider_id`` is possibly useful, but only if the ``provider`` table is a vocabulary table.
The following columns should not be added: ``measurement_id``, ``person_id``,
``measurement_date``, ``measurement_datetime`` and ``visit_occurrence_id``.

You currently have a choice of four null-partitioned generators.
You can have any combination of sampled and suppressed or not, and normal or lognormal.

Normal vs lognormal are simply different distributions. Different measurements will suit one distribution or the other.
Sadly we have to choose one or the other for the whole table.
Generally lognormal seems to work a little better; it will never produce negative values,
which is usually good but will sometimes be bad.

This will produce fairly faithful fake data.
What is completely lacking is correlations between the different rows in the table.
For instance, diastolic and systolic blood pressure readings are taken at times and have values that are independent of each other,
the patients are given random drugs at random times, uncorrelated with their diagnoses or any other aspect of their medical record, etcetera.
To go further we would have to write a :ref:`story generator <story-generators>` in Python which can carry information over from one line to others.
