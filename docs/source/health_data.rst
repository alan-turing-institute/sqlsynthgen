.. _page-example-health-data:

Advanced Example: OMOP Health Data
==================================

The OMOP common data model (CDM) is a widely used format for storing health data.
Here we will show how `SqlSynthGen` (SSG) can be configured to generate data for OMOP.
We also use this as an opportunity to demonstrate some typical patterns in advanced usage of SSG, beyond what is covered in the :ref:`introduction <page-introduction>`, which we encourage the reader to go through first.

There are multiple versions of the OMOP CDM and variations between implementations as well (at schema and data levels).
SSG allows you to generate data irrespective of your schema peculiarities, but the example configuration we demonstrate here was originally built for the `CCHIC dataset <https://pubmed.ncbi.nlm.nih.gov/29500026/>`_ for critical care data.
The full configuration we wrote for the CCHIC data set is available `here <https://github.com/alan-turing-institute/sqlsynthgen/blob/main/examples/cchic_omop/>`__, in this tutorial page we will only discuss some aspects of it.

Before getting into the config itself, we need to discuss a few peculiarities of the OMOP CDM that need to be taken into account:

1. Some versions of OMOP contain a circular foreign key, for instance between the ``vocabulary``, ``concept``, and ``domain`` tables.
2. There are several standardized vocabulary tables (``concept``, ``concept_relationship``, etc).
   These should be marked as such in the sqlsynthgen config file.
   The tables will be exported to ``.yaml`` files during the ``create-generators`` step.
   However, some of these vocabulary tables may be too large to practically be writable to ``.yaml`` files, and will need to be dealt with manually.
   You should also check the license agreement of each standardized vocabulary before sharing any of the ``.yaml`` files.

Dealing with Circular Foreign Keys
++++++++++++++++++++++++++++++++++

SSG is currently unable to handle schemas with circular foreign keys properly.
By this we mean situations where foreign key references form a loop, where for instance table `A` references table `B`, which references table `C`, which in turn references table `A` again.
This is because SSG doesn't know in which order data for these tables should be generated.
If the tables that have circular foreign keys are vocabulary tables, as they are in the case of OMOP, this can easily be circumvented.

To deal with circular foreign keys in vocabulary tables, one would first run ``sqlsynthgen make-tables`` and ``sqlsynthgen create-tables`` (with the appropriate arguments for ``--config-file`` etc.) to create the destination schema, and then disable one of the problematic foreign key constraints temporarily.
For instance, to remove the circular relation between ``concept``, ``vocabulary``, and ``domain`` one can run:

.. code-block:: sql

  ALTER TABLE concept DROP CONSTRAINT concept.concept_vocabulary_id_fkey

and between ``concept`` and ``domain`` with, for example:

.. code-block:: sql

  ALTER TABLE concept DROP CONSTRAINT concept.concept_domain_id_fkey

One can then proceed with ``sqlsynthgen create-vocab`` to copy over the vocabulary tables, and restore the constraints with

.. code-block:: sql

  ALTER TABLE concept ADD CONSTRAINT concept_vocabulary_id_fkey FOREIGN KEY (vocabulary_id) REFERENCES vocabulary(vocabulary_id);
  ALTER TABLE concept ADD CONSTRAINT concept_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES domain(domain_id);

If the problematic foreign key constraints would be between non-vocabulary tables, one would need to keep them disabled for the whole duration of creating synthetic data, while putting in a manual mechanism that guarantees that the synthetic data created does respect the constraint, and then reenable the constraint at the end.
Fortunately with OMOP this is not necessary.

Vocabulary Tables
+++++++++++++++++++++

The OMOP schema has many vocabulary tables. Here's an excerpt from the CCHIC OMOP config file we've written:

.. code-block:: yaml

  tables:
    concept:
      # This one is a vocab, but its too big to handle the usual way
      ignore: true
      # vocabulary_table: true
    concept_ancestor:
      # This one is a vocab, but its too big to handle the usual way
      ignore: true
      # vocabulary_table: true
    vocabulary:
      vocabulary_table: true
    domain:
      vocabulary_table: true
    concept_class:
      vocabulary_table: true
    concept_synonym:
      # This one is a vocab, but its too big to handle the usual way
      ignore: true
      # vocabulary_table: true
    concept_relationship:
      # This one is a vocab, but its too big to handle the usual way
      ignore: true
      # vocabulary_table: true
    drug_strength:
      # This one is a vocab, but its too big to handle the usual way
      ignore: true
      # vocabulary_table: true
    relationship:
      vocabulary_table: true
    source_to_concept_map:
      vocabulary_table: true
    location:
      vocabulary_table: true
    care_site:
      vocabulary_table: true
    provider:
      vocabulary_table: true
    cdm_source:
      vocabulary_table: true

All the above are vocabulary tables.
However, as you can see, we have rather marked some of them with ``ignore: true``.
This is because they are too big to handle the usual way.

The usual way is to run

.. code-block:: shell

  sqlsynthgen create-generators --config-file=config.yaml
  sqlsynthgen create-vocab --config-file=config.yaml

``create-generators`` downloads all the vocabulary tables to your local machine as YAML files and ``create-vocab`` uploads them to the target database.
In the CCHIC dataset we were looking at some of the vocabulary tables were several gigabytes, and downloading those as YAML files was a bad idea.
Thus we rather set SSG to ignore those tables and copied them over from the source schema to the destination schema manually, which was easier to do (in our case the source and the destination were just different schemas within the same database).

The ``ignore: true`` option can also be used to make SSG ignore tables that we are not interested in at all.
Note though that if one of the ignored tables is foreign key referenced by one of the tables we are `not` ignoring, the ignored table is still included in the ``orm.py`` and created by ``create-tables``, although ignored by ``create-generators`` and ``create-data``.
This is necessary to not break the network of foreign key relations.
It is also good, because it means that after we copy the big vocabulary tables over manually, all foreign key references and things like automatically generating default values for referencing columns work as usual.

Configuration for OMOP
++++++++++++++++++++++

With the above speed bumps cleared we can focus on the usual work of using SSG:
writing generators and source statistics queries to increase fidelity of the synthetic data.
The complete CCHIC config we've written is available `here <https://github.com/alan-turing-institute/sqlsynthgen/blob/main/examples/cchic_omop/>`__.
It consists of a ``config.yaml``, ``row_generators.py``, and ``story_generators.py``.

The row generators do little, most of the work is in the one big story generator, ``patient_story``, and the ``src-stats`` queries that it uses.
``patient_story`` story creates a patient, a visit occurrence for that patient, and a number of observations, measurements, specimen samples, etc. that occur during the visit.

A word on the fidelity of the data created by the config.
Each row that holds a single measurement or observation or other event is quite realistic:
The config checks the typical values for various events and replicates those in the synthetic data, so that drugs are given in roughly the right doses and the route of administration is correct, blood pressure readings are in a realistic range and have the right units, and so forth.
What is completely lacking is correlations between the different events.
For instance, diastolic and systolic blood pressure readings are taken at times and have values that are independent of each other, the patients are given random drugs at random times, uncorrelated with their diagnoses or any other aspect of their medical record, etc.
This is the level of fidelity we found to be the best balance between the needs of our use case, the effort of implementing the generators, and the privacy guarantees our src-stats queries have.
All the source stats queries use differential privacy.

There are some aspects of the configuration that are bespoke to how the CCHIC data set uses the OMOP CDM.
For instance, some columns that are all ``null`` in the CCHIC data are made ``null`` here, and some tables that were empty are left empty in the synthetic data as well.
One thus shouldn't take this as a generic OMOP SSG configuration.
It is, however, an excellent starting point to develop other OMOP configs for particular datasets.

The configuration is extensive because of the many tables involved:
`config.yaml <https://github.com/alan-turing-institute/sqlsynthgen/blob/main/examples/cchic_omop/>`_ is around 2,700 lines long, although the structure of it is quite repetitive which bloats the size significantly.
We will not go through it in any detail but will rather go over only a few basic aspects to illustrate.
You are welcome to browse the full config for more examples and inspiration.

Here is our config for the person table:

.. code-block:: yaml

  row_generators_module: row_generators
  tables:
    person:
      num_rows_per_pass: 0
      row_generators:
        - name: row_generators.birth_datetime
          args: [generic, SRC_STATS]
          columns_assigned:
            [
              "year_of_birth",
              "month_of_birth",
              "day_of_birth",
              "birth_datetime",
            ]

        - name: row_generators.gender
          args: [generic, SRC_STATS]
          columns_assigned:
            [
              "gender_concept_id",
              "gender_source_value",
              "gender_source_concept_id",
            ]

        - name: row_generators.ethnicity_race
          args: [generic, SRC_STATS]
          columns_assigned:
            [
              "race_concept_id",
              "race_source_value",
              "race_source_concept_id",
              "ethnicity_concept_id",
              "ethnicity_source_value",
              "ethnicity_source_concept_id",
            ]

        - name: generic.null_provider.null
          columns_assigned: person_source_value
        - name: generic.null_provider.null
          columns_assigned: provider_id
        - name: generic.null_provider.null
          columns_assigned: care_site_id

``num_rows_per_pass`` is set to 0, because all rows are generated by the story generator.
Let's use the gender columns as an example.
Here is the relevant function from ``row_generators.py``.

.. code-block:: python

  def gender(generic: Generic, src_stats: SrcStats) -> GenderRows:
      """Generate values for the four gender columns of the OMOP schema.

      Samples from the src_stats["count_gender"] result.
      """
      return cast(
          GenderRows,
          generic.sql_group_by_provider.sample(
              src_stats["count_gender"],
              "num",
              value_columns=[
                  "gender_concept_id",
                  "gender_source_value",
                  "gender_source_concept_id",
              ],
          ),
      )

Clearly this just off-loads all the work onto ``generic.sql_group_by_provider.sample``, which is a built-in provided by SSG.
Let's take a look at its source code from ``providers.py``:

.. code-block:: python

  def sample(
      self,
      group_by_result: list[dict[str, Any]],
      weights_column: str,
      value_columns: Optional[Union[str, list[str]]] = None,
      filter_dict: Optional[dict[str, Any]] = None,
  ) -> Union[Any, dict[str, Any], tuple[Any, ...]]:
      """Random sample a row from the result of a SQL `GROUP BY` query.

      The result of the query is assumed to be in the format that sqlsynthgen's
      make-stats outputs.

      For example, if one executes the following src-stats query
      ```
      SELECT COUNT(*) AS num, nationality, gender, age
      FROM person
      GROUP BY nationality, gender, age
      ```
      and calls it the `count_demographics` query, one can then use
      ```
      generic.sql_group_by_provider.sample(
          SRC_STATS["count_demographics"],
          weights_column="num",
          value_columns=["gender", "nationality"],
          filter_dict={"age": 23},
      )
      ```
      to restrict the results of the query to only people aged 23, and random sample a
      pair of `gender` and `nationality` values (returned as a tuple in that order),
      with the weights of the sampling given by the counts `num`.

      Arguments:
          group_by_result: Result of the query. A list of rows, with each row being a
              dictionary with names of columns as keys.
          weights_column: Name of the column which holds the weights based on which to
              sample. Typically the result of a `COUNT(*)`.
          value_columns: Name(s) of the column(s) to include in the result. Either a
              string for a single column, an iterable of strings for multiple
              columns, or `None` for all columns (default).
          filter_dict: Dictionary of `{name_of_column: value_it_must_have}`, to
              restrict the sampling to a subset of `group_by_result`. Optional.

      Returns:
          * a single value if `value_columns` is a single column name,
          * a tuple of values in the same order as `value_columns` if `value_columns`
            is an iterable of strings.
          * a dictionary of {name_of_column: value} if `value_columns` is `None`
      """
      if filter_dict is not None:

          def filter_func(row: dict) -> bool:
              for key, value in filter_dict.items():
                  if row[key] != value:
                      return False
              return True

          group_by_result = [row for row in group_by_result if filter_func(row)]
          if not group_by_result:
              raise ValueError("No group_by_result left after filter")

      weights = [cast(int, row[weights_column]) for row in group_by_result]
      weights = [w if w >= 0 else 1 for w in weights]
      random_choice = random.choices(group_by_result, weights)[0]
      if isinstance(value_columns, str):
          return random_choice[value_columns]
      if value_columns is not None:
          values = tuple(random_choice[col] for col in value_columns)
          return values
      return random_choice

The docstring explains the function quite well, but to reiterate:
Its purpose is to take the output of a src-stats query that does a ``GROUP BY`` by some column(s) and a ``COUNT``, and sample a row from the results, with the sampling weights given by the counts.
In our case we've done a ``GROUP BY`` over the three columns relating to gender, and thus are sampling from the same distribution of genders as in the source data, when creating our synthetic data.
Note that this would also automatically replicate features such as ``NULL`` values or mismatches between the three gender columns, if they exist in the source data.
The relevant source stats query is defined in this part of the config:

.. code-block:: yaml

  src-stats:
    - name: count_gender
      query: >
        SELECT person_id, gender_concept_id, gender_source_value, gender_source_concept_id
        FROM person
        LIMIT 100000

      dp-query: >
        SELECT COUNT(*) AS num, gender_concept_id, gender_source_value, gender_source_concept_id
        FROM query_result
        GROUP BY gender_concept_id, gender_source_value, gender_source_concept_id

      epsilon: 0.5
      delta: 0.000001
      snsql-metadata:
        max_ids: 1
        person_id:
          type: int
          private_id: true
        gender_concept_id:
          type: int
        gender_source_value:
          type: string
        gender_source_concept_id:
          type: int

Without differential privacy, this block would simply read

.. code-block:: yaml

  src-stats:
    - name: count_gender
      query: >
        SELECT COUNT(*) AS num, gender_concept_id, gender_source_value, gender_source_concept_id
        FROM person
        GROUP BY gender_concept_id, gender_source_value, gender_source_concept_id

With differential privacy, the query has to be done in two stages.
First, we simply get 100,000 rows from the person table.
These are downloaded to the local machine running SSG, hence the maximum limit on number of rows.
Then the second part, the ``dp-query``, is run on those rows, using the `smartnoise-sql <https://github.com/opendp/smartnoise-sdk/tree/main/sql>`_ package, which adds noise to the result of any query to guarantee differential privacy.
The ``epsilon`` and ``delta`` are given to smartnoise-sql (SNSQL from now on) to determine how much noise needs to be added (lower values mean more noise and stronger privacy guarantees) and the ``snsql-metadata`` block gives SNSQL information about the columns.
Notice that the 100,000 rows downloaded to the local machine need to include the ``person_id`` column, even though it is not used by the ``dp-query``.
This is because SNSQL needs to know which rows belong to the same person, to estimate how much noise needs to be added to protect the privacy of any one indvidual, and the ``private_id: true`` bit tells it that the ``person_id`` column holds that information.
In this case there is only one row per person, hence the ``max_ids: 1``, but in other queries this is not the case.

Using SNSQL and differential privacy can be tricky.
We encourage to read up on the basics of differential privacy to understand the ``epsilon`` and ``delta`` parameters, and the `SNSQL docs <https://docs.smartnoise.org/sql/index.html>`_ to understand the metadata needed.
SNSQL is quite limited in what kinds of queries it is able to execute, and thus in many cases the preceding ``query``, the ``query_result`` of which the ``dp-query`` runs on, needs to do some preprocessing.
You can find examples of this in the `full configuration <https://github.com/alan-turing-institute/sqlsynthgen/blob/main/examples/cchic_omop/>`_.

After creating a person, ``patient_story`` creates possibly an entry in the ``death`` table, and then one for ``visit_occurrence``.
The configurations and generators for these aren't very interesting, their main point is to make the chronology and time scales make sense, so that people born a long time ago are more likely to have died, and the order of birth, visit start, visit end, and possible death is correct.

After that the story generates a set of rows for tables like ``observation``, ``measurement``, ``condition_occurrence``, etc., the ones that involve procedures and events that took place during the hospital stay.
The procedure is very similar for each one of these, we'll discuss ``measurement`` as an example.

The first stop is the ``avg_measurements_per_hour`` src-stats query, which looks like this

.. code-block:: yaml

  - name: avg_measurements_per_hour
    query: >
      select num / (extract(epoch from los) / 3600) :: float as num_per_hour, person_id
      from (
        select
          count(*) as num,
          (vo.visit_end_datetime - vo.visit_start_datetime) as los,
          m.visit_occurrence_id,
          m.person_id
        from measurement m
        join visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
        group by m.person_id, m.visit_occurrence_id, los
        limit 100000
      ) sub

    dp-query: >
      select avg(num_per_hour) as avg_per_hour
      from query_result

    epsilon: 0.5
    delta: 0.000001
    snsql-metadata:
      max_ids: 1
      person_id:
        type: int
        private_id: true
      num_per_hour:
        type: float
        lower: 0
        upper: 100

Note how the ``query`` part, which is executed on the database server, tries to do as much of the work as possible:
It extracts the number of ``measurement`` entries, divided by the length of the hospital stay, for each person.
The ``dp-query`` then only computes the average.
This is both to circumvent the limitations of SNSQL, which can't for instance do subqueries or differences between columns, and also to minimise the data transferred to and work done on the local machine running SSG.

Based on that information, we generate a set of times, roughly at the right frequency, at which a ``measurement`` entry should generated for our synthetic patient.
The relevant `src-stats queries <https://github.com/alan-turing-institute/sqlsynthgen/blob/main/examples/cchic_omop/>`_ for this are

* ``count_measurements``, which counts the relative frequencies of various types of measurements, like blood pressure, pulse taking, different lab results, etc.
* ``measurement_categoricals``, which does a count query to understand typical values in a row, based on the measurement type. For instance, what is the right unit for each lab result type, do they come with lower and upper bounds, are values negative or positive, etc.
* ``avg_measurement_value_as_number``, which gets the average numerical value, if any, for each measurement type. We assume all values are normally distributed with a standard deviation that is the square root of the mean.

As an example, let's look at ``measurement_categoricals``.

.. code-block:: yaml

  - name: measurement_categoricals
    query: >
      with
        m as (
          select measurement.*
          from measurement as measurement
          join concept concept on concept.concept_id = measurement.measurement_concept_id
          where
            -- This is a manually curated list of measurements we are interested in.
            concept_name in (
              'Pulse rate',
              'Arterial oxygen saturation',
              'Respiratory rate',
              'Systolic blood pressure',
              'Diastolic blood pressure',
              'Urine output 1 hour',
              'Body temperature',
              'Inspired oxygen concentration',
              'SOFA (Sequential Organ Failure Assessment) score',
              'Oral fluid input',
              'Tidal volume',
              'Ventilator delivered minute volume',
              'End tidal carbon dioxide concentration',
              'Total breath rate',
              'Peak inspiratory pressure',
              'pH of Blood',
              'Carbon dioxide [Partial pressure] in Blood',
              'Oxygen [Partial pressure] in Blood]',
              'Base excess in Blood by calculation',
              'Chloride [Moles/volume] in Blood',
              'Leukocytes [#/volume] in Blood by Automated count'
          )
          limit 10000000
        )
      select
        count(*) as num,
        measurement_concept_id,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_concept_id,
        unit_concept_id,
        CASE
            WHEN value_as_number IS NULL THEN 'NULL'
            WHEN value_as_number < 0 THEN '<0'
            WHEN value_as_number >= 0 THEN '>=0'
        END AS value_as_number_sign,
        CASE
            WHEN range_low IS NULL THEN 'NULL'
            WHEN range_low < 0 THEN '<0'
            WHEN range_low >= 0 THEN '>=0'
        END AS range_low_sign,
        CASE
            WHEN range_high IS NULL THEN 'NULL'
            WHEN range_high < 0 THEN '<0'
            WHEN range_high >= 0 THEN '>=0'
        END AS range_high_sign,
        provider_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        person_id
      from m
      group by
        measurement_concept_id,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_concept_id,
        unit_concept_id,
        value_as_number_sign,
        range_low_sign,
        range_high_sign,
        provider_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        person_id
      limit 100000

    dp-query: >
      select
        sum(num) as num,
        measurement_concept_id,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_concept_id,
        unit_concept_id,
        value_as_number_sign,
        range_low_sign,
        range_high_sign,
        provider_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value
      from query_result
      group by
        measurement_concept_id,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_concept_id,
        unit_concept_id,
        value_as_number_sign,
        range_low_sign,
        range_high_sign,
        provider_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value

    epsilon: 1.0
    delta: 0.000001
    snsql-metadata:
      max_ids: 20
      person_id:
        type: int
        private_id: true
      num:
        type: int
        lower: 0
        upper: 200
      measurement_concept_id:
        type: int
      measurement_type_concept_id:
        type: int
      operator_concept_id:
        type: int
      value_as_concept_id:
        type: int
      unit_concept_id:
        type: int
      value_as_number_sign:
        type: string
      range_low_sign:
        type: string
      range_high_sign:
        type: string
      provider_id:
        type: int
      visit_detail_id:
        type: int
      measurement_source_value:
        type: string
      measurement_source_concept_id:
        type: int
      unit_source_value:
        type: string

The first ``with m as`` common table expression (CTE) picks from the measurement table only the types of measurements we've chosen to include in our analysis.
It also restricts the total number of rows considered, to limit how long the query can take to execute.
The rest of ``query`` is a simple ``COUNT(*) ... GROUP BY`` query, that essentially makes a histogram over all the listed variables, including ``person_id``.
The ``dp-query`` then adds up the counts from each person with ``SUM(num) AS num ... GROUP BY``, where the ``GROUP BY`` is over all the same columns as before, except not ``person_id``.
This is a very typical pattern for categorical variables.
Note that in principle, with such a large number of variables being grouped over, we could have many, many rows in the result, but in practice the variables are highly correlated, and most measurement types only return one row with a significantly large ``num``.

The rest of the src-stats block sets the differential privacy parameters.
Notably we have to both set a ``max_ids``, which limits how many different measurement types a single patient can have, and an upper bound for the value of ``num``, i.e. a bound for how many instances of a single measurement type any one patient can have.
The limits we use are low enough that they might sometimes be exceeded in the real data, which results in the data being clipped to fit within the bounds.
However, increasing the bounds increases the amount of noise SNSQL needs to add to guarantee differential privacy, which can quickly lead to the result of the query being too noisy to be useful.
SNSQL also drops rows where ``num`` is too small, to avoid small histogram bins causing privacy leaks, and if the bounds are made too large (or ``epsilon`` too small), SNSQL may judge most of the bins to be too small, resulting the output of the query missing data for many types of measurements.

In ``patient_story`` we use ``sample_from_sql_group_by`` to sample from the result of ``measurement_categoricals`` what a typical row of a particular measurement type looks like.
For the details see the ``gen_measurement`` function in `story_generators.py <https://github.com/alan-turing-institute/sqlsynthgen/blob/main/examples/cchic_omop/>`_.
