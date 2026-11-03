Built-in generators and proposers
==================================

This page is a reference for the generator functions that ship with ``datafaker``,
and for the *proposers* that suggest which of those functions (and which arguments)
to use for a given column. If you just want a quick pointer from "my column
looks like this" to "try this generator", see :doc:`choosing_a_generator`
instead. If you want to write your own generators, see
:doc:`custom_generators`.

Generators vs. proposers
-------------------------

A **generator** is the actual callable that produces fake values, referenced by name
in ``config.yaml`` (for example ``dist_gen.normal`` or ``generic.person.first_name``).
Generators are simple: given some arguments (often summary statistics pulled from
``src-stats.yaml``), they return one random value per call.

A **proposer** is an internal ``datafaker`` object, not something you reference
directly in ``config.yaml``. When you run ``configure-generators`` and use the
``propose`` command on a column, ``datafaker`` runs every applicable proposer against
that column (and, for multi-column proposers, against that group of columns). Each
proposer:

* decides whether it applies at all, based on the column's SQL type and (for some
  proposers) properties of the source data or ``config.yaml``,
* works out what summary queries need to be added to ``src-stats.yaml`` (via
  ``select_aggregate_clauses`` or ``custom_queries``),
* works out the generator function name and keyword arguments to write into
  ``config.yaml`` if you ``set`` it.

``set`` simply writes the winning proposer's generator name and arguments into
``config.yaml``; from then on, only the underlying generator function is used ---
the proposer itself is not needed again for that column.

``propose`` shows a ranked table produced by a statistical evaluation
pipeline (see :ref:`evaluating-and-ranking-proposals` below) that scores
every candidate on fidelity, novelty and diversity and names a
``Recommended`` generator.

Default generators assigned automatically
------------------------------------------

Before you run ``propose``/``set`` at all, ``configure-generators`` already
assigns a default generator to every column, purely from its
SQL type (and whether it is a foreign key or primary key). ``propose`` lets you
replace this default with something that better matches the real data.

.. list-table:: Default generator by SQL column type
   :widths: 25 45 30
   :header-rows: 1

   * - SQL type
     - Default generator
     - Notes
   * - Foreign key column (any type)
     - ``generic.column_value_provider.column_value``
     - Picks a random existing value from the referenced column in the destination
       database.
   * - Integer primary key
     - ``generic.column_value_provider.increment``
     - Continues counting up from the highest existing value in the destination
       table; see :ref:`the increment proposer <increment-proposer>` below for how
       this is also offered as an explicit proposal.
   * - Other ``Integer``
     - ``generic.numeric.integer_number``
     -
   * - ``Numeric`` (decimal/float)
     - ``generic.numeric.float_number``
     - If the column has a fixed ``scale``, the range is set to fit within that
       many decimal digits.
   * - ``String``
     - ``generic.person.password``, or ``generic.text.color`` if the column has no
       maximum length
     - The password length is set to the column's maximum length so the value
       always fits.
   * - ``Boolean``
     - ``generic.development.boolean``
     -
   * - ``Date``
     - ``generic.datetime.date``
     -
   * - ``DateTime``
     - ``generic.datetime.datetime``
     -
   * - ``LargeBinary``
     - ``generic.bytes_provider.bytes``
     -
   * - ``Uuid`` / PostgreSQL ``UUID`` / MS-SQL ``UNIQUEIDENTIFIER``
     - ``generic.cryptographic.uuid``
     -
   * - Anything else unsupported
     - ``generic.null_provider.null``
     - ``datafaker`` logs a warning; you should configure a real generator for
       this column by hand.

Built-in proposers
-------------------

Everything below is registered in ``datafaker.proposers.everything_factory`` and is
tried automatically for every column (or, for the multi-column proposers, every
group of columns you select) when you run ``propose``.

Single-column value proposers (Mimesis)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These proposers wrap `Mimesis <https://mimesis.name/>`_ generator functions
(``generic.*``, using the ``en_GB`` locale) that produce a plausible but
unrelated fake value, with no reference to the real data's actual distribution.
Fit is estimated by comparing the length (for strings) or value (for numbers) of
generated samples against buckets built from the real column.

.. list-table::
   :widths: 20 60 20
   :header-rows: 1

   * - Applies to
     - Generators proposed
     - Notes
   * - ``String``
     - Every name in :ref:`the Mimesis string generator list <mimesis-string-list>`
       (``generic.<name>``)
     - If the column has a maximum length, each candidate is wrapped in
       ``dist_gen.truncated_string`` so the output never overflows the column.
   * - ``Numeric``
     - ``generic.person.height``
     -
   * - ``Numeric`` or ``Integer``
     - ``generic.person.weight``
     -
   * - ``Date``
     - ``generic.datetime.date``
     - Range (``start``/``end`` years) is taken from the earliest/latest years
       actually found in the column.
   * - ``DateTime``
     - ``generic.datetime.datetime``
     - Same year-range behaviour as ``Date``.
   * - ``Time``
     - ``generic.datetime.time``
     -

.. _mimesis-string-list:

The full list of Mimesis string generators tried for ``String`` columns:
``address.calling_code``, ``address.city``, ``address.continent``,
``address.country``, ``address.country_code``, ``address.postal_code``,
``address.province``, ``address.street_number``, ``address.street_name``,
``address.street_suffix``, ``person.blood_type``, ``person.email``,
``person.first_name``, ``person.last_name``, ``person.full_name``,
``person.gender``, ``person.language``, ``person.nationality``,
``person.occupation``, ``person.password``, ``person.title``,
``person.university``, ``person.username``, ``person.worldview``,
``text.answer``, ``text.color``, ``text.level``, ``text.quote``,
``text.sentence``, ``text.text``, ``text.word``.

Continuous distribution proposers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These proposers fit a single numeric column to a well-known distribution, using
the column's actual mean and standard deviation (queried into ``src-stats.yaml``
as ``mean__<column>`` / ``stddev__<column>``, or their log equivalents).

.. list-table::
   :widths: 20 25 55
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - ``Numeric`` or ``Integer``
     - ``dist_gen.normal``
     - Gaussian distribution with the observed mean and standard deviation.
   * - ``Numeric`` or ``Integer``
     - ``dist_gen.uniform_ms``
     - Uniform distribution with the same mean and standard deviation as the real
       data (rather than explicit min/max bounds).
   * - ``Numeric`` or ``Integer``, values ``> 0``
     - ``dist_gen.lognormal``
     - Log-normal distribution, fitted to the mean/standard deviation of the logs
       of the (positive) values.

Choice proposers
^^^^^^^^^^^^^^^^^

Used for columns with a manageable number of distinct values (up to 500). Each of
the three distribution shapes below is proposed both for **all** distinct values
and, separately, for a version with rare values (seen 7 or fewer times) suppressed
--- and again for both of those computed from a random sample of up to 500 rows,
for large tables where scanning every row would be too slow.

.. list-table::
   :widths: 20 25 55
   :header-rows: 1

   * - Generator
     - Function name
     - Distribution
   * - Uniform choice
     - ``dist_gen.choice``
     - Every distinct value equally likely.
   * - Zipf choice
     - ``dist_gen.zipf_choice``
     - Values ranked by real-world frequency; the *n*\ th most common value is
       chosen ``1/n`` as often as the most common one (a Zipf/power-law shape,
       without needing to store every count).
   * - Weighted choice
     - ``dist_gen.weighted_choice``
     - Reproduces the real frequency of each value directly (stores a count
       alongside each value in ``src-stats.yaml``).

Constant proposer
^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 25 55
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - Any single nullable column, or ``String``/``Numeric``/``Integer``
     - ``dist_gen.constant``
     - Always returns the same value: ``None`` if the column is nullable,
       otherwise ``""``, ``0.0`` or ``0`` depending on type. Useful as a
       placeholder, or a baseline to compare fit against.

.. _increment-proposer:

Sequence proposer
^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 25 55
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - Integer primary key that is **not** also a foreign key
     - ``generic.column_value_provider.increment``
     - Counts up past the highest value already present, guaranteeing fresh,
       unique values. This is the same mechanism used as the automatic default
       for integer primary keys (see above); proposing it explicitly just makes
       it visible and comparable alongside other candidates.

Multivariate proposers
^^^^^^^^^^^^^^^^^^^^^^^^

Offered when you select **two or more** numeric columns together (so that
``datafaker`` can preserve correlations between them, rather than generating each
column independently). A covariate (means/covariance) matrix is queried from the
source data.

.. list-table::
   :widths: 20 25 55
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - 2+ ``Numeric``/``Integer`` columns
     - ``dist_gen.multivariate_normal``
     - Multivariate Gaussian distribution over all the selected columns.
   * - 2+ ``Numeric``/``Integer`` columns, values ``> 0``
     - ``dist_gen.multivariate_lognormal``
     - Multivariate log-normal distribution (covariates computed on the logs of
       the values).

Null-partitioned multivariate proposers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A more powerful variant of the multivariate proposers above, for a group of
columns (numeric and/or categorical) that don't all follow the same pattern of
missing (``NULL``) values. The source data is split into partitions, one per
distinct combination of which columns in the group are ``NULL``; each partition
gets its own covariate matrix (or, if it has too few rows, is treated as a
suppressed group), and at generation time a partition is picked at random
(weighted by how common it was in the source data) before that partition's
distribution is sampled.

.. list-table::
   :widths: 20 25 55
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - Group of columns with mixed nullability
     - ``dist_gen.alternatives`` wrapping ``dist_gen.grouped_multivariate_normal``
     - Shown as *"null-partitioned grouped_multivariate_normal"* in ``propose``.
   * - Group of columns with mixed nullability, values ``> 0``
     - ``dist_gen.alternatives`` wrapping ``dist_gen.grouped_multivariate_lognormal``
     - Shown as *"null-partitioned grouped_multivariate_lognormal"*.
   * - (both of the above)
     - *[sampled and suppressed]* variants
     - Computed from a random sample of the source table with rare partitions
       suppressed, for large tables.

Date interval ("anchored date") proposers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Propose a date/datetime as an offset from another date, rather than independently.
This only appears once at least one ``Date``/``DateTime`` column in the same
table (or a directly related table) has been given the ``start`` role --- for
example an admission date. Every *other* date/datetime column in scope (an
implicit "end", such as a discharge date) is then offered a proposal anchored
to it; the anchor column itself is not offered a self-referential proposal.

Roles are set per column and stored in ``config.yaml`` under
``tables.<table>.columns.<column>.roles``. The easiest way to set one is the
``role`` command inside ``configure-generators``:

.. code-block:: shell

   (admission.start_date) role set start
   (admission.start_date) role list
   start
   (discharge.end_date) role on admission.start_date list

``role list`` shows the roles on the current column, ``role set <role>`` /
``role delete <role>`` add or remove one, and adding ``on <table.column>`` (or
just ``on <column>`` for a column in the current table) targets a different
column without navigating to it first. Like generator changes, role changes
are only written to ``config.yaml`` when you ``quit`` and confirm --- ``quit``
will list any pending role changes alongside pending generator changes before
asking you to save.

Two roles currently exist: ``start`` (consumed by this proposer, as described
above) and ``source`` (consumed by the copy proposer, described below).
You can still set either role by hand-editing ``config.yaml`` instead
of using ``role``, if you prefer; both routes update the same field.

.. list-table::
   :widths: 20 30 50
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - ``Date``/``DateTime`` column with an anchor column in the *same* table
     - ``generic.anchored_provider.normal_date``
     - Adds a clamped, normally-distributed number of seconds (mean/standard
       deviation taken from the real interval lengths) to the anchor column's
       generated value; never earlier than the anchor. The sample values shown
       by ``propose``/``compare`` are built from real anchor values sampled
       from the source database (recycled if fewer than requested), rather
       than a single fixed dummy anchor, so the preview reflects realistic
       intervals.
   * - ``Date``/``DateTime`` column anchored to a column in a *related* table
       (via a foreign key)
     - ``generic.anchored_provider.normal_date_fk``
     - Same idea, but looks up the anchor value from the related row in the
       destination database at generation time.

Copy proposers
^^^^^^^^^^^^^^

Propose copying a column's already-generated value verbatim, rather than
generating it independently --- useful for columns that must always agree with
another column by construction (for example a denormalised ``person_id``
duplicated across related tables). This only appears once another column in
the same table, or a directly related table (via a foreign key), has been
given the ``source`` role.

Roles are set per column and stored in ``config.yaml`` under
``tables.<table>.columns.<column>.roles``. The easiest way to set one is the
``role`` command inside ``configure-generators``:

.. code-block:: shell

   (rental.rental_date) role set source
   (rental.rental_date) role list
   source

You can use ``on`` to specify a different column to view or set:

.. code-block:: shell

   (payment.payment_date) role on rental.rental_date list
   source

A proposal is only offered when it is safe to copy:

* if the source and destination are both foreign keys, they must target the same table and column
* if exactly one of the source and destination is a foreign key, copying is never safe
* a nullable source will not be offered to a non-nullable destination
* a source in a table linked through a nullable foreign key will not be offered to a non-nullable destination

.. list-table::
   :widths: 20 30 50
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - Any column with a column with role ``source`` in the *same* table
     - ``generic.anchored_provider.copy``
     - Copies the other column's generated value for this row directly.
   * - Any column with a column with role ``source`` in a *related* table
       (via a foreign key)
     - ``generic.anchored_provider.copy_fk``
     - Looks up the related row already generated in the destination database
       and copies its value, the same way ``normal_date_fk`` looks up its
       anchor.

Unlike the other proposers in this section, ``copy`` / ``copy_fk`` is never
scored against the real data (it always reports a neutral fit) --- datafaker
cannot tell you whether the two columns you've linked are actually always
equal in the source database. Only use the ``source`` role where that
equality genuinely holds by design (e.g. a known denormalisation); copying
between two columns that merely happen to share a foreign key target but
represent different real-world entities (device concept versus device type
concept for example) will produce unfaithful synthetic data!

Date component extraction proposers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Propose deriving a column's value from a ``DateTime`` column already generated
elsewhere in the same row, rather than generating it independently -- useful when,
for example, a ``year_of_birth`` column should always agree with a
``date_of_birth`` column.

.. list-table::
   :widths: 20 30 50
   :header-rows: 1

   * - Applies to
     - Generator proposed
     - Notes
   * - ``Date``, based on a ``DateTime`` column in the same table
     - ``generic.extract_provider.date``
     - Takes just the date part of the referenced ``DateTime`` value.
   * - ``Integer``, based on a ``DateTime`` column in the same table
     - ``generic.extract_provider.year``, ``generic.extract_provider.month``,
       ``generic.extract_provider.day``
     - One proposal per component, per ``DateTime`` column found in the table.

.. _evaluating-and-ranking-proposals:

Evaluating and ranking proposals
----------------------------------

For a single-column ``propose``, every candidate proposer is scored by
``datafaker.evaluators.ColumnEvaluator`` and ranked by
``datafaker.evaluators.proposal_ranking.rank_proposals``. This produces a
table shown by ``propose``, with a ``Recommended: N. <name>`` line
above it.

Evaluation profile
^^^^^^^^^^^^^^^^^^^

The column is first classified into one ``EvaluationProfile``, based on a
sample of up to 4000 real rows:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Profile
     - How it's chosen
   * - ``TEMPORAL``
     - Column is ``Date``/``DateTime``/``Time``.
   * - ``CATEGORICAL`` or ``IDENTIFIER``
     - Column is ``Numeric``/``Integer``: ``CATEGORICAL`` if fewer than 20% of
       the sampled values are distinct (a status code, a small foreign key
       range), otherwise ``IDENTIFIER``.
   * - ``EMAIL``
     - Column is a string and more than half the sampled non-empty values
       look like an email address (contain ``@``, with a non-empty local part
       and a domain containing a ``.``).
   * - ``SHORT_TEXT``
     - Column is a string, not email-like, average length under 30
       characters and more than 80% of sampled values are distinct.
   * - ``CATEGORICAL``
     - Column is a string, not email-like or short-text, and fewer than 20%
       of sampled values are distinct.
   * - ``FREE_TEXT``
     - Remaining string columns with an average length over 50 characters or
       a high space ratio (multi-word content).
   * - ``SHORT_TEXT``
     - Any remaining string column (the fallback).

Three scoring dimensions
^^^^^^^^^^^^^^^^^^^^^^^^^^

Each candidate is scored, from a 4000-value synthetic sample, on three
independent axes, each normalized to ``[0, 1]`` across the candidates being
compared (so these are *relative* scores, not absolute ones):

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Axis
     - Meaning
   * - **Fidelity**
     - How closely the synthetic sample's distribution matches the real
       column's, measured by a profile-specific weighted combination of
       feature extractors run through ``MeanSquaredError`` or
       ``JensenShannon`` (see :ref:`statistical-fidelity-criteria` below),
       then inverted so higher is better.
   * - **Novelty**
     - The fraction of synthetic values (case/whitespace-normalized) that do
       *not* already appear among the real sampled values. 1.0 means every
       synthetic value is new.
   * - **Diversity**
     - How closely the synthetic sample's own internal variety (normalized
       Shannon entropy over distinct values) matches the real sample's --- not
       "more diverse is better", but "as diverse as the real data".

.. _statistical-fidelity-criteria:

Fidelity criteria by profile
""""""""""""""""""""""""""""""

Fidelity is computed by one of six fixed sets of criteria, chosen by profile,
each a weighted combination of feature extractors compared via
``MeanSquaredError`` (on a length/count histogram) or ``JensenShannon``
divergence (on a category distribution):

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Profile
     - Criteria (feature: weight)
   * - ``IDENTIFIER``
     - identifier (value histogram): 1.0
   * - ``CATEGORICAL``
     - category (exact value): 1.0
   * - ``SHORT_TEXT``
     - length: 0.25, character bigrams: 0.35, first letter: 0.10,
       last letter: 0.10, vowel/consonant pattern: 0.20
   * - ``EMAIL``
     - length: 0.10, local part: 0.40, domain: 0.15, top-level domain: 0.15,
       address format validity: 0.20
   * - ``FREE_TEXT``
     - length: 0.10, word count: 0.25, sentence count: 0.15, words: 0.30,
       character bigrams: 0.20
   * - ``TEMPORAL``
     - year+month+day, as one joint quantity: 0.7, day-of-week: 0.3

Combining the scores, penalties and a keyword hint
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The three normalized scores are combined into one ``Score`` per candidate as

.. code-block:: text

   Score = clamp( (fid_w * Fidelity + nov_w * Novelty + div_w * Diversity + Keyword) * Penalty, 0, 1 )

* **Weights** (``fid_w``/``nov_w``/``div_w``) depend on the profile: 0.85 /
  0.05 / 0.10 for ``IDENTIFIER`` (fidelity dominates); 0.7 / 0.15 / 0.15 for
  ``CATEGORICAL``, ``SHORT_TEXT``, ``EMAIL`` and ``TEMPORAL``; 0.5 / 0.3 / 0.2
  for ``FREE_TEXT`` (open-ended text tolerates, even rewards, novelty).
* **Keyword** is a flat ``+0.15`` boost when the column's own name hints at a
  particular kind of value (e.g. a column named ``first_name`` or
  ``customer_first_name`` boosts any ``person.first_name``-based candidate;
  see ``KEYWORD_GENERATOR_HINTS`` in ``proposal_ranking.py`` for the full
  list) -- a cheap, complementary signal used only to break near-ties, never
  to override a clearly better statistical result.
* **Penalty** is the product of two independent factors, each scaled by how
  unique the real column actually is:

  .. code-block:: text

     Penalty = resample_penalty * self_duplication_penalty

  * ``resample_penalty = 1 - real_uniqueness * copy_fraction`` is a privacy
    safeguard against reproducing real values verbatim. ``copy_fraction`` is
    the fraction of a candidate's synthetic sample that exactly matches a
    real value. It only applies to choice-style proposers
    (``dist_gen.choice``/``weighted_choice``/``zipf_choice``) -- or, for a
    numeric column, *any* proposer -- since resampling real values is
    expected and harmless for a low-uniqueness column (a gender, a status)
    but a genuine leak for a near-unique one (an email, a real ID). It has no
    effect (``1.0``) on other proposers for non-numeric columns, where an
    overlap with real values is usually just a naturally shared vocabulary
    (e.g. common first names), not memorization.
  * ``self_duplication_penalty = 1 - real_uniqueness * (1 - synthetic_uniqueness)``
    guards against a candidate duplicating against its *own* output --
    ``synthetic_uniqueness`` is the fraction of distinct values in its 4000
    synthetic samples. This is a different failure mode from
    ``resample_penalty``: a candidate can have a tiny fixed pool of possible
    outputs (e.g. a canned-quote generator with only a few dozen distinct
    strings) without ever coincidentally matching a *real* value, yet still
    be unusable for a column that needs many distinct rows. It's computed
    for every candidate, but only applied at full strength when the column
    actually needs unique values (a primary key or a ``UNIQUE``-constrained
    column) -- otherwise it's floored at ``0.4``, discounting a
    self-duplicating-but-otherwise-plausible candidate rather than crushing
    its score to near zero over a constraint that doesn't apply here. The
    ``Synth.Uniq`` column in the ``propose`` table shows the raw value this
    factor is computed from.

Candidates are also grouped into Pareto fronts (front 1 = not
strictly dominated on fidelity, novelty *and* diversity simultaneously by any
other candidate); ``propose`` (without ``all``) shows only front 1, capped at
10 rows by ``Score`` (always keeping the recommended candidate even if it
would otherwise be cut), and tells you how many candidates were hidden. Run
``propose all`` to see every candidate regardless of front or score ---
including, for example, a continuous-float proposer against an ``Integer``
column; whether that's a valid choice for your database is on you to check,
since ``propose`` doesn't filter or penalize candidates by output type.

The recommended candidate's row is marked with a leading ``*`` and (if your
terminal theme supports colour) highlighted, whichever Pareto front it's in.

The **recommendation** is simply the candidate with the highest ``Score``
(ties broken by NSGA-II crowding distance, i.e. preferring a candidate that's
less redundant with its front-mates). Two caveats are surfaced when they
apply:

* For a primary key or unique column, if *every* candidate's own synthetic
  sample is duplicated enough that it couldn't satisfy a uniqueness
  constraint, ``propose`` recommends nothing at all rather than a false
  positive --- look for the sequence proposer instead (see
  :ref:`increment-proposer` above).
* If the candidates that fit the real data best were heavily discounted by
  the resample penalty above, leaving a poorer-fitting candidate to "win" by
  elimination, ``propose`` prints a warning naming the discounted candidates
  and flags the recommendation as a fallback rather than a confident pick.

Generator function reference
------------------------------

The tables above name the generator functions that proposers can select. This
section documents each one, grouped by the Mimesis provider it belongs to, for
when you want to reference or combine them by hand in ``config.yaml`` (see
:doc:`custom_generators`).

``dist_gen`` -- ``datafaker.providers.DistributionProvider``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Function
     - Description
   * - ``uniform(low, high)``
     - Uniform distribution between explicit bounds.
   * - ``uniform_ms(mean, sd)``
     - Uniform distribution with a given mean and standard deviation.
   * - ``normal(mean, sd)``
     - Gaussian (normal) distribution.
   * - ``lognormal(logmean, logsd)``
     - Log-normal distribution.
   * - ``choice(a)`` / ``choice_direct(a)``
     - Uniform choice between a list of values. ``choice`` takes
       ``{"value": ...}`` dicts (as stored in ``src-stats.yaml``);
       ``choice_direct`` takes plain values.
   * - ``zipf_choice(a, n=None)`` / ``zipf_choice_direct(a, n=None)``
     - Choice following a Zipf distribution over values ranked most-to-least
       frequent.
   * - ``weighted_choice(a)``
     - Choice weighted by an explicit ``count`` stored alongside each value.
   * - ``constant(value)``
     - Always returns ``value``.
   * - ``multivariate_normal(cov)`` / ``multivariate_lognormal(cov)``
     - Draws a list of correlated values from a covariate matrix (means and
       covariances, keyed as ``mN``/``cN_M``).
   * - ``grouped_multivariate_normal(covs)`` / ``grouped_multivariate_lognormal(covs)``
     - As above, but first picks one covariate matrix from a list, weighted by
       each group's ``count``. Used for the null-partitioned proposers.
   * - ``alternatives(alternative_configs, counts=None)``
     - Picks between other named generators, weighted by count; this is how the
       null-partitioned proposers choose a missingness pattern before delegating
       to ``grouped_multivariate_normal``/``grouped_multivariate_lognormal``.
   * - ``with_constants_at(constants_at, subgen, params)``
     - Runs another generator and splices fixed values into the result list at
       given positions (used to reinsert ``NULL``/category columns alongside
       generated numeric ones).
   * - ``truncated_string(subgen_fn, params, length)``
     - Runs a string-producing generator and truncates the result to ``length``
       characters.

``generic`` -- Mimesis providers (locale ``en_GB``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most of ``generic.address.*``, ``generic.person.*``, ``generic.text.*``,
``generic.datetime.*`` and ``generic.numeric.*`` come straight from the
`Mimesis library <https://mimesis.name/en/master/api.html>`_ and aren't
listed again here; see :ref:`the Mimesis string generator list
<mimesis-string-list>` above for the ones ``propose`` tries automatically,
plus ``person.height``, ``person.weight``, ``datetime.date``,
``datetime.datetime`` and ``datetime.time``.

``datafaker`` adds the following extra Mimesis providers of its own
(``datafaker/providers.py``):

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Function
     - Description
   * - ``column_value_provider.column_value(db_connection, orm_class, column_name)``
     - Returns a random existing value from another column -- the default
       generator for foreign key columns.
   * - ``column_value_provider.increment(db_connection, column)``
     - Returns a number one higher than the previous call for this column,
       starting one above the highest existing value in the destination
       database -- the default generator for integer primary keys.
   * - ``bytes_provider.bytes()``
     - Random binary data.
   * - ``timedelta_provider.timedelta(...)``
     - Random ``datetime.timedelta`` value.
   * - ``timespan_provider.timespan(...)``
     - Random pair of dates/times forming a span.
   * - ``weighted_boolean_provider.bool(probability)``
     - ``True`` with the given probability.
   * - ``sql_group_by_provider.sample(...)``
     - Samples rows grouped and filtered according to SQL-like criteria.
   * - ``null_provider.null()``
     - Always returns ``None``. Used as the last-resort default for column
       types ``datafaker`` doesn't otherwise recognise.
   * - ``extract_provider.year(extract_from)`` / ``month(...)`` / ``day(...)`` / ``date(...)``
     - Pulls a component out of a ``datetime`` produced elsewhere in the same
       row (see the date component extraction proposers above).
   * - ``anchored_provider.normal_date(mean_seconds, sd_seconds, anchor)``
     - A date offset from ``anchor`` in the same table by a clamped normally
       distributed number of seconds.
   * - ``anchored_provider.normal_date_fk(dst_db_conn, mean_seconds, sd_seconds, table, on_column, anchor_row, anchor_column)``
     - As above, but ``anchor`` is looked up from a related table via a foreign
       key at generation time.

See also
---------

* :doc:`choosing_a_generator` -- a quick lookup from column shape to
  generator, for when you want more than the ``propose`` recommendation.
* :doc:`custom_generators` -- writing your own row generators and story
  generators.
* :doc:`quickstart` and :doc:`introduction` -- walkthroughs of
  ``configure-generators``, ``propose``, ``compare`` and ``set`` in action.
* :doc:`glossary`
