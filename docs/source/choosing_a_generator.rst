Choosing a generator for a column
==================================

This page is a quick decision aid for the ``propose`` command: "my column looks
like *this* -- which generator should I expect, and why?" For the full list of
generator functions and proposers, and how ``propose`` scores and ranks them,
see :doc:`builtin_generators`.

Start here
----------

In almost all cases, just run ``propose`` on the column and take the
``Recommended`` generator -- it already scores every applicable candidate on
fidelity, novelty and diversity against your real data, which is more
reliable than guessing from the column's type alone. The table below is for
the two situations where you need more than the recommendation:

* you want to sanity-check *why* a particular generator was recommended, or
* the recommendation looks wrong (or none was given) and you want to know
  what else is worth trying.

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Your column looks like...
     - Try this generator
     - Why
   * - A unique ID, e.g. an integer primary key
     - ``generic.column_value_provider.increment``
     - Guarantees fresh, unique values by counting up past the highest
       existing one; ``propose`` offers this explicitly and it's also the
       automatic default for integer primary keys. See
       :ref:`the sequence proposer <increment-proposer>`.
   * - A foreign key to another table
     - ``generic.column_value_provider.column_value``
     - Picks a random existing value from the referenced column, so
       referential integrity is preserved. This is the automatic default for
       any foreign key column.
   * - A near-unique value that must look real but not *be* real, e.g. an
       email address or free-text ID
     - Whichever ``generic.*`` value proposer ``propose`` recommends
     - ``propose`` applies an extra privacy safeguard here: a candidate that
       reproduces real values verbatim is scored down in proportion to how
       unique the real column is, so the recommendation already accounts for
       leak risk. See :ref:`evaluating-and-ranking-proposals`.
   * - A small set of repeating values, e.g. a status code, category or flag
     - ``dist_gen.weighted_choice`` (matches real frequencies exactly), or
       ``dist_gen.zipf_choice`` / ``dist_gen.choice`` for a looser fit
     - These are the *choice proposers*, offered for any column with up to
       500 distinct values; they differ in how closely they copy the real
       frequency of each value. ``weighted_choice`` stores and reproduces
       the exact real frequency of every value -- the closest fit, but the
       most exposing of how common each value really is. ``zipf_choice``
       instead only ranks values from most- to least-common and assigns
       each a Zipf/power-law frequency (the *n*\ th most common value is
       chosen ``1/n`` as often as the most common one), which approximates
       a typical real-world skew without storing per-value counts.
       ``choice`` drops frequency information entirely and picks uniformly
       at random -- the loosest fit, but the simplest and most private
       option if flattening the distribution is acceptable.
   * - A continuous measurement, e.g. a length, weight or price
     - ``dist_gen.normal``, ``dist_gen.lognormal`` (if all values are
       positive and skewed), or ``dist_gen.uniform_ms``
     - The *continuous distribution proposers* fit directly to the real
       column's mean and standard deviation.
   * - Two or more numeric columns that vary together, e.g. width and height
     - ``dist_gen.multivariate_normal`` / ``dist_gen.multivariate_lognormal``
     - Select all the columns together (``merge``) before running
       ``propose`` so it can offer these -- they preserve the correlation
       between columns rather than generating each independently.
   * - The same, but some rows have one column ``NULL`` and others don't
     - the *null-partitioned* generators, e.g. ``dist_gen.alternatives``
       wrapping ``dist_gen.grouped_multivariate_normal``
     - Real data is split into partitions by which columns are ``NULL``;
       each partition keeps its own correlation, and one is picked at
       generation time weighted by how common it was in the source data.
   * - A recognisable human-ish string, e.g. a first name, city or username
     - The matching ``generic.person.*`` / ``generic.address.*`` /
       ``generic.text.*`` generator
     - ``propose`` gives a flat ``+0.15`` boost when the column's *name*
       hints at its content (e.g. a column called ``first_name`` boosts
       ``generic.person.first_name``) -- a tie-breaker, not an override of a
       clearly better statistical fit.
   * - A date/time that should stay consistent with another date on the same
       row, e.g. a ``discharge_date`` after an ``admission_date``
     - ``generic.anchored_provider.normal_date`` (same table) or
       ``.normal_date_fk`` (related table via foreign key)
     - Only offered once the earlier date has been given the ``start`` role
       (``role set start``). The offset is a clamped, normally distributed
       number of seconds taken from the real interval lengths, so the
       generated date is never earlier than its anchor.
   * - A column that should just agree with a ``datetime`` column elsewhere
       in the row, e.g. ``year_of_birth`` alongside ``date_of_birth``
     - ``generic.extract_provider.date`` / ``.year`` / ``.month`` / ``.day``
     - Derives the value from the other column instead of generating it
       independently, so the two can never disagree.
   * - Nothing else fits, or you just want a placeholder
     - ``dist_gen.constant``
     - Always returns the same value (``None`` if the column is nullable).
       Also useful as a baseline to compare other candidates' fit against.

If a column doesn't match any row above, it's still worth running
``propose`` -- the ranking in :doc:`builtin_generators` covers cases (and
combinations of ``String``/``Numeric``/``Date`` type with real-data shape)
that are awkward to summarise as a simple lookup table.

When ``propose`` recommends nothing
------------------------------------

For a primary key or other unique column, ``propose`` will withhold a
recommendation entirely if every candidate's synthetic sample duplicates too
often to satisfy uniqueness -- rather than confidently suggesting something
that would break a constraint. Look for the sequence proposer
(``generic.column_value_provider.increment``) instead.

See also
---------

* :doc:`builtin_generators` -- full generator and proposer reference, plus
  how the ``Recommended`` score is computed.
* :doc:`quickstart` -- walkthrough of ``propose``, ``compare`` and ``set`` in
  the ``configure-generators`` CLI.
