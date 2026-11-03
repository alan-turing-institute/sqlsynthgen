"""This module contains Mimesis Provider sub-classes."""
import datetime as dt
import functools
import math
import random
from collections.abc import Mapping
from typing import Any, Callable, Generator, Optional, Union, cast

import numpy as np
from mimesis import Datetime, Text
from mimesis.providers.base import BaseDataProvider, BaseProvider
from sqlalchemy import Column, Connection, MetaData
from sqlalchemy.sql import func, select

from datafaker.dialects import Random
from datafaker.utils import T, logger


class ColumnValueProvider(BaseProvider):
    """A Mimesis provider of random values from the source database."""

    class Meta:
        """Meta-class for ColumnValueProvider settings."""

        name = "column_value_provider"

    @staticmethod
    def column_value(
        db_connection: Connection, orm_class: Any, column_name: str
    ) -> Any:
        """Return a random value from the column specified."""
        query = select(orm_class).order_by(Random()).limit(1)
        random_row = db_connection.execute(query).first()

        if random_row:
            return getattr(random_row, column_name)
        return None

    def __init__(self, *, seed: int | None = None, **kwargs: Any) -> None:
        """Initialise the column value provider."""
        super().__init__(seed=seed, **kwargs)
        self.accumulators: dict[str, int] = {}

    def increment(self, db_connection: Connection, column: Column) -> int:
        """Return incrementing value for the column specified."""
        name = f"{column.table.name}.{column.name}"
        result = self.accumulators.get(name, None)
        if result is None:
            row = db_connection.execute(select(func.max(column))).first()
            result = 0 if row is None or row[0] is None else row[0]
        value = result + 1
        self.accumulators[name] = value
        return value


class BytesProvider(BaseDataProvider):
    """A Mimesis provider of binary data."""

    class Meta:
        """Meta-class for BytesProvider settings."""

        name = "bytes_provider"

    def bytes(self) -> bytes:
        """Return a UTF-8 encoded sentence."""
        return Text(self.locale).sentence().encode("utf-8")


class TimedeltaProvider(BaseProvider):
    """A Mimesis provider of timedeltas."""

    class Meta:
        """Meta-class for TimedeltaProvider settings."""

        name = "timedelta_provider"

    @staticmethod
    def timedelta(
        min_dt: dt.timedelta = dt.timedelta(seconds=0),
        # ints bigger than this cause trouble
        max_dt: dt.timedelta = dt.timedelta(seconds=2**32),
    ) -> dt.timedelta:
        """Return a random timedelta object."""
        min_s = min_dt.total_seconds()
        max_s = max_dt.total_seconds()
        seconds = random.randint(int(min_s), int(max_s))
        return dt.timedelta(seconds=seconds)


class TimespanProvider(BaseProvider):
    """A Mimesis provider for timespans.

    A timespan consits of start datetime, end datetime, and the timedelta in between.
    Returns a 3-tuple.
    """

    class Meta:
        """Meta-class for TimespanProvider settings."""

        name = "timespan_provider"

    @staticmethod
    def timespan(
        earliest_start_year: int,
        last_start_year: int,
        min_dt: dt.timedelta = dt.timedelta(seconds=0),
        # ints bigger than this cause trouble
        max_dt: dt.timedelta = dt.timedelta(seconds=2**32),
    ) -> tuple[dt.datetime, dt.datetime, dt.timedelta]:
        """Return a timespan as a 3-tuple of (start, end, delta)."""
        delta = TimedeltaProvider().timedelta(min_dt, max_dt)
        start = Datetime().datetime(start=earliest_start_year, end=last_start_year)
        end = start + delta
        return start, end, delta


class WeightedBooleanProvider(BaseProvider):
    """A Mimesis provider for booleans with a given probability for True."""

    class Meta:
        """Meta-class for WeightedBooleanProvider settings."""

        name = "weighted_boolean_provider"

    def bool(self, probability: float) -> bool:
        """Return True with given `probability`, otherwise False."""
        return self.random.uniform(0, 1) < probability


class SQLGroupByProvider(BaseProvider):
    """A Mimesis provider that samples from the results of a SQL `GROUP BY` query."""

    class Meta:
        """Meta-class for SQLGroupByProvider settings."""

        name = "sql_group_by_provider"

    def sample(
        self,
        group_by_result: list[dict[str, Any]],
        weights_column: str,
        value_columns: Optional[Union[str, list[str]]] = None,
        filter_dict: Optional[dict[str, Any]] = None,
    ) -> Union[Any, dict[str, Any], tuple[Any, ...]]:
        """Random sample a row from the result of a SQL `GROUP BY` query.

        The result of the query is assumed to be in the format that datafaker's
        make-stats outputs.

        For example, if one executes the following src-stats query

        .. code-block:: sql

          SELECT COUNT(*) AS num, nationality, gender, age
          FROM person
          GROUP BY nationality, gender, age

        and calls it the `count_demographics` query, one can then use

        .. code-block:: python

          generic.sql_group_by_provider.sample(
              SRC_STATS["count_demographics"],
              weights_column="num",
              value_columns=["gender", "nationality"],
              filter_dict={"age": 23},
          )

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


class NullProvider(BaseProvider):
    """A Mimesis provider that always returns `None`."""

    class Meta:
        """Meta-class for NullProvider settings."""

        name = "null_provider"

    @staticmethod
    def null() -> None:
        """Return `None`."""
        return None


class InappropriateGeneratorException(Exception):
    """Exception thrown if a generator is requested that is not appropriate."""


class NothingToGenerateException(Exception):
    """Exception thrown when no value can be generated."""

    def __init__(self, message: str):
        """Initialise the exception with a human-readable message."""
        super().__init__(message)


@functools.cache
def zipf_weights(size: int) -> list[float]:
    """Get the weights of a Zipf distribution of a given size."""
    total = sum(map(lambda n: 1 / n, range(1, size + 1)))
    return [1 / (n * total) for n in range(1, size + 1)]


def merge_with_constants(
    xs: list[T], constants_at: dict[int, T]
) -> Generator[T, None, None]:
    """
    Merge a list of items with other items that must be placed at certain indices.

    :param constants_at: A map of indices to objects that must be placed at
        those indices.
    :param xs: Items that fill in the gaps left by ``constants_at``.
    :return: ``xs`` with ``constants_at`` inserted at the appropriate
        points. If there are not enough elements in ``xs`` to fill in the gaps
        in ``constants_at``, the elements of ``constants_at`` after the gap
        are dropped.
    """
    outi = 0
    xi = 0
    constant_count = len(constants_at)
    while constant_count != 0:
        if outi in constants_at:
            yield constants_at[outi]
            constant_count -= 1
        else:
            if xi == len(xs):
                return
            yield xs[xi]
            xi += 1
        outi += 1
    yield from xs[xi:]


class DistributionProvider(BaseProvider):
    """A Mimesis provider for various distributions."""

    class Meta:
        """Meta-class for various distributions."""

        name = "distribution_provider"

    root3 = math.sqrt(3)

    def __init__(self, *, seed: int | None = None, **kwargs: Any) -> None:
        """Initialise a DistributionProvider."""
        super().__init__(seed=seed, **kwargs)
        np_seed = seed if isinstance(seed, int) else None
        self.np_gen = np.random.default_rng(seed=np_seed)

    def uniform(self, low: float, high: float) -> float:
        """
        Choose a value according to a uniform distribution.

        :param low: The lowest value that can be chosen.
        :param high: The highest value that can be chosen.
        :return: The output value.
        """
        return random.uniform(float(low), float(high))

    def uniform_ms(self, mean: float, sd: float) -> float:
        """
        Choose a value according to a uniform distribution.

        :param mean: The mean of the output values.
        :param sd: The standard deviation of the output values.
        :return: The output value.
        """
        m = float(mean)
        h = self.root3 * float(sd)
        return random.uniform(m - h, m + h)

    def normal(self, mean: float, sd: float) -> float:
        """
        Choose a value according to a Gaussian (normal) distribution.

        :param mean: The mean of the output values.
        :param sd: The standard deviation of the output values.
        :return: The output value.
        """
        return random.normalvariate(float(mean), float(sd))

    def lognormal(self, logmean: float, logsd: float) -> float:
        """
        Choose a value according to a lognormal distribution.

        :param logmean: The mean of the logs of the output values.
        :param logsd: The standard deviation of the logs of the output values.
        :return: The output value.
        """
        return random.lognormvariate(float(logmean), float(logsd))

    def choice_direct(self, a: list[T]) -> T:
        """
        Choose a value with equal probability.

        :param a: The list of values to output.
        :return: The chosen value.
        """
        return random.choice(a)

    def choice(self, a: list[Mapping[str, T]]) -> T | None:
        """
        Choose a value with equal probability.

        :param a: The list of values to output. Each element is a mapping with
            a key ``value`` and the key is the value to return.
        :return: The chosen value.
        """
        return self.choice_direct(a).get("value", None)

    def zipf_choice_direct(self, a: list[T], n: int | None = None) -> T:
        """
        Choose a value according to the Zipf distribution.

        The nth value (starting from 1) is chosen with a frequency
        1/n times as frequently as the first value is chosen.

        :param a: The list of values to output, most frequent first.
        :return: The chosen value.
        """
        if n is None:
            n = len(a)
        return random.choices(a, weights=zipf_weights(n))[0]

    def zipf_choice(self, a: list[Mapping[str, T]], n: int | None = None) -> T | None:
        """
        Choose a value according to the Zipf distribution.

        The nth value (starting from 1) is chosen with a frequency
        1/n times as frequently as the first value is chosen.

        :param a: The list of rows to choose between, most frequent first.
            Each element is a mapping with a key ``value`` and the key is the
            value to return.
        :return: The chosen value.
        """
        c = self.zipf_choice_direct(a, n)
        return c.get("value", None)

    def weighted_choice(self, a: list[dict[str, Any]]) -> Any:
        """
        Choice weighted by the count in the original dataset.

        :param a: a list of dicts, each with a ``value`` key
            holding the value to be returned and a ``count`` key holding the
            number of that value found in the original dataset
        :return: The chosen ``value``, or None if ``a`` is an empty list.
        """
        if not a:
            return None
        vs = []
        counts = []
        for vc in a:
            count = vc.get("count", 0)
            if count:
                counts.append(count)
                vs.append(vc.get("value", None))
        c = random.choices(vs, weights=counts)[0]
        return c

    def constant(self, value: T) -> T:
        """Return the same value always."""
        return value

    def multivariate_normal_np(self, cov: dict[str, Any]) -> np.typing.NDArray:
        r"""
        Return an array of values chosen from the given covariates.

        :param cov: Keys are ``rank``: The number of values to output;
            ``mN``: The mean of variable ``N`` (where ``N`` is between 0 and
            one less than ``rank``). ``cN_M`` (where 0 < ``N`` <= ``M`` < ``rank``):
            the covariance between the ``N``\th and the ``M``\th variables.
        :return: A numpy array of results.
        """
        rank = int(cov["rank"])
        if rank == 0:
            return np.empty(shape=(0,))
        mean = [float(cov[f"m{i}"]) for i in range(rank)]
        covs = [
            [
                float(cov[f"c{i}_{j}"] if i <= j else cov[f"c{j}_{i}"])
                for i in range(rank)
            ]
            for j in range(rank)
        ]
        return self.np_gen.multivariate_normal(mean, covs)

    def _select_group(self, alts: list[dict[str, Any]]) -> Any:
        """Choose one of the ``alts`` weighted by their ``"count"`` elements."""
        total = 0
        for alt in alts:
            if alt["count"] < 0:
                logger.warning(
                    "Alternative count is %d, but should not be negative", alt["count"]
                )
            else:
                total += alt["count"]
        if total == 0:
            raise NothingToGenerateException("No counts in any alternative")
        choice = random.randrange(total)
        for alt in alts:
            choice -= alt["count"]
            if choice < 0:
                return alt
        raise NothingToGenerateException(
            "Internal error: ran out of choices in _select_group"
        )

    def _find_constants(self, result: dict[str, Any]) -> dict[int, Any]:
        """
        Find all keys ``kN``, returning a dictionary of ``N: kNN``.

        This can be passed into ``merge_with_constants`` as the
        ``constants_at`` argument.
        """
        out: dict[int, Any] = {}
        for k, v in result.items():
            if k.startswith("k") and k[1:].isnumeric():
                out[int(k[1:])] = v
        return out

    PERMITTED_SUBGENS = {
        "multivariate_lognormal",
        "multivariate_normal",
        "grouped_multivariate_lognormal",
        "grouped_multivariate_normal",
        "constant",
        "weighted_choice",
        "with_constants_at",
    }

    def multivariate_normal(self, cov: dict[str, Any]) -> list[float]:
        r"""
        Produce a list of values pulled from a multivariate distribution.

        :param cov: A dict with various keys: ``rank`` is the number of
            output values, ``m0``, ``m1``, ... are the means of the
            distributions (``rank`` of them). ``c0_0``, ``c0_1``, ``c1_1``, ...
            are the covariates, ``cN_M`` is the covariate of the ``N``\th and
            ``M``\th varaibles, with 0 <= ``N`` <= ``M`` < ``rank``.
        :return: list of ``rank`` floating point values
        """
        out: list[float] = self.multivariate_normal_np(cov).tolist()
        return out

    def multivariate_lognormal(self, cov: dict[str, Any]) -> list[float]:
        r"""
        Produce a list of values pulled from a multivariate distribution.

        :param cov: A dict with various keys: ``rank`` is the number of
            output values, ``m0``, ``m1``, ... are the means of the
            distributions (``rank`` of them). ``c0_0``, ``c0_1``, ``c1_1``, ...
            are the covariates, ``cN_M`` is the covariate of the ``N``\th and
            ``M``\th varaibles, with 0 <= ``N`` <= ``M`` < ``rank``. These
            are all the means and covariants of the logs of the data.
        :return: list of ``rank`` floating point values
        """
        out: list[Any] = np.exp(self.multivariate_normal_np(cov)).tolist()
        return out

    def grouped_multivariate_normal(self, covs: list[dict[str, Any]]) -> list[Any]:
        """Produce a list of values pulled from a set of multivariate distributions."""
        cov = self._select_group(covs)
        logger.debug("Multivariate normal group selected: %s", cov)
        constants = self._find_constants(cov)
        nums = self.multivariate_normal(cov)
        return list(merge_with_constants(nums, constants))

    def grouped_multivariate_lognormal(self, covs: list[dict[str, Any]]) -> list[Any]:
        """Produce a list of values pulled from a set of multivariate distributions."""
        cov = self._select_group(covs)
        logger.debug("Multivariate lognormal group selected: %s", cov)
        constants = self._find_constants(cov)
        nums = np.exp(self.multivariate_normal_np(cov)).tolist()
        return list(merge_with_constants(nums, constants))

    def _check_generator_name(self, name: str) -> None:
        if name not in self.PERMITTED_SUBGENS:
            raise InappropriateGeneratorException(
                f"{name} is not a permitted generator"
            )

    def alternatives(
        self,
        alternative_configs: list[dict[str, Any]],
        counts: list[dict[str, int]] | None = None,
    ) -> Any:
        """
        Pick between other generators.

        :param alternative_configs: List of alternative generators.
            Each alternative has the following keys: "count" -- a weight for
            how often to use this alternative; "name" -- which generator
            for this partition, for example "composite"; "params" -- the
            parameters for this alternative.
        :param counts: A list of weights for each alternative. If None, the
            "count" value of each alternative is used. Each count is a dict
            with a "count" key.
        :return: list of values
        """
        if counts is not None:
            while True:
                count = self._select_group(counts)
                alt = alternative_configs[count["index"]]
                name = alt["name"]
                self._check_generator_name(name)
                try:
                    return getattr(self, name)(**alt["params"])
                except NothingToGenerateException:
                    # Prevent this alternative from being chosen again
                    count["count"] = 0
        alt = self._select_group(alternative_configs)
        name = alt["name"]
        self._check_generator_name(name)
        return getattr(self, name)(**alt["params"])

    def with_constants_at(
        self, constants_at: dict[int, T], subgen: str, params: dict[str, T]
    ) -> list[T]:
        """
        Insert constants into the results of a different generator.

        :param constants_at: A dictionary of positions and objects to insert
            into the return list at those positions.
        :param subgen: The name of the function to call to get the results
            that will have the constants inserted into.
        :param params: Keyword arguments to the ``subgen`` function.
        :return: A list of results from calling ``subgen(**params)``
            with ``constants_at`` inserted in at the appropriate indices.
        """
        if subgen not in self.PERMITTED_SUBGENS:
            logger.error(
                "subgenerator %s is not a valid name. Valid names are %s.",
                subgen,
                self.PERMITTED_SUBGENS,
            )
        subout = getattr(self, subgen)(**params)
        logger.debug("Merging constants %s", constants_at)
        return list(merge_with_constants(subout, constants_at))

    def truncated_string(
        self, subgen_fn: Callable[..., list[T]], params: dict, length: int
    ) -> list[T]:
        """Call ``subgen_fn(**params)`` and truncate the results to ``length``."""
        result = subgen_fn(**params)
        if result is None:
            return None
        return result[:length]


class ExtractProvider(BaseProvider):
    """A provider of values extracted from a date previously generated."""

    class Meta:
        """Meta-class for AnchoredProvider settings."""

        name = "extract_provider"

    def __init__(self, *, seed: int | None = None, **kwargs: Any) -> None:
        """Initialise the extract provider."""
        super().__init__(seed=seed, **kwargs)

    def year(self, extract_from: dt.datetime | None) -> int | None:
        """Extract the year from an existing value."""
        if extract_from is None:
            return None
        return extract_from.year

    def month(self, extract_from: dt.datetime | None) -> int | None:
        """Extract the month from an existing value."""
        if extract_from is None:
            return None
        return extract_from.month

    def day(self, extract_from: dt.datetime | None) -> int | None:
        """Extract the day from an existing value."""
        if extract_from is None:
            return None
        return extract_from.day

    def date(self, extract_from: dt.datetime | None) -> dt.date | None:
        """Cast datetime to date."""
        if extract_from is None:
            return None
        return extract_from.date()


class AnchoredProvider(BaseProvider):
    """A provider of values based on other values previously generated."""

    class Meta:
        """Meta-class for AnchoredProvider settings."""

        name = "anchored_provider"

    def __init__(
        self, *, metadata: MetaData, seed: int | None = None, **kwargs: Any
    ) -> None:
        """Initialise the anchored provider."""
        super().__init__(seed=seed, **kwargs)
        self._metadata = metadata

    def normal_date(
        self,
        mean_seconds: float,
        sd_seconds: float,
        anchor: dt.datetime | str,
    ) -> dt.datetime:
        """
        Generate a date based on a normally distributed interval.

        The date will be a clamped normally distributed amount of time
        after another date

        :param mean_seconds: Average number of seconds the interval lasts.
        :param sd_seconds: Standard deviation of the intervals' lengths in seconds.
        :param anchor: The start of the interval either as a Python ``datetime``
         object or an ISO8601 string.
        :return: The end of the interval; will be no earlier than the start of the
         interval. This is clamped; note that ``mean_seconds`` and
         ``sd_seconds`` refer to the unclamped distribution.
        """
        if isinstance(anchor, str):
            anchor = dt.datetime.fromisoformat(anchor)
        if isinstance(anchor, dt.date):
            anchor = dt.datetime.combine(anchor, dt.time())
        interval = random.normalvariate(float(mean_seconds), float(sd_seconds))
        if interval < 0:
            return anchor
        return anchor + dt.timedelta(seconds=interval)

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def normal_date_fk(
        self,
        dst_db_conn: Connection,
        mean_seconds: float,
        sd_seconds: float,
        table: str,
        on_column: str,
        anchor_row: Any,
        anchor_column: str,
    ) -> dt.datetime | None:
        """
        Generate a date based on a normally distributed interval.

        The date will be a clamped normally distributed amount of time after a
        column in a different table related to this one.

        :param dst_db_conn: Connection to the destination database.
        :param mean_seconds: Average number of seconds the interval lasts.
        :param sd_seconds: Standard deviation of the intervals' lengths in seconds.
        :param table: The name of the table for the anchor column.
        :param on_column: The name of the column in the foreign table
         that ``anchor_row`` must match.
        :param anchor_row: The value in the ``on_column`` column in table ``table``
         for the row providing the start of the interval.
        :param anchor_column: The name of the column in ``table``
         providing the start of the interval.
        :return: The end of the interval; will be no earlier than the start of the
         interval. This is clamped; note that ``mean_seconds`` and
         ``sd_seconds`` refer to the unclamped distribution.
        """
        mt = self._metadata.tables[table]
        query = select(mt.c[anchor_column].label("out")).where(
            mt.c[on_column] == anchor_row,
        )
        anchor = dst_db_conn.execute(query).first()
        out = getattr(anchor, "out", None)
        if not isinstance(out, (str, dt.date, dt.datetime)):
            return None
        return self.normal_date(mean_seconds, sd_seconds, out)

    def copy(self, anchor: Any) -> Any:
        """Copy another column from the same table."""
        return anchor

    def copy_fk(
        self,
        dst_db_conn: Connection,
        table: str,
        on_column: str,
        anchor_row: Any,
        anchor_column: str,
    ) -> Any:
        """
        Copy a column from a different table.

        :param dst_db_conn: Connection to the destination database.
        :param table: The name of the table for the anchor column.
        :param on_column: The name of the column in the foreign table
         that ``anchor_row`` must match.
        :param anchor_row: The value in the ``on_column`` column in table ``table``
         for the row providing the source to copy.
        :param anchor_column: The name of the column in ``table`` to copy.
        :return: The value to be copied.
        """
        mt = self._metadata.tables[table]
        query = select(mt.c[anchor_column].label("out")).where(
            mt.c[on_column] == anchor_row,
        )
        anchor = dst_db_conn.execute(query).first()
        return getattr(anchor, "out", None)
