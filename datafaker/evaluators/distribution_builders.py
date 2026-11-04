"""Build real/synthetic value distributions for statistical fidelity scoring."""

import statistics
from abc import ABC, abstractmethod
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select

from datafaker.dialects import Random
from datafaker.evaluators.feature_extractors import FeatureExtractor, IdentityExtractor


@dataclass
class Distribution:
    """A discrete probability distribution over feature values."""

    probabilities: dict[Any, float]

    @property
    def vocabulary(self) -> set[Any]:
        """The set of feature values this distribution assigns mass to."""
        return set(self.probabilities.keys())

    def probability(self, key: Any) -> float:
        """Return the probability mass assigned to ``key`` (0.0 if unseen)."""
        return self.probabilities.get(key, 0.0)

    def as_vector(self, vocabulary: list[Any]) -> list[float]:
        """Render this distribution as a probability vector over ``vocabulary``."""
        return [self.probability(v) for v in vocabulary]


class DistributionBuilder(ABC):
    """Build a real or synthetic value's ``Distribution`` for one feature."""

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        engine,
        table,
        column,
        extractor: FeatureExtractor = IdentityExtractor(),
        sample_size=4000,
    ):
        """Initialize the engine/table/column/extractor/sample_size shared by every builder."""
        self.engine = engine
        self.table = table
        self.column = column
        self.extractor = extractor
        self.sample_size = sample_size

    @abstractmethod
    def build_from_table(self) -> Distribution:
        """Build the distribution of this feature over the real column."""

    @abstractmethod
    def build_from_values(self, values) -> Distribution:
        """Build the distribution of this feature over synthetic ``values``."""


# pylint: disable=too-many-instance-attributes
class HistogramBuilder(DistributionBuilder):
    """Bucket a continuous feature into a fixed-width histogram."""

    # pylint: disable=too-many-arguments too-many-positional-arguments
    def __init__(
        self,
        engine,
        table,
        column,
        bins=10,
        extractor: FeatureExtractor = IdentityExtractor(),
        sample_size=4000,
    ):
        """Initialize a histogram builder for one column feature."""
        super().__init__(engine, table, column, extractor, sample_size)
        self.bins = bins

        # Learned from the real data
        self.mean = None
        self.stddev = None
        self.bottom = None
        self.width = None
        self.edges = None

    def build_from_table(self) -> Distribution:
        """Build the real column's histogram distribution."""
        feature_expr = self.extractor.expression(self.column)

        #
        # Read a bounded, randomly-ordered sample of values for computing
        # statistics (mean/stddev), rather than pulling the entire column
        # into memory. Random ordering matters: an unordered LIMIT returns
        # whatever the DB naturally returns first (often insertion order),
        # which can systematically bias this sample for a column whose
        # value correlates with row order (e.g. a "rental_date" column
        # inserted roughly chronologically) - and since this is the "real"
        # side of every fidelity comparison, that bias directly skews which
        # generator looks like the better fit.
        #
        with self.engine.connect() as conn:
            values = (
                conn.execute(
                    select(feature_expr)
                    .select_from(self.table)
                    .order_by(Random())
                    .limit(self.sample_size)
                )
                .scalars()
                .all()
            )

        values = [float(v) for v in values if v is not None]

        if not values:
            return Distribution({})

        self.mean = statistics.mean(values)
        self.stddev = statistics.stdev(values) if len(values) > 1 else 0.0

        width = 0.5 if self.stddev == 0 else self.stddev / 2
        self.edges = [
            float("-inf"),
            self.mean - 4 * width,
            self.mean - 3 * width,
            self.mean - 2 * width,
            self.mean - 1 * width,
            self.mean,
            self.mean + 1 * width,
            self.mean + 2 * width,
            self.mean + 3 * width,
            self.mean + 4 * width,
            float("inf"),
        ]
        bottom = self.mean - 4 * width

        if width == 0:
            width = 1.0

        #
        # SQL histogram, computed exactly over the full table (this is a
        # bounded, server-side aggregate query - it returns at most `bins`
        # rows - so it doesn't need the sample_size cap that applies to the
        # raw-value pulls above).
        #
        with self.engine.connect() as conn:
            # Compute the bucket expression once, in an inner subquery, and
            # group by the resulting materialized column in the outer query.
            # MSSQL rejects both `GROUP BY <alias>` (a SELECT-list alias
            # referenced from GROUP BY) and `GROUP BY <repeated expression>`
            # (its query planner does not recognize two separately-bound
            # occurrences of the same parameterized expression as
            # equivalent) - grouping by an actual column of a subquery is
            # the one form every dialect (Postgres/DuckDB/MSSQL) accepts.
            bucket_expr = func.floor((feature_expr - bottom) / width)
            inner = (
                select(bucket_expr.label("bucket")).select_from(self.table).subquery()
            )
            rows = conn.execute(
                select(
                    inner.c.bucket,
                    func.count().label("count"),  # pylint: disable=not-callable
                ).group_by(inner.c.bucket)
            ).all()

        # total must come from this same full-table query, not from the
        # (possibly sampled) `values` above, so the probabilities sum to 1.
        total = sum(count for bucket, count in rows if bucket is not None)

        if total == 0:
            return Distribution({})

        probs = {}

        for bucket, count in rows:
            if bucket is None:
                continue

            bucket = min(
                self.bins - 1,
                max(0, int(bucket) + 1),
            )

            probs[bucket] = probs.get(bucket, 0) + count / total

        return Distribution(probs)

    def build_from_values(self, values) -> Distribution:
        """Build the synthetic sample's histogram distribution."""
        if self.edges is None:
            # build_from_table() never learned edges - either it hasn't run
            # yet, or the real column had no usable values. Either way there
            # is nothing to bucket synthetic values against.
            return Distribution({})

        counts = Counter()
        total = 0

        for value in values:
            for feature in self.extractor.extract(value):
                feature = float(feature)
                bucket = bisect_right(self.edges, feature) - 1
                bucket = max(0, min(bucket, len(self.edges) - 2))
                counts[bucket] += 1
                total += 1

        if total == 0:
            return Distribution({})

        return Distribution({b: c / total for b, c in counts.items()})


class CategoryBuilder(DistributionBuilder):
    """Build a categorical distribution over a discrete feature's values."""

    def build_from_table(self):
        """Build the real column's categorical distribution."""
        counter = Counter()

        with self.engine.connect() as conn:
            # Bounded, randomly-ordered sample rather than the entire column,
            # so this scales to large tables (matches the synthetic sample
            # size for a fair, like-for-like comparison) without an
            # unordered LIMIT biasing the "real" distribution toward
            # whatever the DB returns first (see HistogramBuilder above).
            rows = conn.execute(
                select(self.column)
                .select_from(self.table)
                .order_by(Random())
                .limit(self.sample_size)
            )

            for (value,) in rows:
                if value is None:
                    continue
                for feature in self.extractor.extract(value):
                    if feature is None:
                        continue
                    counter[feature] += 1

        total = sum(counter.values())
        if total == 0:
            return Distribution({})

        return Distribution({k: v / total for k, v in counter.items()})

    def build_from_values(self, values):
        """Build the synthetic sample's categorical distribution."""
        counter = Counter()

        for value in values:
            if value is None:
                continue
            for feature in self.extractor.extract(value):
                if feature is None:
                    continue
                counter[feature] += 1

        total = sum(counter.values())
        if total == 0:
            # No features were extracted from the synthetic values; represent this
            # explicitly with a special missing token so metrics treat it as a
            # disjoint distribution (maximally different) compared to a real
            # distribution with concrete feature values.
            return Distribution({"__MISSING__": 1.0})

        return Distribution({k: v / total for k, v in counter.items()})
