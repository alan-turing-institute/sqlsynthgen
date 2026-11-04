"""Metrics for comparing a real column's data against a synthetic sample."""

import math
from abc import ABC, abstractmethod
from collections import Counter
from decimal import Decimal

from datafaker.evaluators.distribution_builders import Distribution


def canonicalize_value(value):
    """Normalize a value for real-vs-synthetic equality comparison.

    A column's real values may come back as decimal.Decimal (SQLAlchemy's
    default for a Numeric column) or as a tuple (a merged/composite
    column's row), while a proposer's synthetic values for the same data
    are plain float/list. Comparing str(Decimal("123.40")) against
    str(123.4), or str((1.2, 3.4)) against str([1.2, 3.4]), would never
    match even when the underlying values are identical - normalize both
    sides to the same numeric/sequence type first so an exact match survives.
    """
    if isinstance(value, (list, tuple)):
        return tuple(canonicalize_value(v) for v in value)
    if isinstance(value, Decimal):
        return float(value)
    return value


class Metric(ABC):
    """A comparison between a real and a synthetic distribution or sample."""

    @abstractmethod
    def compare(
        self,
        real: Distribution,
        synthetic: Distribution,
    ) -> float:
        """Score how closely ``synthetic`` matches ``real``."""


def _canonicalize_distribution(dist: Distribution) -> Distribution:
    """Normalize keys so None and mixed key types are comparable and sortable."""
    normalized = {
        ("" if key is None else str(key)): value
        for key, value in dist.probabilities.items()
    }
    return Distribution(normalized)


class MeanSquaredError(Metric):
    """Mean squared error between two probability distributions."""

    def compare(
        self,
        real: Distribution,
        synthetic: Distribution,
    ) -> float:
        """Score the mean squared error between ``real`` and ``synthetic``."""
        real = _canonicalize_distribution(real)
        synthetic = _canonicalize_distribution(synthetic)
        vocab = sorted(real.vocabulary | synthetic.vocabulary)

        if not vocab:
            return 0.0

        x = real.as_vector(vocab)
        y = synthetic.as_vector(vocab)

        squared_error = sum((a - b) ** 2 for a, b in zip(x, y))

        # x and y are probability vectors (each sums to 1), so their squared
        # Euclidean distance is bounded in [0, 2] regardless of vocab size.
        # Dividing by that fixed bound (rather than len(vocab)**2) keeps the
        # score on a comparable scale to JensenShannon's [0, ln 2] range, so
        # criterion weights combining the two behave as intended.
        score = squared_error / 2
        return score


class JensenShannon(Metric):
    """Jensen-Shannon divergence between two probability distributions."""

    EPS = 1e-12

    def compare(
        self,
        real: Distribution,
        synthetic: Distribution,
    ) -> float:
        """Score the Jensen-Shannon divergence between ``real`` and ``synthetic``."""
        real = _canonicalize_distribution(real)
        synthetic = _canonicalize_distribution(synthetic)
        vocab = sorted(real.vocabulary | synthetic.vocabulary)

        if not vocab:
            return 0.0

        p = real.as_vector(vocab)
        q = synthetic.as_vector(vocab)

        m = [(a + b) / 2 for a, b in zip(p, q)]

        def kl(a, b):
            total = 0
            for x, y in zip(a, b):
                if x > 0:
                    total += x * math.log(x / max(y, self.EPS))
            return total

        return (kl(p, m) + kl(q, m)) / 2


class NoveltyMetric(Metric):
    """Fraction of synthetic values that don't already appear in the real data."""

    def compare(
        self,
        real,
        synthetic,
    ):
        """Score how much of ``synthetic`` is novel relative to ``real``."""
        real_set = {
            str(canonicalize_value(v)).strip().lower() for v in real if v is not None
        }

        generated = {
            str(canonicalize_value(v)).strip().lower()
            for v in synthetic
            if v is not None
        }

        if not generated:
            return 0.0

        overlap = generated & real_set

        return 1.0 - len(overlap) / len(generated)


class DiversityMetric(Metric):
    """Compare the internal diversity of a real and a synthetic sample.

    Measures how closely the synthetic sample's internal diversity matches
    the real sample's diversity, rather than rewarding diversity outright.

    Each sample is scored by its normalized Shannon entropy (entropy divided
    by log(k), k = number of distinct values), which is 0 for a single
    repeated value and 1 for a uniform distribution over all its distinct
    values. The metric returns 1 minus the absolute difference between the
    real and synthetic normalized entropies: 1.0 when the synthetic sample
    is exactly as diverse (relative to its own value count) as the real
    sample, dropping toward 0 the more it over- or under-diversifies
    relative to the real data. This avoids favoring generators that spread
    probability more uniformly than the real data actually does.
    """

    @staticmethod
    def _normalized_entropy(values) -> float:
        """Compute the normalized Shannon entropy of ``values``."""
        vals = [str(v).strip().lower() for v in values if v is not None]
        if not vals:
            return 0.0
        counts = Counter(vals)
        k = len(counts)
        if k <= 1:
            return 0.0
        total = sum(counts.values())
        probs = [c / total for c in counts.values()]
        ent = -sum(p * math.log(p) for p in probs if p > 0)
        return ent / math.log(k)

    def compare(self, real, synthetic):
        """Score how closely ``synthetic``'s diversity matches ``real``'s."""
        real_diversity = self._normalized_entropy(real)
        synthetic_diversity = self._normalized_entropy(synthetic)
        # clamp against floating-point noise (e.g. two near-1.0 entropies
        # differing by a rounding error) so the result stays within [0, 1]
        return max(0.0, 1.0 - abs(synthetic_diversity - real_diversity))
