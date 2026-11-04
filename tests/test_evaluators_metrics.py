"""Unit tests for datafaker.evaluators.metrics."""
import math

from datafaker.evaluators.distribution_builders import Distribution
from datafaker.evaluators.metrics import (
    DiversityMetric,
    JensenShannon,
    MeanSquaredError,
    NoveltyMetric,
)
from tests.utils import DatafakerTestCase


class MeanSquaredErrorTests(DatafakerTestCase):
    """Test case for the MeanSquaredError metric."""

    def setUp(self) -> None:
        super().setUp()
        self.metric = MeanSquaredError()

    def test_identical_distributions_score_zero(self) -> None:
        """Two identical distributions should have zero error."""
        dist = Distribution({"a": 0.5, "b": 0.5})
        self.assertEqual(0.0, self.metric.compare(dist, dist))

    def test_completely_disjoint_distributions(self) -> None:
        """Disjoint single-value distributions score the maximum of 1.0."""
        real = Distribution({"a": 1.0})
        synthetic = Distribution({"b": 1.0})
        # squared error = (1-0)**2 + (0-1)**2 = 2, halved = 1.0
        self.assertAlmostEqual(1.0, self.metric.compare(real, synthetic))

    def test_partial_overlap(self) -> None:
        """Score reflects the squared difference in shared probability mass."""
        real = Distribution({"a": 0.6, "b": 0.4})
        synthetic = Distribution({"a": 0.4, "b": 0.6})
        expected = ((0.2) ** 2 + (0.2) ** 2) / 2
        self.assertAlmostEqual(expected, self.metric.compare(real, synthetic))

    def test_empty_vocabulary_scores_zero(self) -> None:
        """Two empty distributions have no vocabulary and score zero."""
        self.assertEqual(0.0, self.metric.compare(Distribution({}), Distribution({})))

    def test_none_and_mixed_type_keys_are_canonicalized(self) -> None:
        """None and non-string keys are normalized so they compare correctly."""
        real = Distribution({None: 1.0})
        synthetic = Distribution({"": 1.0})
        self.assertEqual(0.0, self.metric.compare(real, synthetic))

        real_int_key = Distribution({3: 1.0})
        synthetic_str_key = Distribution({"3": 1.0})
        self.assertEqual(0.0, self.metric.compare(real_int_key, synthetic_str_key))


class JensenShannonTests(DatafakerTestCase):
    """Test case for the JensenShannon metric."""

    def setUp(self) -> None:
        super().setUp()
        self.metric = JensenShannon()

    def test_identical_distributions_score_zero(self) -> None:
        """Two identical distributions have zero divergence."""
        dist = Distribution({"a": 0.3, "b": 0.7})
        self.assertAlmostEqual(0.0, self.metric.compare(dist, dist))

    def test_completely_disjoint_distributions_score_ln2(self) -> None:
        """Disjoint distributions hit the theoretical maximum of ln(2)."""
        real = Distribution({"a": 1.0})
        synthetic = Distribution({"b": 1.0})
        self.assertAlmostEqual(math.log(2), self.metric.compare(real, synthetic))

    def test_partial_overlap_between_zero_and_max(self) -> None:
        """A partially-overlapping pair scores strictly between 0 and ln(2)."""
        real = Distribution({"a": 0.9, "b": 0.1})
        synthetic = Distribution({"a": 0.1, "b": 0.9})
        score = self.metric.compare(real, synthetic)
        self.assertGreater(score, 0.0)
        self.assertLess(score, math.log(2))

    def test_empty_vocabulary_scores_zero(self) -> None:
        """Two empty distributions score zero."""
        self.assertEqual(0.0, self.metric.compare(Distribution({}), Distribution({})))

    def test_is_symmetric(self) -> None:
        """Jensen-Shannon divergence is symmetric in its two arguments."""
        real = Distribution({"a": 0.2, "b": 0.8})
        synthetic = Distribution({"a": 0.7, "b": 0.3})
        # pylint: disable-next=arguments-out-of-order
        reversed_score = self.metric.compare(synthetic, real)
        self.assertAlmostEqual(self.metric.compare(real, synthetic), reversed_score)


class NoveltyMetricTests(DatafakerTestCase):
    """Test case for the NoveltyMetric metric."""

    def setUp(self) -> None:
        super().setUp()
        self.metric = NoveltyMetric()

    def test_no_overlap_is_fully_novel(self) -> None:
        """Synthetic values sharing nothing with the real data score 1.0."""
        self.assertEqual(1.0, self.metric.compare(["a", "b"], ["c", "d"]))

    def test_full_overlap_is_not_novel(self) -> None:
        """Synthetic values that are all copies of real values score 0.0."""
        self.assertEqual(0.0, self.metric.compare(["a", "b"], ["a", "b"]))

    def test_partial_overlap(self) -> None:
        """Score is the fraction of distinct synthetic values not seen in real."""
        real = ["a", "b"]
        synthetic = ["a", "c"]
        self.assertAlmostEqual(0.5, self.metric.compare(real, synthetic))

    def test_empty_synthetic_scores_zero(self) -> None:
        """No synthetic values at all is treated as no novelty."""
        self.assertEqual(0.0, self.metric.compare(["a"], []))

    def test_comparison_is_case_and_whitespace_insensitive(self) -> None:
        """Values are compared after stripping whitespace and lowercasing."""
        self.assertEqual(0.0, self.metric.compare(["Alice"], [" alice "]))

    def test_none_values_are_ignored(self) -> None:
        """None entries in either sequence are dropped before comparison."""
        self.assertEqual(0.0, self.metric.compare(["a", None], ["a"]))


class DiversityMetricTests(DatafakerTestCase):
    """Test case for the DiversityMetric metric."""

    def setUp(self) -> None:
        super().setUp()
        self.metric = DiversityMetric()

    def test_matching_diversity_scores_one(self) -> None:
        """Equally-diverse real and synthetic samples score 1.0."""
        real = ["a", "b", "c", "d"]
        synthetic = ["w", "x", "y", "z"]
        self.assertAlmostEqual(1.0, self.metric.compare(real, synthetic))

    def test_single_repeated_value_has_zero_entropy_on_both_sides(self) -> None:
        """A single repeated value on both sides is a diversity match."""
        self.assertAlmostEqual(1.0, self.metric.compare(["a", "a", "a"], ["b", "b"]))

    def test_less_diverse_synthetic_scores_below_one(self) -> None:
        """A synthetic sample far less diverse than the real one is penalized."""
        real = ["a", "b", "c", "d"]
        synthetic = ["x", "x", "x", "x"]
        score = self.metric.compare(real, synthetic)
        self.assertLess(score, 1.0)
        self.assertGreaterEqual(score, 0.0)

    def test_empty_values_treated_as_zero_entropy(self) -> None:
        """Empty or all-None sequences have zero normalized entropy."""
        self.assertAlmostEqual(1.0, self.metric.compare([], []))
        self.assertAlmostEqual(1.0, self.metric.compare([None, None], [None]))

    def test_score_is_bounded_below_by_zero(self) -> None:
        """The score never goes negative even for maximally different diversity."""
        real = ["a", "a", "a", "a"]
        synthetic = ["a", "b", "c", "d"]
        score = self.metric.compare(real, synthetic)
        self.assertGreaterEqual(score, 0.0)
