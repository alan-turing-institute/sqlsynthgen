"""Unit tests for datafaker.evaluators.column_evaluator."""
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

from datafaker.evaluators.column_evaluator import (
    ColumnEvaluator,
    ColumnStats,
    EvaluationProfile,
    ProposalEvaluation,
    analyse_column,
    choose_numeric_profile,
    choose_profile,
    looks_like_email,
)
from datafaker.proposers.base import ConstantProposer
from tests.utils import DatafakerTestCase


class AnalyseColumnTests(DatafakerTestCase):
    """Test case for analyse_column."""

    def test_counts_rows_and_unique_values(self) -> None:
        """row_count and unique_count reflect the (non-None) input values."""
        stats = analyse_column(["a", "b", "a"])
        self.assertEqual(3, stats.row_count)
        self.assertEqual(2, stats.unique_count)

    def test_none_values_are_excluded(self) -> None:
        """None entries don't count towards row_count or unique_count."""
        stats = analyse_column(["a", None, None])
        self.assertEqual(1, stats.row_count)
        self.assertEqual(1, stats.unique_count)

    def test_avg_length_and_ratios(self) -> None:
        """avg_length, space_ratio, digit_ratio and punctuation_ratio are computed.

        Uses a different count of each (2 spaces, 1 digit, 3 punctuation out
        of 7 characters) so a ratio written to the wrong attribute would
        actually fail the corresponding assertion, rather than all three
        happening to share one value.
        """
        stats = analyse_column(["a  1!!!"])
        self.assertEqual(7, stats.avg_length)
        self.assertAlmostEqual(2 / 7, stats.space_ratio)
        self.assertAlmostEqual(1 / 7, stats.digit_ratio)
        self.assertAlmostEqual(3 / 7, stats.punctuation_ratio)

    def test_empty_input_does_not_divide_by_zero(self) -> None:
        """An empty (or all-None) column yields all-zero stats, not an error."""
        stats = analyse_column([None, None])
        self.assertEqual(0, stats.row_count)
        self.assertEqual(0, stats.avg_length)
        self.assertEqual(0, stats.space_ratio)
        self.assertEqual(0, stats.digit_ratio)
        self.assertEqual(0, stats.punctuation_ratio)

    def test_non_string_values_are_stringified(self) -> None:
        """Non-string values (e.g. ints) are converted to str first."""
        stats = analyse_column([12345])
        self.assertEqual(5, stats.avg_length)
        self.assertEqual(1.0, stats.digit_ratio)


class ColumnStatsTests(DatafakerTestCase):
    """Test case for the ColumnStats dataclass."""

    def test_uniqueness_is_unique_over_row_count(self) -> None:
        """uniqueness divides unique_count by row_count."""
        stats = ColumnStats(
            row_count=4,
            unique_count=2,
            avg_length=0,
            space_ratio=0,
            digit_ratio=0,
            punctuation_ratio=0,
        )
        self.assertEqual(0.5, stats.uniqueness)

    def test_uniqueness_with_zero_rows_does_not_divide_by_zero(self) -> None:
        """A zero row_count is floored to 1 to avoid a ZeroDivisionError."""
        stats = ColumnStats(
            row_count=0,
            unique_count=0,
            avg_length=0,
            space_ratio=0,
            digit_ratio=0,
            punctuation_ratio=0,
        )
        self.assertEqual(0.0, stats.uniqueness)


class ChooseProfileTests(DatafakerTestCase):
    """Test case for choose_profile."""

    def _stats(
        self, avg_length=10.0, uniqueness_row=(8, 10), space_ratio=0.0
    ) -> ColumnStats:
        unique_count, row_count = uniqueness_row
        return ColumnStats(
            row_count=row_count,
            unique_count=unique_count,
            avg_length=avg_length,
            space_ratio=space_ratio,
            digit_ratio=0.0,
            punctuation_ratio=0.0,
        )

    def test_short_and_mostly_unique_is_short_text(self) -> None:
        """Short, mostly-distinct values look like names/identifiers."""
        stats = self._stats(avg_length=10, uniqueness_row=(9, 10))
        self.assertEqual(EvaluationProfile.SHORT_TEXT, choose_profile(stats))

    def test_low_uniqueness_is_categorical(self) -> None:
        """Values repeated often enough look like a small category set."""
        stats = self._stats(avg_length=10, uniqueness_row=(1, 100))
        self.assertEqual(EvaluationProfile.CATEGORICAL, choose_profile(stats))

    def test_long_average_length_is_free_text(self) -> None:
        """Long values that aren't near-unique look like free text."""
        stats = self._stats(avg_length=60, uniqueness_row=(5, 10))
        self.assertEqual(EvaluationProfile.FREE_TEXT, choose_profile(stats))

    def test_high_space_ratio_is_free_text(self) -> None:
        """Many spaces per character looks like prose even if average length is short."""
        stats = self._stats(avg_length=10, uniqueness_row=(5, 10), space_ratio=0.2)
        self.assertEqual(EvaluationProfile.FREE_TEXT, choose_profile(stats))

    def test_fallback_is_short_text(self) -> None:
        """Anything not matching the other rules falls back to SHORT_TEXT."""
        stats = self._stats(avg_length=35, uniqueness_row=(5, 10), space_ratio=0.05)
        self.assertEqual(EvaluationProfile.SHORT_TEXT, choose_profile(stats))


class ChooseNumericProfileTests(DatafakerTestCase):
    """Test case for choose_numeric_profile."""

    def test_low_cardinality_is_categorical(self) -> None:
        """Few distinct values among many rows looks like a status/flag column."""
        values = [1, 2] * 10  # 2 distinct out of 20: uniqueness 0.1
        self.assertEqual(EvaluationProfile.CATEGORICAL, choose_numeric_profile(values))

    def test_high_cardinality_is_identifier(self) -> None:
        """Mostly-distinct numeric values look like an id/age/salary column."""
        values = list(range(100))
        self.assertEqual(EvaluationProfile.IDENTIFIER, choose_numeric_profile(values))

    def test_all_none_defaults_to_identifier(self) -> None:
        """An empty/all-None column can't be judged, so defaults to IDENTIFIER."""
        self.assertEqual(
            EvaluationProfile.IDENTIFIER, choose_numeric_profile([None, None])
        )

    def test_none_values_are_excluded_from_the_ratio(self) -> None:
        """None entries don't count as either distinct or total values."""
        # 2 distinct values among 20 non-null entries: uniqueness 0.1. If the
        # Nones were counted in the denominator without being excluded first,
        # this would look even more categorical, not less - the point here is
        # just that the Nones don't crash or skew the ratio unexpectedly.
        values = [1] * 19 + [2] + [None] * 5
        self.assertEqual(EvaluationProfile.CATEGORICAL, choose_numeric_profile(values))


class LooksLikeEmailTests(DatafakerTestCase):
    """Test case for looks_like_email."""

    def test_mostly_email_values_is_true(self) -> None:
        """A column where most values look like emails is detected."""
        values = ["a@example.com", "b@example.com", "not-an-email"]
        self.assertTrue(looks_like_email(values))

    def test_mostly_non_email_values_is_false(self) -> None:
        """A column where most values don't look like emails is rejected."""
        values = ["a@example.com", "plain text", "another plain value"]
        self.assertFalse(looks_like_email(values))

    def test_empty_input_is_false(self) -> None:
        """No values at all can't look like emails."""
        self.assertFalse(looks_like_email([]))
        self.assertFalse(looks_like_email([None, "  "]))

    def test_value_without_local_part_or_domain_is_not_email_like(self) -> None:
        """'@' alone, or with an empty side, doesn't count as email-like."""
        values = ["@example.com", "a@", "@"]
        self.assertFalse(looks_like_email(values))


class ProposalEvaluationTests(DatafakerTestCase):
    """Test case for the ProposalEvaluation dataclass's display string."""

    def test_str_includes_key_metrics_and_criterion_scores(self) -> None:
        """The string form surfaces the headline scores and per-criterion detail."""
        evaluation = ProposalEvaluation(
            proposer=ConstantProposer("x"),
            novelty=0.5,
            diversity=0.25,
            overall_score=0.125,
            criterion_scores={"length": 0.1, "words": 0.2},
            copy_fraction=0.0,
            synthetic_uniqueness=1.0,
        )
        text = str(evaluation)
        self.assert_str_in("dist_gen.constant", text)
        self.assert_str_in("0.125000", text)
        self.assert_str_in("0.500000", text)
        self.assert_str_in("0.250000", text)
        self.assert_str_in("length", text)
        self.assert_str_in("words", text)


class ColumnEvaluatorIntegrationTests(DatafakerTestCase):
    """End-to-end test of ColumnEvaluator against a real (DuckDB) table."""

    def test_evaluate_a_constant_proposer_against_a_categorical_column(self) -> None:
        """Construction profiles the column; evaluate() scores a real proposer."""
        engine = create_engine("duckdb:///:memory:")
        metadata = MetaData()
        table = Table(
            "statuses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("status", String),
        )
        metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(
                table.insert(),
                [{"id": i, "status": ["active", "inactive"][i % 2]} for i in range(20)],
            )

        evaluator = ColumnEvaluator([table.c.status], engine)
        self.assertEqual(EvaluationProfile.CATEGORICAL, evaluator.profile)

        result = evaluator.evaluate(ConstantProposer("active"))
        self.assertIsInstance(result, ProposalEvaluation)
        self.assertEqual(1.0, result.copy_fraction)
        # A constant proposer emits the same value every time: only one
        # distinct value out of the whole synthetic sample.
        self.assertLess(result.synthetic_uniqueness, 0.01)
        self.assertIn("category", result.criterion_scores)
