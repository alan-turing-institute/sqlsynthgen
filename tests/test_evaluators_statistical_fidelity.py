"""Unit tests for datafaker.evaluators.statistical_fidelity."""
from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine

from datafaker.evaluators.column_evaluator import EvaluationProfile
from datafaker.evaluators.statistical_fidelity import (
    EMAIL_CRITERIA,
    PROFILE_CRITERIA,
    StatisticalFidelity,
)
from tests.utils import DatafakerTestCase


class ProfileCriteriaTests(DatafakerTestCase):
    """Structural sanity checks for the built-in evaluation criteria."""

    def test_every_evaluation_profile_has_criteria(self) -> None:
        """Each EvaluationProfile enum member maps to configured criteria."""
        for profile in EvaluationProfile:
            self.assertIn(profile, PROFILE_CRITERIA)
            self.assertTrue(PROFILE_CRITERIA[profile])

    def test_criterion_weights_sum_to_one(self) -> None:
        """Each profile's criterion weights add up to 1.0, for a sane weighted average."""
        for profile, criteria in PROFILE_CRITERIA.items():
            with self.subTest(profile=profile.name):
                total_weight = sum(c.weight for c in criteria)
                self.assertAlmostEqual(1.0, total_weight)

    def test_criterion_names_are_unique_within_a_profile(self) -> None:
        """Criterion names double as dict keys in calculate_scores, so must be unique."""
        for profile, criteria in PROFILE_CRITERIA.items():
            with self.subTest(profile=profile.name):
                names = [c.name for c in criteria]
                self.assertEqual(len(names), len(set(names)))


def _make_table(columns: list[Column], rows: list[dict]) -> tuple:
    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()
    table = Table("data", metadata, *columns)
    metadata.create_all(engine)
    if rows:
        with engine.begin() as conn:
            conn.execute(table.insert(), rows)
    return engine, table


class StatisticalFidelityTests(DatafakerTestCase):
    """Test case for StatisticalFidelity."""

    def test_identical_real_and_synthetic_data_scores_perfectly(self) -> None:
        """A synthetic sample identical to the real data has zero divergence."""
        engine, table = _make_table(
            [Column("email", String)],
            [{"email": f"user{i}@example.com"} for i in range(30)],
        )
        fidelity = StatisticalFidelity(table.c.email, engine, sample_size=1000)
        fidelity.set_eval_criteria(EvaluationProfile.EMAIL)

        synthetic = [f"user{i}@example.com" for i in range(30)]
        overall_score, criterion_scores = fidelity.calculate_scores(synthetic)

        self.assertAlmostEqual(0.0, overall_score)
        self.assertEqual({c.name for c in EMAIL_CRITERIA}, set(criterion_scores))
        for name, score in criterion_scores.items():
            with self.subTest(criterion=name):
                self.assertAlmostEqual(0.0, score)

    def test_completely_different_synthetic_data_scores_worse(self) -> None:
        """A synthetic sample sharing nothing with the real data scores worse."""
        engine, table = _make_table(
            [Column("category", String)],
            [{"category": "alpha"} for _ in range(30)],
        )
        fidelity = StatisticalFidelity(table.c.category, engine, sample_size=1000)
        fidelity.set_eval_criteria(EvaluationProfile.CATEGORICAL)

        matching_score, _ = fidelity.calculate_scores(["alpha"] * 30)
        different_score, _ = fidelity.calculate_scores(["zzz_never_seen"] * 30)

        self.assertGreater(different_score, matching_score)

    def test_temporal_profile_scores_a_matching_date_column(self) -> None:
        """The TEMPORAL profile's two criteria run cleanly against a date column."""
        engine, table = _make_table(
            [Column("ts", DateTime)],
            [{"ts": datetime(2020, 1, (i % 27) + 1)} for i in range(40)],
        )
        fidelity = StatisticalFidelity(table.c.ts, engine, sample_size=1000)
        fidelity.set_eval_criteria(EvaluationProfile.TEMPORAL)

        synthetic = [datetime(2020, 1, (i % 27) + 1) for i in range(40)]
        overall_score, criterion_scores = fidelity.calculate_scores(synthetic)

        self.assertAlmostEqual(0.0, overall_score)
        self.assertEqual({"timestamp", "day_of_week"}, set(criterion_scores))

    def test_no_criteria_set_scores_zero(self) -> None:
        """Without calling set_eval_criteria, there's nothing to score."""
        engine, table = _make_table(
            [Column("n", Integer)], [{"n": i} for i in range(10)]
        )
        fidelity = StatisticalFidelity(table.c.n, engine, sample_size=1000)
        overall_score, criterion_scores = fidelity.calculate_scores([1, 2, 3])
        self.assertEqual(0.0, overall_score)
        self.assertEqual({}, criterion_scores)
