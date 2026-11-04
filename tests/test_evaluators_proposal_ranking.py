"""Unit tests for datafaker.evaluators.proposal_ranking."""
from typing import Any

from sqlalchemy import Column, MetaData, Table, Text

from datafaker.evaluators.column_evaluator import EvaluationProfile, ProposalEvaluation
from datafaker.evaluators.proposal_ranking import (
    ProposalRanking,
    _crowding_distances,
    _pareto_fronts,
    format_ranking_display,
    keyword_match,
    normalize_list,
    rank_proposals,
)
from datafaker.proposers.base import Proposer
from datafaker.proposers.choice import UniformChoiceProposer
from tests.utils import DatafakerTestCase


class FakeProposer(Proposer):
    """A minimal, DB-free stand-in for a real Proposer, for ranking tests."""

    def __init__(self, name: str) -> None:
        self._name = name

    def function_name(self) -> str:
        return self._name

    def nominal_kwargs(self) -> dict[str, str]:
        return {}

    def actual_kwargs(self) -> dict[str, Any]:
        return {}

    def generate_data(self, count: int) -> list[Any]:
        return [None] * count


def make_result(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    name: str = "generic.text",
    overall_score: float = 0.5,
    novelty: float = 0.5,
    diversity: float = 0.5,
    copy_fraction: float = 0.0,
    synthetic_uniqueness: float = 1.0,
    proposer: Proposer | None = None,
) -> ProposalEvaluation:
    """Build a ProposalEvaluation for ranking tests."""
    return ProposalEvaluation(
        proposer=proposer or FakeProposer(name),
        novelty=novelty,
        diversity=diversity,
        overall_score=overall_score,
        criterion_scores={},
        copy_fraction=copy_fraction,
        synthetic_uniqueness=synthetic_uniqueness,
    )


class KeywordMatchTests(DatafakerTestCase):
    """Test case for keyword_match."""

    def test_matching_keyword_and_generator_gives_boost(self) -> None:
        """A column/generator pair that both mention 'email' gets the boost."""
        boost, keyword = keyword_match("customer_email", "generic.person.email")
        self.assertEqual(0.15, boost)
        self.assertEqual("email", keyword)

    def test_compound_column_name_matches_by_substring(self) -> None:
        """Substring matching catches compound names like 'customer_first_name'."""
        boost, keyword = keyword_match(
            "customer_first_name", "generic.person.first_name"
        )
        self.assertEqual(0.15, boost)
        self.assertEqual("first_name", keyword)

    def test_no_column_name_gives_no_boost(self) -> None:
        """A missing column name can't match anything."""
        self.assertEqual((0.0, None), keyword_match(None, "generic.person.email"))
        self.assertEqual((0.0, None), keyword_match("", "generic.person.email"))

    def test_column_matches_but_generator_does_not(self) -> None:
        """Column name hints at a kind, but this generator isn't of that kind."""
        boost, keyword = keyword_match("email", "generic.text.word")
        self.assertEqual(0.0, boost)
        self.assertIsNone(keyword)

    def test_is_case_insensitive(self) -> None:
        """Matching ignores case in both the column and generator names."""
        boost, keyword = keyword_match("EMAIL", "GENERIC.PERSON.EMAIL")
        self.assertEqual(0.15, boost)
        self.assertEqual("email", keyword)


class NormalizeListTests(DatafakerTestCase):
    """Test case for normalize_list."""

    def test_empty_list(self) -> None:
        """An empty input returns an empty output."""
        self.assertEqual([], normalize_list([]))

    def test_spread_out_values_map_to_zero_one(self) -> None:
        """Values are linearly rescaled so the min is 0 and the max is 1."""
        self.assertEqual([0.0, 0.5, 1.0], normalize_list([10, 20, 30]))

    def test_all_identical_values_are_treated_as_a_tie(self) -> None:
        """Exactly equal values become 0.5 rather than dividing by zero."""
        self.assertEqual([0.5, 0.5, 0.5], normalize_list([3.0, 3.0, 3.0]))

    def test_near_identical_values_are_also_treated_as_a_tie(self) -> None:
        """Floating-point noise around a shared value doesn't get stretched out."""
        result = normalize_list([0.6931471805599453, 0.6931471805599454])
        self.assertEqual([0.5, 0.5], result)

    def test_single_value_is_a_tie(self) -> None:
        """A single value has no spread, so it's treated as a tie too."""
        self.assertEqual([0.5], normalize_list([42.0]))


class ParetoFrontsTests(DatafakerTestCase):
    """Test case for the internal _pareto_fronts helper."""

    def test_strictly_dominated_point_is_in_a_later_front(self) -> None:
        """A point beaten or matched on every dimension lands behind the winner."""
        points = [(1.0, 1.0, 1.0), (0.5, 0.5, 0.5)]
        fronts = _pareto_fronts(points)
        self.assertEqual([[0], [1]], fronts)

    def test_non_dominated_points_share_the_first_front(self) -> None:
        """Points that each win on a different dimension are mutually non-dominated."""
        points = [(1.0, 0.0, 0.0), (0.0, 1.0, 0.0), (0.0, 0.0, 1.0)]
        fronts = _pareto_fronts(points)
        self.assertEqual([[0, 1, 2]], fronts)

    def test_empty_points(self) -> None:
        """No points means no fronts."""
        self.assertEqual([], _pareto_fronts([]))


class CrowdingDistancesTests(DatafakerTestCase):
    """Test case for the internal _crowding_distances helper."""

    def test_front_boundary_points_get_infinite_crowding(self) -> None:
        """The extreme points of a front are always maximally 'crowded out'."""
        points = [(0.0, 0.0, 0.0), (0.5, 0.5, 0.5), (1.0, 1.0, 1.0)]
        fronts = [[0, 1, 2]]
        objectives = (
            [p[0] for p in points],
            [p[1] for p in points],
            [p[2] for p in points],
        )
        crowding = _crowding_distances(points, fronts, objectives)
        self.assertEqual(float("inf"), crowding[0])
        self.assertEqual(float("inf"), crowding[2])
        self.assertLess(crowding[1], float("inf"))

    def test_singleton_front_gets_infinite_crowding(self) -> None:
        """A front with just one point has nothing to be crowded by."""
        points = [(0.5, 0.5, 0.5)]
        fronts = [[0]]
        objectives = ([0.5], [0.5], [0.5])
        crowding = _crowding_distances(points, fronts, objectives)
        self.assertEqual([float("inf")], crowding)


class RankProposalsTests(DatafakerTestCase):
    """Test case for rank_proposals."""

    def test_no_results_gives_an_empty_ranking(self) -> None:
        """With nothing to rank, no recommendation is made."""
        ranking = rank_proposals([], EvaluationProfile.SHORT_TEXT)
        self.assertIsNone(ranking.recommended_index)
        self.assertIsNone(ranking.recommended_reason)
        self.assertEqual([], ranking.rows)
        self.assertEqual((0.5, 0.25, 0.25), ranking.weights)

    def test_single_result_is_trivially_recommended(self) -> None:
        """A single candidate is always the recommendation."""
        results = [make_result("only_one")]
        ranking = rank_proposals(results, EvaluationProfile.SHORT_TEXT)
        self.assertEqual(0, ranking.recommended_index)
        self.assertEqual("best combined score", ranking.recommended_reason)
        self.assertEqual(1, len(ranking.rows))

    def test_lower_error_wins_on_fidelity(self) -> None:
        """Among otherwise-equal candidates, the one with lower overall_score wins."""
        results = [
            make_result("bad_fit", overall_score=0.9, novelty=0.5, diversity=0.5),
            make_result("good_fit", overall_score=0.1, novelty=0.5, diversity=0.5),
        ]
        ranking = rank_proposals(results, EvaluationProfile.IDENTIFIER)
        self.assertEqual(1, ranking.recommended_index)

    def test_identifier_profile_weighs_fidelity_heavily(self) -> None:
        """IDENTIFIER profile weights are (0.85, 0.05, 0.10)."""
        ranking = rank_proposals([make_result()], EvaluationProfile.IDENTIFIER)
        self.assertEqual((0.85, 0.05, 0.10), ranking.weights)

    def test_unknown_profile_falls_back_to_default_weights(self) -> None:
        """A profile with no explicit entry uses the (0.5, 0.25, 0.25) default."""
        ranking = rank_proposals([make_result()], None)
        self.assertEqual((0.5, 0.25, 0.25), ranking.weights)

    def test_exact_tie_breaks_towards_the_earlier_candidate(self) -> None:
        """Identical candidates are both max-crowding; the first one wins."""
        results = [make_result("first"), make_result("second")]
        ranking = rank_proposals(results, EvaluationProfile.SHORT_TEXT)
        self.assertEqual(0, ranking.recommended_index)
        self.assertEqual("tiebreak by crowding distance", ranking.recommended_reason)

    def test_keyword_boost_can_flip_the_recommendation(self) -> None:
        """A column-name/generator match can outweigh an otherwise-tied fidelity."""
        results = [
            make_result(
                "generic.text.word", overall_score=0.1, novelty=0.5, diversity=0.5
            ),
            make_result(
                "generic.person.email", overall_score=0.1, novelty=0.5, diversity=0.5
            ),
        ]
        # Without the keyword boost this is an exact tie, decided by crowding
        # distance (which favors the first candidate); the boost should flip it.
        ranking = rank_proposals(
            results, EvaluationProfile.EMAIL, column_name="user_email"
        )
        self.assertEqual(1, ranking.recommended_index)
        self.assertEqual("best combined score", ranking.recommended_reason)

    def test_choice_proposer_penalized_by_resampling_on_unique_column(self) -> None:
        """ChoiceProposer's score is discounted by real_uniqueness x copy_fraction."""
        table = Table("t", MetaData(), Column("c", Text()))
        choice_proposer = UniformChoiceProposer(table.c.c, ["a", "b"], [1, 1])
        results = [
            make_result(overall_score=0.0, copy_fraction=1.0, proposer=choice_proposer),
            make_result(overall_score=0.5, copy_fraction=0.0),
        ]
        ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, real_uniqueness=1.0
        )
        # The ChoiceProposer perfectly fits but is fully discounted (penalty
        # multiplier 0), leaving the other candidate to win despite a worse fit.
        self.assertEqual(1, ranking.recommended_index)

    def test_non_choice_proposer_is_not_resample_penalized_on_string_column(
        self,
    ) -> None:
        """Only ChoiceProposer (or a numeric column) is hit by the resample penalty."""
        results = [make_result(overall_score=0.0, copy_fraction=1.0)]
        ranking = rank_proposals(
            results, EvaluationProfile.SHORT_TEXT, real_uniqueness=1.0
        )
        self.assertFalse(ranking.penalty_applies_to_all)
        # combined score should be unaffected by the (non-applicable) penalty:
        # a single candidate normalizes every dimension to a 0.5 tie.
        fid_w, nov_w, div_w = ranking.weights
        expected = (fid_w + nov_w + div_w) * 0.5
        self.assertAlmostEqual(expected, ranking.scores[0])

    def test_numeric_column_penalizes_every_proposer(self) -> None:
        """A numeric column applies the resample-style penalty to all candidates."""
        results = [make_result(overall_score=0.0, copy_fraction=1.0)]
        ranking = rank_proposals(
            results,
            EvaluationProfile.IDENTIFIER,
            real_uniqueness=1.0,
            is_numeric_column=True,
        )
        self.assertTrue(ranking.penalty_applies_to_all)
        self.assertEqual(0.0, ranking.scores[0])

    def test_self_duplication_penalty_applies_regardless_of_proposer_type(self) -> None:
        """Low synthetic_uniqueness is penalized even for a non-resampling proposer."""
        results = [
            make_result("full_of_dupes", overall_score=0.0, synthetic_uniqueness=0.0),
            make_result("all_unique", overall_score=0.0, synthetic_uniqueness=1.0),
        ]
        ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, real_uniqueness=1.0
        )
        self.assertEqual(1, ranking.recommended_index)

    def test_no_uniqueness_guarantee_when_every_candidate_duplicates(self) -> None:
        """A primary key column with only duplicate-prone candidates has no safe pick."""
        results = [
            make_result("dupe_a", synthetic_uniqueness=0.5),
            make_result("dupe_b", synthetic_uniqueness=0.9),
        ]
        ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, is_primary_key=True
        )
        self.assertTrue(ranking.no_uniqueness_guarantee)

    def test_uniqueness_guarantee_satisfied_by_one_candidate(self) -> None:
        """If at least one candidate guarantees uniqueness, there's no warning."""
        results = [
            make_result("dupe", synthetic_uniqueness=0.5),
            make_result("unique", synthetic_uniqueness=1.0),
        ]
        ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, is_primary_key=True
        )
        self.assertFalse(ranking.no_uniqueness_guarantee)

    def test_combined_score_is_clamped_to_one(self) -> None:
        """The keyword boost can't push a score above 1.0."""
        results = [make_result("generic.person.email", overall_score=0.0)]
        ranking = rank_proposals(results, EvaluationProfile.EMAIL, column_name="email")
        self.assertLessEqual(ranking.scores[0], 1.0)

    def test_weak_recommendation_warning_when_best_fit_was_suppressed(self) -> None:
        """A heavily-discounted best-fit candidate triggers the fallback warning."""
        table = Table("t", MetaData(), Column("c", Text()))
        choice_proposer = UniformChoiceProposer(table.c.c, ["a", "b"], [1, 1])
        results = [
            # Fits almost perfectly but is a resampler on a near-unique column:
            # gets discounted to (near) zero.
            make_result(
                overall_score=0.0,
                novelty=0.0,
                diversity=0.0,
                copy_fraction=1.0,
                synthetic_uniqueness=1.0,
                proposer=choice_proposer,
            ),
            # A poor fit that isn't penalized, so it "wins" by elimination.
            # synthetic_uniqueness is deliberately below the guarantee
            # threshold so the weak-recommendation warning isn't suppressed
            # by the "winner guarantees uniqueness" exemption.
            make_result(
                overall_score=1.0,
                novelty=0.0,
                diversity=0.0,
                copy_fraction=0.0,
                synthetic_uniqueness=0.5,
                proposer=FakeProposer("generic.text.word"),
            ),
        ]
        ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, real_uniqueness=1.0
        )
        self.assertEqual(1, ranking.recommended_index)
        self.assertIsNotNone(ranking.weak_recommendation_warning)
        self.assert_str_in("resample penalty", ranking.weak_recommendation_warning)


class FormatRankingDisplayTests(DatafakerTestCase):
    """Test case for format_ranking_display."""

    def test_recommendation_line_names_the_winner(self) -> None:
        """The recommendation string names the recommended candidate by number/name."""
        results = [make_result("winner", overall_score=0.0)]
        ranking = rank_proposals(results, EvaluationProfile.SHORT_TEXT)
        display = format_ranking_display(ranking, results, EvaluationProfile.SHORT_TEXT)
        self.assert_str_in("Recommended: 1. winner", display.recommendation)
        self.assert_str_in("best combined score", display.recommendation)

    def test_no_uniqueness_guarantee_overrides_recommendation_text(self) -> None:
        """When no candidate can guarantee uniqueness, no winner is named."""
        results = [make_result("a", synthetic_uniqueness=0.5)]
        ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, is_primary_key=True
        )
        display = format_ranking_display(ranking, results, EvaluationProfile.IDENTIFIER)
        self.assert_str_in("Recommended: none", display.recommendation)

    def test_penalty_scope_text_reflects_numeric_flag(self) -> None:
        """The profile summary explains whether the penalty hit every candidate."""
        results = [make_result("a")]
        numeric_ranking = rank_proposals(
            results, EvaluationProfile.IDENTIFIER, is_numeric_column=True
        )
        numeric_display = format_ranking_display(
            numeric_ranking, results, EvaluationProfile.IDENTIFIER
        )
        self.assert_str_in(
            "every generator (numeric column)", numeric_display.profile_summary
        )

        string_ranking = rank_proposals(results, EvaluationProfile.SHORT_TEXT)
        string_display = format_ranking_display(
            string_ranking, results, EvaluationProfile.SHORT_TEXT
        )
        self.assert_str_in(
            "resamplers only (dist_gen.choice/weighted_choice/zipf_choice)",
            string_display.profile_summary,
        )

    def test_rows_and_fronts_pass_through_unchanged(self) -> None:
        """rows and fronts are carried over verbatim from the ProposalRanking."""
        results = [make_result("winner")]
        ranking = ProposalRanking(
            recommended_index=0,
            recommended_reason="best combined score",
            rows=[("1", "x")],
            profile=EvaluationProfile.SHORT_TEXT,
            weights=(0.5, 0.25, 0.25),
            fronts=[1],
        )
        display = format_ranking_display(ranking, results, EvaluationProfile.SHORT_TEXT)
        self.assertEqual([("1", "x")], display.rows)
        self.assertEqual([1], display.fronts)
