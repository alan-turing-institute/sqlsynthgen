"""Utilities for ranking proposal evaluations against multiple objectives."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Sequence

from datafaker.evaluators.column_evaluator import ProposalEvaluation
from datafaker.evaluators.evaluation_profile import EvaluationProfile
from datafaker.proposers.choice import ChoiceProposer
from datafaker.theme import THEME, Theme, ThemeEntry

# Column-name keywords mapped to substrings of the generator's dotted name
# (e.g. "generic.person.first_name") that they hint at. Column naming
# conventions are a strong, cheap, complementary signal to the statistical
# fidelity/novelty/diversity evaluation: they catch cases the statistics
# genuinely can't discriminate (e.g. a column with a small realistic
# vocabulary where every candidate's exact-value fidelity ties) without
# having to rely on the generator library's own internal naming, which
# rarely matches real-world column names anyway.
#
# This is a soft boost, not a filter: a column name matching nothing here,
# or a generator matching no hint, is entirely unaffected - it never
# excludes a candidate the statistics would otherwise have picked. Keep this
# list small and grow it opportunistically as real columns turn up cases
# worth covering, rather than trying to anticipate every naming convention.
KEYWORD_GENERATOR_HINTS: list[tuple[list[str], list[str]]] = [
    (
        ["first_name", "firstname", "fname", "given_name", "forename"],
        ["person.first_name"],
    ),
    (
        ["last_name", "lastname", "lname", "surname", "family_name"],
        ["person.last_name"],
    ),
    (["full_name", "fullname"], ["person.full_name"]),
    (["email"], ["person.email"]),
    (["phone", "telephone", "mobile"], ["person.telephone", "address.calling_code"]),
    (["country"], ["address.country", "address.country_code"]),
    (["city"], ["address.city"]),
    (
        ["street"],
        ["address.street_name", "address.street_number", "address.street_suffix"],
    ),
    (["postcode", "postal_code", "zip"], ["address.postal_code"]),
    (["gender", "sex"], ["person.gender"]),
    (["occupation", "job_title"], ["person.occupation"]),
    (["nationality"], ["person.nationality"]),
    (["username", "login"], ["person.username"]),
]

# Small enough not to override a clearly-better statistical result, large
# enough to break the near-ties this is meant to resolve.
KEYWORD_BOOST = 0.15

# Below this fraction of distinct values in a candidate's own 4000-value
# synthetic sample, it is producing enough duplicates against itself that it
# could not satisfy a real PRIMARY KEY/UNIQUE constraint once row counts grow
# past a handful - regardless of how well it otherwise fits the real
# distribution. Slack below 1.0 tolerates a rare coincidental collision from
# a genuinely-unique-generating proposer (e.g. a continuous float sampler)
# without treating it the same as a resampler or constant that duplicates
# constantly.
SYNTHETIC_UNIQUENESS_GUARANTEE_THRESHOLD = 0.999

# The weak-recommendation warning (below) is only worth showing when the
# winner's own combined score is actually weak - otherwise it's just noise
# on top of a perfectly good pick.
WEAK_WINNER_SCORE_THRESHOLD = 0.5

# Floor applied to the self-duplication penalty (see rank_proposals) for a
# column that doesn't actually need guaranteed-unique values (no PRIMARY
# KEY/UNIQUE constraint). Self-duplication still matters there - a
# generator whose synthetic sample is mostly repeats of a tiny fixed pool
# (e.g. a canned-quote generator) makes for an obviously repetitive column
# even without breaking a constraint - but it shouldn't be crushed as hard
# as it would be for a column where duplicates are a correctness bug.
SELF_DUPLICATION_PENALTY_FLOOR = 0.4


def keyword_match(
    column_name: str | None, proposer_name: str
) -> tuple[float, str | None]:
    """Give a soft ranking boost when the column name hints at this generator's kind.

    Returns the boost plus the specific column keyword responsible (or None)
    so the caller can show why.

    Uses substring matching (not exact tokenization), so this also catches
    compound names like "customer_first_name".
    """
    if not column_name:
        return 0.0, None
    name = column_name.lower()
    generator_name = proposer_name.lower()
    for column_keywords, generator_keywords in KEYWORD_GENERATOR_HINTS:
        matched_keyword = next((kw for kw in column_keywords if kw in name), None)
        if matched_keyword is not None and any(
            gkw in generator_name for gkw in generator_keywords
        ):
            return KEYWORD_BOOST, matched_keyword
    return 0.0, None


@dataclass
class ProposalRankingDisplay:
    """Formatted text for a ranked proposal table."""

    recommendation: str | None
    profile_summary: str
    rows: list[tuple[str, ...]]
    fronts: list[int]
    weak_recommendation_warning: str | None = None
    no_uniqueness_guarantee: bool = False
    type_incompatible: list[bool] = field(default_factory=list)


# pylint: disable=too-many-instance-attributes
@dataclass
class ProposalRanking:
    """A ranked set of proposal evaluations."""

    recommended_index: int | None
    recommended_reason: str | None
    rows: list[tuple[str, ...]]
    profile: EvaluationProfile | None
    weights: tuple[float, float, float]
    real_uniqueness: float = 0.0
    penalty_applies_to_all: bool = False
    self_duplication_floored: bool = False
    fronts: list[int] = field(default_factory=list)
    scores: list[float] = field(default_factory=list)
    weak_recommendation_warning: str | None = None
    no_uniqueness_guarantee: bool = False
    type_incompatible: list[bool] = field(default_factory=list)


def normalize_list(vals: Sequence[float]) -> list[float]:
    """Normalize values to the range [0, 1] for comparisons."""
    if not vals:
        return []
    lo = min(vals)
    hi = max(vals)
    # Treat near-identical values as a tie rather than an exact one: e.g.
    # when every candidate's synthetic sample shares no categories with the
    # real data, their Jensen-Shannon divergence all sit at the same
    # theoretical maximum (ln 2) but differ in the 10th+ decimal place from
    # floating-point rounding. Min-max normalizing that noise would stretch
    # it across the full [0, 1] range and manufacture a fake ranking.
    if math.isclose(hi, lo, rel_tol=1e-9, abs_tol=1e-12):
        return [0.5 for _ in vals]
    return [(v - lo) / (hi - lo) for v in vals]


def _pareto_fronts(points: Sequence[tuple[float, float, float]]) -> list[list[int]]:
    """Compute Pareto fronts for a list of 3D points."""
    remaining = set(range(len(points)))
    fronts: list[list[int]] = []
    while remaining:
        current_front: set[int] = set()
        for i in remaining:
            dominated = False
            for j in remaining:
                if i == j:
                    continue
                if all(
                    points[j][d] >= points[i][d] for d in range(len(points[i]))
                ) and any(points[j][d] > points[i][d] for d in range(len(points[i]))):
                    dominated = True
                    break
            if not dominated:
                current_front.add(i)
        fronts.append(sorted(current_front))
        remaining -= current_front
    return fronts


def _crowding_distances(
    points: Sequence[tuple[float, float, float]],
    fronts: Sequence[Sequence[int]],
    objectives: tuple[Sequence[float], Sequence[float], Sequence[float]],
) -> list[float]:
    """Compute NSGA-II style crowding distances."""
    crowding = [0.0 for _ in range(len(points))]
    for front in fronts:
        if len(front) <= 1:
            for idx in front:
                crowding[idx] = float("inf")
            continue
        for dim_vals in objectives:
            vals = [(idx, dim_vals[idx]) for idx in front]
            vals.sort(key=lambda item: item[1])
            lo = vals[0][1]
            hi = vals[-1][1]
            crowding[vals[0][0]] = float("inf")
            crowding[vals[-1][0]] = float("inf")
            if hi == lo:
                continue
            for k in range(1, len(vals) - 1):
                prev_v = vals[k - 1][1]
                next_v = vals[k + 1][1]
                dist = (next_v - prev_v) / (hi - lo)
                idx = vals[k][0]
                if crowding[idx] != float("inf"):
                    crowding[idx] += dist
    return crowding


def _penalty_for_candidate(
    result: ProposalEvaluation,
    real_uniqueness: float,
    apply_to_every_proposer: bool,
    needs_uniqueness: bool,
) -> float:
    """Combine the resample and self-duplication penalties for one candidate.

    ChoiceProposer and its variants (dist_gen.choice/weighted_choice/
    zipf_choice) resample directly from the column's own observed values -
    by construction, they can only ever emit values already in the real
    data. That's the *correct* thing to do for a column with a small,
    shared vocabulary (a status code, a gender) - reproducing real values
    there is expected and unavoidable - but a privacy problem for a column
    whose real values are meant to be unique per row (an email, a genuine
    ID). Numeric columns get the same treatment regardless of proposer
    (apply_to_every_proposer): a plain number has no shared human
    vocabulary to explain a coincidental match, so a high copy_fraction
    there means the real value range is dense, not a legitimate overlap.

    Separately, self_duplication_penalty catches a candidate duplicating
    against its OWN output (synthetic_uniqueness) rather than against the
    real data (copy_fraction, above) - e.g. dist_gen.constant on a column
    whose fixed value happens not to appear in the sampled real data gets
    copy_fraction=0 (no resample penalty) despite duplicating every row
    against itself. It applies to every candidate, but its full strength
    is only warranted when the column actually needs unique values (a
    PRIMARY KEY/UNIQUE constraint); it's floored otherwise, since a merely
    observed-unique column (e.g. free-text descriptions that happen to all
    differ) doesn't have a correctness reason to punish a small,
    repetitive-but-otherwise-plausible pool as hard as a real key would.

    :param result: the candidate's own evaluation, supplying its
        ``copy_fraction`` (resample penalty) and ``synthetic_uniqueness``
        (self-duplication penalty).
    :param real_uniqueness: fraction of the real column's sampled values
        that are distinct - scales how hard both penalties bite.
    :param apply_to_every_proposer: whether the resample penalty applies to
        every candidate (numeric columns) rather than just ``ChoiceProposer``
        and its variants.
    :param needs_uniqueness: whether the column actually needs unique values
        (a PRIMARY KEY/UNIQUE constraint) - if not, the self-duplication
        penalty is floored at ``SELF_DUPLICATION_PENALTY_FLOOR`` instead of
        reaching full strength.
    :return: the combined penalty multiplier (lower is worse) to apply to
        the candidate's score.
    """
    resample_penalty = 1.0
    if isinstance(result.proposer, ChoiceProposer) or apply_to_every_proposer:
        resample_penalty = 1.0 - real_uniqueness * result.copy_fraction
    self_duplication_penalty = 1.0 - real_uniqueness * (
        1.0 - result.synthetic_uniqueness
    )
    if not needs_uniqueness:
        self_duplication_penalty = max(
            self_duplication_penalty, SELF_DUPLICATION_PENALTY_FLOOR
        )
    return resample_penalty * self_duplication_penalty


# pylint: disable=too-many-arguments too-many-positional-arguments
# pylint: disable=too-many-locals too-many-statements too-many-branches
def rank_proposals(
    results: Sequence[ProposalEvaluation],
    profile: EvaluationProfile | None,
    theme: Theme = THEME[ThemeEntry.NONE],
    column_name: str | None = None,
    real_uniqueness: float = 0.0,
    is_numeric_column: bool = False,
    is_primary_key: bool = False,
    is_unique_constrained: bool = False,
    type_incompatible: Sequence[bool] | None = None,
) -> ProposalRanking:
    """Rank proposals by fidelity, novelty and diversity using Pareto fronts.

    The ranking logic is deliberately separated from the interactive shell so it can
    be tested and reused independently of presentation concerns.

    :param results: one evaluation per candidate proposer, already scored
        against the real column by ``ColumnEvaluator``.
    :param profile: the column's evaluation profile - selects the
        fidelity/novelty/diversity weighting (see ``profile_weights``).
    :param theme: the active display theme, used only to colour-highlight
        the recommended row in the returned table; ranking itself is
        theme-independent. Defaults to the no-colour theme.
    :param column_name: the column's own name, used only for the keyword
        boost (see ``KEYWORD_GENERATOR_HINTS``) - ``None`` disables it.
    :param real_uniqueness: fraction of the real column's sampled values
        that are distinct. Scales how hard the resample/self-duplication
        penalties bite (see ``_penalty_for_candidate``): a low-uniqueness
        column (a gender, a status) is expected to have repeats, so
        reproducing real values there is harmless; a near-unique one (an
        email, a real ID) is not.
    :param is_numeric_column: whether the resample penalty applies to every
        candidate (numeric columns have no shared real-world vocabulary
        that could explain a coincidental match) rather than just
        choice-style resamplers.
    :param is_primary_key: whether the column is a primary key - needs
        guaranteed-unique values, so a candidate with ``synthetic_uniqueness``
        too low to ever satisfy that is excluded from the recommendation
        (see ``no_uniqueness_guarantee``), and a candidate that structurally
        guarantees freshness takes precedence over the generic score (see
        ``Proposer.guarantees_fresh_uniqueness``).
    :param is_unique_constrained: as ``is_primary_key``, for a UNIQUE
        constraint/index that isn't itself the primary key.
    :param type_incompatible: parallel to ``results`` - True for a candidate
        whose output type doesn't actually match the column (e.g. a
        continuous-float proposer for a genuinely Integer, IDENTIFIER-profile
        column). Such a candidate is never picked as the recommendation, even
        if its raw score would otherwise win - the fidelity/novelty/diversity
        metrics have no way to penalize a type mismatch themselves, since
        they only ever compare numeric shape, not validity for the column.
    :return: the ranked candidates, the recommended index/reason, and the
        formatted table rows (see ``ProposalRanking``).
    """
    if not results:
        return ProposalRanking(None, None, [], profile, (0.5, 0.25, 0.25))

    # prepare multi-objective ranking: normalize metrics so larger-is-better
    # overall_score is lower-is-better (distance/error), so invert after normalization
    scores = [result.overall_score for result in results]
    novelties = [result.novelty for result in results]
    diversities = [result.diversity for result in results]

    norm_scores = normalize_list(scores)
    novelty_scores = normalize_list(novelties)
    diversity_scores = normalize_list(diversities)

    # fidelity (higher-is-better) is inverted normalized overall_score
    fidelity_scores = [1.0 - v for v in norm_scores]

    # build points for Pareto (higher-is-better in all dims)
    points = list(zip(fidelity_scores, novelty_scores, diversity_scores))

    # non-dominated sort into Pareto fronts (simple O(n^2) algorithm)
    fronts = _pareto_fronts(points)
    front_of = [0] * len(points)
    for front_index, front in enumerate(fronts, start=1):
        for idx in front:
            front_of[idx] = front_index

    # compute crowding distances per front (NSGA-II style) to prefer diverse solutions
    crowding = _crowding_distances(
        points, fronts, (fidelity_scores, novelty_scores, diversity_scores)
    )

    # determine profile and weights for scoring
    #
    # SHORT_TEXT, EMAIL and TEMPORAL all draw from a bounded, structured
    # real-world vocabulary (names, addresses, dates), the same way
    # CATEGORICAL/IDENTIFIER data does. For that kind of column, novelty and
    # fidelity are mechanically in tension for the *best* generators: a
    # generator that accurately models the real distribution is more, not
    # less, likely to occasionally reproduce a real value by chance. Giving
    # novelty a higher weight than fidelity here (as before) let it override
    # an even strongly-discriminating fidelity signal and penalize the
    # generator that best matched the real data. FREE_TEXT is kept
    # novelty-tolerant since open-ended text has no such bounded vocabulary -
    # genuinely novel sentences there are a good sign, not a red flag.
    profile_weights = {
        EvaluationProfile.IDENTIFIER: (0.85, 0.05, 0.10),
        EvaluationProfile.CATEGORICAL: (0.7, 0.15, 0.15),
        EvaluationProfile.SHORT_TEXT: (0.7, 0.15, 0.15),
        EvaluationProfile.EMAIL: (0.7, 0.15, 0.15),
        EvaluationProfile.FREE_TEXT: (0.5, 0.3, 0.2),
        EvaluationProfile.TEMPORAL: (0.7, 0.15, 0.15),
    }
    fid_w, nov_w, div_w = (
        profile_weights[profile] if profile in profile_weights else (0.5, 0.25, 0.25)
    )

    # compute profile-specific score (higher-is-better)
    combined_scores = [fid_w * f + nov_w * n + div_w * d for (f, n, d) in points]

    # See _penalty_for_candidate for what resample_penalty and
    # self_duplication_penalty actually guard against and why.
    apply_to_every_proposer = is_numeric_column
    needs_uniqueness = is_primary_key or is_unique_constrained
    penalty_multipliers = [
        _penalty_for_candidate(
            result, real_uniqueness, apply_to_every_proposer, needs_uniqueness
        )
        for result in results
    ]
    for idx in range(len(results)):
        combined_scores[idx] *= penalty_multipliers[idx]

    # soft boost for generators whose kind the column name itself hints at
    # (see KEYWORD_GENERATOR_HINTS) - a cheap, complementary signal to the
    # statistical evaluation above. Keep the matched keyword per candidate
    # so the display can show *why* a boost was applied.
    keyword_matches: list[str | None] = []
    for idx, result in enumerate(results):
        boost, matched_keyword = keyword_match(column_name, result.proposer.name())
        combined_scores[idx] += boost
        keyword_matches.append(matched_keyword)

    # combined_scores is a weighted sum of three [0, 1]-normalized dimensions
    # with weights summing to 1.0, so it's bounded to [0, 1] on its own - but
    # the keyword boost above is deliberately applied outside that weighted
    # sum, and can otherwise push a score above 1. Clamp so "Score" stays a
    # comparable, interpretable quantity. (The ChoiceProposer penalty above
    # only ever shrinks a score toward 0, so it can't push below the range.)
    combined_scores = [max(0.0, min(1.0, s)) for s in combined_scores]

    # A primary key/unique column needs values that are actually unique
    # per-row, not just a good statistical fit. If every single candidate
    # duplicates enough within its own synthetic sample (see
    # synthetic_uniqueness) that it could never satisfy that constraint,
    # there is no honest "Recommended: X" to give - naming a winner would
    # imply a fitness that doesn't exist and invite a pick that later trips
    # a real PRIMARY KEY/UNIQUE violation. This is independent of the
    # resample penalty above (which only measures overlap with the *real*
    # values): a proposer can have a low copy_fraction and still duplicate
    # constantly against itself (e.g. dist_gen.constant).
    no_uniqueness_guarantee = (
        is_primary_key
        and bool(results)
        and all(
            result.synthetic_uniqueness < SYNTHETIC_UNIQUENESS_GUARANTEE_THRESHOLD
            for result in results
        )
    )

    type_incompatible_flags = (
        list(type_incompatible)
        if type_incompatible is not None
        else [False] * len(results)
    )

    # pick recommended index: highest combined score across all candidates,
    # tiebreaker by crowding distance. This used to restrict the pick to
    # Pareto front 1, but the keyword boost and resample penalty above are
    # applied after fronts are computed from the raw fidelity/novelty/
    # diversity values - so a technically-dominated candidate can end up
    # with a genuinely higher combined_score than a front-1 one (e.g. a
    # column-name match earning +0.15). Restricting to front 1 would
    # silently override that, defeating the point of adding those signals.
    # Front/crowding are still computed and shown per row for transparency
    # (which candidates are non-dominated trade-off alternatives vs.
    # objectively worse), just no longer used to gate the recommendation.
    #
    # Two further restrictions narrow the *eligible* pool before that
    # highest-score search, both because fidelity/novelty/diversity have no
    # way to see either distinction themselves:
    #
    # - A type-incompatible candidate (see the ``type_incompatible`` param)
    #   is never eligible to win, however high its score - a continuous-
    #   float proposer fit directly to the real column's own numeric range
    #   can score almost perfectly on an IDENTIFIER profile while still
    #   being invalid to insert into an Integer column.
    # - For a column that needs guaranteed-unique values (PK/UNIQUE), a
    #   candidate that structurally guarantees freshness (see
    #   ``Proposer.guarantees_fresh_uniqueness``, e.g. a sequence
    #   continuing past the observed max) takes precedence over the
    #   generic score - the fidelity metric can only ever read "deliberately
    #   diverges from the observed range" as a poor fit, never as the
    #   correct, intended behavior of this kind of proposer.
    all_idxs = list(range(len(combined_scores)))
    valid_type_idxs = [
        idx for idx in all_idxs if not type_incompatible_flags[idx]
    ] or all_idxs
    guarantee_idxs = (
        [
            idx
            for idx in valid_type_idxs
            if results[idx].proposer.guarantees_fresh_uniqueness()
        ]
        if needs_uniqueness
        else []
    )
    eligible_idxs = guarantee_idxs or valid_type_idxs

    best_score = max(combined_scores[idx] for idx in eligible_idxs)
    candidates = [idx for idx in eligible_idxs if combined_scores[idx] == best_score]
    if len(candidates) == 1:
        recommended_index = candidates[0]
        recommended_reason = (
            "guarantees fresh, unique values"
            if guarantee_idxs
            else "best combined score"
        )
    else:
        best_crowding = max(crowding[idx] for idx in candidates)
        selected = [idx for idx in candidates if crowding[idx] == best_crowding]
        recommended_index = selected[0]
        recommended_reason = (
            "guarantees fresh, unique values; tiebreak by crowding distance"
            if guarantee_idxs
            else "tiebreak by crowding distance"
        )

    # Flag the case where the candidates that actually fit the real data
    # best were suppressed by the resample penalty, leaving something that
    # was never a good fit to "win" by elimination rather than merit (e.g.
    # every high-fidelity resampler on a unique ID column gets zeroed out,
    # and dist_gen.constant is left to win despite fitting poorly itself).
    # Deliberately narrow: only fires when a *specific* candidate was both
    # heavily discounted (< 0.2x) and would otherwise have fit noticeably
    # better (> 0.3 higher fidelity) - not just "the winner's score is low",
    # which can also just mean a close call among decent options. Also
    # requires the winner's own combined score to actually be weak: a
    # suppressed resampler's fidelity for a unique-per-row column is
    # inflated by the exact memorization the penalty exists to catch, so it
    # being nominally higher than a genuinely good winner's (e.g. a keyword-
    # matched generic.person.email with strong novelty/diversity) isn't a
    # meaningful comparison and shouldn't cast doubt on that pick. Likewise,
    # don't fire it when the winner itself is a candidate that guarantees
    # fresh values (synthetic_uniqueness >= threshold, e.g. a sequence
    # continuing past the observed max): a low combined score there is a
    # structural artifact of the fidelity metric penalizing values that
    # deliberately fall outside the observed real range, not a sign the
    # recommendation is actually shaky - it's exactly the intended,
    # principled pick for a column that needs guaranteed-unique values.
    weak_recommendation_warning = None
    if not no_uniqueness_guarantee and recommended_index is not None:
        winner_fidelity = fidelity_scores[recommended_index]
        winner_score = combined_scores[recommended_index]
        winner_guarantees_uniqueness = (
            results[recommended_index].synthetic_uniqueness
            >= SYNTHETIC_UNIQUENESS_GUARANTEE_THRESHOLD
        )
        suppressed = [
            idx
            for idx in range(len(results))
            if penalty_multipliers[idx] < 0.2
            and fidelity_scores[idx] > winner_fidelity + 0.3
        ]
        if (
            suppressed
            and winner_score < WEAK_WINNER_SCORE_THRESHOLD
            and not winner_guarantees_uniqueness
        ):
            names = ", ".join(results[idx].proposer.name() for idx in suppressed)
            weak_recommendation_warning = (
                f"Note: {names} fit the real data much better but were heavily "
                "discounted by the resample penalty (see Penalty column) - reproducing "
                "real values almost exactly isn't valid for a unique-per-row column. No "
                "remaining candidate is a strong fit here; treat this recommendation as "
                "a fallback, not a confident pick."
            )

    # prepare rows with Front and Score, marking the recommended row.
    # Crowding distance is still computed above (used as a tiebreak when
    # scores are exactly equal) but isn't shown - it's an NSGA-II internal
    # detail (how isolated a candidate is from its front-mates in objective
    # space), not a quality signal a user picking a generator can reason
    # about, and in practice it's "∞" for most rows anyway.
    rows: list[tuple[str, ...]] = []
    for i, result in enumerate(results, start=1):
        idx = i - 1
        front = front_of[idx]
        is_recommended = idx == recommended_index
        # '*' marks the recommended row even with theming turned off, where
        # the colour highlight below has no visible effect at all.
        index_cell = f"*{i}" if is_recommended else str(i)
        # Profile and (real) Uniqueness describe the source column, not this
        # candidate - constant across every row, and already shown once in
        # profile_summary above the table, so they're left out here rather
        # than repeated on every line.
        cells = [
            index_cell,
            result.proposer.name(),
            str(front),
            f"{combined_scores[idx]:.6f}",
            keyword_matches[idx] or "",
            f"{fidelity_scores[idx]:.6f}",
            f"{novelty_scores[idx]:.6f}",
            f"{diversity_scores[idx]:.6f}",
            f"{result.copy_fraction:.3f}",
            f"{result.synthetic_uniqueness:.3f}",
            f"{penalty_multipliers[idx]:.3f}",
        ]
        # Highlight the recommended row distinctly from the rest of its
        # Pareto front 1 "near miss" companions (non-dominated alternatives
        # worth a second look), rather than one colour for the whole front.
        if is_recommended:
            cells = [f"{theme.recommend}{cell}{theme.reset}" for cell in cells]
        elif front == 1:
            cells = [f"{theme.near_miss}{cell}{theme.reset}" for cell in cells]
        # keep original types compatible with print_table (it will cast to list)
        rows.append(tuple(cells))

    return ProposalRanking(
        recommended_index=recommended_index,
        recommended_reason=recommended_reason,
        rows=rows,
        profile=profile,
        weights=(fid_w, nov_w, div_w),
        real_uniqueness=real_uniqueness,
        penalty_applies_to_all=apply_to_every_proposer,
        self_duplication_floored=not (is_primary_key or is_unique_constrained),
        fronts=list(front_of),
        scores=list(combined_scores),
        weak_recommendation_warning=weak_recommendation_warning,
        no_uniqueness_guarantee=no_uniqueness_guarantee,
        type_incompatible=type_incompatible_flags,
    )


def format_ranking_display(
    ranking: ProposalRanking,
    results: Sequence[ProposalEvaluation],
    profile: EvaluationProfile | None,
) -> ProposalRankingDisplay:
    """Prepare user-facing strings for a ranked proposal table."""
    fid_w, nov_w, div_w = ranking.weights
    recommendation = None
    if ranking.no_uniqueness_guarantee:
        recommendation = (
            "Recommended: none — no proposer can guarantee uniqueness for this column\n"
        )
    elif ranking.recommended_index is not None:
        rec_num = ranking.recommended_index + 1
        rec_name = results[ranking.recommended_index].proposer.name()
        recommendation = (
            f"Recommended: {rec_num}. {rec_name} — {ranking.recommended_reason}\n"
        )
    resample_scope = (
        "every generator (numeric column)"
        if ranking.penalty_applies_to_all
        else "resamplers only (dist_gen.choice/weighted_choice/zipf_choice)"
    )
    self_dup_scope = (
        f"floored at {SELF_DUPLICATION_PENALTY_FLOOR:.1f} (no PK/UNIQUE constraint)"
        if ranking.self_duplication_floored
        else "full strength (PK/UNIQUE constrained)"
    )
    profile_summary = (
        f"Profile: {profile.name if profile is not None else 'UNKNOWN'}  |  "
        f"Real uniqueness: {ranking.real_uniqueness:.3f}  |  "
        "'*' marks the recommended row; it and other Pareto front 1 rows"
        " (non-dominated alternatives) are colour-highlighted, distinctly"
        " from each other, if your theme supports it\n"
        f"Score = clamp( ({fid_w:.2f}*Fidelity + {nov_w:.2f}*Novelty"
        f" + {div_w:.2f}*Diversity + Keyword) "
        "x Penalty ,  0, 1 )\n"
        "  Keyword: +0.15 for a column-name hint (see Keyword column)\n"
        "  Penalty = resample_penalty x self_duplication_penalty"
        " (see Copies / Synth.Uniq columns)\n"
        f"    resample_penalty (1 - real_uniqueness x copy_fraction): {resample_scope}\n"
        "    self_duplication_penalty (1 - real_uniqueness x (1 - synthetic_uniqueness)):"
        f" {self_dup_scope}"
    )
    return ProposalRankingDisplay(
        recommendation=recommendation,
        profile_summary=profile_summary,
        rows=ranking.rows,
        fronts=ranking.fronts,
        weak_recommendation_warning=ranking.weak_recommendation_warning,
        no_uniqueness_guarantee=ranking.no_uniqueness_guarantee,
        type_incompatible=ranking.type_incompatible,
    )
