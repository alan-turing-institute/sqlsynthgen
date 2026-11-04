"""Evaluate how well a proposer's synthetic data matches a real column."""

import string
from dataclasses import dataclass
from typing import Any, Sequence

from sqlalchemy import Column, Engine, Table, UniqueConstraint, select
from sqlalchemy.types import Date, DateTime, Time

from datafaker.db_utils import is_numeric_sql_type
from datafaker.dialects import Random
from datafaker.evaluators.evaluation_profile import EvaluationProfile
from datafaker.evaluators.metrics import (
    DiversityMetric,
    NoveltyMetric,
    canonicalize_value,
)
from datafaker.evaluators.statistical_fidelity import StatisticalFidelity
from datafaker.proposers.base import Proposer, get_column_type

__all__ = ["EvaluationProfile", "ColumnEvaluator", "ColumnStats", "ProposalEvaluation"]


@dataclass
class ColumnStats:
    """Summary statistics of a column's (string-rendered) real values."""

    row_count: int
    unique_count: int
    avg_length: float
    space_ratio: float
    digit_ratio: float
    punctuation_ratio: float

    @property
    def uniqueness(self) -> float:
        """Fraction of rows whose value is distinct from the others."""
        return self.unique_count / max(self.row_count, 1)


@dataclass
class ProposalEvaluation:
    """The scored result of evaluating one proposer against a column."""

    proposer: Proposer
    novelty: float
    diversity: float
    overall_score: float
    criterion_scores: dict[str, float]
    copy_fraction: float = 0.0
    synthetic_uniqueness: float = 1.0

    def __str__(self) -> str:
        """Render a human-readable summary of the evaluation."""
        lines = [
            f"{self.proposer.name()} "
            f" (error={self.overall_score:.6f})"
            f" (novelty={self.novelty:.6f})"
            f" (diversity={self.diversity:.6f})"
            f" (copies={self.copy_fraction:.6f})"
        ]

        for name, score in self.criterion_scores.items():
            lines.append(f"  {name:<15} {score:.6f}")

        return "\n".join(lines)

    __repr__ = __str__


def analyse_column(values: list[Any]) -> ColumnStats:
    """Compute length/space/digit/punctuation statistics over ``values``."""
    # setup() already routes Numeric/Integer and Date/DateTime/Time columns
    # elsewhere, so this only ever sees whatever's left (Boolean/UUID/Enum/
    # JSON/...) - stringify defensively since those aren't already str.
    values = [str(v) for v in values if v is not None]

    total_chars = sum(len(v) for v in values)

    spaces = sum(v.count(" ") for v in values)

    digits = sum(c.isdigit() for v in values for c in v)

    punctuation = sum(c in string.punctuation for v in values for c in v)

    return ColumnStats(
        row_count=len(values),
        unique_count=len(set(values)),
        avg_length=(total_chars / len(values) if values else 0),
        space_ratio=(spaces / total_chars if total_chars else 0),
        digit_ratio=(digits / total_chars if total_chars else 0),
        punctuation_ratio=(punctuation / total_chars if total_chars else 0),
    )


# Below this fraction of distinct values, a column is treated as a small,
# repeated set of categories rather than a free-form or continuous quantity.
CATEGORICAL_UNIQUENESS_THRESHOLD = 0.2


def choose_profile(stats: ColumnStats) -> EvaluationProfile:
    """Pick a string column's evaluation profile from its statistics."""
    # mostly unique short values
    if stats.avg_length < 30 and stats.uniqueness > 0.8:
        return EvaluationProfile.SHORT_TEXT

    # repeated values
    if stats.uniqueness < CATEGORICAL_UNIQUENESS_THRESHOLD:
        return EvaluationProfile.CATEGORICAL

    # long text
    if stats.avg_length > 50 or stats.space_ratio > 0.15:
        return EvaluationProfile.FREE_TEXT

    return EvaluationProfile.SHORT_TEXT


def choose_numeric_profile(values: Sequence[Any]) -> EvaluationProfile:
    """Pick a numeric column's evaluation profile from its cardinality.

    Decide whether an Integer/Numeric column behaves like a small set of
    repeated categories (a status code, rating, or flag) or like a
    continuous / high-cardinality quantity (an age, salary, or identifier).
    A fixed assumption from SQL type alone can't tell these apart - an
    Integer column isn't always categorical (an age, say) and a Numeric
    one isn't always an identifier - so this applies the same cardinality
    check already used for string columns (see ``choose_profile``) instead.
    """
    non_null = [v for v in values if v is not None]
    if not non_null:
        return EvaluationProfile.IDENTIFIER

    uniqueness = len(set(non_null)) / len(non_null)
    if uniqueness < CATEGORICAL_UNIQUENESS_THRESHOLD:
        return EvaluationProfile.CATEGORICAL

    return EvaluationProfile.IDENTIFIER


def looks_like_email(values: Sequence[Any]) -> bool:
    """Heuristically decide whether a sample of values looks like emails."""
    nonempty = [str(v).strip() for v in values if v is not None and str(v).strip()]
    if not nonempty:
        return False

    email_like = 0
    for value in nonempty:
        if "@" not in value:
            continue
        local_part, sep, domain = value.partition("@")
        if not sep or not local_part or not domain:
            continue
        if "." in domain:
            email_like += 1

    return email_like / len(nonempty) > 0.5


class ColumnEvaluator:  # pylint: disable=too-many-instance-attributes
    """Evaluate candidate proposers against one (or several merged) columns."""

    def __init__(self, columns: list[Column], engine: Engine) -> None:
        """Sample the real column(s) and pick the evaluation profile/criteria."""
        assert columns, "ColumnEvaluator needs at least one column"
        self.sample_size: int = 4000
        self.engine: Engine = engine
        self.novelty_metric: NoveltyMetric = NoveltyMetric()
        self.diversity_metric: DiversityMetric = DiversityMetric()

        self.columns: list[Column] = columns
        self.column: Column | None = columns[0] if len(columns) == 1 else None
        self.table: Table = columns[0].table
        # A composite key needs every constituent column marked primary_key;
        # this reduces to the single column's own flag in the common case.
        self.is_primary_key = bool(columns) and all(c.primary_key for c in columns)
        # A real UNIQUE constraint/index needs the same uniqueness guarantee
        # as a primary key, even though it isn't one - e.g. a UNIQUE email
        # column. Distinct from is_primary_key so the two can be OR'd
        # together (needs_uniqueness) without conflating "is the key" with
        # "must be unique".
        self.is_unique_constrained = self.is_primary_key or self._has_unique_constraint(
            columns
        )

        with engine.connect() as conn:
            if len(columns) == 1:
                # Cap how many real rows we pull into memory so this scales to
                # large tables, matching the synthetic sample_size above rather
                # than materializing the entire column. Order randomly first:
                # an unordered LIMIT returns whatever the DB naturally returns
                # first (often insertion order), which can systematically miss
                # real values for a column whose value correlates with row
                # order (e.g. a "last_update" timestamp) - corrupting
                # real_uniqueness/copy_fraction/novelty/diversity, all of
                # which are computed from this sample.
                rows = conn.execute(
                    select(columns[0])
                    .select_from(columns[0].table)
                    .order_by(Random())
                    .limit(self.sample_size)
                )
                self.real_values = [row[0] for row in rows]
                column_type = get_column_type(columns[0])
            else:
                rows = conn.execute(
                    select(*columns)
                    .select_from(columns[0].table)
                    .order_by(Random())
                    .limit(self.sample_size)
                )
                self.real_values = [tuple(row) for row in rows]
                column_type = None

        # Numeric columns (whether they end up profiled IDENTIFIER or, for a
        # low-cardinality one like a foreign key, CATEGORICAL) have no shared
        # real-world vocabulary that could explain a generator coincidentally
        # matching real values - unlike a string column (e.g. common first
        # names), where that overlap is expected and legitimate. Used to
        # decide how broadly the resample-penalty in proposal_ranking.py
        # applies.
        self.column_is_numeric = is_numeric_sql_type(column_type)

        # fraction of real values that are distinct - used to judge whether a
        # generator that resamples from the real data verbatim (e.g. a
        # ChoiceProposer) is doing the right thing (a small, shared
        # vocabulary, where reproducing real values is expected/correct) or
        # a privacy-losing shortcut (near-unique real values, where
        # reproducing them verbatim just leaks real records).
        non_null_real_values = [v for v in self.real_values if v is not None]
        self.real_uniqueness = (
            len({str(v) for v in non_null_real_values}) / len(non_null_real_values)
            if non_null_real_values
            else 0.0
        )

        # Evaluation dimensions: fidelity, novelty, diversity
        if len(columns) == 1:
            self.statistical_fidelity = StatisticalFidelity(
                self.column, self.engine, sample_size=self.sample_size
            )
            if isinstance(column_type, (Date, DateTime, Time)):
                profile = EvaluationProfile.TEMPORAL
            elif is_numeric_sql_type(column_type):
                profile = choose_numeric_profile(self.real_values)
            else:
                stats = analyse_column(self.real_values)
                if looks_like_email(self.real_values):
                    profile = EvaluationProfile.EMAIL
                else:
                    profile = choose_profile(stats)
            self.profile = profile
            self.statistical_fidelity.set_eval_criteria(self.profile)
        else:
            self.statistical_fidelity = None
            self.profile = EvaluationProfile.IDENTIFIER

    @staticmethod
    def _has_unique_constraint(columns: list[Column]) -> bool:
        """Whether exactly this column set is covered by a UNIQUE constraint/index."""
        if not columns:
            return False
        table = columns[0].table
        column_set = set(columns)
        for constraint in table.constraints:
            if (
                isinstance(constraint, UniqueConstraint)
                and set(constraint.columns) == column_set
            ):
                return True
        for index in table.indexes:
            if index.unique and set(index.columns) == column_set:
                return True
        return False

    def evaluate(self, proposer: Proposer) -> ProposalEvaluation:
        """Score one proposer's synthetic data against the sampled real data."""
        synthetic_samples = proposer.generate_data(self.sample_size)

        novelty_score = self.novelty_metric.compare(
            self.real_values,
            synthetic_samples,
        )

        diversity_score = self.diversity_metric.compare(
            self.real_values,
            synthetic_samples,
        )

        # calculate copy fraction: fraction of synthetic samples that exactly match a real value
        try:
            real_set = set(
                str(canonicalize_value(v)) for v in self.real_values if v is not None
            )
            if synthetic_samples:
                copy_count = sum(
                    1
                    for s in synthetic_samples
                    if str(canonicalize_value(s)) in real_set
                )
                copy_frac = copy_count / max(len(synthetic_samples), 1)
            else:
                copy_frac = 0.0
        except Exception:  # pylint: disable=broad-exception-caught
            # A proposer's own output is untrusted here - anything unhashable
            # or unstringifiable should degrade to "no copies detected"
            # rather than crash the whole propose/evaluate flow.
            copy_frac = 0.0

        # fraction of the synthetic sample's own values that are distinct from
        # each other - a direct, real-data-independent measure of whether this
        # proposer could satisfy a PRIMARY KEY/UNIQUE constraint at all. This
        # is different from copy_fraction (which only checks overlap against
        # the *real* values): a proposer resampling from a small vocabulary,
        # or one like dist_gen.constant that emits the same value every time,
        # duplicates against its own output regardless of whether that output
        # happens to match any real value.
        try:
            synthetic_uniqueness = (
                len({str(s) for s in synthetic_samples}) / len(synthetic_samples)
                if synthetic_samples
                else 0.0
            )
        except Exception:  # pylint: disable=broad-exception-caught
            # Same rationale as the copy_frac catch above.
            synthetic_uniqueness = 0.0

        # calculate fidelity scores for each evaluation criterion. Multi-column
        # proposal evaluation does not map cleanly onto the single-column fidelity
        # criteria, so fall back to a neutral score instead of crashing.
        if self.statistical_fidelity is None:
            stat_fidelity_overall_score = 0.0
            stat_fidelity_criterion_scores = {}
        else:
            (
                stat_fidelity_overall_score,
                stat_fidelity_criterion_scores,
            ) = self.statistical_fidelity.calculate_scores(synthetic_samples)

        return ProposalEvaluation(
            proposer=proposer,
            novelty=novelty_score,
            diversity=diversity_score,
            overall_score=stat_fidelity_overall_score,
            criterion_scores=stat_fidelity_criterion_scores,
            copy_fraction=copy_frac,
            synthetic_uniqueness=synthetic_uniqueness,
        )
