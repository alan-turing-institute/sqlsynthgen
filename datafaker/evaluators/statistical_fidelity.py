"""Per-profile fidelity criteria that score a proposer against a column."""

from dataclasses import dataclass

from datafaker.evaluators.distribution_builders import (
    CategoryBuilder,
    DistributionBuilder,
    HistogramBuilder,
)
from datafaker.evaluators.evaluation_profile import EvaluationProfile
from datafaker.evaluators.feature_extractors import (
    CharacterBigramExtractor,
    EmailDomainExtractor,
    EmailLocalPartExtractor,
    EmailTopLevelDomainExtractor,
    EmailValidityExtractor,
    FeatureExtractor,
    FirstLetterExtractor,
    IdentityExtractor,
    LastLetterExtractor,
    LengthExtractor,
    SentenceCountExtractor,
    TimestampExtractor,
    VowelConsonantPatternExtractor,
    WeekdayExtractor,
    WordCountExtractor,
    WordExtractor,
)
from datafaker.evaluators.metrics import JensenShannon, MeanSquaredError, Metric


@dataclass
class EvaluationCriterion:
    """One named, weighted feature-comparison criterion within a profile's fidelity score."""

    name: str
    builder: type[DistributionBuilder]
    metric: Metric
    extractor: FeatureExtractor
    weight: float = 1.0


# Short text profile (names, cities, etc.)
SHORT_TEXT_CRITERIA = [
    EvaluationCriterion(
        name="length",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=0.25,
        extractor=LengthExtractor(),
    ),
    EvaluationCriterion(
        name="bigrams",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.35,
        extractor=CharacterBigramExtractor(),
    ),
    EvaluationCriterion(
        name="first_letter",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.10,
        extractor=FirstLetterExtractor(),
    ),
    EvaluationCriterion(
        name="last_letter",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.10,
        extractor=LastLetterExtractor(),
    ),
    EvaluationCriterion(
        name="pattern",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.20,
        extractor=VowelConsonantPatternExtractor(),
    ),
]

EMAIL_CRITERIA = [
    EvaluationCriterion(
        name="length",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=0.10,
        extractor=LengthExtractor(),
    ),
    EvaluationCriterion(
        name="local_part",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.40,
        extractor=EmailLocalPartExtractor(),
    ),
    EvaluationCriterion(
        name="domain",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.15,
        extractor=EmailDomainExtractor(),
    ),
    EvaluationCriterion(
        name="tld",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.15,
        extractor=EmailTopLevelDomainExtractor(),
    ),
    EvaluationCriterion(
        name="format_validity",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.20,
        extractor=EmailValidityExtractor(),
    ),
]

# for country, gender, status, category
CATEGORICAL_CRITERIA = [
    EvaluationCriterion(
        name="category",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=1.0,
        extractor=IdentityExtractor(),
    )
]

IDENTIFIER_CRITERIA = [
    EvaluationCriterion(
        name="identifier",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=1.0,
        extractor=IdentityExtractor(),
    )
]

# Two criteria instead of five independent year/month/day/hour dimensions:
# "timestamp" scores year+month+day jointly as one continuous quantity (so a
# generator is judged on the real calendar-position distribution as a whole,
# not on marginal components that can each look fine while their
# combination never does), and "day_of_week" is kept separately since it's a
# cyclic pattern a continuous days-since-epoch value can't capture on its
# own.
TEMPORAL_CRITERIA = [
    EvaluationCriterion(
        name="timestamp",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=0.7,
        extractor=TimestampExtractor(),
    ),
    EvaluationCriterion(
        name="day_of_week",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.3,
        extractor=WeekdayExtractor(),
    ),
]

# for comments, description, review
FREE_TEXT_CRITERIA = [
    EvaluationCriterion(
        name="length",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=0.10,
        extractor=LengthExtractor(),
    ),
    EvaluationCriterion(
        name="word_count",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=0.25,
        extractor=WordCountExtractor(),
    ),
    EvaluationCriterion(
        name="sentence_count",
        builder=HistogramBuilder,
        metric=MeanSquaredError(),
        weight=0.15,
        extractor=SentenceCountExtractor(),
    ),
    EvaluationCriterion(
        name="words",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.30,
        extractor=WordExtractor(),
    ),
    EvaluationCriterion(
        name="bigrams",
        builder=CategoryBuilder,
        metric=JensenShannon(),
        weight=0.20,
        extractor=CharacterBigramExtractor(),
    ),
]

PROFILE_CRITERIA = {
    EvaluationProfile.IDENTIFIER: IDENTIFIER_CRITERIA,
    EvaluationProfile.SHORT_TEXT: SHORT_TEXT_CRITERIA,
    EvaluationProfile.EMAIL: EMAIL_CRITERIA,
    EvaluationProfile.CATEGORICAL: CATEGORICAL_CRITERIA,
    EvaluationProfile.FREE_TEXT: FREE_TEXT_CRITERIA,
    EvaluationProfile.TEMPORAL: TEMPORAL_CRITERIA,
}


class StatisticalFidelity:
    """
    Measure statistical fidelity of the synthetic sample.

    This class evaluates the statistical fidelity of a synthetic sample
    against a real sample by comparing the distributions of features extracted
    from the data. It uses a set of evaluation criteria tailored to different
    types of data (e.g., short text, categorical, free text, email, identifier, temporal)
    to compute a weighted score that reflects how closely the synthetic data matches the real data
    in terms of statistical properties.
    """

    def __init__(self, column, engine, sample_size=4000):
        """Initialize with no criteria set; call set_eval_criteria() first."""
        self.engine = engine
        self.column = column
        self.table = self.column.table
        self.eval_criteria = {}
        # caps how many real rows each criterion's builder pulls into memory
        # when computing the "real" distribution, so this scales to large
        # tables instead of scanning the whole column per criterion.
        self.sample_size = sample_size
        # (builder, real distribution) per criterion name, populated lazily
        # the first time calculate_scores runs each criterion. The real
        # distribution depends only on the column, not on which proposer is
        # being scored, so it only needs computing once per column rather
        # than once per calculate_scores() call (one per candidate proposer).
        self._real_distributions = {}

    def set_eval_criteria(self, profile):
        """
        Set evaluation criteria based on the specified profile.

        :param profile: The evaluation profile to use.
        """
        self.eval_criteria = PROFILE_CRITERIA[profile]
        self._real_distributions = {}

    def calculate_scores(self, synthetic_samples):
        """
        Calculate the overall fidelity score and each criterion's own score.

        :param synthetic_samples: The synthetic samples to evaluate.
        :return: A tuple of (overall score, {criterion name: criterion score}).
        """
        criterion_scores = {}
        weighted_score = 0.0
        total_weight = 0.0

        for criterion in self.eval_criteria:
            cached = self._real_distributions.get(criterion.name)
            if cached is None:
                builder = criterion.builder(
                    self.engine,
                    self.table,
                    self.column,
                    extractor=criterion.extractor,
                    sample_size=self.sample_size,
                )
                # Distribution from real table - independent of the
                # proposer being scored, so cache it (and the builder,
                # which HistogramBuilder.build_from_values needs state
                # from - e.g. self.edges - set by this same call).
                real = builder.build_from_table()
                self._real_distributions[criterion.name] = (builder, real)
            else:
                builder, real = cached

            # Distribution from synthetic values
            synthetic = builder.build_from_values(synthetic_samples)

            score = criterion.metric.compare(
                real,
                synthetic,
            )

            criterion_scores[criterion.name] = score

            weighted_score += criterion.weight * score
            total_weight += criterion.weight

        overall_score = weighted_score / total_weight if total_weight > 0 else 0.0

        return overall_score, criterion_scores
