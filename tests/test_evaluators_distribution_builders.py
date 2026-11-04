"""Unit tests for datafaker.evaluators.distribution_builders."""
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

from datafaker.evaluators.distribution_builders import (
    CategoryBuilder,
    Distribution,
    HistogramBuilder,
)
from datafaker.evaluators.feature_extractors import (
    FirstLetterExtractor,
    LengthExtractor,
)
from tests.utils import DatafakerTestCase


class DistributionTests(DatafakerTestCase):
    """Test case for the Distribution dataclass."""

    def test_vocabulary_is_the_set_of_keys(self) -> None:
        """vocabulary exposes the keys of the probability mapping."""
        dist = Distribution({"a": 0.5, "b": 0.5})
        self.assertEqual({"a", "b"}, dist.vocabulary)

    def test_probability_of_known_key(self) -> None:
        """probability() returns the stored value for a known key."""
        dist = Distribution({"a": 0.75})
        self.assertEqual(0.75, dist.probability("a"))

    def test_probability_of_unknown_key_is_zero(self) -> None:
        """probability() returns 0.0 for a key that isn't in the distribution."""
        dist = Distribution({"a": 0.75})
        self.assertEqual(0.0, dist.probability("missing"))

    def test_as_vector_orders_by_given_vocabulary(self) -> None:
        """as_vector() maps a vocabulary list to matching probabilities, in order."""
        dist = Distribution({"a": 0.2, "b": 0.8})
        self.assertEqual([0.8, 0.2, 0.0], dist.as_vector(["b", "a", "c"]))


def _make_duckdb_table(rows: list[dict]) -> tuple:
    """Create an in-memory DuckDB engine with one populated table."""
    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()
    table = Table(
        "people",
        metadata,
        Column("age", Integer),
        Column("name", String),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(table.insert(), rows)
    return engine, table


class HistogramBuilderBuildFromTableTests(DatafakerTestCase):
    """Test case for HistogramBuilder.build_from_table."""

    def test_builds_a_distribution_that_sums_to_one(self) -> None:
        """The resulting histogram is a valid probability distribution."""
        engine, table = _make_duckdb_table(
            [{"age": 20 + (i % 10), "name": "x"} for i in range(100)]
        )
        builder = HistogramBuilder(engine, table, table.c.age, sample_size=1000)
        dist = builder.build_from_table()
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))
        self.assertIsNotNone(builder.mean)
        self.assertIsNotNone(builder.stddev)

    def test_empty_table_yields_empty_distribution(self) -> None:
        """An empty table produces an empty distribution rather than an error."""
        engine, table = _make_duckdb_table([])
        builder = HistogramBuilder(engine, table, table.c.age, sample_size=1000)
        dist = builder.build_from_table()
        self.assertEqual({}, dist.probabilities)

    def test_constant_column_does_not_crash_on_zero_stddev(self) -> None:
        """A column with a single repeated value has zero stddev but still works."""
        engine, table = _make_duckdb_table(
            [{"age": 42, "name": "x"} for _ in range(10)]
        )
        builder = HistogramBuilder(engine, table, table.c.age, sample_size=1000)
        dist = builder.build_from_table()
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))
        self.assertEqual(0.0, builder.stddev)

    def test_uses_feature_extractor_expression(self) -> None:
        """The configured extractor's SQL expression is used, not the raw column."""
        engine, table = _make_duckdb_table(
            [{"age": 0, "name": name} for name in ["ab", "cde", "fghi"] * 5]
        )
        builder = HistogramBuilder(
            engine, table, table.c.name, extractor=LengthExtractor(), sample_size=1000
        )
        dist = builder.build_from_table()
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))


class HistogramBuilderBuildFromValuesTests(DatafakerTestCase):
    """Test case for HistogramBuilder.build_from_values."""

    def setUp(self) -> None:
        super().setUp()
        engine, table = _make_duckdb_table(
            [{"age": 20 + (i % 10), "name": "x"} for i in range(100)]
        )
        self.builder = HistogramBuilder(engine, table, table.c.age, sample_size=1000)
        # Learn edges from the real data first, as calculate_scores() does.
        self.builder.build_from_table()

    def test_result_sums_to_one(self) -> None:
        """Bucketed synthetic values form a valid probability distribution."""
        dist = self.builder.build_from_values([20, 21, 22, 23, 24, 25])
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))

    def test_empty_values_yield_empty_distribution(self) -> None:
        """No synthetic values produces an empty distribution."""
        dist = self.builder.build_from_values([])
        self.assertEqual({}, dist.probabilities)

    def test_values_outside_edges_are_clamped_to_end_buckets(self) -> None:
        """Values far below/above the learned range land in the outer buckets."""
        dist = self.builder.build_from_values([-1000, 1000])
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))
        buckets = set(dist.probabilities.keys())
        assert self.builder.edges is not None
        self.assertTrue(all(0 <= b <= len(self.builder.edges) - 2 for b in buckets))


class CategoryBuilderBuildFromTableTests(DatafakerTestCase):
    """Test case for CategoryBuilder.build_from_table."""

    def test_builds_a_distribution_that_sums_to_one(self) -> None:
        """The resulting category histogram sums to (approximately) 1."""
        engine, table = _make_duckdb_table(
            [{"age": 0, "name": n} for n in ["alice", "bob", "carol"] * 10]
        )
        builder = CategoryBuilder(engine, table, table.c.name, sample_size=1000)
        dist = builder.build_from_table()
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))
        self.assertEqual({"alice", "bob", "carol"}, dist.vocabulary)

    def test_empty_table_yields_empty_distribution(self) -> None:
        """An empty table produces an empty distribution."""
        engine, table = _make_duckdb_table([])
        builder = CategoryBuilder(engine, table, table.c.name, sample_size=1000)
        dist = builder.build_from_table()
        self.assertEqual({}, dist.probabilities)

    def test_uses_feature_extractor(self) -> None:
        """The category counts are keyed by extracted features, not raw values."""
        engine, table = _make_duckdb_table(
            [{"age": 0, "name": n} for n in ["Alice", "Anna", "Bob"]]
        )
        builder = CategoryBuilder(
            engine,
            table,
            table.c.name,
            extractor=FirstLetterExtractor(),
            sample_size=1000,
        )
        dist = builder.build_from_table()
        self.assertEqual({"a", "b"}, dist.vocabulary)
        self.assertAlmostEqual(2 / 3, dist.probability("a"))
        self.assertAlmostEqual(1 / 3, dist.probability("b"))


class CategoryBuilderBuildFromValuesTests(DatafakerTestCase):
    """Test case for CategoryBuilder.build_from_values."""

    def setUp(self) -> None:
        super().setUp()
        self.builder = CategoryBuilder(
            engine=None, table=None, column=None, sample_size=1000
        )

    def test_builds_a_distribution_that_sums_to_one(self) -> None:
        """Category counts from a plain Python list sum to one."""
        dist = self.builder.build_from_values(["a", "a", "b"])
        self.assertAlmostEqual(1.0, sum(dist.probabilities.values()))
        self.assertAlmostEqual(2 / 3, dist.probability("a"))
        self.assertAlmostEqual(1 / 3, dist.probability("b"))

    def test_none_values_are_ignored(self) -> None:
        """None entries don't contribute counts."""
        dist = self.builder.build_from_values(["a", None, "a"])
        self.assertEqual({"a": 1.0}, dist.probabilities)

    def test_no_extracted_features_yields_sentinel_missing_distribution(self) -> None:
        """An empty result set is represented with an explicit __MISSING__ token."""
        dist = self.builder.build_from_values([None, None])
        self.assertEqual({"__MISSING__": 1.0}, dist.probabilities)

    def test_uses_feature_extractor(self) -> None:
        """Values are converted through the configured extractor before counting."""
        builder = CategoryBuilder(
            engine=None, table=None, column=None, extractor=LengthExtractor()
        )
        dist = builder.build_from_values(["ab", "cd", "efg"])
        self.assertAlmostEqual(2 / 3, dist.probability(2))
        self.assertAlmostEqual(1 / 3, dist.probability(3))
