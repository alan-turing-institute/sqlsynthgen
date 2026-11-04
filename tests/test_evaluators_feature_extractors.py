"""Unit tests for datafaker.evaluators.feature_extractors."""
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import Column, Date, MetaData, Table, create_engine, select

from datafaker.evaluators.feature_extractors import (
    CharacterBigramExtractor,
    CharacterExtractor,
    CharacterTrigramExtractor,
    EmailDomainExtractor,
    EmailLocalPartExtractor,
    EmailTopLevelDomainExtractor,
    EmailValidityExtractor,
    FirstLetterExtractor,
    IdentityExtractor,
    LastLetterExtractor,
    LengthExtractor,
    PrefixExtractor,
    SentenceCountExtractor,
    SuffixExtractor,
    TimestampExtractor,
    VowelConsonantPatternExtractor,
    WeekdayExtractor,
    WordCountExtractor,
    WordExtractor,
)
from tests.utils import DatafakerTestCase


class IdentityExtractorTests(DatafakerTestCase):
    """Test case for IdentityExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = IdentityExtractor()

    def test_yields_the_value_unchanged(self) -> None:
        """Non-None values pass through untouched."""
        self.assertEqual([42], list(self.extractor.extract(42)))
        self.assertEqual(["hello"], list(self.extractor.extract("hello")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_expression_returns_the_column_itself(self) -> None:
        """The SQL expression is just the column, unmodified."""
        column = object()
        self.assertIs(column, self.extractor.expression(column))


class FirstLetterExtractorTests(DatafakerTestCase):
    """Test case for FirstLetterExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = FirstLetterExtractor()

    def test_extracts_lowercased_first_letter(self) -> None:
        """The first character is extracted and lowercased."""
        self.assertEqual(["a"], list(self.extractor.extract("Alice")))

    def test_strips_surrounding_whitespace_first(self) -> None:
        """Leading whitespace does not become the 'first letter'."""
        self.assertEqual(["a"], list(self.extractor.extract("  alice")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_empty_string_yields_nothing(self) -> None:
        """A blank/whitespace-only string produces no features."""
        self.assertEqual([], list(self.extractor.extract("   ")))

    def test_expression_does_not_raise(self) -> None:
        """A SQL expression is implemented for this extractor."""
        self.extractor.expression("col")


class LastLetterExtractorTests(DatafakerTestCase):
    """Test case for LastLetterExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = LastLetterExtractor()

    def test_extracts_lowercased_last_letter(self) -> None:
        """The last character is extracted and lowercased."""
        self.assertEqual(["e"], list(self.extractor.extract("Alice")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_empty_string_yields_nothing(self) -> None:
        """A blank/whitespace-only string produces no features."""
        self.assertEqual([], list(self.extractor.extract("  ")))


class LengthExtractorTests(DatafakerTestCase):
    """Test case for LengthExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = LengthExtractor()

    def test_extracts_string_length(self) -> None:
        """The length of str(value) is extracted."""
        self.assertEqual([5], list(self.extractor.extract("Alice")))

    def test_extracts_length_of_non_string_values(self) -> None:
        """Non-string values are stringified before measuring length."""
        self.assertEqual([len(str(12345))], list(self.extractor.extract(12345)))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))


class WordExtractorTests(DatafakerTestCase):
    """Test case for WordExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = WordExtractor()

    def test_tokenizes_and_lowercases_words(self) -> None:
        """Words are split on non-word characters and lowercased."""
        self.assertEqual(
            ["the", "quick", "fox"],
            list(self.extractor.extract("The quick, fox!")),
        )

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_no_sql_expression_is_implemented(self) -> None:
        """Word extraction has no SQL equivalent and must raise."""
        with self.assertRaises(NotImplementedError):
            self.extractor.expression("col")


class CharacterExtractorTests(DatafakerTestCase):
    """Test case for CharacterExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = CharacterExtractor()

    def test_extracts_each_character(self) -> None:
        """Each character of the value is yielded separately."""
        self.assertEqual(["a", "b", "c"], list(self.extractor.extract("abc")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_no_sql_expression_is_implemented(self) -> None:
        """Per-character extraction has no SQL equivalent and must raise."""
        with self.assertRaises(NotImplementedError):
            self.extractor.expression("col")


class CharacterBigramExtractorTests(DatafakerTestCase):
    """Test case for CharacterBigramExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = CharacterBigramExtractor()

    def test_extracts_overlapping_bigrams(self) -> None:
        """Adjacent character pairs are extracted, lowercased."""
        self.assertEqual(["ab", "bc"], list(self.extractor.extract("ABc")))

    def test_short_strings_yield_no_bigrams(self) -> None:
        """A string shorter than 2 characters has no bigrams."""
        self.assertEqual([], list(self.extractor.extract("a")))
        self.assertEqual([], list(self.extractor.extract("")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))


class CharacterTrigramExtractorTests(DatafakerTestCase):
    """Test case for CharacterTrigramExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = CharacterTrigramExtractor()

    def test_extracts_overlapping_trigrams(self) -> None:
        """Adjacent character triples are extracted, lowercased."""
        self.assertEqual(["abc", "bcd"], list(self.extractor.extract("ABCd")))

    def test_short_strings_yield_no_trigrams(self) -> None:
        """A string shorter than 3 characters has no trigrams."""
        self.assertEqual([], list(self.extractor.extract("ab")))


class PrefixExtractorTests(DatafakerTestCase):
    """Test case for PrefixExtractor."""

    def test_default_length_is_two(self) -> None:
        """With no explicit length, a 2-character prefix is extracted."""
        extractor = PrefixExtractor()
        self.assertEqual(["al"], list(extractor.extract("Alice")))

    def test_custom_length(self) -> None:
        """A custom prefix length is honored."""
        extractor = PrefixExtractor(length=4)
        self.assertEqual(["alic"], list(extractor.extract("Alice")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(PrefixExtractor().extract(None)))

    def test_empty_string_yields_nothing(self) -> None:
        """A blank/whitespace-only string produces no features."""
        self.assertEqual([], list(PrefixExtractor().extract("   ")))


class SuffixExtractorTests(DatafakerTestCase):
    """Test case for SuffixExtractor."""

    def test_default_length_is_two(self) -> None:
        """With no explicit length, a 2-character suffix is extracted."""
        extractor = SuffixExtractor()
        self.assertEqual(["ce"], list(extractor.extract("Alice")))

    def test_custom_length(self) -> None:
        """A custom suffix length is honored."""
        extractor = SuffixExtractor(length=3)
        self.assertEqual(["ice"], list(extractor.extract("Alice")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(SuffixExtractor().extract(None)))


class VowelConsonantPatternExtractorTests(DatafakerTestCase):
    """Test case for VowelConsonantPatternExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = VowelConsonantPatternExtractor()

    def test_extracts_vowel_consonant_pattern(self) -> None:
        """Each alphabetic character is classified as V or C."""
        self.assertEqual(["VCVCV"], list(self.extractor.extract("Alice")))

    def test_non_alphabetic_characters_are_skipped(self) -> None:
        """Digits and punctuation don't appear in the pattern."""
        self.assertEqual(["VCC"], list(self.extractor.extract("a1-b2c!")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_all_non_alphabetic_yields_nothing(self) -> None:
        """A value with no letters at all produces no pattern."""
        self.assertEqual([], list(self.extractor.extract("123!!")))

    def test_accented_vowels_count_as_vowels(self) -> None:
        """An accented vowel (common in non-English names) is still a 'V'."""
        self.assertEqual(["CVCV"], list(self.extractor.extract("René")))
        self.assertEqual(["CVV"], list(self.extractor.extract("Zoë")))

    def test_cedilla_and_tilde_consonants_stay_consonants(self) -> None:
        """A diacritic on a consonant (ç, ñ) must not be miscounted as a vowel."""
        self.assertEqual(["CVCVC"], list(self.extractor.extract("façon")))
        self.assertEqual(["CVCV"], list(self.extractor.extract("niño")))


class TimestampExtractorTests(DatafakerTestCase):
    """Test case for TimestampExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = TimestampExtractor()

    def test_epoch_datetime_extracts_zero(self) -> None:
        """The Unix epoch itself extracts to day 0."""
        self.assertEqual([0.0], list(self.extractor.extract(datetime(1970, 1, 1))))

    def test_extracts_days_since_epoch(self) -> None:
        """A later date extracts to the correct day offset."""
        self.assertEqual([1.0], list(self.extractor.extract(datetime(1970, 1, 2))))

    def test_accepts_date_objects(self) -> None:
        """A plain date (no time component) is also supported."""
        self.assertEqual([1.0], list(self.extractor.extract(date(1970, 1, 2))))

    def test_accepts_iso_format_strings(self) -> None:
        """A string that came from a query losing type info is coerced."""
        self.assertEqual([1.0], list(self.extractor.extract("1970-01-02")))

    def test_invalid_string_yields_nothing(self) -> None:
        """A string that isn't a valid date produces no feature."""
        self.assertEqual([], list(self.extractor.extract("not-a-date")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_non_date_non_string_value_yields_nothing(self) -> None:
        """A value with no date meaning at all (e.g. an int) is dropped."""
        self.assertEqual([], list(self.extractor.extract(42)))

    def test_timezone_aware_datetime_is_normalized(self) -> None:
        """Timezone info is stripped rather than causing a crash or a shift."""
        aware = datetime(1970, 1, 2, tzinfo=timezone.utc)
        self.assertEqual([1.0], list(self.extractor.extract(aware)))


class WeekdayExtractorTests(DatafakerTestCase):
    """Test case for WeekdayExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = WeekdayExtractor()

    def test_extracts_iso_weekday_index(self) -> None:
        """1970-01-01 was a Thursday: weekday() == 3."""
        self.assertEqual([3], list(self.extractor.extract(datetime(1970, 1, 1))))

    def test_accepts_iso_format_strings(self) -> None:
        """String dates are coerced the same way as TimestampExtractor."""
        self.assertEqual([3], list(self.extractor.extract("1970-01-01")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_invalid_string_yields_nothing(self) -> None:
        """An unparseable string produces no feature."""
        self.assertEqual([], list(self.extractor.extract("nope")))

    def test_sql_expression_agrees_with_python_weekday(self) -> None:
        """The SQL expression must use the same Monday=0 convention as extract().

        Regression test: a dialect's own day-of-week function (Postgres'
        EXTRACT(DOW), MSSQL's DATEPART(weekday)) uses a different, and for
        MSSQL session-configurable, numbering convention than Python's
        date.weekday() - comparing the two directly would silently treat a
        perfect real/synthetic match as a mismatch (see expression()'s
        docstring).
        """
        engine = create_engine("duckdb:///:memory:")
        metadata = MetaData()
        table = Table("d", metadata, Column("dt", Date))
        metadata.create_all(engine)
        base = date(1970, 1, 1)
        days = [base + timedelta(days=i) for i in range(14)]
        with engine.begin() as conn:
            conn.execute(table.insert(), [{"dt": d} for d in days])

        expr = self.extractor.expression(table.c.dt)
        with engine.connect() as conn:
            rows = conn.execute(select(table.c.dt, expr)).all()

        self.assertEqual(14, len(rows))
        for day, sql_weekday in rows:
            self.assertEqual(
                day.weekday(),
                sql_weekday,
                f"{day} ({day.strftime('%A')}): SQL gave {sql_weekday},"
                f" Python gives {day.weekday()}",
            )


class EmailLocalPartExtractorTests(DatafakerTestCase):
    """Test case for EmailLocalPartExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = EmailLocalPartExtractor()

    def test_extracts_lowercased_local_part(self) -> None:
        """The portion before '@' is extracted and lowercased."""
        self.assertEqual(
            ["alice.smith"], list(self.extractor.extract("Alice.Smith@Example.com"))
        )

    def test_value_without_at_sign_yields_nothing(self) -> None:
        """A non-email string has no local part."""
        self.assertEqual([], list(self.extractor.extract("not-an-email")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))


class EmailDomainExtractorTests(DatafakerTestCase):
    """Test case for EmailDomainExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = EmailDomainExtractor()

    def test_extracts_lowercased_domain(self) -> None:
        """The portion after '@' is extracted and lowercased."""
        self.assertEqual(
            ["example.com"], list(self.extractor.extract("alice@Example.COM"))
        )

    def test_value_without_at_sign_yields_nothing(self) -> None:
        """A non-email string has no domain."""
        self.assertEqual([], list(self.extractor.extract("not-an-email")))


class EmailTopLevelDomainExtractorTests(DatafakerTestCase):
    """Test case for EmailTopLevelDomainExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = EmailTopLevelDomainExtractor()

    def test_extracts_lowercased_tld(self) -> None:
        """The final domain segment is extracted and lowercased."""
        self.assertEqual(
            ["com"], list(self.extractor.extract("alice@mail.example.COM"))
        )

    def test_domain_without_dot_yields_nothing(self) -> None:
        """A domain with no TLD separator produces no feature."""
        self.assertEqual([], list(self.extractor.extract("alice@localhost")))

    def test_value_without_at_sign_yields_nothing(self) -> None:
        """A non-email string has no TLD."""
        self.assertEqual([], list(self.extractor.extract("not-an-email")))


class EmailValidityExtractorTests(DatafakerTestCase):
    """Test case for EmailValidityExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = EmailValidityExtractor()

    def test_well_formed_email_is_valid(self) -> None:
        """A syntactically valid email is classified 'valid'."""
        self.assertEqual(["valid"], list(self.extractor.extract("alice@example.com")))

    def test_malformed_value_is_invalid(self) -> None:
        """A string with no '@' or domain is classified 'invalid'."""
        self.assertEqual(["invalid"], list(self.extractor.extract("not-an-email")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_blank_string_yields_nothing(self) -> None:
        """An empty/whitespace-only string produces no features."""
        self.assertEqual([], list(self.extractor.extract("   ")))


class WordCountExtractorTests(DatafakerTestCase):
    """Test case for WordCountExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = WordCountExtractor()

    def test_counts_words(self) -> None:
        """Word count matches the number of \\w+ tokens."""
        self.assertEqual([4], list(self.extractor.extract("The quick brown fox.")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_blank_string_yields_nothing(self) -> None:
        """An empty/whitespace-only string produces no features."""
        self.assertEqual([], list(self.extractor.extract("   ")))


class SentenceCountExtractorTests(DatafakerTestCase):
    """Test case for SentenceCountExtractor."""

    def setUp(self) -> None:
        super().setUp()
        self.extractor = SentenceCountExtractor()

    def test_counts_sentences_by_terminal_punctuation(self) -> None:
        """Sentences are split on ./!/? and counted if they contain a word."""
        self.assertEqual(
            [3], list(self.extractor.extract("Hi there! How are you? I am fine."))
        )

    def test_trailing_punctuation_without_words_is_not_a_sentence(self) -> None:
        """A run of terminal punctuation with no word content doesn't count."""
        self.assertEqual([1], list(self.extractor.extract("Hello there...")))

    def test_none_yields_nothing(self) -> None:
        """None produces no features."""
        self.assertEqual([], list(self.extractor.extract(None)))

    def test_blank_string_yields_nothing(self) -> None:
        """An empty/whitespace-only string produces no features."""
        self.assertEqual([], list(self.extractor.extract("   ")))
