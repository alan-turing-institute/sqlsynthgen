"""Extract a comparable feature from a column's real/synthetic values."""

import re
import unicodedata
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any

from sqlalchemy import Integer, String, func, literal

from datafaker.dialects import SecondsDifference, SentenceCount, WordCount

TOKEN = re.compile(r"\w+")

_EPOCH = datetime(1970, 1, 1)
_EPOCH_DATE = date(1970, 1, 1)


def _coerce_datetime(value: Any) -> date | datetime | None:
    """
    Best-effort coercion of a value into something with .year/.month/etc.

    A value that genuinely represents a real datetime can still arrive here
    as a plain string rather than a datetime/date object - e.g. because the
    SQL query that produced it (some proposers build queries from raw
    column-name strings, losing type information) didn't preserve its type.
    Without this, a temporal extractor would silently treat it as "not a
    date" and drop it entirely, rather than raising a visible error -
    corrupting the resulting distribution for one candidate while giving no
    indication anything went wrong.

    :return: ``value`` unchanged if it already looks date-like, the parsed
        ``datetime`` if ``value`` is an ISO-format string, or ``None`` if
        neither applies.
    """
    if hasattr(value, "year"):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


class FeatureExtractor(ABC):
    """Extract one comparable feature from a real or synthetic value."""

    @abstractmethod
    def extract(self, value: Any):
        """
        Convert one cell into one or more feature values (synthetic/Python side).

        Returns:
            iterable of feature values
        """

    @abstractmethod
    def expression(self, column):
        """Build the equivalent SQLAlchemy expression for the real/database side."""


class IdentityExtractor(FeatureExtractor):
    """Use the value itself as the feature."""

    def extract(self, value):
        """Yield the value unchanged."""
        if value is None:
            return
        yield value

    def expression(self, column):
        """Return the column unchanged."""
        return column


class FirstLetterExtractor(FeatureExtractor):
    """Extract a string value's first letter, lowercased."""

    def extract(self, value):
        """Yield the value's first letter, lowercased."""
        if value is None:
            return
        value = str(value).strip()
        if value:
            yield value[0].lower()

    def expression(self, column):
        """Build the SQL expression for the column's first letter."""
        return func.lower(func.substr(column, 1, 1))


class LastLetterExtractor(FeatureExtractor):
    """Extract a string value's last letter, lowercased."""

    def extract(self, value):
        """Yield the value's last letter, lowercased."""
        if value is None:
            return

        value = str(value).strip().lower()

        if value:
            yield value[-1]

    def expression(self, column):
        """Build the SQL expression for the column's last letter."""
        return func.lower(func.substr(column, -1, 1))


class LengthExtractor(FeatureExtractor):
    """Extract a value's string length."""

    def extract(self, value):
        """Yield the value's string length."""
        if value is None:
            return

        yield len(str(value))

    def expression(self, column):
        """Build the SQL expression for the column's string length."""
        return func.length(column)


class WordExtractor(FeatureExtractor):
    """Extract a string value's individual word tokens."""

    def extract(self, value):
        """Yield each word token in the value, lowercased."""
        if value is None:
            return

        yield from TOKEN.findall(str(value).lower())

    def expression(self, column):
        """Not implemented: word tokenization has no SQL equivalent here."""
        raise NotImplementedError("WordExtractor SQL expression is not implemented.")


class CharacterExtractor(FeatureExtractor):
    """Extract a string value's individual characters."""

    def extract(self, value):
        """Yield each character in the value."""
        if value is None:
            return

        yield from str(value)

    def expression(self, column):
        """Not implemented: per-character extraction has no SQL equivalent here."""
        raise NotImplementedError(
            "CharacterExtractor SQL expression is not implemented."
        )


class CharacterBigramExtractor(FeatureExtractor):
    """Extract a string value's overlapping 2-character sequences."""

    def extract(self, value):
        """Yield each overlapping 2-character bigram, lowercased."""
        if value is None:
            return

        value = str(value).lower()

        for i in range(len(value) - 1):
            yield value[i : i + 2]

    def expression(self, column):
        """Not implemented: bigram extraction has no SQL equivalent here."""
        raise NotImplementedError()


class CharacterTrigramExtractor(FeatureExtractor):
    """Extract a string value's overlapping 3-character sequences."""

    def extract(self, value):
        """Yield each overlapping 3-character trigram, lowercased."""
        if value is None:
            return

        value = str(value).lower()

        for i in range(len(value) - 2):
            yield value[i : i + 3]

    def expression(self, column):
        """Not implemented: trigram extraction has no SQL equivalent here."""
        raise NotImplementedError()


class PrefixExtractor(FeatureExtractor):
    """Extract a string value's first ``length`` characters."""

    def __init__(self, length=2):
        """Initialize with the prefix length to extract."""
        self.length = length

    def extract(self, value):
        """Yield the value's first ``length`` characters, lowercased."""
        if value is None:
            return

        value = str(value).strip().lower()

        if value:
            yield value[: self.length]

    def expression(self, column):
        """Not implemented: prefix extraction has no SQL equivalent here."""
        raise NotImplementedError()


class SuffixExtractor(FeatureExtractor):
    """Extract a string value's last ``length`` characters."""

    def __init__(self, length=2):
        """Initialize with the suffix length to extract."""
        self.length = length

    def extract(self, value):
        """Yield the value's last ``length`` characters, lowercased."""
        if value is None:
            return

        value = str(value).strip().lower()

        if value:
            yield value[-self.length :]

    def expression(self, column):
        """Not implemented: suffix extraction has no SQL equivalent here."""
        raise NotImplementedError()


VOWELS = set("aeiou")


def _is_vowel(ch: str) -> bool:
    """Whether ``ch`` is a vowel, treating an accented vowel as its base letter.

    Unicode NFKD-decomposes a precomposed accented character (e.g. "é") into
    its base letter plus a combining accent mark ("e" + U+0301); taking the
    first character of that decomposition recovers the base letter. Without
    this, every accented vowel in non-English real data (common in person
    names) would be miscounted as a consonant, skewing the real-side
    distribution this feature is evaluated against.
    """
    return unicodedata.normalize("NFKD", ch)[0] in VOWELS


class VowelConsonantPatternExtractor(FeatureExtractor):
    """Extract a string value's vowel/consonant pattern (e.g. "CVCV")."""

    def extract(self, value):
        """Yield the value's vowel/consonant pattern string."""
        if value is None:
            return

        pattern = []

        for ch in str(value).lower():
            if not ch.isalpha():
                continue

            pattern.append("V" if _is_vowel(ch) else "C")

        if pattern:
            yield "".join(pattern)

    def expression(self, column):
        """Not implemented: pattern extraction has no SQL equivalent here."""
        raise NotImplementedError()


class TimestampExtractor(FeatureExtractor):
    """Extract a date/datetime as a single continuous days-since-epoch value.

    This captures year, month and day jointly as one quantity for a
    HistogramBuilder, rather than scoring them as independent,
    uncorrelated dimensions - a generator that draws month/day/hour
    independently (uniformly within a calibrated year range) will never
    reproduce real joint patterns (e.g. seasonal clustering) even if each
    marginal component looks reasonable alone, and previously scored
    accordingly poorly per-dimension despite being reasonably calibrated;
    a single joint measure is a fairer, simpler fidelity signal.
    """

    def extract(self, value):
        """Yield the value's days-since-epoch, as a single continuous float."""
        value = _coerce_datetime(value)
        if value is None:
            return
        if not isinstance(value, datetime):
            value = datetime(value.year, value.month, value.day)
        if getattr(value, "tzinfo", None) is not None:
            value = value.replace(tzinfo=None)
        yield (value - _EPOCH).total_seconds() / 86400.0

    def expression(self, column):
        """Build the SQL expression for the column's days-since-epoch."""
        # func.extract("epoch", ...) has no MSSQL equivalent (DATEPART has no
        # "epoch" field); SecondsDifference already solves exactly this via a
        # dialect-specific DATEDIFF compilation on MSSQL.
        epoch = literal(_EPOCH_DATE, type_=column.type)
        return SecondsDifference(column, epoch) / 86400.0


class WeekdayExtractor(FeatureExtractor):
    """Extract day-of-week: a cyclic pattern a continuous value can't capture.

    A continuous days-since-epoch value can't capture a cyclic pattern
    (e.g. weekday/weekend clustering) on its own, since it isn't a function
    of absolute date position.
    """

    def extract(self, value):
        """Yield the value's day of the week (Monday=0)."""
        value = _coerce_datetime(value)
        if value is None:
            return
        yield value.weekday()

    def expression(self, column):
        """Build the SQL expression for the column's day of the week (Monday=0).

        Deliberately not ``func.extract("dow", column)``/``DATEPART(weekday,
        ...)``: Postgres' DOW (Sunday=0), MSSQL's DATEPART(weekday) (session-
        configurable via ``DATEFIRST``, default Sunday=1) and Python's
        ``date.weekday()`` (Monday=0) all disagree with each other, which
        would silently compare mismatched category labels between the real
        (SQL-computed) and synthetic (Python-computed) sides here - a
        generator that reproduces the real day-of-week distribution exactly
        would still score as a total mismatch. floor()/modulo arithmetic on
        the same days-since-epoch value `extract()` uses is portable across
        every dialect and trivially kept in sync with Python's convention:
        1970-01-01 (epoch) was a Thursday, i.e. weekday()==3, hence +3 below.
        """
        epoch = literal(_EPOCH_DATE, type_=column.type)
        days_since_epoch = func.floor(SecondsDifference(column, epoch) / 86400.0)
        return (days_since_epoch.cast(Integer) + 3) % 7


class EmailLocalPartExtractor(FeatureExtractor):
    """Extract the local part (before ``@``) of an email-like string."""

    def extract(self, value):
        """Yield the value's local part, lowercased."""
        if value is None:
            return
        text = str(value).strip()
        if not text or "@" not in text:
            return
        local_part, _, _ = text.partition("@")
        if local_part:
            yield local_part.lower()

    def expression(self, column):
        """Not implemented: local-part extraction has no SQL equivalent here."""
        raise NotImplementedError()


class EmailDomainExtractor(FeatureExtractor):
    """Extract the domain (after ``@``) of an email-like string."""

    def extract(self, value):
        """Yield the value's domain, lowercased."""
        if value is None:
            return
        text = str(value).strip()
        if not text or "@" not in text:
            return
        _, _, domain = text.partition("@")
        if domain:
            yield domain.lower()

    def expression(self, column):
        """Not implemented: domain extraction has no SQL equivalent here."""
        raise NotImplementedError()


class EmailTopLevelDomainExtractor(FeatureExtractor):
    """Extract the top-level domain of an email-like string."""

    def extract(self, value):
        """Yield the value's top-level domain, lowercased."""
        if value is None:
            return
        text = str(value).strip()
        if not text or "@" not in text:
            return
        _, _, domain = text.partition("@")
        if "." not in domain:
            return
        tld = domain.rsplit(".", 1)[-1].lower()
        if tld:
            yield tld

    def expression(self, column):
        """Not implemented: TLD extraction has no SQL equivalent here."""
        raise NotImplementedError()


class EmailValidityExtractor(FeatureExtractor):
    """Extract a 'valid'/'invalid' token for an email-like string.

    Uses the email-validator package when available, falling back to a
    conservative regex.
    """

    # conservative regex fallback
    _SIMPLE_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

    def __init__(self):
        """Try importing the robust validator; fall back to regex if unavailable."""
        try:
            from email_validator import (  # type: ignore # pylint: disable=import-outside-toplevel,import-error
                validate_email,
            )

            self._validator = validate_email
        except ImportError:
            self._validator = None

    def extract(self, value):
        """Yield 'valid' or 'invalid' for the value's email syntax."""
        if value is None:
            return
        text = str(value).strip()
        if not text:
            return
        valid = False
        if self._validator is not None:
            try:
                # check_deliverability=False for syntactic check only
                _ = self._validator(text, check_deliverability=False)
                valid = True
            except Exception:  # pylint: disable=broad-exception-caught
                # email_validator raises its own EmailNotValidError, but
                # this is optional/best-effort - any failure here should
                # just mean "invalid", not crash the whole evaluation.
                valid = False
        else:
            # fallback to simple regex
            valid = bool(self._SIMPLE_RE.match(text))

        yield "valid" if valid else "invalid"

    def expression(self, column):
        """Not implemented: validity checking has no SQL equivalent here."""
        raise NotImplementedError()


class WordCountExtractor(FeatureExtractor):
    """Extract a string value's word count."""

    def extract(self, value):
        """Yield the value's word count."""
        if value is None:
            return
        text = str(value).strip()
        if not text:
            return
        # count words using TOKEN
        count = len(TOKEN.findall(text))
        yield count

    def expression(self, column):
        """Build the SQL expression for the column's word count."""
        # Cast column to text to handle tsvector columns (e.g. fulltext)
        # where regexp_split_to_array may not accept the tsvector type
        # directly. WordCount compiles to an exact regexp-based count on
        # Postgres/DuckDB and a string-function approximation on MSSQL
        # (which has no regex support) - see dialects.py.
        return WordCount(column.cast(String))


class SentenceCountExtractor(FeatureExtractor):
    """Extract a string value's sentence count."""

    def extract(self, value):
        """Yield the value's sentence count."""
        if value is None:
            return
        text = str(value).strip()
        if not text:
            return
        # split on sentence enders
        parts = re.split(r"[.!?]+", text)
        # count non-empty segments containing a word
        count = sum(1 for p in parts if TOKEN.search(p))
        yield count

    def expression(self, column):
        """Build the SQL expression for the column's sentence count."""
        # Cast column to text to handle tsvector columns (e.g. fulltext)
        # where regexp_split_to_array may not accept the tsvector type
        # directly. SentenceCount compiles to an exact regexp-based count
        # on Postgres/DuckDB and a string-function approximation on MSSQL
        # (which has no regex support) - see dialects.py.
        return SentenceCount(column.cast(String))
