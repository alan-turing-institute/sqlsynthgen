"""Registry for mapping measurement concepts to generator callables."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Union

import story_types as stypes
from sqlsynthgen.utils import logger

MeasurementEvent = tuple[str, stypes.SqlRow]
MeasurementKey = Union[int, str]
MeasurementGenerator = Callable[
    [Sequence[MeasurementKey], stypes.SqlRow, stypes.SqlRow, stypes.SrcStats, bool],
    Iterable[MeasurementEvent],
]
DefaultMeasurementGenerator = Callable[
    [Sequence[MeasurementKey], stypes.SqlRow, stypes.SqlRow, stypes.SrcStats, bool],
    Iterable[MeasurementEvent],
]

_registry: dict[MeasurementKey, MeasurementGenerator] = {}
_default_generator: DefaultMeasurementGenerator | None = None


def _normalize_key(key: MeasurementKey) -> MeasurementKey:
    """Normalize measurement identifiers for registry lookups."""
    if isinstance(key, int):
        return key
    normalized = key.strip()
    if not normalized:
        return normalized
    try:
        return int(normalized)
    except ValueError:
        return normalized.lower()


def register_measurement_generator(
    keys: Sequence[MeasurementKey], generator: MeasurementGenerator
) -> None:
    """Register a measurement generator for one or more identifiers."""
    for raw_key in keys:
        key = _normalize_key(raw_key)
        existing = _registry.get(key)
        if existing and existing is not generator:
            logger.warning(
                "Measurement key %s already registered to %s; replacing with %s",
                raw_key,
                existing,
                generator,
            )
        _registry[key] = generator


def register_default_measurement_generator(
    generator: DefaultMeasurementGenerator,
) -> None:
    """Register a fallback generator for tokens without explicit stories."""
    global _default_generator
    if _default_generator and _default_generator is not generator:
        logger.warning("Replacing existing default measurement generator")
    _default_generator = generator


def dispatch_measurement_generators(
    measurement_tokens: Sequence[MeasurementKey],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
    visit_unique: bool = False,
) -> Iterable[MeasurementEvent]:
    """Yield measurement events for the provided measurement tokens."""
    unmatched: list[MeasurementKey] = []
    generator_tokens: dict[MeasurementGenerator, list[MeasurementKey]] = {}
    for token in measurement_tokens:
        key = _normalize_key(token)
        generator = _registry.get(key)
        if not generator:
            unmatched.append(token)
            continue
        generator_tokens.setdefault(generator, []).append(token)

    for generator, tokens in generator_tokens.items():
        yield from generator(tokens, person, visit_occurrence, src_stats, visit_unique)

    if unmatched and _default_generator:
        yield from _default_generator(unmatched, person, visit_occurrence, src_stats, visit_unique)


def has_generator_for(token: MeasurementKey) -> bool:
    """Return True if a generator exists for the given measurement token."""
    return _normalize_key(token) in _registry


def registered_measurement_keys() -> tuple[MeasurementKey, ...]:
    """Expose a copy of registered measurement identifiers (mainly for debugging)."""
    return tuple(_registry.keys())
