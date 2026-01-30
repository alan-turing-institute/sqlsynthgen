"""Registry for mapping measurement concepts to generator callables."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Union

import story_types as stypes
from event_registry import EventRegistry

MeasurementEvent = tuple[str, stypes.SqlRow]
MeasurementKey = Union[int, str]
MeasurementGenerator = Callable[
    [Sequence[MeasurementKey], stypes.SqlRow, stypes.SqlRow, stypes.SrcStats],
    Iterable[MeasurementEvent],
]

_REGISTRY = EventRegistry("Measurement")


def register_measurement_generator(
    keys: Sequence[MeasurementKey], generator: MeasurementGenerator
) -> None:
    _REGISTRY.register(keys, generator)


def register_default_measurement_generator(generator: MeasurementGenerator) -> None:
    _REGISTRY.register_default(generator)


def dispatch_measurement_generators(
    measurement_tokens: Sequence[MeasurementKey],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
) -> Iterable[MeasurementEvent]:
    return _REGISTRY.dispatch(measurement_tokens, person, visit_occurrence, src_stats)


def has_generator_for(token: MeasurementKey) -> bool:
    return _REGISTRY.has_generator_for(token)


def registered_measurement_keys() -> tuple[MeasurementKey, ...]:
    return _REGISTRY.registered_keys()
