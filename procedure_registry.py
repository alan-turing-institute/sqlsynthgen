"""Registry for mapping procedure concepts to generator callables."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Union

import story_types as stypes
from event_registry import EventRegistry

ProcedureEvent = tuple[str, stypes.SqlRow]
ProcedureKey = Union[int, str]
ProcedureGenerator = Callable[
    [Sequence[ProcedureKey], stypes.SqlRow, stypes.SqlRow, stypes.SrcStats],
    Iterable[ProcedureEvent],
]

_REGISTRY = EventRegistry("Procedure")


def register_procedure_generator(
    keys: Sequence[ProcedureKey], generator: ProcedureGenerator
) -> None:
    _REGISTRY.register(keys, generator)


def register_default_procedure_generator(generator: ProcedureGenerator) -> None:
    _REGISTRY.register_default(generator)


def dispatch_procedure_generators(
    procedure_tokens: Sequence[ProcedureKey],
    person: stypes.SqlRow,
    visit_occurrence: stypes.SqlRow,
    src_stats: stypes.SrcStats,
) -> Iterable[ProcedureEvent]:
    return _REGISTRY.dispatch(procedure_tokens, person, visit_occurrence, src_stats)


def has_generator_for(token: ProcedureKey) -> bool:
    return _REGISTRY.has_generator_for(token)


def registered_procedure_keys() -> tuple[ProcedureKey, ...]:
    return _REGISTRY.registered_keys()

