"""Reusable registry/dispatcher for visit-level event generators."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Union

import story_types as stypes
from sqlsynthgen.utils import logger

EventKey = Union[int, str]
EventEvent = tuple[str, stypes.SqlRow]
EventGenerator = Callable[
    [Sequence[EventKey], stypes.SqlRow, stypes.SqlRow, stypes.SrcStats],
    Iterable[EventEvent],
]


class EventRegistry:
    """Manage generator dispatch for concept-token driven events."""

    def __init__(self, label: str) -> None:
        self._label = label
        self._registry: dict[EventKey, EventGenerator] = {}
        self._default: EventGenerator | None = None

    @staticmethod
    def _normalize_key(key: EventKey) -> EventKey:
        if isinstance(key, int):
            return key
        normalized = key.strip()
        if not normalized:
            return normalized
        try:
            return int(normalized)
        except ValueError:
            return normalized.lower()

    def register(self, keys: Sequence[EventKey], generator: EventGenerator) -> None:
        for raw_key in keys:
            key = self._normalize_key(raw_key)
            existing = self._registry.get(key)
            if existing and existing is not generator:
                logger.warning(
                    "%s key %s already registered to %s; replacing with %s",
                    self._label,
                    raw_key,
                    existing,
                    generator,
                )
            self._registry[key] = generator

    def register_default(self, generator: EventGenerator) -> None:
        if self._default and self._default is not generator:
            logger.warning("Replacing existing default %s generator", self._label)
        self._default = generator

    def dispatch(
        self,
        tokens: Sequence[EventKey],
        person: stypes.SqlRow,
        visit_occurrence: stypes.SqlRow,
        src_stats: stypes.SrcStats,
    ) -> Iterable[EventEvent]:
        unmatched: list[EventKey] = []
        generator_tokens: dict[EventGenerator, list[EventKey]] = {}
        for token in tokens:
            key = self._normalize_key(token)
            generator = self._registry.get(key)
            if not generator:
                unmatched.append(token)
                continue
            generator_tokens.setdefault(generator, []).append(token)

        for generator, keyed_tokens in generator_tokens.items():
            yield from generator(keyed_tokens, person, visit_occurrence, src_stats)

        if unmatched and self._default:
            yield from self._default(unmatched, person, visit_occurrence, src_stats)

    def has_generator_for(self, token: EventKey) -> bool:
        return self._normalize_key(token) in self._registry

    def registered_keys(self) -> tuple[EventKey, ...]:
        return tuple(self._registry.keys())

