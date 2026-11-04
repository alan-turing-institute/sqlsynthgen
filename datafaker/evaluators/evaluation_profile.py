"""The evaluation profile enum, kept in its own module to avoid a cycle.

``column_evaluator`` and ``statistical_fidelity`` both need this type, and
each also needs something from the other module - keeping the enum here lets
both import it directly instead of one importing it transitively through the
other.
"""

from enum import Enum, auto


class EvaluationProfile(Enum):
    """The kind of value a column holds, used to pick its fidelity criteria."""

    SHORT_TEXT = auto()
    CATEGORICAL = auto()
    FREE_TEXT = auto()
    EMAIL = auto()
    IDENTIFIER = auto()
    TEMPORAL = auto()
