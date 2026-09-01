"""Canonical OVER/UNDER direction, relation and context semantics for P3."""

from decimal import Decimal
from typing import Literal


OVER = "OVER"
UNDER = "UNDER"
NEUTRAL = "NEUTRAL"
CONVERGENCE_OVER = "CONVERGENCE_OVER"
CONVERGENCE_UNDER = "CONVERGENCE_UNDER"
DIVERGENCE = "DIVERGENCE"
OPEN_BIAS = "OPEN_BIAS"
CLOSED_BIAS = "CLOSED_BIAS"
NEUTRAL_BIAS = "NEUTRAL_BIAS"

Direction = Literal["OVER", "UNDER", "NEUTRAL"]
Relation = Literal[
    "CONVERGENCE_OVER",
    "CONVERGENCE_UNDER",
    "DIVERGENCE",
    "NEUTRAL",
]
ContextDirection = Literal["OPEN_BIAS", "CLOSED_BIAS", "NEUTRAL_BIAS"]


def direction(edge: Decimal) -> Direction:
    if edge > 0:
        return OVER
    if edge < 0:
        return UNDER
    return NEUTRAL


def relation(left: Direction, right: Direction) -> Relation:
    if NEUTRAL in (left, right):
        return NEUTRAL
    if left == right == OVER:
        return CONVERGENCE_OVER
    if left == right == UNDER:
        return CONVERGENCE_UNDER
    return DIVERGENCE


def context_direction(value: Direction | None) -> ContextDirection | None:
    if value == OVER:
        return OPEN_BIAS
    if value == UNDER:
        return CLOSED_BIAS
    if value == NEUTRAL:
        return NEUTRAL_BIAS
    return None


__all__ = [
    "CLOSED_BIAS",
    "CONVERGENCE_OVER",
    "CONVERGENCE_UNDER",
    "ContextDirection",
    "DIVERGENCE",
    "Direction",
    "NEUTRAL",
    "NEUTRAL_BIAS",
    "OPEN_BIAS",
    "OVER",
    "Relation",
    "UNDER",
    "context_direction",
    "direction",
    "relation",
]
