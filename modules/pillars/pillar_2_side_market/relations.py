"""Canonical directional and relational semantics for Pillar 2."""

from decimal import Decimal
from typing import Literal


HOME = "HOME"
AWAY = "AWAY"
NEUTRAL = "NEUTRAL"
CONVERGENCE_HOME = "CONVERGENCE_HOME"
CONVERGENCE_AWAY = "CONVERGENCE_AWAY"
DIVERGENCE = "DIVERGENCE"

Direction = Literal["HOME", "AWAY", "NEUTRAL"]
Relation = Literal["CONVERGENCE_HOME", "CONVERGENCE_AWAY", "DIVERGENCE", "NEUTRAL"]


def direction(edge: Decimal) -> Direction:
    if edge > 0:
        return HOME
    if edge < 0:
        return AWAY
    return NEUTRAL


def relation(left: Direction, right: Direction) -> Relation:
    if NEUTRAL in (left, right):
        return NEUTRAL
    if left == right == HOME:
        return CONVERGENCE_HOME
    if left == right == AWAY:
        return CONVERGENCE_AWAY
    return DIVERGENCE


__all__ = [
    "AWAY", "CONVERGENCE_AWAY", "CONVERGENCE_HOME", "DIVERGENCE",
    "Direction", "HOME", "NEUTRAL", "Relation", "direction", "relation",
]
