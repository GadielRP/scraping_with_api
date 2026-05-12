"""Shared types and helpers for the pillar/module architecture."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Centralized strength thresholds (used by all modules)
# ---------------------------------------------------------------------------

_STRENGTH_THRESHOLDS: List[tuple[float, str]] = [
    (0.05, "IGNORE"),
    (0.15, "LOW"),
    (0.30, "MEDIUM"),
    (0.60, "HIGH"),
]
_STRENGTH_MAX_LABEL = "EXTREME"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def clamp(value: float, min_value: float = -1.0, max_value: float = 1.0) -> float:
    """Clamp *value* to [min_value, max_value]."""
    return max(min_value, min(value, max_value))


def calculate_bias(edge: float) -> str:
    """Return ``'HOME'``, ``'AWAY'``, or ``'NEUTRAL'`` based on *edge* sign."""
    if edge > 0:
        return "HOME"
    if edge < 0:
        return "AWAY"
    return "NEUTRAL"


def classify_strength(edge: float) -> str:
    """Classify the absolute edge into a human-readable strength label.

    Uses the centralized thresholds defined in ``_STRENGTH_THRESHOLDS``.
    """
    abs_edge = abs(edge)
    for threshold, label in _STRENGTH_THRESHOLDS:
        if abs_edge < threshold:
            return label
    return _STRENGTH_MAX_LABEL


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ModuleComponentResult:
    """Result for a single component within a module."""

    name: str
    edge: float
    bias: str
    strength: str
    weight: float
    weighted_edge: float
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModuleResult:
    """Result produced by a single module (e.g. M1 Base Strength)."""

    pillar_id: str
    module_id: str
    module_name: str
    event_id: int
    participants: str
    value: float
    bias: str
    strength: str
    components: List[ModuleComponentResult] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)
