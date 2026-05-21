"""Shared types and helpers for the pillar/module architecture."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


DEFAULT_STRENGTH_THRESHOLDS: List[tuple[float, str]] = [
    (0.05, "IGNORE"),
    (0.15, "LOW"),
    (0.30, "MEDIUM"),
    (0.60, "HIGH"),
]
DEFAULT_STRENGTH_MAX_LABEL = "EXTREME"

# Backward-compatible aliases for older imports/internal references.
_STRENGTH_THRESHOLDS = DEFAULT_STRENGTH_THRESHOLDS
_STRENGTH_MAX_LABEL = DEFAULT_STRENGTH_MAX_LABEL


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


def classify_strength(
    edge: float,
    thresholds: list[tuple[float, str]] | None = None,
    max_label: str = DEFAULT_STRENGTH_MAX_LABEL,
) -> str:
    """Classify the absolute edge into a human-readable strength label.

    Uses the centralized thresholds defined in ``DEFAULT_STRENGTH_THRESHOLDS``
    unless custom thresholds are provided.
    """
    abs_edge = abs(edge)
    active_thresholds = thresholds if thresholds is not None else DEFAULT_STRENGTH_THRESHOLDS

    for threshold, label in active_thresholds:
        if abs_edge < threshold:
            return label
    return max_label


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
