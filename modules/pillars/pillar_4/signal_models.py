"""Typed analytical series before canonical result serialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from modules.pillars.trajectory_sampling import TrajectoryPoint, TrajectoryPointValue


@dataclass(frozen=True, slots=True)
class P4SeriesResult:
    series_id: str
    market: dict[str, Any]
    points: tuple[TrajectoryPoint | TrajectoryPointValue, ...]
    legs: tuple[dict[str, Any], ...]
    raw_temporal_features: dict[str, Any]
    structural_signals: dict[str, Any]
    traceability: dict[str, Any]
    status: str



__all__ = ["P4SeriesResult"]
