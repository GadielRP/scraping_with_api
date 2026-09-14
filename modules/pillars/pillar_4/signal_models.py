"""Serializable DTOs for the canonical P4 signal profile."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class P4SeriesResult:
    series_id: str
    market: dict[str, Any]
    points: tuple[dict[str, Any], ...]
    legs: tuple[dict[str, Any], ...]
    raw_temporal_features: dict[str, Any]
    structural_signals: dict[str, Any]
    traceability: dict[str, Any]
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "SERIES_ID": self.series_id,
            "STATUS": self.status,
            "MARKET": self.market,
            "POINTS": list(self.points),
            "LEGS": list(self.legs),
            "RAW_TEMPORAL_FEATURES": self.raw_temporal_features,
            "STRUCTURAL_SIGNALS": self.structural_signals,
            "TRACEABILITY": self.traceability,
        }


@dataclass(frozen=True, slots=True)
class P4ViewProfile:
    source_mode: str
    status: str
    series: tuple[P4SeriesResult, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "SOURCE_MODE": self.source_mode,
            "STATUS": self.status,
            "SERIES": [item.to_dict() for item in self.series],
        }


@dataclass(frozen=True, slots=True)
class P4SignalProfile:
    meta: dict[str, Any]
    adaptive_view: P4ViewProfile | None
    checkpoint_view: P4ViewProfile | None
    structural_domain_summary: dict[str, Any]
    summary: dict[str, Any]
    traceability: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "META": self.meta,
            "ADAPTIVE_VIEW": (
                None if self.adaptive_view is None else self.adaptive_view.to_dict()
            ),
            "CHECKPOINT_VIEW": (
                None if self.checkpoint_view is None else self.checkpoint_view.to_dict()
            ),
            "STRUCTURAL_DOMAIN_SUMMARY": self.structural_domain_summary,
            "SUMMARY": self.summary,
            "TRACEABILITY": self.traceability,
        }


__all__ = ["P4SeriesResult", "P4SignalProfile", "P4ViewProfile"]
