"""Typed calculation contracts for Pillar 5 exact price memory."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

MarketShape = Literal["TWO_WAY", "THREE_WAY"]


def _json_number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


@dataclass(frozen=True, slots=True)
class PopulationFilters:
    """Optional restrictions applied on top of the mandatory sport population."""

    competition_id: int | None = None
    season_id: int | None = None
    country: str | None = None

    def to_dict(self) -> dict[str, int | str | None]:
        return {
            "competition_id": self.competition_id,
            "season_id": self.season_id,
            "country": self.country,
        }


@dataclass(frozen=True, slots=True)
class MemoryQueryKey:
    """Complete semantic key used to query one bookmaker memory."""

    sport: str
    bookie_id: int
    market_group: str
    market_period: str
    market_shape: MarketShape
    odds_home: Decimal
    odds_draw: Decimal | None
    odds_away: Decimal

    @property
    def has_draw(self) -> bool:
        return self.market_shape == "THREE_WAY"

    def price_vector(self) -> dict[str, float | None]:
        return {
            "HOME": float(self.odds_home),
            "DRAW": _json_number(self.odds_draw),
            "AWAY": float(self.odds_away),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "sport": self.sport,
            "bookie_id": self.bookie_id,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "market_shape": self.market_shape,
            "has_draw": self.has_draw,
            "odds_home": float(self.odds_home),
            "odds_draw": _json_number(self.odds_draw),
            "odds_away": float(self.odds_away),
        }


@dataclass(frozen=True, slots=True)
class MemorySample:
    """Complete eligible and de-duplicated historical population for one key."""

    key: MemoryQueryKey
    sample_size: int
    wins_home: int
    wins_draw: int
    wins_away: int
    exclusions: dict[str, int] = field(default_factory=dict)
    sample_id: str | None = None


@dataclass(frozen=True, slots=True)
class BookmakerMemoryProfile:
    """Independent public result for one standard bookmaker."""

    bookmaker: str
    bookie_id: int
    p5_status: str
    p5_valid: bool
    p5_direction: str
    p5: Decimal | None
    p5_strength: str
    market_group: str | None
    market_period: str | None
    market_shape: MarketShape | None
    target_minute: int | None
    current_price_vector: dict[str, float | None] | None
    memory_status: str
    reason: str | None
    sample_size: int | None = None
    wins_home: int | None = None
    wins_draw: int | None = None
    wins_away: int | None = None
    is_tie: bool | None = None
    dominant_result: str | None = None
    wins_dominant: int | None = None
    p_hist_dominant: Decimal | None = None
    baseline: Decimal | None = None
    hist_edge: Decimal | None = None
    consistency: Decimal | None = None
    sample_factor: Decimal | None = None
    msri_raw: Decimal | None = None
    msri_signal: Decimal | None = None
    sample_weight: Decimal | None = None
    sample_id: str | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bookmaker": self.bookmaker,
            "bookie_id": self.bookie_id,
            "P5_STATUS": self.p5_status,
            "P5_VALID": self.p5_valid,
            "P5_DIRECTION": self.p5_direction,
            "P5": _json_number(self.p5),
            "P5_STRENGTH": self.p5_strength,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "market_shape": self.market_shape,
            "target_minute": self.target_minute,
            "current_price_vector": self.current_price_vector,
            "memory_status": self.memory_status,
            "reason": self.reason,
            "sample_size": self.sample_size,
            "wins_home": self.wins_home,
            "wins_draw": self.wins_draw,
            "wins_away": self.wins_away,
            "is_tie": self.is_tie,
            "DOMINANT_RESULT": self.dominant_result,
            "wins_dominant": self.wins_dominant,
            "P_hist_DOMINANT": _json_number(self.p_hist_dominant),
            "BASELINE": _json_number(self.baseline),
            "HIST_EDGE": _json_number(self.hist_edge),
            "CONSISTENCY": _json_number(self.consistency),
            "SAMPLE_FACTOR": _json_number(self.sample_factor),
            "MSRI_RAW": _json_number(self.msri_raw),
            "MSRI_SIGNAL": _json_number(self.msri_signal),
            "SAMPLE_WEIGHT": _json_number(self.sample_weight),
            "sample_id": self.sample_id,
            "diagnostics": self.diagnostics,
        }


__all__ = [
    "BookmakerMemoryProfile",
    "MarketShape",
    "MemoryQueryKey",
    "MemorySample",
    "PopulationFilters",
]
