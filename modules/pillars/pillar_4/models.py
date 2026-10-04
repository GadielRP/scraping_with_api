"""Typed extraction models for causal P4 trajectories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .periods import bookmaker_role
from modules.pillars.trajectory_sampling import TrajectoryPoint, TrajectoryPointValue


@dataclass(frozen=True, slots=True)
class P4SeriesInput:
    series_id: str
    base_series_id: str
    domain: str
    view: str
    value_type: str
    market_id: int | None
    market_group: str
    market_period: str
    market_name: str
    line_value: str | None
    line_value_key: str
    choice_name: str
    choice_id: int | None
    main_line: bool | None
    bookie_id: int | None
    bookie_name: str
    source: str | None
    exchange_side: str | None
    exchange_level: int
    quote_id: int | None
    points: tuple[TrajectoryPoint | TrajectoryPointValue, ...]
    expected_target_minutes: tuple[int, ...] = ()
    missing_target_minutes: tuple[int, ...] = ()
    diagnostics: tuple[str, ...] = ()
    constituent_series_ids: tuple[str, ...] = ()
    operative_endpoint_present: bool = True

    def market_dict(self) -> dict[str, Any]:
        return {
            "DOMAIN": self.domain,
            "MARKET_ID": self.market_id,
            "MARKET_GROUP": self.market_group,
            "MARKET_PERIOD": self.market_period,
            "MARKET_NAME": self.market_name,
            "CHOICE_GROUP_KEY": self.line_value_key,
            "CHOICE_GROUP": self.line_value,
            "CHOICE_NAME": self.choice_name,
            "CHOICE_ID": self.choice_id,
            "MAIN_LINE": self.main_line,
            "BOOKIE_ID": self.bookie_id,
            "BOOKIE_NAME": self.bookie_name,
            "SOURCE_ROLE": bookmaker_role(self.bookie_id),
            "SOURCE": self.source,
            "EXCHANGE_SIDE": self.exchange_side,
            "EXCHANGE_LEVEL": self.exchange_level,
            "QUOTE_ID": self.quote_id,
            "VALUE_TYPE": self.value_type,
            "VIEW": self.view,
        }


@dataclass(frozen=True, slots=True)
class P4ExtractionResult:
    event_id: int
    target_minute: int | None
    operative_as_of: datetime | None
    nominal_target_as_of: datetime | None = None
    evaluation_as_of: datetime | None = None
    adaptive_series: tuple[P4SeriesInput, ...] = ()
    checkpoint_series: tuple[P4SeriesInput, ...] = ()
    missing_inputs: tuple[str, ...] = ()
    missing_endpoint_details: tuple[dict[str, Any], ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    excluded_future_points: int = 0
    source_series_seen: int = 0
    endpoint_series_present: int = 0
    reason: str | None = None

    @property
    def usable(self) -> bool:
        return bool(self.adaptive_series or self.checkpoint_series)


__all__ = ["P4ExtractionResult", "P4SeriesInput"]
