"""Typed extraction models for causal P4 trajectories."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _timestamp(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


@dataclass(frozen=True, slots=True)
class P4Point:
    point_id: str
    value: Decimal
    effective_at: datetime
    availability_at: datetime
    minutes_before_start: Decimal
    snapshot_id: int | None = None
    quote_id: int | None = None
    collected_at: datetime | None = None
    source_collected_at: datetime | None = None
    source_limit: Decimal | None = None
    exchange_size: Decimal | None = None
    observation_kind: str = "PERSISTED_SNAPSHOT"
    target_minute: int | None = None
    distance_from_target_minutes: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "POINT_ID": self.point_id,
            "VALUE": _number(self.value),
            "EFFECTIVE_AT": _timestamp(self.effective_at),
            "AVAILABILITY_AT": _timestamp(self.availability_at),
            "MINUTES_BEFORE_START": _number(self.minutes_before_start),
            "SNAPSHOT_ID": self.snapshot_id,
            "QUOTE_ID": self.quote_id,
            "COLLECTED_AT": _timestamp(self.collected_at),
            "SOURCE_COLLECTED_AT": _timestamp(self.source_collected_at),
            "SOURCE_LIMIT": _number(self.source_limit),
            "EXCHANGE_SIZE": _number(self.exchange_size),
            "OBSERVATION_KIND": self.observation_kind,
            "TARGET_MINUTE": self.target_minute,
            "DISTANCE_FROM_TARGET_MINUTES": _number(
                self.distance_from_target_minutes
            ),
        }


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
    choice_group: str | None
    choice_group_key: str
    choice_name: str
    choice_id: int | None
    main_line: bool | None
    bookie_id: int | None
    bookie_name: str
    source: str | None
    exchange_side: str | None
    exchange_level: int
    quote_id: int | None
    points: tuple[P4Point, ...]
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
            "CHOICE_GROUP_KEY": self.choice_group_key,
            "CHOICE_GROUP": self.choice_group,
            "CHOICE_NAME": self.choice_name,
            "CHOICE_ID": self.choice_id,
            "MAIN_LINE": self.main_line,
            "BOOKIE_ID": self.bookie_id,
            "BOOKIE_NAME": self.bookie_name,
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
    target_minute: int
    operative_as_of: datetime
    adaptive_series: tuple[P4SeriesInput, ...] = ()
    checkpoint_series: tuple[P4SeriesInput, ...] = ()
    periods: dict[str, Any] = field(default_factory=dict)
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    excluded_future_points: int = 0
    source_series_seen: int = 0
    endpoint_series_present: int = 0
    reason: str | None = None

    @property
    def usable(self) -> bool:
        return self.endpoint_series_present > 0 and bool(
            self.adaptive_series or self.checkpoint_series
        )


__all__ = ["P4ExtractionResult", "P4Point", "P4SeriesInput"]
