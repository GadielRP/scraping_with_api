"""Declarative period and input vocabulary for Pillar 2."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from modules.pillars.market_snapshot_extractor import MarketIdentity


PERIOD_STATUS_COMPLETE = "COMPLETE"
PERIOD_STATUS_AMBIGUOUS = "AMBIGUOUS"
PERIOD_STATUS_INVALID = "INVALID"
PERIOD_STATUS_INCOMPLETE = "INCOMPLETE"

EXCHANGE_ODDS_INPUT_NAMES = (
    "BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE",
    "BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE",
    "BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE",
    "BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE",
    "BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE",
    "BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE",
)

EXCHANGE_SIZE_TRACE_INPUT_NAMES = (
    "BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE",
)


@dataclass(frozen=True, slots=True)
class TwoWayMarketSpec:
    """One independently requested two-way or Asian Handicap market."""

    identities: tuple[MarketIdentity, ...]
    pinnacle_home: str
    pinnacle_away: str
    bet365_home: str
    bet365_away: str
    pinnacle_line: str | None = None
    bet365_line: str | None = None

    def input_names(self) -> tuple[str, ...]:
        names = [self.pinnacle_home, self.pinnacle_away, self.bet365_home, self.bet365_away]
        if self.pinnacle_line:
            names.append(self.pinnacle_line)
        if self.bet365_line:
            names.append(self.bet365_line)
        return tuple(names)


@dataclass(frozen=True, slots=True)
class SidePeriodScope:
    """Identity and input policy for one independently validated period."""

    key: str
    display_name: str
    metric_token: str
    required: bool
    one_x_two: TwoWayMarketSpec
    asian_handicap: TwoWayMarketSpec
    includes_exchange: bool

    def __post_init__(self) -> None:
        if not self.key or self.key != self.key.casefold():
            raise ValueError("period scope key must be a non-empty lowercase value")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", self.metric_token):
            raise ValueError("period scope metric_token must be uppercase snake case")
        if not self.one_x_two.identities or not self.asian_handicap.identities:
            raise ValueError("period scope must declare 1X2 and AH identities")

    def required_input_names(self) -> tuple[str, ...]:
        names = self.one_x_two.input_names() + self.asian_handicap.input_names()
        return names + EXCHANGE_ODDS_INPUT_NAMES if self.includes_exchange else names

    def trace_input_names(self) -> tuple[str, ...]:
        return EXCHANGE_SIZE_TRACE_INPUT_NAMES if self.includes_exchange else ()

    def input_names(self) -> tuple[str, ...]:
        return self.required_input_names() + self.trace_input_names()


FULL_TIME_SIDE_SCOPE = SidePeriodScope(
    key="full_time",
    display_name="Full Time",
    metric_token="FULL_TIME",
    required=True,
    one_x_two=TwoWayMarketSpec(
        identities=tuple(
            MarketIdentity("1X2", period, "1X2 Full Time")
            for period in ("Full Time", "Full Time Including Overtime")
        ),
        pinnacle_home="PIN_HOME_1X2_FULL_TIME_ODDS_PRICE",
        pinnacle_away="PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE",
        bet365_home="B365_HOME_1X2_FULL_TIME_ODDS_PRICE",
        bet365_away="B365_AWAY_1X2_FULL_TIME_ODDS_PRICE",
    ),
    asian_handicap=TwoWayMarketSpec(
        identities=tuple(
            MarketIdentity("Asian Handicap", period, "Asian Handicap Full Time")
            for period in ("Full Time", "Full Time Including Overtime")
        ),
        pinnacle_home="PIN_AH_HOME_FULL_TIME_ODDS_PRICE",
        pinnacle_away="PIN_AH_AWAY_FULL_TIME_ODDS_PRICE",
        bet365_home="B365_AH_HOME_FULL_TIME_ODDS_PRICE",
        bet365_away="B365_AH_AWAY_FULL_TIME_ODDS_PRICE",
        pinnacle_line="PIN_AH_FULL_TIME_LINE",
        bet365_line="B365_AH_FULL_TIME_LINE",
    ),
    includes_exchange=True,
)

FIRST_HALF_SIDE_SCOPE = SidePeriodScope(
    key="first_half",
    display_name="First Half",
    metric_token="FIRST_HALF",
    required=False,
    one_x_two=TwoWayMarketSpec(
        identities=(MarketIdentity("1X2", "1st Half", "1X2 1st Half"),),
        pinnacle_home="PIN_HOME_1X2_1H_ODDS_PRICE",
        pinnacle_away="PIN_AWAY_1X2_1H_ODDS_PRICE",
        bet365_home="B365_HOME_1X2_1H_ODDS_PRICE",
        bet365_away="B365_AWAY_1X2_1H_ODDS_PRICE",
    ),
    asian_handicap=TwoWayMarketSpec(
        identities=(MarketIdentity("Asian Handicap", "1st Half", "Asian Handicap 1st Half"),),
        pinnacle_home="PIN_AH_1H_HOME_PRICE",
        pinnacle_away="PIN_AH_1H_AWAY_PRICE",
        bet365_home="B365_AH_1H_HOME_PRICE",
        bet365_away="B365_AH_1H_AWAY_PRICE",
        pinnacle_line="PIN_AH_1H_LINE",
        bet365_line="B365_AH_1H_LINE",
    ),
    includes_exchange=False,
)

P2_SIDE_PERIOD_SCOPES: tuple[SidePeriodScope, ...] = (
    FULL_TIME_SIDE_SCOPE,
    FIRST_HALF_SIDE_SCOPE,
)
DEFAULT_P2_SIDE_PERIOD_SCOPE = FULL_TIME_SIDE_SCOPE


def period_scope_from_key(key: object) -> SidePeriodScope | None:
    normalized = str(key or "").strip().casefold()
    return next((scope for scope in P2_SIDE_PERIOD_SCOPES if scope.key == normalized), None)


def period_scope_from_token(token: object) -> SidePeriodScope | None:
    normalized = str(token or "").strip().upper()
    return next((scope for scope in P2_SIDE_PERIOD_SCOPES if scope.metric_token == normalized), None)


def resolve_period_status(
    *,
    snapshot: object | None,
    missing_inputs: Iterable[str],
    invalid_inputs: Iterable[str],
    ambiguous_inputs: Iterable[str],
) -> str:
    if snapshot is not None:
        return PERIOD_STATUS_COMPLETE
    if any(ambiguous_inputs):
        return PERIOD_STATUS_AMBIGUOUS
    if any(invalid_inputs):
        return PERIOD_STATUS_INVALID
    return PERIOD_STATUS_INCOMPLETE


def resolve_pillar_status(*, required_complete: bool, optional_complete: bool) -> str:
    if not required_complete:
        return "INSUFFICIENT_DATA"
    if not optional_complete:
        return "PARTIAL"
    return "ACTIVE"


__all__ = [
    "DEFAULT_P2_SIDE_PERIOD_SCOPE",
    "EXCHANGE_ODDS_INPUT_NAMES",
    "EXCHANGE_SIZE_TRACE_INPUT_NAMES",
    "FIRST_HALF_SIDE_SCOPE",
    "FULL_TIME_SIDE_SCOPE",
    "P2_SIDE_PERIOD_SCOPES",
    "SidePeriodScope",
    "TwoWayMarketSpec",
    "period_scope_from_key",
    "period_scope_from_token",
    "resolve_period_status",
    "resolve_pillar_status",
]
