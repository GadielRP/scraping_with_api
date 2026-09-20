"""Declarative period and input vocabulary for Pillar 5."""

from __future__ import annotations

import re
from dataclasses import dataclass

from modules.pillars.market_coverage import (
    resolve_period_status,
    resolve_pillar_status,
)
from modules.pillars.market_snapshot_extractor import MarketIdentity

# Pinnacle 1X2 Full Time
PIN_HOME_1X2_FULL_TIME_ODDS_PRICE = "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"
PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE = "PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE"
PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE = "PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE"
PINNACLE_1X2_INPUT_NAMES = (
    PIN_HOME_1X2_FULL_TIME_ODDS_PRICE,
    PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE,
    PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE,
)

# Bet365 1X2 Full Time
B365_HOME_1X2_FULL_TIME_ODDS_PRICE = "B365_HOME_1X2_FULL_TIME_ODDS_PRICE"
B365_DRAW_1X2_FULL_TIME_ODDS_PRICE = "B365_DRAW_1X2_FULL_TIME_ODDS_PRICE"
B365_AWAY_1X2_FULL_TIME_ODDS_PRICE = "B365_AWAY_1X2_FULL_TIME_ODDS_PRICE"
BET365_1X2_INPUT_NAMES = (
    B365_HOME_1X2_FULL_TIME_ODDS_PRICE,
    B365_DRAW_1X2_FULL_TIME_ODDS_PRICE,
    B365_AWAY_1X2_FULL_TIME_ODDS_PRICE,
)

# SofaScore 1X2 Full Time
SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE = "SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE"
SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE = "SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE"
SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE = "SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE"
SOFASCORE_1X2_INPUT_NAMES = (
    SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE,
    SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE,
    SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE,
)

# Betfair Exchange 1X2 Full Time Odds
BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE = "BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE"
BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE = "BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE"
BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE = "BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE"
BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE = "BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE"
BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE = "BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE"
BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE = "BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE"
EXCHANGE_ODDS_INPUT_NAMES = (
    BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE,
    BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE,
    BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE,
    BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE,
    BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE,
    BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE,
)

# Betfair Exchange 1X2 Full Time Sizes
BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE = "BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"
BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE = "BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE"
BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE = "BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"
BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE = "BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE"
BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE = "BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"
BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE = "BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE"
EXCHANGE_SIZE_TRACE_INPUT_NAMES = (
    BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE,
    BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE,
    BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE,
    BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE,
    BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE,
    BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE,
)


@dataclass(frozen=True, slots=True)
class Book1X2InputSpec:
    """Inputs for a standard bookmaker 1X2 or Home/Away market."""

    home: str
    away: str
    draw: str | None = None

    def input_names(self) -> tuple[str, ...]:
        if self.draw is not None:
            return (self.home, self.draw, self.away)
        return (self.home, self.away)


@dataclass(frozen=True, slots=True)
class PriceMemoryPeriodScope:
    """Identity and input policy for Pillar 5 Full Time price memory scope."""

    key: str
    display_name: str
    metric_token: str
    required: bool
    identities: tuple[MarketIdentity, ...]
    pinnacle: Book1X2InputSpec
    bet365: Book1X2InputSpec
    sofascore: Book1X2InputSpec
    includes_exchange: bool = True

    def __post_init__(self) -> None:
        if not self.key or self.key != self.key.casefold():
            raise ValueError("period scope key must be a non-empty lowercase value")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", self.metric_token):
            raise ValueError("period scope metric_token must be uppercase snake case")
        if not self.identities:
            raise ValueError("period scope must declare at least one market identity")

    def input_names(self) -> tuple[str, ...]:
        names = list(
            self.pinnacle.input_names()
            + self.bet365.input_names()
            + self.sofascore.input_names()
        )
        if self.includes_exchange:
            names.extend(EXCHANGE_ODDS_INPUT_NAMES)
            names.extend(EXCHANGE_SIZE_TRACE_INPUT_NAMES)
        return tuple(names)


FULL_TIME_PRICE_MEMORY_SCOPE = PriceMemoryPeriodScope(
    key="full_time",
    display_name="Full Time",
    metric_token="FULL_TIME",
    required=True,
    identities=(
        MarketIdentity("1X2", "Full Time", "1X2 Full Time"),
        MarketIdentity("1X2", "Full Time Including Overtime", "1X2 Full Time"),
        MarketIdentity("1X2", "Full Time Including Overtime", "1X2 Full Time Including Overtime"),
        MarketIdentity("Home/Away", "Full Time", "Home/Away Full Time"),
        MarketIdentity("Home/Away", "Full Time Including Overtime", "Home/Away Full Time Including Overtime"),
        MarketIdentity("Home/Away", "Full Time Including Overtime", "Home/Away Full Time"),
        MarketIdentity("Home/Away", "Full Time", "Home/Away Full Time Including Overtime"),
    ),
    pinnacle=Book1X2InputSpec(
        home=PIN_HOME_1X2_FULL_TIME_ODDS_PRICE,
        draw=PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE,
        away=PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE,
    ),
    bet365=Book1X2InputSpec(
        home=B365_HOME_1X2_FULL_TIME_ODDS_PRICE,
        draw=B365_DRAW_1X2_FULL_TIME_ODDS_PRICE,
        away=B365_AWAY_1X2_FULL_TIME_ODDS_PRICE,
    ),
    sofascore=Book1X2InputSpec(
        home=SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE,
        draw=SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE,
        away=SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE,
    ),
    includes_exchange=True,
)

P5_PRICE_MEMORY_MARKET_GROUPS: tuple[str, ...] = ("1X2", "Home/Away")
P5_PRICE_MEMORY_MARKET_PERIODS: tuple[str, ...] = (
    "Full Time",
    "Full Time Including Overtime",
)

P5_PRICE_MEMORY_PERIOD_SCOPES: tuple[PriceMemoryPeriodScope, ...] = (
    FULL_TIME_PRICE_MEMORY_SCOPE,
)
DEFAULT_P5_PERIOD_SCOPE = FULL_TIME_PRICE_MEMORY_SCOPE


def period_scope_from_key(key: object) -> PriceMemoryPeriodScope | None:
    normalized = str(key or "").strip().casefold()
    return next((scope for scope in P5_PRICE_MEMORY_PERIOD_SCOPES if scope.key == normalized), None)


def period_scope_from_token(token: object) -> PriceMemoryPeriodScope | None:
    normalized = str(token or "").strip().upper()
    return next((scope for scope in P5_PRICE_MEMORY_PERIOD_SCOPES if scope.metric_token == normalized), None)


__all__ = [
    "B365_AWAY_1X2_FULL_TIME_ODDS_PRICE",
    "B365_DRAW_1X2_FULL_TIME_ODDS_PRICE",
    "B365_HOME_1X2_FULL_TIME_ODDS_PRICE",
    "BET365_1X2_INPUT_NAMES",
    "BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE",
    "BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE",
    "BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE",
    "BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE",
    "BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE",
    "BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE",
    "BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE",
    "Book1X2InputSpec",
    "DEFAULT_P5_PERIOD_SCOPE",
    "EXCHANGE_ODDS_INPUT_NAMES",
    "EXCHANGE_SIZE_TRACE_INPUT_NAMES",
    "FULL_TIME_PRICE_MEMORY_SCOPE",
    "P5_PRICE_MEMORY_MARKET_GROUPS",
    "P5_PRICE_MEMORY_MARKET_PERIODS",
    "P5_PRICE_MEMORY_PERIOD_SCOPES",
    "PINNACLE_1X2_INPUT_NAMES",
    "PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE",
    "PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE",
    "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE",
    "PriceMemoryPeriodScope",
    "SOFASCORE_1X2_INPUT_NAMES",
    "SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE",
    "SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE",
    "SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE",
    "period_scope_from_key",
    "period_scope_from_token",
    "resolve_period_status",
    "resolve_pillar_status",
]
