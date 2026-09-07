"""Declarative period and input vocabulary for Pillar 3."""

from __future__ import annotations

import re
from dataclasses import dataclass

from modules.pillars.market_snapshot_extractor import MarketIdentity


from modules.pillars.market_coverage import (
    PERIOD_STATUS_COMPLETE, PERIOD_STATUS_PARTIAL, PERIOD_STATUS_AMBIGUOUS,
    PERIOD_STATUS_INVALID, PERIOD_STATUS_INCOMPLETE,
    resolve_period_status, resolve_pillar_status,
)


EXCHANGE_OU_LINE_INPUT_NAME = "BF_OU_FULL_TIME_LINE"
EXCHANGE_OU_ODDS_INPUT_NAMES = (
    "BF_OU_BACK_OVER_FULL_TIME_ODDS_PRICE",
    "BF_OU_BACK_UNDER_FULL_TIME_ODDS_PRICE",
    "BF_OU_LAY_OVER_FULL_TIME_ODDS_PRICE",
    "BF_OU_LAY_UNDER_FULL_TIME_ODDS_PRICE",
)
EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES = (
    "BF_OU_BACK_OVER_FULL_TIME_EXCHANGE_SIZE",
    "BF_OU_BACK_UNDER_FULL_TIME_EXCHANGE_SIZE",
    "BF_OU_LAY_OVER_FULL_TIME_EXCHANGE_SIZE",
    "BF_OU_LAY_UNDER_FULL_TIME_EXCHANGE_SIZE",
)
EXCHANGE_OU_1H_LINE_INPUT_NAME = "BF_OU_1H_LINE"
EXCHANGE_OU_1H_ODDS_INPUT_NAMES = (
    "BF_OU_BACK_OVER_1H_ODDS_PRICE",
    "BF_OU_BACK_UNDER_1H_ODDS_PRICE",
    "BF_OU_LAY_OVER_1H_ODDS_PRICE",
    "BF_OU_LAY_UNDER_1H_ODDS_PRICE",
)
EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES = (
    "BF_OU_BACK_OVER_1H_EXCHANGE_SIZE",
    "BF_OU_BACK_UNDER_1H_EXCHANGE_SIZE",
    "BF_OU_LAY_OVER_1H_EXCHANGE_SIZE",
    "BF_OU_LAY_UNDER_1H_EXCHANGE_SIZE",
)


@dataclass(frozen=True, slots=True)
class TotalsBookInputSpec:
    line: str
    over: str
    under: str

    def names(self) -> tuple[str, str, str]:
        return self.line, self.over, self.under


@dataclass(frozen=True, slots=True)
class TotalsPeriodScope:
    """One independently extracted Over/Under period."""

    key: str
    display_name: str
    metric_token: str
    required: bool
    identities: tuple[MarketIdentity, ...]
    pinnacle: TotalsBookInputSpec
    bet365: TotalsBookInputSpec

    def __post_init__(self) -> None:
        if not self.key or self.key != self.key.casefold():
            raise ValueError("period scope key must be a non-empty lowercase value")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", self.metric_token):
            raise ValueError("period scope metric_token must be uppercase snake case")
        if not self.identities:
            raise ValueError("period scope must declare at least one market identity")

    def input_names(self) -> tuple[str, ...]:
        return (*self.pinnacle.names(), *self.bet365.names())


FULL_TIME_TOTALS_SCOPE = TotalsPeriodScope(
    key="full_time",
    display_name="Full Time",
    metric_token="FULL_TIME",
    required=True,
    identities=(
        MarketIdentity("Over/Under", "Full Time", "Over/Under Full Time"),
        MarketIdentity(
            "Over/Under",
            "Full Time Including Overtime",
            "Over/Under Full Time Including Overtime",
        ),
    ),
    pinnacle=TotalsBookInputSpec(
        line="PIN_FT_OU_LINE",
        over="PIN_FT_OVER_ODDS",
        under="PIN_FT_UNDER_ODDS",
    ),
    bet365=TotalsBookInputSpec(
        line="B365_FT_OU_LINE",
        over="B365_FT_OVER_ODDS",
        under="B365_FT_UNDER_ODDS",
    ),
)

FIRST_HALF_TOTALS_SCOPE = TotalsPeriodScope(
    key="first_half",
    display_name="First Half",
    metric_token="FIRST_HALF",
    required=False,
    identities=(
        MarketIdentity("Over/Under", "1st Half", "Over/Under 1st Half"),
    ),
    pinnacle=TotalsBookInputSpec(
        line="PIN_1H_OU_LINE",
        over="PIN_1H_OVER_ODDS",
        under="PIN_1H_UNDER_ODDS",
    ),
    bet365=TotalsBookInputSpec(
        line="B365_1H_OU_LINE",
        over="B365_1H_OVER_ODDS",
        under="B365_1H_UNDER_ODDS",
    ),
)

P3_TOTALS_PERIOD_SCOPES: tuple[TotalsPeriodScope, ...] = (
    FULL_TIME_TOTALS_SCOPE,
    FIRST_HALF_TOTALS_SCOPE,
)


__all__ = [
    "FIRST_HALF_TOTALS_SCOPE",
    "EXCHANGE_OU_LINE_INPUT_NAME",
    "EXCHANGE_OU_ODDS_INPUT_NAMES",
    "EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES",
    "EXCHANGE_OU_1H_LINE_INPUT_NAME",
    "EXCHANGE_OU_1H_ODDS_INPUT_NAMES",
    "EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES",
    "FULL_TIME_TOTALS_SCOPE",
    "P3_TOTALS_PERIOD_SCOPES",
    "PERIOD_STATUS_AMBIGUOUS",
    "PERIOD_STATUS_COMPLETE",
    "PERIOD_STATUS_PARTIAL",
    "PERIOD_STATUS_INCOMPLETE",
    "PERIOD_STATUS_INVALID",
    "TotalsBookInputSpec",
    "TotalsPeriodScope",
    "resolve_period_status",
    "resolve_pillar_status",
]
