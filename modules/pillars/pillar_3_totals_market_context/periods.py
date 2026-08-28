"""Declarative period scopes and derived metric names for Pillar 3."""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from typing import Iterable

from modules.pillars.market_snapshot_extractor import MarketIdentity


PERIOD_STATUS_COMPLETE = "COMPLETE"
PERIOD_STATUS_AMBIGUOUS = "AMBIGUOUS"
PERIOD_STATUS_INVALID = "INVALID"
PERIOD_STATUS_INCOMPLETE = "INCOMPLETE"


P3_MINIMUM_INPUT_NAMES = (
    "PIN_TOTAL_LINE",
    "PIN_OVER_PRICE",
    "PIN_UNDER_PRICE",
    "B365_TOTAL_LINE",
    "B365_OVER_PRICE",
    "B365_UNDER_PRICE",
)


@dataclass(frozen=True)
class TotalsPeriodScope:
    """One independently calculable totals-market period."""

    key: str
    display_name: str
    metric_token: str
    required: bool
    identities: tuple[MarketIdentity, ...]

    def __post_init__(self) -> None:
        if not self.key or self.key != self.key.casefold():
            raise ValueError("period scope key must be a non-empty lowercase value")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", self.metric_token):
            raise ValueError(
                "period scope metric_token must be uppercase snake case"
            )
        if not self.identities:
            raise ValueError("period scope must declare at least one market identity")

    def input_names(self) -> tuple[str, ...]:
        return P3_MINIMUM_INPUT_NAMES

    def derived_metrics(self) -> "P3DerivedMetricNames":
        return derived_metric_names(self)


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
)

# Adding a period means registering another immutable scope here. Extraction,
# calculation, logging and mining consume the selected scope declaratively.
# First Half is intentionally not registered yet.
P3_TOTALS_PERIOD_SCOPES: tuple[TotalsPeriodScope, ...] = (
    FULL_TIME_TOTALS_SCOPE,
)
DEFAULT_P3_TOTALS_PERIOD_SCOPE = FULL_TIME_TOTALS_SCOPE


@dataclass(frozen=True)
class P3DerivedMetricNames:
    pin_edge: str
    pin_direction: str
    b365_edge: str
    b365_direction: str
    line_diff: str
    line_gap: str
    price_gap: str
    pin_weight: str
    b365_weight: str
    market_edge: str
    p3_direction: str
    context_direction: str
    completeness: str

    def all(self) -> tuple[str, ...]:
        return tuple(getattr(self, item.name) for item in fields(self))


def derived_metric_names(scope: TotalsPeriodScope) -> P3DerivedMetricNames:
    """Build the complete public metric vocabulary for one period scope."""
    period = scope.metric_token
    return P3DerivedMetricNames(
        pin_edge=f"PIN_TOTAL_{period}_EDGE",
        pin_direction=f"PIN_TOTAL_{period}_DIRECTION_RAW",
        b365_edge=f"B365_TOTAL_{period}_EDGE",
        b365_direction=f"B365_TOTAL_{period}_DIRECTION_RAW",
        line_diff=f"TOTAL_{period}_LINE_DIFF_RAW",
        line_gap=f"TOTAL_{period}_LINE_GAP",
        price_gap=f"TOTAL_{period}_PRICE_GAP",
        pin_weight=f"W_PIN_TOTALS_{period}",
        b365_weight=f"W_B365_TOTALS_{period}",
        market_edge=f"TOTALS_MARKET_{period}_EDGE",
        p3_direction=f"P3_{period}_DIRECTION_RAW",
        context_direction=f"CONTEXT_{period}_DIRECTION_RAW",
        completeness=f"Q_COMPLETE_TOTALS_{period}",
    )


def period_scope_from_key(key: object) -> TotalsPeriodScope | None:
    normalized = str(key or "").strip().casefold()
    return next(
        (scope for scope in P3_TOTALS_PERIOD_SCOPES if scope.key == normalized),
        None,
    )


def period_scope_from_token(token: object) -> TotalsPeriodScope | None:
    """Resolve a configured scope from its stable public metric token."""
    normalized = str(token or "").strip().upper()
    return next(
        (
            scope
            for scope in P3_TOTALS_PERIOD_SCOPES
            if scope.metric_token == normalized
        ),
        None,
    )


def resolve_period_status(
    *,
    snapshot: object | None,
    missing_inputs: Iterable[str],
    invalid_inputs: Iterable[str],
    ambiguous_inputs: Iterable[str],
) -> str:
    """Classify one period from its local snapshot and diagnostic sets."""
    if snapshot is not None:
        return PERIOD_STATUS_COMPLETE
    if any(ambiguous_inputs):
        return PERIOD_STATUS_AMBIGUOUS
    if any(invalid_inputs):
        return PERIOD_STATUS_INVALID
    return PERIOD_STATUS_INCOMPLETE


def resolve_pillar_status(
    *,
    required_complete: bool,
    optional_complete: bool,
    signal_present: bool,
    available_input_count: int = 0,
) -> str:
    """Translate independent period gates into the public pillar status."""
    if not required_complete:
        return "INSUFFICIENT_DATA"
    if not optional_complete:
        return "PARTIAL"
    if signal_present:
        return "ACTIVE"
    if available_input_count:
        return "PARTIAL"
    return "INSUFFICIENT_DATA"


__all__ = [
    "DEFAULT_P3_TOTALS_PERIOD_SCOPE",
    "FULL_TIME_TOTALS_SCOPE",
    "P3DerivedMetricNames",
    "P3_MINIMUM_INPUT_NAMES",
    "P3_TOTALS_PERIOD_SCOPES",
    "TotalsPeriodScope",
    "derived_metric_names",
    "period_scope_from_key",
    "period_scope_from_token",
    "resolve_period_status",
    "resolve_pillar_status",
]
