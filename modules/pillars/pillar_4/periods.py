"""Canonical scope and status vocabulary for Pillar 4."""

from __future__ import annotations

from typing import Any


P4_PILLAR_ID = "pillar_4_temporal_market_drift"
P4_MODULE_ID = "p4_signal_engine"
P4_MODULE_NAME = "Temporal Market Drift Engine"

EXCHANGE_BOOKIE_ID = 4
# Regular bookmakers provide P4's core status; the exchange adds optional signals.
REQUIRED_BOOKIE_IDS = frozenset({302, 3})
OPTIONAL_BOOKIE_IDS = frozenset({EXCHANGE_BOOKIE_ID})
SUPPORTED_BOOKIE_IDS = REQUIRED_BOOKIE_IDS | OPTIONAL_BOOKIE_IDS
BOOKMAKER_NAMES_BY_ID = {
    302: "Pinnacle Sports",
    3: "bet365",
    EXCHANGE_BOOKIE_ID: "Betfair Exchange",
}


def bookmaker_role(bookie_id: Any) -> str:
    """Classify whether a supported source gates P4's overall status."""
    try:
        normalized_id = int(bookie_id)
    except (TypeError, ValueError):
        return "DERIVED"
    if normalized_id in REQUIRED_BOOKIE_IDS:
        return "REQUIRED"
    if normalized_id in OPTIONAL_BOOKIE_IDS:
        return "OPTIONAL"
    return "UNCLASSIFIED"


def bookmaker_name(bookie_id: Any, fallback: str | None = None) -> str | None:
    """Return the observed bookmaker name or its stable P4 label."""
    if fallback:
        return str(fallback)
    try:
        normalized_id = int(bookie_id)
    except (TypeError, ValueError):
        return None
    return BOOKMAKER_NAMES_BY_ID.get(normalized_id)

_SIDE_GROUPS = frozenset(
    {
        "1x2",
        "home/away",
        "home away",
        "moneyline",
        "money line",
        "asian handicap",
        "asian_handicap",
        "ah",
    }
)
_TOTAL_GROUPS = frozenset(
    {
        "over/under",
        "over under",
        "over_under",
        "o/u",
        "totals",
        "total",
    }
)


def normalize_token(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").split())


def resolve_domain(market_group: Any, market_name: Any = None) -> str | None:
    """Return P4's structural domain without admitting standard handicap."""
    group = normalize_token(market_group)
    name = normalize_token(market_name)
    if group in _SIDE_GROUPS:
        return "SIDE"
    if group in _TOTAL_GROUPS:
        return "TOTALS"
    if "asian handicap" in group or "asian handicap" in name:
        return "SIDE"
    if "over/under" in group or "over/under" in name or "totals" in group:
        return "TOTALS"
    if group in {"handicap", "standard handicap", "standard_handicap"}:
        return None
    return None


def period_key(value: Any) -> str:
    token = normalize_token(value)
    if token in {"full time", "ft", "match"}:
        return "FULL_TIME"
    if token in {"first half", "1h", "1st half"}:
        return "FIRST_HALF"
    return str(value or "UNKNOWN").strip().upper().replace(" ", "_")


__all__ = [
    "BOOKMAKER_NAMES_BY_ID",
    "EXCHANGE_BOOKIE_ID",
    "OPTIONAL_BOOKIE_IDS",
    "P4_MODULE_ID",
    "P4_MODULE_NAME",
    "P4_PILLAR_ID",
    "REQUIRED_BOOKIE_IDS",
    "SUPPORTED_BOOKIE_IDS",
    "bookmaker_name",
    "bookmaker_role",
    "normalize_token",
    "period_key",
    "resolve_domain",
]
