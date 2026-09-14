"""Canonical scope and status vocabulary for Pillar 4."""

from __future__ import annotations

from typing import Any


P4_PILLAR_ID = "pillar_4_temporal_market_drift"
P4_MODULE_ID = "p4_signal_engine"
P4_MODULE_NAME = "Temporal Market Drift Engine"

SUPPORTED_BOOKIE_IDS = frozenset({302, 3, 4})
EXCHANGE_BOOKIE_ID = 4

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
    "EXCHANGE_BOOKIE_ID",
    "P4_MODULE_ID",
    "P4_MODULE_NAME",
    "P4_PILLAR_ID",
    "SUPPORTED_BOOKIE_IDS",
    "normalize_token",
    "period_key",
    "resolve_domain",
]
