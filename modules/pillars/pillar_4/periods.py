"""Canonical scope and status vocabulary for Pillar 4."""

from __future__ import annotations

from typing import Any

P4_PILLAR_ID = "pillar_4_temporal_market_drift"

from modules.pillars.market_evaluation import (
    PINNACLE,
    BET365,
    BETFAIR,
    READING_CAPABILITIES,
)

EXCHANGE_BOOKIE_ID = BETFAIR.id
REGULAR_BOOKIE_IDS = frozenset({PINNACLE.id, BET365.id})
SUPPORTED_BOOKIE_IDS = frozenset(
    bookie_id
    for capability in READING_CAPABILITIES
    if capability.pillar == 4
    for bookie_id in capability.book_ids
)


def bookmaker_role(bookie_id: Any) -> str:
    """Describe a source without making it mandatory for execution."""
    try:
        normalized_id = int(bookie_id)
    except (TypeError, ValueError):
        return "DERIVED"
    if normalized_id in REGULAR_BOOKIE_IDS:
        return "REGULAR"
    if normalized_id == EXCHANGE_BOOKIE_ID:
        return "EXCHANGE"
    return "UNCLASSIFIED"


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


__all__ = [
    "EXCHANGE_BOOKIE_ID",
    "P4_PILLAR_ID",
    "REGULAR_BOOKIE_IDS",
    "SUPPORTED_BOOKIE_IDS",
    "bookmaker_role",
    "normalize_token",
    "resolve_domain",
]
