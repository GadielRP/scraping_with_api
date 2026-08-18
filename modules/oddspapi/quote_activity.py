"""Shared OddsPapi activity filters for markets and quotes."""

from __future__ import annotations

from typing import Any


def should_skip_inactive_market(
    market_data: Any,
    *,
    require_active_quotes: bool,
) -> bool:
    """Return True when a market must be ignored because it is marked inactive.

    ``ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES`` owns both player ``active`` and
    market ``marketActive``. When the flag is false, suspended markets that still
    publish prices remain eligible.
    """
    if not require_active_quotes:
        return False
    if not isinstance(market_data, dict):
        return False
    return market_data.get("marketActive") is False
