"""Shared OddsPapi activity filters for markets and quotes."""

from __future__ import annotations

from typing import Any

from modules.oddspapi.format_utils import normalize_source_id


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


def find_stale_inactive_duplicate_mainline_market_ids(
    markets_data: Any,
    *,
    market_mapping_index: Any = None,
    source_sport_id: Any = None,
    source: str = "oddspapi",
) -> set[str]:
    """Identify market IDs that are marked mainLine=True but are inactive (stale)
    when another market of the same canonical market type has active mainLine=True.

    Rules:
    - If a canonical market type has only one market with mainLine=True, it is
      never considered a duplicate and is not discarded.
    - If all markets with mainLine=True are inactive, none is discarded.
    - Only inactive markets that coexist with at least one active market for the
      same canonical market type are returned.
    """
    if not isinstance(markets_data, dict):
        return set()

    # Group candidate mainline markets by canonical market key (or fallback key)
    grouped: dict[Any, list[tuple[str, bool]]] = {}

    for source_market_id, market_data in markets_data.items():
        if not isinstance(market_data, dict):
            continue
        normalized_market_id = normalize_source_id(source_market_id)
        if normalized_market_id is None:
            continue

        outcomes = market_data.get("outcomes") or {}
        has_mainline = False
        has_active_mainline = False
        market_active = market_data.get("marketActive") is not False

        for outcome_data in outcomes.values():
            if not isinstance(outcome_data, dict):
                continue
            players = outcome_data.get("players")
            if not players and "price" in outcome_data:
                players = [outcome_data]
            if isinstance(players, dict):
                player_list = list(players.values())
            elif isinstance(players, list):
                player_list = players
            else:
                player_list = []

            for player in player_list:
                if isinstance(player, dict) and player.get("mainLine") is True:
                    has_mainline = True
                    if market_active and player.get("active") is not False:
                        has_active_mainline = True

        if not has_mainline:
            continue

        group_key: Any = normalized_market_id
        if market_mapping_index is not None:
            from infrastructure.persistence.repositories.market_mapping_repository import (
                MarketMappingRepository,
            )

            res = MarketMappingRepository.resolve_market(
                market_mapping_index,
                source=source,
                source_sport_id=source_sport_id,
                source_market_id=normalized_market_id,
            )
            if res.resolved and res.canonical_market_key:
                group_key = res.canonical_market_key
            else:
                mtype = market_data.get("marketType")
                mperiod = market_data.get("period")
                if mtype:
                    group_key = (mtype, mperiod)

        grouped.setdefault(group_key, []).append((normalized_market_id, has_active_mainline))

    discarded_market_ids: set[str] = set()
    for _, candidates in grouped.items():
        if len(candidates) > 1:
            active_ids = [m_id for m_id, is_act in candidates if is_act]
            inactive_ids = [m_id for m_id, is_act in candidates if not is_act]
            if active_ids and inactive_ids:
                discarded_market_ids.update(inactive_ids)

    return discarded_market_ids

