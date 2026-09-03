"""Select fixture-specific exchange outcomes eligible for historical lookup."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
    MarketMappingRepository,
)
from modules.oddspapi.format_utils import normalize_source_id
from modules.oddspapi.quote_activity import should_skip_inactive_market
from modules.odds_ingestion.oddspapi_line_selection import LineSelection, select_current_lines

from .constants import ODDSPAPI_SOURCE


@dataclass(frozen=True)
class ExchangeHistoricalSelection:
    """One exchange/outcome pair accepted for a historical request."""

    bookmaker_slug: str
    source_market_id: str
    source_outcome_id: str
    canonical_market_key: str


@dataclass
class ExchangeOutcomeSelectionResult:
    selections: list[ExchangeHistoricalSelection] = field(default_factory=list)
    skipped_unmapped_markets: int = 0
    skipped_unmapped_outcomes: int = 0
    skipped_market_key: int = 0
    skipped_non_main_line: int = 0
    skipped_player_props: int = 0
    truncated: int = 0


class OddspapiExchangeOutcomeSelector:
    """Resolve current exchange offers to a bounded historical request plan."""

    @staticmethod
    def _entries(value: Any) -> Iterable[tuple[str, dict]]:
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, dict):
                    yield str(key), item
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if not isinstance(item, dict):
                    continue
                key = item.get("slug") or item.get("outcomeId") or index
                yield str(key), item

    @staticmethod
    def _eligible_players(
        outcome_data: dict,
        *,
        require_active_quotes: bool = True,
    ) -> list[tuple[str, dict]]:
        players = outcome_data.get("players")
        if not players and "price" in outcome_data:
            players = {"0": outcome_data}
        return [
            (player_id, player)
            for player_id, player in OddspapiExchangeOutcomeSelector._entries(
                players or {}
            )
            if (
                player.get("price") not in (None, "")
                and (
                    not require_active_quotes
                    or player.get("active") is not False
                )
            )
        ]

    @staticmethod
    def _is_player_prop(players: list[tuple[str, dict]]) -> bool:
        return any(
            str(player_id) != "0"
            or player.get("playerName") not in (None, "")
            for player_id, player in players
        )

    @classmethod
    def select(
        cls,
        odds_response: dict,
        *,
        exchange_bookmakers: list[str],
        market_mapping_index: MarketMappingIndex,
        allowed_market_keys: list[str] | set[str] | tuple[str, ...] | None,
        main_line_only: bool = True,
        include_player_props: bool = False,
        max_outcomes: int | None = None,
        require_active_quotes: bool = True,
    ) -> ExchangeOutcomeSelectionResult:
        payload = odds_response if isinstance(odds_response, dict) else {}
        source_sport_id = payload.get("sportId")
        requested_bookmakers = {
            str(slug).strip().lower()
            for slug in exchange_bookmakers or []
            if str(slug).strip()
        }
        allowed_keys = {
            str(key).strip()
            for key in allowed_market_keys or []
            if str(key).strip()
        }
        result = ExchangeOutcomeSelectionResult()
        seen: set[tuple[str, str]] = set()

        for bookmaker_key, bookmaker_data in cls._entries(
            payload.get("bookmakerOdds", {})
        ):
            slug = str(
                bookmaker_data.get("slug") or bookmaker_key
            ).strip().lower()
            if slug not in requested_bookmakers:
                continue

            line_selection = LineSelection()
            if main_line_only:
                line_selection = select_current_lines(
                    bookmaker_data.get("markets", {}),
                    market_mapping_index=market_mapping_index,
                    source_sport_id=source_sport_id,
                    is_live=bool(payload.get("isLive", False)),
                )

            for source_market_id, market_data in cls._entries(
                bookmaker_data.get("markets", {})
            ):
                if should_skip_inactive_market(
                    market_data,
                    require_active_quotes=require_active_quotes,
                ):
                    continue
                market_resolution = MarketMappingRepository.resolve_market(
                    market_mapping_index,
                    source=ODDSPAPI_SOURCE,
                    source_sport_id=source_sport_id,
                    source_market_id=source_market_id,
                )
                if not market_resolution.resolved:
                    result.skipped_unmapped_markets += 1
                    continue
                if (
                    allowed_keys
                    and market_resolution.canonical_market_key not in allowed_keys
                ):
                    result.skipped_market_key += 1
                    continue

                normalized_market_id = normalize_source_id(source_market_id)
                if normalized_market_id is None:
                    result.skipped_unmapped_markets += 1
                    continue
                if normalized_market_id in line_selection.excluded_market_ids:
                    result.skipped_non_main_line += len(market_data.get("outcomes", {}))
                    continue

                for source_outcome_id, outcome_data in cls._entries(
                    market_data.get("outcomes", {})
                ):
                    outcome_resolution = MarketMappingRepository.resolve_outcome(
                        market_mapping_index,
                        market_source_mapping_id=market_resolution.mapping_id,
                        source_outcome_id=source_outcome_id,
                    )
                    if not outcome_resolution.resolved:
                        result.skipped_unmapped_outcomes += 1
                        continue

                    players = cls._eligible_players(
                        outcome_data,
                        require_active_quotes=require_active_quotes,
                    )
                    if not players:
                        continue
                    if not include_player_props and cls._is_player_prop(players):
                        result.skipped_player_props += 1
                        continue
                    if (
                        main_line_only
                        and market_resolution.requires_choice_group
                        and normalized_market_id not in line_selection.selected_market_ids
                        and not any(player.get("mainLine") is True for _, player in players)
                    ):
                        result.skipped_non_main_line += 1
                        continue

                    normalized_outcome_id = normalize_source_id(source_outcome_id)
                    if normalized_outcome_id is None:
                        result.skipped_unmapped_outcomes += 1
                        continue
                    request_key = (slug, normalized_outcome_id)
                    if request_key in seen:
                        continue
                    if (
                        max_outcomes is not None
                        and len(result.selections) >= max_outcomes
                    ):
                        result.truncated += 1
                        continue

                    seen.add(request_key)
                    result.selections.append(
                        ExchangeHistoricalSelection(
                            bookmaker_slug=slug,
                            source_market_id=normalized_market_id,
                            source_outcome_id=normalized_outcome_id,
                            canonical_market_key=(
                                market_resolution.canonical_market_key or ""
                            ),
                        )
                    )

        return result
