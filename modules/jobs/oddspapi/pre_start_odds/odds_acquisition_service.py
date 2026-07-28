"""Compose current and historical OddsPapi requests for one fixture."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
)
from modules.odds_ingestion.fetch_result import OddsFetchResult

from .constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
)
from .exchange_outcome_selector import OddspapiExchangeOutcomeSelector
from .historical_odds_enricher import OddspapiHistoricalOddsEnricher
from .odds_fetcher import OddspapiOddsFetcher

logger = logging.getLogger(__name__)


@dataclass
class OddspapiOddsAcquisitionResult:
    payload: dict | None = None
    endpoint_missing: bool = False
    http_requests_attempted: int = 0
    exchange_historical_requests_attempted: int = 0
    exchange_historical_requests_failed: int = 0
    exchange_outcomes_selected: int = 0
    exchange_outcomes_skipped_budget: int = 0
    exchange_selection_diagnostics: dict[str, int] = field(default_factory=dict)


class OddspapiPreStartOddsAcquisitionService:
    """Acquire one fixture while keeping provider-specific branching isolated."""

    def __init__(self, fetcher: OddspapiOddsFetcher | None = None):
        self.fetcher = fetcher or OddspapiOddsFetcher()

    @staticmethod
    def _unique_bookmakers(*groups: list[str] | None) -> list[str]:
        unique: list[str] = []
        seen: set[str] = set()
        for group in groups:
            for value in group or []:
                slug = str(value).strip().lower()
                if slug and slug not in seen:
                    seen.add(slug)
                    unique.append(slug)
        return unique

    @staticmethod
    def _validate_bookmaker_groups(
        regular_bookmakers: list[str],
        exchange_bookmakers: list[str],
    ) -> None:
        overlap = set(regular_bookmakers).intersection(exchange_bookmakers)
        if overlap:
            raise ValueError(
                "Oddspapi bookmakers cannot be configured as both regular "
                f"and exchange: {', '.join(sorted(overlap))}"
            )

    def _fetch(
        self,
        fixture_id: str,
        *,
        bookmakers: list[str],
        endpoint: str,
        source_sport_id: str | int | None,
        outcome_id: int | None = None,
        minimum_initial_span_minutes: float = 0.0,
    ) -> OddsFetchResult:
        return self.fetcher.fetch_odds(
            fixture_id,
            bookmakers=bookmakers,
            endpoint=endpoint,
            source_sport_id=source_sport_id,
            outcome_id=outcome_id,
            minimum_initial_span_minutes=minimum_initial_span_minutes,
        )

    def acquire(
        self,
        fixture_id: str,
        *,
        source_sport_id: str | int | None,
        minutes_until_start: int | float | None,
        selected_endpoint: str,
        regular_bookmakers: list[str] | None,
        exchange_bookmakers: list[str] | None,
        market_mapping_index: MarketMappingIndex,
        exchange_market_keys: list[str] | None,
        exchange_main_line_only: bool,
        exchange_include_player_props: bool,
        exchange_historical_moments: list[int],
        exchange_max_outcomes_per_event: int,
        exchange_request_budget: int | None,
        minimum_initial_span_minutes: float,
        current_odds_available: bool,
    ) -> OddspapiOddsAcquisitionResult:
        regular = self._unique_bookmakers(regular_bookmakers)
        exchange = self._unique_bookmakers(exchange_bookmakers)
        self._validate_bookmaker_groups(regular, exchange)
        combined = self._unique_bookmakers(regular, exchange)
        result = OddspapiOddsAcquisitionResult()

        current_payload: dict | None = None
        current_missing = False
        if combined and current_odds_available:
            result.http_requests_attempted += 1
            current_result = self._fetch(
                fixture_id,
                bookmakers=combined,
                endpoint=ODDSPAPI_CURRENT_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
            )
            current_missing = current_result.endpoint_missing
            current_payload = current_result.payload

        payload = current_payload
        historical_missing = False
        if selected_endpoint == ODDSPAPI_HISTORICAL_ODDS_ENDPOINT and regular:
            result.http_requests_attempted += 1
            historical_result = self._fetch(
                fixture_id,
                bookmakers=regular,
                endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
            )
            historical_missing = historical_result.endpoint_missing
            if current_payload:
                payload = OddspapiHistoricalOddsEnricher.merge_initial_prices(
                    current_payload,
                    historical_result.payload,
                )
            elif historical_result.payload:
                payload = historical_result.payload

        should_fetch_exchange_history = (
            bool(exchange)
            and current_payload is not None
            and minutes_until_start in set(exchange_historical_moments or [])
        )
        if should_fetch_exchange_history:
            selection_limit = (
                max(0, int(exchange_max_outcomes_per_event))
                if exchange_max_outcomes_per_event
                and int(exchange_max_outcomes_per_event) > 0
                else None
            )
            if exchange_request_budget is not None:
                selection_limit = (
                    min(selection_limit, exchange_request_budget)
                    if selection_limit is not None
                    else max(0, exchange_request_budget)
                )
            selection_result = OddspapiExchangeOutcomeSelector.select(
                current_payload,
                exchange_bookmakers=exchange,
                market_mapping_index=market_mapping_index,
                allowed_market_keys=exchange_market_keys,
                main_line_only=exchange_main_line_only,
                include_player_props=exchange_include_player_props,
                max_outcomes=selection_limit,
            )
            result.exchange_outcomes_selected = len(selection_result.selections)
            result.exchange_selection_diagnostics = {
                "skipped_unmapped_markets": (
                    selection_result.skipped_unmapped_markets
                ),
                "skipped_unmapped_outcomes": (
                    selection_result.skipped_unmapped_outcomes
                ),
                "skipped_market_key": selection_result.skipped_market_key,
                "skipped_non_main_line": (
                    selection_result.skipped_non_main_line
                ),
                "skipped_player_props": (
                    selection_result.skipped_player_props
                ),
                "truncated": selection_result.truncated,
            }
            result.exchange_outcomes_skipped_budget = selection_result.truncated
            logger.info(
                "Oddspapi exchange historical plan fixture_id=%s "
                "minutes_until_start=%s selected=%s diagnostics=%s",
                fixture_id,
                minutes_until_start,
                result.exchange_outcomes_selected,
                result.exchange_selection_diagnostics,
            )

            for selection in selection_result.selections:
                result.http_requests_attempted += 1
                result.exchange_historical_requests_attempted += 1
                try:
                    historical_result = self._fetch(
                        fixture_id,
                        bookmakers=[selection.bookmaker_slug],
                        endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                        source_sport_id=source_sport_id,
                        outcome_id=int(selection.source_outcome_id),
                        minimum_initial_span_minutes=(
                            minimum_initial_span_minutes
                        ),
                    )
                    if historical_result.endpoint_missing:
                        result.exchange_historical_requests_failed += 1
                        continue
                    if historical_result.payload:
                        payload = (
                            OddspapiHistoricalOddsEnricher.merge_initial_prices(
                                payload or current_payload,
                                historical_result.payload,
                            )
                        )
                except Exception as exc:
                    result.exchange_historical_requests_failed += 1
                    logger.warning(
                        "Oddspapi exchange historical request failed "
                        "fixture_id=%s bookmaker=%s market_id=%s "
                        "outcome_id=%s: %s",
                        fixture_id,
                        selection.bookmaker_slug,
                        selection.source_market_id,
                        selection.source_outcome_id,
                        exc,
                    )

        result.payload = payload
        result.endpoint_missing = (
            current_missing
            if selected_endpoint == ODDSPAPI_CURRENT_ODDS_ENDPOINT
            else historical_missing and payload is None
        )
        return result
