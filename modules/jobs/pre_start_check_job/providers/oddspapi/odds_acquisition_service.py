"""Compose current and historical OddsPapi requests for one fixture."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import logging

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
)
from infrastructure.persistence.repositories.oddspapi_mainline_cache_repository import (
    OddspapiMainlineCacheRepository,
)
from modules.oddspapi.historical_odds_as_of import OddspapiHistoricalOddsAsOf
from modules.odds_ingestion.fetch_result import OddsFetchResult

from .constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
)
from .exchange_historical_fetch_executor import (
    OddspapiExchangeHistoricalFetchExecutor,
)
from .exchange_outcome_selector import (
    ExchangeHistoricalSelection,
    OddspapiExchangeOutcomeSelector,
)
from .historical_odds_enricher import OddspapiHistoricalOddsEnricher
from .mainline_outcome_extractor import OddspapiMainlineOutcomeExtractor
from .odds_fetcher import OddspapiOddsFetcher

logger = logging.getLogger(__name__)


@dataclass
class OddspapiOddsAcquisitionResult:
    payload: dict | None = None
    debug_raw_payload: dict | None = None
    debug_endpoint: str | None = None
    debug_bookmakers: list[str] = field(default_factory=list)
    bookies_requested: int = 0
    bookmaker_slugs_requested: list[str] = field(default_factory=list)
    endpoint_missing: bool = False
    http_requests_attempted: int = 0
    exchange_historical_requests_attempted: int = 0
    exchange_historical_requests_failed: int = 0
    exchange_outcomes_selected: int = 0
    exchange_outcomes_skipped_budget: int = 0
    exchange_selection_diagnostics: dict[str, int] = field(default_factory=dict)
    mainline_outcomes_cached: int = 0
    as_of_quotes: list = field(default_factory=list)


class OddspapiPreStartOddsAcquisitionService:
    """Acquire one fixture while keeping provider-specific branching isolated."""

    def __init__(
        self,
        fetcher: OddspapiOddsFetcher | None = None,
        mainline_cache_repository: type[OddspapiMainlineCacheRepository] = (
            OddspapiMainlineCacheRepository
        ),
    ):
        self.fetcher = fetcher or OddspapiOddsFetcher()
        self.mainline_cache_repository = mainline_cache_repository

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

    @staticmethod
    def _opening_moment_minutes(
        exchange_historical_moments: list[int] | None,
    ) -> int:
        moments = [
            int(moment)
            for moment in (exchange_historical_moments or [120])
            if moment is not None
        ]
        return max(moments) if moments else 120

    @classmethod
    def _should_enrich_opening_odds(
        cls,
        minutes_until_start: int | float | None,
        exchange_historical_moments: list[int] | None,
    ) -> bool:
        if minutes_until_start is None:
            return False
        opening_moment = cls._opening_moment_minutes(exchange_historical_moments)
        return float(minutes_until_start) >= float(opening_moment)

    def _fetch(
        self,
        fixture_id: str,
        *,
        bookmakers: list[str],
        endpoint: str,
        source_sport_id: str | int | None,
        outcome_id: int | None = None,
        minimum_initial_span_minutes: float = 0.0,
        require_active_quotes: bool = True,
        capture_raw_response: bool = False,
        as_of_targets: list[tuple[int, datetime, datetime]] | None = None,
    ) -> OddsFetchResult:
        return self.fetcher.fetch_odds(
            fixture_id,
            bookmakers=bookmakers,
            endpoint=endpoint,
            source_sport_id=source_sport_id,
            outcome_id=outcome_id,
            minimum_initial_span_minutes=minimum_initial_span_minutes,
            require_active_quotes=require_active_quotes,
            capture_raw_response=capture_raw_response,
            as_of_targets=as_of_targets,
        )

    @staticmethod
    def _record_as_of_quotes(
        result: OddspapiOddsAcquisitionResult,
        fetch_result: OddsFetchResult | None,
    ) -> None:
        quotes = getattr(fetch_result, "as_of_quotes", None) or ()
        if quotes:
            result.as_of_quotes.extend(quotes)

    def _selection_limit(
        self,
        *,
        exchange_max_outcomes_per_event: int,
        exchange_request_budget: int | None,
    ) -> int | None:
        selection_limit = (
            max(0, int(exchange_max_outcomes_per_event))
            if exchange_max_outcomes_per_event
            and int(exchange_max_outcomes_per_event) > 0
            else None
        )
        if exchange_request_budget is None:
            return selection_limit
        if selection_limit is None:
            return max(0, exchange_request_budget)
        return min(selection_limit, max(0, exchange_request_budget))

    def _apply_exchange_historical_result(
        self,
        fixture_id: str,
        selection: ExchangeHistoricalSelection,
        historical_result: OddsFetchResult | None,
        error: Exception | None,
        *,
        payload: dict | None,
        result: OddspapiOddsAcquisitionResult,
        merge_full_bookmakers: bool,
    ) -> dict | None:
        if error is not None:
            result.exchange_historical_requests_failed += 1
            logger.warning(
                "Oddspapi exchange historical request failed "
                "fixture_id=%s bookmaker=%s market_id=%s "
                "outcome_id=%s: %s",
                fixture_id,
                selection.bookmaker_slug,
                selection.source_market_id,
                selection.source_outcome_id,
                error,
            )
            return payload
        if historical_result is None or historical_result.endpoint_missing:
            result.exchange_historical_requests_failed += 1
            return payload
        self._record_as_of_quotes(result, historical_result)
        if not historical_result.payload:
            return payload
        if merge_full_bookmakers:
            return OddspapiHistoricalOddsEnricher.merge_bookmaker_odds(
                payload,
                historical_result.payload,
            )
        return OddspapiHistoricalOddsEnricher.merge_initial_prices(
            payload or {},
            historical_result.payload,
        )

    def _fetch_exchange_historical_selections(
        self,
        fixture_id: str,
        *,
        selections: list[ExchangeHistoricalSelection],
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
        require_active_quotes: bool = True,
        payload: dict | None,
        result: OddspapiOddsAcquisitionResult,
        requested_bookmakers: set[str],
        merge_full_bookmakers: bool,
        fetch_executor: OddspapiExchangeHistoricalFetchExecutor | None = None,
        capture_raw_response: bool = False,
        as_of_targets: list[tuple[int, datetime, datetime]] | None = None,
    ) -> dict | None:
        for selection in selections:
            requested_bookmakers.add(selection.bookmaker_slug)

        if fetch_executor is not None and len(selections) > 1:
            result.http_requests_attempted += len(selections)
            result.exchange_historical_requests_attempted += len(selections)
            outcomes = fetch_executor.fetch_all(
                fixture_id,
                selections=selections,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                capture_raw_response=capture_raw_response,
                as_of_targets=as_of_targets,
            )
            for outcome in outcomes:
                payload = self._apply_exchange_historical_result(
                    fixture_id,
                    outcome.selection,
                    outcome.result,
                    outcome.error,
                    payload=payload,
                    result=result,
                    merge_full_bookmakers=merge_full_bookmakers,
                )
            return payload

        for selection in selections:
            result.http_requests_attempted += 1
            result.exchange_historical_requests_attempted += 1
            historical_result = None
            error = None
            try:
                historical_result = self._fetch(
                    fixture_id,
                    bookmakers=[selection.bookmaker_slug],
                    endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                    source_sport_id=source_sport_id,
                    outcome_id=int(selection.source_outcome_id),
                    minimum_initial_span_minutes=minimum_initial_span_minutes,
                    require_active_quotes=require_active_quotes,
                    capture_raw_response=capture_raw_response,
                    as_of_targets=as_of_targets,
                )
            except Exception as exc:
                error = exc
            payload = self._apply_exchange_historical_result(
                fixture_id,
                selection,
                historical_result,
                error,
                payload=payload,
                result=result,
                merge_full_bookmakers=merge_full_bookmakers,
            )
        return payload

    def _acquire_live(
        self,
        fixture_id: str,
        *,
        event_id: int,
        source_sport_id: str | int | None,
        enable_exchange_historical: bool,
        regular: list[str],
        exchange: list[str],
        exchange_market_keys: Sequence[str] | None = None,
        exchange_max_outcomes_per_event: int,
        exchange_request_budget: int | None,
        minimum_initial_span_minutes: float,
        require_active_quotes: bool = True,
        debug_mode: bool,
        result: OddspapiOddsAcquisitionResult,
        requested_bookmakers: set[str],
        fetch_executor: OddspapiExchangeHistoricalFetchExecutor | None = None,
        start_time_utc: datetime | None = None,
        as_of_moments: list[int] | None = None,
        attach_as_of: bool = False,
    ) -> OddspapiOddsAcquisitionResult:
        payload: dict | None = None
        historical_missing = False

        # Safety net: /historical-odds cannot be parsed without cached mainLine
        # outcome ids captured earlier from /odds.
        cached_event_ids = self.mainline_cache_repository.event_ids_with_cache(
            [event_id]
        )
        if event_id not in cached_event_ids:
            logger.info(
                "🚫 Oddspapi live historical skipped: empty mainline cache "
                "fixture_id=%s event_id=%s",
                fixture_id,
                event_id,
            )
            result.bookies_requested = len(requested_bookmakers)
            result.bookmaker_slugs_requested = sorted(requested_bookmakers)
            result.payload = None
            return result

        as_of_targets = OddspapiHistoricalOddsAsOf.targets_from_start(
            start_time_utc,
            as_of_moments or [],
        )
        capture_raw = debug_mode
        if regular:
            requested_bookmakers.update(regular)
            result.http_requests_attempted += 1
            historical_result = self._fetch(
                fixture_id,
                bookmakers=regular,
                endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                capture_raw_response=capture_raw,
                as_of_targets=as_of_targets,
            )
            historical_missing = historical_result.endpoint_missing
            payload = historical_result.payload
            result.debug_raw_payload = historical_result.raw_payload
            result.debug_endpoint = ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
            result.debug_bookmakers = list(regular)
            self._record_as_of_quotes(result, historical_result)

        if exchange and enable_exchange_historical:
            cached_rows = self.mainline_cache_repository.get_exchange_mainline_selections(
                event_id,
                exchange,
                allowed_market_keys=exchange_market_keys,
            )
            selections = [
                ExchangeHistoricalSelection(
                    bookmaker_slug=str(row.get("bookmaker_slug") or "").strip().lower(),
                    source_market_id=str(row.get("source_market_id") or ""),
                    source_outcome_id=str(row.get("source_outcome_id") or ""),
                    canonical_market_key=str(row.get("canonical_market_key") or ""),
                )
                for row in cached_rows
                if str(row.get("bookmaker_slug") or "").strip()
                and str(row.get("source_outcome_id") or "").strip()
            ]
            selection_limit = self._selection_limit(
                exchange_max_outcomes_per_event=exchange_max_outcomes_per_event,
                exchange_request_budget=exchange_request_budget,
            )
            if selection_limit is not None and len(selections) > selection_limit:
                result.exchange_outcomes_skipped_budget = (
                    len(selections) - selection_limit
                )
                selections = selections[:selection_limit]
            result.exchange_outcomes_selected = len(selections)
            result.exchange_selection_diagnostics = {
                "from_mainline_cache": len(selections),
                "truncated": result.exchange_outcomes_skipped_budget,
            }
            logger.info(
                "Oddspapi live exchange historical plan fixture_id=%s "
                "event_id=%s selected=%s diagnostics=%s",
                fixture_id,
                event_id,
                result.exchange_outcomes_selected,
                result.exchange_selection_diagnostics,
            )
            payload = self._fetch_exchange_historical_selections(
                fixture_id,
                selections=selections,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                payload=payload,
                result=result,
                requested_bookmakers=requested_bookmakers,
                merge_full_bookmakers=True,
                fetch_executor=fetch_executor,
                capture_raw_response=capture_raw,
                as_of_targets=as_of_targets,
            )

        if attach_as_of and result.as_of_quotes:
            payload = OddspapiHistoricalOddsAsOf.attach_to_normalized_payload(
                payload,
                result.as_of_quotes,
            )

        result.bookies_requested = len(requested_bookmakers)
        result.bookmaker_slugs_requested = sorted(requested_bookmakers)
        result.payload = payload
        result.endpoint_missing = historical_missing and payload is None
        return result

    def _acquire_pre_start(
        self,
        fixture_id: str,
        *,
        event_id: int,
        source_sport_id: str | int | None,
        minutes_until_start: int | float | None,
        enable_exchange_historical: bool,
        regular: list[str],
        exchange: list[str],
        market_mapping_index: MarketMappingIndex,
        exchange_market_keys: list[str] | None,
        exchange_main_line_only: bool,
        exchange_include_player_props: bool,
        exchange_historical_moments: list[int],
        exchange_max_outcomes_per_event: int,
        exchange_request_budget: int | None,
        minimum_initial_span_minutes: float,
        current_odds_available: bool,
        require_active_quotes: bool = True,
        debug_mode: bool,
        result: OddspapiOddsAcquisitionResult,
        requested_bookmakers: set[str],
        fetch_executor: OddspapiExchangeHistoricalFetchExecutor | None = None,
    ) -> OddspapiOddsAcquisitionResult:
        combined = self._unique_bookmakers(regular, exchange)
        current_payload: dict | None = None
        current_missing = False

        if combined and current_odds_available:
            requested_bookmakers.update(combined)
            result.http_requests_attempted += 1
            current_result = self._fetch(
                fixture_id,
                bookmakers=combined,
                endpoint=ODDSPAPI_CURRENT_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
                capture_raw_response=debug_mode,
            )
            current_missing = current_result.endpoint_missing
            current_payload = current_result.payload
            result.debug_raw_payload = current_result.raw_payload
            result.debug_endpoint = ODDSPAPI_CURRENT_ODDS_ENDPOINT
            result.debug_bookmakers = list(combined)

        payload = current_payload
        if current_payload:
            mainline_outcomes = OddspapiMainlineOutcomeExtractor.extract(
                current_payload,
                exchange_bookmakers=exchange,
                require_active_quotes=require_active_quotes,
                market_mapping_index=market_mapping_index,
                source_sport_id=source_sport_id,
            )
            if mainline_outcomes:
                result.mainline_outcomes_cached = (
                    self.mainline_cache_repository.save_mainline_outcomes(
                        event_id,
                        fixture_id,
                        (
                            str(source_sport_id)
                            if source_sport_id is not None
                            else None
                        ),
                        mainline_outcomes,
                    )
                )

        if (
            self._should_enrich_opening_odds(
                minutes_until_start,
                exchange_historical_moments,
            )
            and regular
            and current_payload
        ):
            requested_bookmakers.update(regular)
            result.http_requests_attempted += 1
            historical_result = self._fetch(
                fixture_id,
                bookmakers=regular,
                endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
            )
            if historical_result.payload:
                payload = OddspapiHistoricalOddsEnricher.merge_initial_prices(
                    current_payload,
                    historical_result.payload,
                )

        should_fetch_exchange_history = (
            bool(exchange)
            and enable_exchange_historical
            and current_payload is not None
            and minutes_until_start in set(exchange_historical_moments or [])
        )
        if should_fetch_exchange_history:
            selection_result = OddspapiExchangeOutcomeSelector.select(
                current_payload,
                exchange_bookmakers=exchange,
                market_mapping_index=market_mapping_index,
                allowed_market_keys=exchange_market_keys,
                main_line_only=exchange_main_line_only,
                include_player_props=exchange_include_player_props,
                max_outcomes=self._selection_limit(
                    exchange_max_outcomes_per_event=exchange_max_outcomes_per_event,
                    exchange_request_budget=exchange_request_budget,
                ),
                require_active_quotes=require_active_quotes,
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
            payload = self._fetch_exchange_historical_selections(
                fixture_id,
                selections=selection_result.selections,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                payload=payload,
                result=result,
                requested_bookmakers=requested_bookmakers,
                merge_full_bookmakers=False,
                fetch_executor=fetch_executor,
            )

        result.bookies_requested = len(requested_bookmakers)
        result.bookmaker_slugs_requested = sorted(requested_bookmakers)
        result.payload = payload
        result.endpoint_missing = current_missing
        return result

    def acquire(
        self,
        fixture_id: str,
        *,
        event_id: int,
        source_sport_id: str | int | None,
        minutes_until_start: int | float | None,
        is_live: bool,
        enable_exchange_historical: bool = True,
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
        require_active_quotes: bool = True,
        debug_mode: bool = False,
        exchange_fetch_executor: OddspapiExchangeHistoricalFetchExecutor | None = None,
        start_time_utc: datetime | None = None,
        as_of_moments: list[int] | None = None,
        attach_as_of: bool = False,
    ) -> OddspapiOddsAcquisitionResult:
        regular = self._unique_bookmakers(regular_bookmakers)
        exchange = self._unique_bookmakers(exchange_bookmakers)
        self._validate_bookmaker_groups(regular, exchange)
        result = OddspapiOddsAcquisitionResult()
        requested_bookmakers: set[str] = set()

        if is_live:
            return self._acquire_live(
                fixture_id,
                event_id=event_id,
                source_sport_id=source_sport_id,
                enable_exchange_historical=enable_exchange_historical,
                regular=regular,
                exchange=exchange,
                exchange_market_keys=exchange_market_keys,
                exchange_max_outcomes_per_event=exchange_max_outcomes_per_event,
                exchange_request_budget=exchange_request_budget,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                debug_mode=debug_mode,
                result=result,
                requested_bookmakers=requested_bookmakers,
                fetch_executor=exchange_fetch_executor,
                start_time_utc=start_time_utc,
                as_of_moments=as_of_moments,
                attach_as_of=attach_as_of,
            )

        return self._acquire_pre_start(
            fixture_id,
            event_id=event_id,
            source_sport_id=source_sport_id,
            minutes_until_start=minutes_until_start,
            enable_exchange_historical=enable_exchange_historical,
            regular=regular,
            exchange=exchange,
            market_mapping_index=market_mapping_index,
            exchange_market_keys=exchange_market_keys,
            exchange_main_line_only=exchange_main_line_only,
            exchange_include_player_props=exchange_include_player_props,
            exchange_historical_moments=exchange_historical_moments,
            exchange_max_outcomes_per_event=exchange_max_outcomes_per_event,
            exchange_request_budget=exchange_request_budget,
            minimum_initial_span_minutes=minimum_initial_span_minutes,
            current_odds_available=current_odds_available,
            require_active_quotes=require_active_quotes,
            debug_mode=debug_mode,
            result=result,
            requested_bookmakers=requested_bookmakers,
            fetch_executor=exchange_fetch_executor,
        )
