"""Batch coordinator for requesting and ingesting mapped Oddspapi odds."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import logging
from typing import Callable

from infrastructure.persistence.repositories import (
    MarketMappingRepository,
    OddspapiMainlineCacheRepository,
)
from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
)
from infrastructure.settings import Config
from modules.competition.tracked_competitions import is_tracked_competition
from modules.jobs.pre_start_check_job.moment_policy import (
    is_closing_odds_moment,
    is_live_odds_moment,
)
from modules.odds_ingestion import (
    MarketOddsIngestionService,
    ProviderOddsSummary,
    mark_missing_endpoints_unavailable,
)
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.api_keys import (
    free_endpoint_api_keys,
    odds_endpoint_api_keys,
    parallel_worker_count,
    unique_api_keys,
)
from modules.oddspapi.api_key_scheduler import OddsPapiApiKeyScheduler
from modules.oddspapi.runtime import get_oddspapi_key_scheduler
from modules.oddspapi.exceptions import OddsPapiQuotaExhaustedError

from .constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
    ODDSPAPI_INGESTION_SOURCE,
    ODDSPAPI_PRE_START_ODDS_ENDPOINTS,
    ODDSPAPI_SOURCE,
)
from .debug_response_writer import OddspapiDebugResponseWriter
from .event_selector import OddspapiPreStartCandidate
from .exchange_historical_fetch_executor import (
    OddspapiExchangeHistoricalFetchExecutor,
)
from .odds_fetcher import OddspapiOddsFetcher
from .odds_acquisition_service import OddspapiPreStartOddsAcquisitionService
from .historical_odds_as_of_shadow import log_historical_odds_as_of_shadow

logger = logging.getLogger(__name__)


@dataclass
class OddspapiPreStartOddsEventResult:
    event_id: int
    fixture_id: str | None
    minutes_until_start: int | float | None
    requested: bool = False
    skipped: bool = False
    skip_reason: str | None = None
    markets_detected: int = 0
    choices_detected: int = 0
    snapshots_detected: int = 0
    markets_saved: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0
    bookies_requested: int = 0
    bookmaker_slugs_requested: list[str] = field(default_factory=list)
    bookies_detected: int = 0
    bookmaker_slugs_detected: list[str] = field(default_factory=list)
    bookies_processed: int = 0
    unmapped_markets_detected: int = 0
    unmapped_outcomes_detected: int = 0
    skipped_missing_handicap_detected: int = 0
    skipped_incomplete_markets_detected: int = 0
    http_requests_attempted: int = 0
    exchange_outcomes_selected: int = 0
    exchange_historical_requests_attempted: int = 0
    exchange_historical_requests_failed: int = 0
    exchange_outcomes_skipped_budget: int = 0
    error: str | None = None


@dataclass
class OddspapiPreStartOddsSummary(ProviderOddsSummary):
    """Oddspapi-specific counters on top of the shared provider phase summary."""

    candidates_with_mapping: int = 0
    responses_received: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0
    unmapped_markets_detected: int = 0
    unmapped_outcomes_detected: int = 0
    skipped_missing_handicap_detected: int = 0
    skipped_incomplete_markets_detected: int = 0
    http_requests_attempted: int = 0
    exchange_outcomes_selected: int = 0
    exchange_historical_requests_attempted: int = 0
    exchange_historical_requests_failed: int = 0
    exchange_outcomes_skipped_budget: int = 0
    api_key_assignments: dict[str, int] = field(default_factory=dict)
    api_key_diagnostics: dict[str, int] = field(default_factory=dict)
    disabled: bool = False
    skip_reason: str | None = None
    results: list[OddspapiPreStartOddsEventResult] = field(default_factory=list)


class OddspapiPreStartOddsBatchProcessor:
    _SUMMARY_COUNT_FIELDS = (
        "candidates_seen",
        "candidates_with_mapping",
        "requests_attempted",
        "responses_received",
        "events_ingested",
        "events_skipped",
        "events_failed",
        "missing_endpoints",
        "markets_saved",
        "choices_saved",
        "snapshots_saved",
        "unmapped_markets_detected",
        "unmapped_outcomes_detected",
        "skipped_missing_handicap_detected",
        "skipped_incomplete_markets_detected",
        "http_requests_attempted",
        "exchange_outcomes_selected",
        "exchange_historical_requests_attempted",
        "exchange_historical_requests_failed",
        "exchange_outcomes_skipped_budget",
    )

    def __init__(
        self,
        fetcher: OddspapiOddsFetcher | None = None,
        ingestion_service: type[MarketOddsIngestionService] = MarketOddsIngestionService,
        acquisition_service: OddspapiPreStartOddsAcquisitionService | None = None,
        client_factory: Callable[..., OddsPapiClient] = OddsPapiClient,
        key_scheduler: OddsPapiApiKeyScheduler | None = None,
    ):
        self.fetcher = fetcher
        self.acquisition_service = acquisition_service
        if self.acquisition_service is None and self.fetcher is not None:
            self.acquisition_service = OddspapiPreStartOddsAcquisitionService(
                fetcher=self.fetcher
            )
        self.ingestion_service = ingestion_service
        self.client_factory = client_factory
        self.key_scheduler = key_scheduler
        self._custom_pipeline = (
            fetcher is not None or acquisition_service is not None
        )
        self._clients_by_slot: dict[int, OddsPapiClient] = {}

    def _client_for_slot(self, slot: int) -> OddsPapiClient:
        client = self._clients_by_slot.get(slot)
        if client is None:
            scheduler = self.key_scheduler or get_oddspapi_key_scheduler()
            client = self.client_factory(key_scheduler=scheduler)
            self._clients_by_slot[slot] = client
        return client

    def _close_owned_clients(self) -> None:
        for client in self._clients_by_slot.values():
            close = getattr(client, "close", None)
            if callable(close):
                close()
        self._clients_by_slot = {}

    def _acquisition_service_for_slot(
        self,
        slot: int,
    ) -> OddspapiPreStartOddsAcquisitionService:
        if self.acquisition_service is not None:
            return self.acquisition_service
        self.fetcher = OddspapiOddsFetcher(client=self._client_for_slot(slot))
        return OddspapiPreStartOddsAcquisitionService(fetcher=self.fetcher)

    @classmethod
    def _merge_worker_summaries(
        cls,
        candidates: list[OddspapiPreStartCandidate],
        worker_summaries: list[OddspapiPreStartOddsSummary],
    ) -> OddspapiPreStartOddsSummary:
        merged = OddspapiPreStartOddsSummary()
        results_by_event_id = {}
        for worker_summary in worker_summaries:
            for field_name in cls._SUMMARY_COUNT_FIELDS:
                setattr(
                    merged,
                    field_name,
                    getattr(merged, field_name)
                    + getattr(worker_summary, field_name),
                )
            for result in worker_summary.results:
                results_by_event_id[result.event_id] = result
        merged.results = [
            results_by_event_id[candidate.event_id]
            for candidate in candidates
            if candidate.event_id in results_by_event_id
        ]
        return merged

    @staticmethod
    def _failed_worker_summary(
        candidates: list[OddspapiPreStartCandidate],
        exc: Exception,
    ) -> OddspapiPreStartOddsSummary:
        summary = OddspapiPreStartOddsSummary(
            candidates_seen=len(candidates),
            candidates_with_mapping=sum(
                candidate.fixture_id is not None for candidate in candidates
            ),
            requests_attempted=len(candidates),
            events_failed=len(candidates),
        )
        for candidate in candidates:
            result = OddspapiPreStartOddsEventResult(
                event_id=candidate.event_id,
                fixture_id=candidate.fixture_id,
                minutes_until_start=candidate.minutes_until_start,
                requested=True,
                error=str(exc),
            )
            summary.results.append(result)
        return summary

    @staticmethod
    def _non_requestable_summary(
        candidates: list[OddspapiPreStartCandidate],
        *,
        respects_stored_availability: bool,
    ) -> OddspapiPreStartOddsSummary:
        summary = OddspapiPreStartOddsSummary(
            candidates_seen=len(candidates),
            candidates_with_mapping=sum(
                candidate.fixture_id is not None for candidate in candidates
            ),
        )
        for candidate in candidates:
            result = OddspapiPreStartOddsBatchProcessor._event_result(candidate)
            result.skipped = True
            if not candidate.fixture_id:
                result.skip_reason = "missing_oddspapi_mapping"
            elif OddspapiPreStartOddsBatchProcessor._is_live_candidate(candidate):
                result.skip_reason = "missing_mainline_cache"
            elif getattr(Config, "ODDSPAPI_PRE_START_CLOSING_ONLY", False):
                result.skip_reason = "oddspapi_closing_only"
            elif respects_stored_availability and not candidate.has_odds:
                result.skip_reason = "oddspapi_odds_unavailable"
            else:
                result.skip_reason = "oddspapi_event_not_requestable"
            summary.events_skipped += 1
            summary.results.append(result)
        return summary

    def _process_parallel_workers(
        self,
        candidates: list[OddspapiPreStartCandidate],
        *,
        api_keys: list[str],
        max_workers: int,
        market_mapping_index,
        process_kwargs: dict,
    ) -> OddspapiPreStartOddsSummary:
        worker_count = parallel_worker_count(
            max_workers=max_workers,
            api_key_count=len(api_keys),
            item_count=len(candidates),
        )
        chunks = [
            candidates[index::worker_count]
            for index in range(worker_count)
        ]

        def run_worker(worker_index: int):
            scheduler = self.key_scheduler or get_oddspapi_key_scheduler()
            client = self.client_factory(key_scheduler=scheduler)
            try:
                processor = OddspapiPreStartOddsBatchProcessor(
                    fetcher=OddspapiOddsFetcher(client=client),
                    ingestion_service=self.ingestion_service,
                    client_factory=self.client_factory,
                    key_scheduler=scheduler,
                )
                return processor.process(
                    chunks[worker_index],
                    market_mapping_index=market_mapping_index,
                    **process_kwargs,
                )
            finally:
                close = getattr(client, "close", None)
                if callable(close):
                    close()

        logger.info(
            "Oddspapi pre-start parallel ingestion workers=%s events=%s",
            worker_count,
            len(candidates),
        )
        worker_summaries = []
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="oddspapi-pre-start",
        ) as executor:
            future_to_index = {
                executor.submit(run_worker, index): index
                for index in range(worker_count)
            }
            for future in as_completed(future_to_index):
                worker_index = future_to_index[future]
                try:
                    worker_summaries.append(future.result())
                except Exception as exc:
                    logger.warning(
                        "Oddspapi pre-start worker failed worker=%s events=%s: %s",
                        worker_index + 1,
                        len(chunks[worker_index]),
                        exc,
                    )
                    worker_summaries.append(
                        self._failed_worker_summary(
                            chunks[worker_index],
                            exc,
                        )
                    )
        return self._merge_worker_summaries(candidates, worker_summaries)

    @staticmethod
    def _event_result(candidate: OddspapiPreStartCandidate) -> OddspapiPreStartOddsEventResult:
        return OddspapiPreStartOddsEventResult(
            event_id=candidate.event_id,
            fixture_id=candidate.fixture_id,
            minutes_until_start=candidate.minutes_until_start,
        )

    @staticmethod
    def _copy_ingestion_stats(result, ingestion_result) -> None:
        for field_name in (
            "markets_detected", "choices_detected", "snapshots_detected",
            "markets_saved", "choices_saved", "snapshots_saved",
            "bookies_detected", "bookies_processed", "unmapped_markets_detected",
            "unmapped_outcomes_detected", "skipped_missing_handicap_detected",
            "skipped_incomplete_markets_detected",
        ):
            setattr(result, field_name, getattr(ingestion_result, field_name, 0) or 0)
        result.bookmaker_slugs_detected = list(
            getattr(ingestion_result, "bookmaker_slugs_detected", None) or []
        )

    @staticmethod
    def _accumulate(summary: OddspapiPreStartOddsSummary, result: OddspapiPreStartOddsEventResult) -> None:
        for field_name in (
            "markets_saved", "choices_saved", "snapshots_saved",
            "unmapped_markets_detected", "unmapped_outcomes_detected",
            "skipped_missing_handicap_detected",
            "skipped_incomplete_markets_detected",
        ):
            setattr(summary, field_name, getattr(summary, field_name) + getattr(result, field_name))

    @staticmethod
    def _accumulate_acquisition(
        summary: OddspapiPreStartOddsSummary,
        result: OddspapiPreStartOddsEventResult,
    ) -> None:
        for field_name in (
            "http_requests_attempted", "exchange_outcomes_selected",
            "exchange_historical_requests_attempted",
            "exchange_historical_requests_failed",
            "exchange_outcomes_skipped_budget",
        ):
            setattr(summary, field_name, getattr(summary, field_name) + getattr(result, field_name))

    @staticmethod
    def _copy_acquisition_stats(result, acquisition_result) -> None:
        for field_name in (
            "bookies_requested",
            "http_requests_attempted",
            "exchange_outcomes_selected",
            "exchange_historical_requests_attempted",
            "exchange_historical_requests_failed",
            "exchange_outcomes_skipped_budget",
        ):
            setattr(
                result,
                field_name,
                getattr(acquisition_result, field_name, 0) or 0,
            )
        result.bookmaker_slugs_requested = list(
            getattr(acquisition_result, "bookmaker_slugs_requested", None) or []
        )

    @staticmethod
    def _warn_if_bookmaker_coverage_gap(
        result: OddspapiPreStartOddsEventResult,
        *,
        endpoint: str,
    ) -> None:
        """Warn when API-requested bookmakers produced no normalized markets."""
        if result.skipped or result.error is not None:
            return

        requested = [
            str(slug).strip().lower()
            for slug in (result.bookmaker_slugs_requested or [])
            if str(slug).strip()
        ]
        normalized = [
            str(slug).strip().lower()
            for slug in (result.bookmaker_slugs_detected or [])
            if str(slug).strip()
        ]
        if not requested:
            if result.bookies_detected == result.bookies_requested:
                return
            logger.warning(
                "Oddspapi bookmaker coverage gap endpoint=%s event_id=%s "
                "fixture_id=%s requested_count=%s normalized_count=%s "
                "(requested bookmakers were counted, but slug details were unavailable)",
                endpoint,
                result.event_id,
                result.fixture_id,
                result.bookies_requested,
                result.bookies_detected,
            )
            return

        missing = sorted(set(requested) - set(normalized))
        if not missing:
            return
        logger.warning(
            "Oddspapi bookmaker coverage gap endpoint=%s event_id=%s "
            "fixture_id=%s requested=%s normalized=%s missing_after_normalization=%s "
            "(API returned or was asked for these bookmakers, but none of their "
            "markets survived adapter filters such as inactive quotes, "
            "main-line-only, incomplete markets, or unmapped markets)",
            endpoint,
            result.event_id,
            result.fixture_id,
            requested,
            normalized,
            missing,
        )

    @staticmethod
    def _is_live_candidate(candidate: OddspapiPreStartCandidate) -> bool:
        if candidate.minutes_until_start is None:
            return candidate.is_live
        return is_live_odds_moment(candidate.minutes_until_start)

    @staticmethod
    def _has_current_odds(payload: dict | None) -> bool:
        bookmaker_odds = payload.get("bookmakerOdds") if isinstance(payload, dict) else None
        if not isinstance(bookmaker_odds, dict):
            return False
        return any(
            isinstance(bookmaker, dict)
            and isinstance(bookmaker.get("markets"), dict)
            and any(
                isinstance(market, dict)
                and isinstance(market.get("outcomes"), dict)
                and bool(market.get("outcomes"))
                for market in bookmaker["markets"].values()
            )
            for bookmaker in bookmaker_odds.values()
        )

    def process(
        self,
        candidates: list[OddspapiPreStartCandidate],
        *,
        bookmakers: list[str] | None,
        dry_run: bool = False,
        allowed_market_keys: list[str] | None = None,
        allowed_market_groups: list[str] | None = None,
        allowed_market_periods: list[str] | None = None,
        max_events: int | None = None,
        endpoint: str = ODDSPAPI_CURRENT_ODDS_ENDPOINT,
        exchange_bookmakers: list[str] | None = None,
        exchange_market_keys: list[str] | None = None,
        exchange_main_line_only: bool = True,
        exchange_include_player_props: bool = False,
        exchange_historical_moments: list[int] | None = None,
        exchange_max_outcomes_per_event: int = 8,
        exchange_max_requests_per_run: int = 40,
        enable_exchange_historical: bool = True,
        persist_main_line_only: bool = False,
        require_active_quotes: bool = True,
        filter_post_kickoff_ticks: bool = True,
        mainline_fallback_bookmakers: list[str] | tuple[str, ...] | None = None,
        minimum_initial_span_minutes: float = 60.0,
        api_keys: list[str] | None = None,
        max_workers: int = 1,
        market_mapping_index: MarketMappingIndex | None = None,
        debug_mode: bool = False,
    ) -> OddspapiPreStartOddsSummary:
        selected_endpoint = str(endpoint or "").strip().lower()
        if selected_endpoint not in ODDSPAPI_PRE_START_ODDS_ENDPOINTS:
            supported = ", ".join(sorted(ODDSPAPI_PRE_START_ODDS_ENDPOINTS))
            raise ValueError(
                f"Unsupported Oddspapi odds endpoint '{endpoint}'. "
                f"Expected one of: {supported}"
            )

        summary = OddspapiPreStartOddsSummary(candidates_seen=len(candidates or []))
        mapped_candidates = [candidate for candidate in candidates or [] if candidate.fixture_id]
        summary.candidates_with_mapping = len(mapped_candidates)
        live_event_ids = [
            candidate.event_id
            for candidate in mapped_candidates
            if self._is_live_candidate(candidate)
        ]
        # /historical-odds has no mainLine flags. Live/in-play quotes cannot be
        # parsed unless /odds previously populated oddspapi_mainline_outcome_cache.
        cached_live_event_ids = (
            OddspapiMainlineCacheRepository.event_ids_with_cache(live_event_ids)
            if live_event_ids
            else set()
        )
        closing_only = getattr(Config, "ODDSPAPI_PRE_START_CLOSING_ONLY", False)
        requestable_candidates = [
            candidate
            for candidate in mapped_candidates
            if (
                (candidate.has_odds or self._is_live_candidate(candidate))
                and (
                    not self._is_live_candidate(candidate)
                    or candidate.event_id in cached_live_event_ids
                )
                and (
                    not closing_only
                    or self._is_live_candidate(candidate)
                    or is_closing_odds_moment(candidate.minutes_until_start)
                )
            )
        ]
        requested_limit = max_events if max_events and max_events > 0 else None

        # This index is intentionally created once.  The ingestion service accepts it
        # directly, avoiding a database lookup per external response.
        if market_mapping_index is None and requestable_candidates:
            market_mapping_index = MarketMappingRepository.build_index(
                source=ODDSPAPI_SOURCE,
                enabled_only=True,
            )

        keys = unique_api_keys(api_keys)
        if keys:
            odds_api_keys = list(keys)
            historical_api_keys = list(keys)
        else:
            odds_api_keys = odds_endpoint_api_keys()
            historical_api_keys = free_endpoint_api_keys()
        scheduler = self.key_scheduler
        if scheduler is None and not self._custom_pipeline:
            scheduler = get_oddspapi_key_scheduler()
        odds_key_count = (
            len(odds_api_keys)
            if keys or scheduler is None
            else scheduler.available_key_count(ODDSPAPI_CURRENT_ODDS_ENDPOINT)
        )
        historical_key_count = (
            len(historical_api_keys)
            if keys or scheduler is None
            else scheduler.available_key_count(ODDSPAPI_HISTORICAL_ODDS_ENDPOINT)
        )
        bounded_workers = max(1, int(max_workers or 1))
        historical_moments = set(exchange_historical_moments or [120])
        requires_exchange_history = (
            bool(exchange_bookmakers)
            and enable_exchange_historical
            and any(
                self._is_live_candidate(candidate)
                or candidate.minutes_until_start in historical_moments
                for candidate in requestable_candidates
            )
        )
        live_requestable = [
            candidate
            for candidate in requestable_candidates
            if self._is_live_candidate(candidate)
        ]
        odds_requestable = [
            candidate
            for candidate in requestable_candidates
            if not self._is_live_candidate(candidate)
        ]
        paid_owns_odds = odds_api_keys != historical_api_keys
        can_run_parallel = (
            not self._custom_pipeline
            and not requires_exchange_history
            and requested_limit is None
            and bounded_workers > 1
            and historical_key_count > 1
            and (
                (
                    not paid_owns_odds
                    and odds_key_count > 1
                    and len(requestable_candidates) > 1
                )
                or (
                    paid_owns_odds
                    and len(live_requestable) > 1
                    and not odds_requestable
                )
            )
        )
        if can_run_parallel:
            worker_summary = self._process_parallel_workers(
                live_requestable if paid_owns_odds else requestable_candidates,
                api_keys=(
                    historical_api_keys[:historical_key_count]
                    if paid_owns_odds
                    else odds_api_keys[:odds_key_count]
                ),
                max_workers=bounded_workers,
                market_mapping_index=market_mapping_index,
                process_kwargs={
                    "bookmakers": bookmakers,
                    "dry_run": dry_run,
                    "allowed_market_keys": allowed_market_keys,
                    "allowed_market_groups": allowed_market_groups,
                    "allowed_market_periods": allowed_market_periods,
                    "max_events": None,
                    "endpoint": selected_endpoint,
                    "exchange_bookmakers": exchange_bookmakers,
                    "exchange_market_keys": exchange_market_keys,
                    "exchange_main_line_only": exchange_main_line_only,
                    "exchange_include_player_props": (
                        exchange_include_player_props
                    ),
                    "exchange_historical_moments": (
                        exchange_historical_moments
                    ),
                    "exchange_max_outcomes_per_event": (
                        exchange_max_outcomes_per_event
                    ),
                    "exchange_max_requests_per_run": (
                        exchange_max_requests_per_run
                    ),
                    "enable_exchange_historical": enable_exchange_historical,
                    "persist_main_line_only": persist_main_line_only,
                    "require_active_quotes": require_active_quotes,
                    "filter_post_kickoff_ticks": filter_post_kickoff_ticks,
                    "mainline_fallback_bookmakers": mainline_fallback_bookmakers,
                    "minimum_initial_span_minutes": (
                        minimum_initial_span_minutes
                    ),
                    "api_keys": None,
                    "max_workers": 1,
                    "debug_mode": debug_mode,
                },
            )
            non_requestable_candidates = [
                candidate
                for candidate in candidates or []
                if candidate not in requestable_candidates
            ]
            if not non_requestable_candidates:
                return worker_summary
            return self._merge_worker_summaries(
                candidates,
                [
                    worker_summary,
                    self._non_requestable_summary(
                        non_requestable_candidates,
                        respects_stored_availability=True,
                    ),
                ],
            )


        try:
            # Exchange outcome fan-out is intra-fixture: N historical-odds
            # requests for one event, one worker per API key.
            exchange_fetch_executor = (
                OddspapiExchangeHistoricalFetchExecutor(
                    api_keys=historical_api_keys,
                    max_workers=bounded_workers,
                    client_factory=self.client_factory,
                    key_scheduler=scheduler,
                )
                if not self._custom_pipeline
                and historical_key_count > 1
                and requestable_candidates
                else None
            )
            requested_count = 0
            exchange_requests_remaining = (
                int(exchange_max_requests_per_run)
                if exchange_max_requests_per_run
                and int(exchange_max_requests_per_run) > 0
                else None
            )
            odds_unavailable_event_ids: set[int] = set()
            for candidate in candidates or []:
                event_result = self._event_result(candidate)
                summary.results.append(event_result)
                is_live = self._is_live_candidate(candidate)
                if not candidate.fixture_id:
                    event_result.skipped = True
                    event_result.skip_reason = "missing_oddspapi_mapping"
                    summary.events_skipped += 1
                    continue
                if not candidate.has_odds and not is_live:
                    event_result.skipped = True
                    event_result.skip_reason = "oddspapi_odds_unavailable"
                    summary.events_skipped += 1
                    continue
                if is_live and candidate.event_id not in cached_live_event_ids:
                    event_result.skipped = True
                    event_result.skip_reason = "missing_mainline_cache"
                    summary.events_skipped += 1
                    logger.info(
                        "🚫 Oddspapi historical-odds skipped: empty mainline cache "
                        "event_id=%s fixture_id=%s",
                        candidate.event_id,
                        candidate.fixture_id,
                    )
                    continue
                closing_only = getattr(Config, "ODDSPAPI_PRE_START_CLOSING_ONLY", False)
                if (
                    closing_only
                    and not is_live
                    and not is_closing_odds_moment(candidate.minutes_until_start)
                ):
                    event_result.skipped = True
                    event_result.skip_reason = "oddspapi_closing_only"
                    summary.events_skipped += 1
                    continue
                if requested_limit is not None and requested_count >= requested_limit:
                    event_result.skipped = True
                    event_result.skip_reason = "max_events_per_run_reached"
                    summary.events_skipped += 1
                    continue

                event_result.requested = True
                requested_count += 1
                summary.requests_attempted += 1
                acquisition_service = self._acquisition_service_for_slot(
                    0,
                )
                try:
                    # Tracked leagues get openings via OddsPortal at T-120; skip
                    # exchange historical only at that opening moment. Live still
                    # uses the mainline cache for exchange outcome fetches.
                    at_opening_moment = candidate.minutes_until_start in set(
                        exchange_historical_moments
                        if exchange_historical_moments is not None
                        else [120]
                    )
                    acquisition_result = acquisition_service.acquire(
                        candidate.fixture_id,
                        event_id=candidate.event_id,
                        source_sport_id=candidate.source_sport_id,
                        minutes_until_start=candidate.minutes_until_start,
                        is_live=is_live,
                        enable_exchange_historical=(
                            enable_exchange_historical
                            and not (
                                at_opening_moment
                                and is_tracked_competition(candidate.competition_id)
                            )
                        ),
                        regular_bookmakers=bookmakers,
                        exchange_bookmakers=exchange_bookmakers,
                        market_mapping_index=market_mapping_index,
                        exchange_market_keys=exchange_market_keys,
                        exchange_main_line_only=exchange_main_line_only,
                        exchange_include_player_props=(
                            exchange_include_player_props
                        ),
                        exchange_historical_moments=(
                            exchange_historical_moments
                            if exchange_historical_moments is not None
                            else [120]
                        ),
                        exchange_max_outcomes_per_event=(
                            exchange_max_outcomes_per_event
                        ),
                        exchange_request_budget=exchange_requests_remaining,
                        minimum_initial_span_minutes=(
                            minimum_initial_span_minutes
                        ),
                        current_odds_available=candidate.has_odds,
                        require_active_quotes=require_active_quotes,
                        filter_post_kickoff_ticks=filter_post_kickoff_ticks,
                        debug_mode=debug_mode,
                        exchange_fetch_executor=exchange_fetch_executor,
                        start_time_utc=candidate.start_time_utc,
                        as_of_moments=(
                            list(Config.PRE_START_ODDS_MOMENTS)
                            if (
                                getattr(
                                    Config,
                                    "ENABLE_ODDSPAPI_HISTORICAL_AS_OF_SHADOW",
                                    False,
                                )
                                or getattr(
                                    Config,
                                    "ENABLE_ODDSPAPI_HISTORICAL_AS_OF_PERSIST",
                                    False,
                                )
                            )
                            else None
                        ),
                        attach_as_of=getattr(
                            Config,
                            "ENABLE_ODDSPAPI_HISTORICAL_AS_OF_PERSIST",
                            False,
                        ),
                    )
                    if debug_mode and getattr(
                        acquisition_result,
                        "debug_raw_payload",
                        None,
                    ) is not None:
                        OddspapiDebugResponseWriter.save(
                            event_id=candidate.event_id,
                            fixture_id=candidate.fixture_id,
                            bookmakers=getattr(
                                acquisition_result,
                                "debug_bookmakers",
                                None,
                            ) or bookmakers,
                            payload=acquisition_result.debug_raw_payload,
                            endpoint=getattr(
                                acquisition_result,
                                "debug_endpoint",
                                None,
                            ),
                        )
                    self._copy_acquisition_stats(
                        event_result,
                        acquisition_result,
                    )
                    self._accumulate_acquisition(summary, event_result)
                    if exchange_requests_remaining is not None:
                        exchange_requests_remaining = max(
                            0,
                            exchange_requests_remaining
                            - event_result.exchange_historical_requests_attempted,
                        )
                    if acquisition_result.endpoint_missing:
                        if not is_live:
                            odds_unavailable_event_ids.add(candidate.event_id)
                            summary.missing_endpoints += 1
                        event_result.skipped = True
                        event_result.skip_reason = "oddspapi_odds_endpoint_not_found"
                        summary.events_skipped += 1
                        logger.info(
                            "Oddspapi odds endpoint missing endpoint=%s event_id=%s fixture_id=%s",
                            (
                                ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
                                if is_live
                                else ODDSPAPI_CURRENT_ODDS_ENDPOINT
                            ),
                            candidate.event_id,
                            candidate.fixture_id,
                        )
                        continue
                    odds_response = acquisition_result.payload
                    if (
                        is_live
                        and getattr(Config, "ENABLE_ODDSPAPI_HISTORICAL_AS_OF_SHADOW", False)
                    ):
                        log_historical_odds_as_of_shadow(
                            event_id=candidate.event_id,
                            fixture_id=candidate.fixture_id,
                            as_of_quotes=getattr(
                                acquisition_result, "as_of_quotes", None
                            )
                            or [],
                            tolerance_minutes=getattr(
                                Config,
                                "PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES",
                                3,
                            ),
                        )
                    if not odds_response:
                        if not is_live:
                            odds_unavailable_event_ids.add(candidate.event_id)
                        event_result.skipped = True
                        event_result.skip_reason = "no_oddspapi_odds"
                        summary.events_skipped += 1
                        continue
                    if not is_live and not self._has_current_odds(odds_response):
                        odds_unavailable_event_ids.add(candidate.event_id)
                        event_result.skipped = True
                        event_result.skip_reason = "no_oddspapi_odds"
                        summary.events_skipped += 1
                        logger.info(
                            "Oddspapi /odds returned no bookmaker odds event_id=%s fixture_id=%s",
                            candidate.event_id,
                            candidate.fixture_id,
                        )
                        continue
                    summary.responses_received += 1
                    ingestion_result = self.ingestion_service.save_from_oddspapi_response(
                        odds_response,
                        source=ODDSPAPI_INGESTION_SOURCE,
                        dry_run=dry_run,
                        allowed_market_keys=allowed_market_keys,
                        allowed_market_groups=allowed_market_groups,
                        allowed_market_periods=allowed_market_periods,
                        market_mapping_index=market_mapping_index,
                        persist_main_line_only=persist_main_line_only,
                        require_active_quotes=require_active_quotes,
                        use_mainline_cache=is_live,
                        mainline_fallback_bookmakers=mainline_fallback_bookmakers,
                        debug_mode=debug_mode,
                    )
                    self._copy_ingestion_stats(event_result, ingestion_result)
                    self._accumulate(summary, event_result)
                    logger.info(
                        "Oddspapi pre-start response processed endpoint=%s event_id=%s fixture_id=%s "
                        "markets_detected=%s choices_detected=%s bookies_detected=%s "
                        "markets_saved=%s choices_saved=%s snapshots_saved=%s "
                        "unmapped_markets=%s unmapped_outcomes=%s "
                        "incomplete_markets=%s skipped=%s reason=%s",
                        (
                            ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
                            if is_live
                            else ODDSPAPI_CURRENT_ODDS_ENDPOINT
                        ),
                        candidate.event_id,
                        candidate.fixture_id,
                        event_result.markets_detected,
                        event_result.choices_detected,
                        event_result.bookies_detected,
                        event_result.markets_saved,
                        event_result.choices_saved,
                        event_result.snapshots_saved,
                        event_result.unmapped_markets_detected,
                        event_result.unmapped_outcomes_detected,
                        event_result.skipped_incomplete_markets_detected,
                        getattr(ingestion_result, "skipped", False),
                        getattr(ingestion_result, "reason", None),
                    )
                    if getattr(ingestion_result, "skipped", False):
                        event_result.skipped = True
                        event_result.skip_reason = getattr(ingestion_result, "reason", None) or "ingestion_skipped"
                        summary.events_skipped += 1
                    else:
                        summary.events_ingested += 1
                except OddsPapiQuotaExhaustedError as exc:
                    event_result.skipped = True
                    event_result.skip_reason = "oddspapi_quota_exhausted"
                    event_result.error = str(exc)
                    summary.events_skipped += 1
                    logger.warning(
                        "Oddspapi odds skipped because every eligible key is exhausted "
                        "event_id=%s fixture_id=%s",
                        candidate.event_id,
                        candidate.fixture_id,
                    )
                except Exception as exc:
                    event_result.error = str(exc)
                    summary.events_failed += 1
                    logger.warning(
                        "Oddspapi pre-start odds processing failed event_id=%s fixture_id=%s: %s",
                        candidate.event_id,
                        candidate.fixture_id,
                        exc,
                    )
                finally:
                    self._warn_if_bookmaker_coverage_gap(
                        event_result,
                        endpoint=(
                            ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
                            if is_live
                            else ODDSPAPI_CURRENT_ODDS_ENDPOINT
                        ),
                    )
            if not dry_run:
                mark_missing_endpoints_unavailable(odds_unavailable_event_ids, ODDSPAPI_SOURCE)
            return summary
        finally:
            self._close_owned_clients()
