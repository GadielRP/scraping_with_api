"""Batch coordinator for requesting and ingesting mapped Oddspapi odds."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging

from infrastructure.persistence.repositories import (
    EventSourceMappingRepository,
    MarketMappingRepository,
)
from modules.odds_ingestion import MarketOddsIngestionService

from .constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_INGESTION_SOURCE,
    ODDSPAPI_PRE_START_ODDS_ENDPOINTS,
    ODDSPAPI_SOURCE,
)
from .event_selector import OddspapiPreStartCandidate
from .odds_fetcher import OddspapiOddsFetcher

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
    bookies_detected: int = 0
    bookies_processed: int = 0
    unmapped_markets_detected: int = 0
    unmapped_outcomes_detected: int = 0
    skipped_missing_handicap_detected: int = 0
    error: str | None = None


@dataclass
class OddspapiPreStartOddsSummary:
    candidates_seen: int = 0
    candidates_with_mapping: int = 0
    requests_attempted: int = 0
    responses_received: int = 0
    events_ingested: int = 0
    events_skipped: int = 0
    events_failed: int = 0
    markets_saved: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0
    unmapped_markets_detected: int = 0
    unmapped_outcomes_detected: int = 0
    skipped_missing_handicap_detected: int = 0
    disabled: bool = False
    skip_reason: str | None = None
    results: list[OddspapiPreStartOddsEventResult] = field(default_factory=list)


class OddspapiPreStartOddsBatchProcessor:
    def __init__(
        self,
        fetcher: OddspapiOddsFetcher | None = None,
        ingestion_service: type[MarketOddsIngestionService] = MarketOddsIngestionService,
    ):
        self.fetcher = fetcher or OddspapiOddsFetcher()
        self.ingestion_service = ingestion_service

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
        ):
            setattr(result, field_name, getattr(ingestion_result, field_name, 0) or 0)

    @staticmethod
    def _accumulate(summary: OddspapiPreStartOddsSummary, result: OddspapiPreStartOddsEventResult) -> None:
        for field_name in (
            "markets_saved", "choices_saved", "snapshots_saved",
            "unmapped_markets_detected", "unmapped_outcomes_detected",
            "skipped_missing_handicap_detected",
        ):
            setattr(summary, field_name, getattr(summary, field_name) + getattr(result, field_name))

    def process(
        self,
        candidates: list[OddspapiPreStartCandidate],
        *,
        bookmakers: list[str] | None,
        dry_run: bool = False,
        allowed_market_groups: list[str] | None = None,
        allowed_market_periods: list[str] | None = None,
        max_events: int | None = None,
        endpoint: str = ODDSPAPI_CURRENT_ODDS_ENDPOINT,
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
        respects_stored_availability = (
            selected_endpoint == ODDSPAPI_CURRENT_ODDS_ENDPOINT
        )
        requestable_candidates = [
            candidate
            for candidate in mapped_candidates
            if candidate.has_odds or not respects_stored_availability
        ]
        requested_limit = max_events if max_events and max_events > 0 else None

        # This index is intentionally created once.  The ingestion service accepts it
        # directly, avoiding a database lookup per external response.
        market_mapping_index = (
            MarketMappingRepository.build_index(source=ODDSPAPI_SOURCE, enabled_only=True)
            if requestable_candidates else None
        )
        requested_count = 0
        odds_not_found_event_ids: set[int] = set()
        for candidate in candidates or []:
            event_result = self._event_result(candidate)
            summary.results.append(event_result)
            if not candidate.fixture_id:
                event_result.skipped = True
                event_result.skip_reason = "missing_oddspapi_mapping"
                summary.events_skipped += 1
                continue
            if not candidate.has_odds and respects_stored_availability:
                event_result.skipped = True
                event_result.skip_reason = "oddspapi_odds_unavailable"
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
            try:
                fetch_result = self.fetcher.fetch_odds(
                    candidate.fixture_id,
                    bookmakers=bookmakers,
                    endpoint=selected_endpoint,
                    source_sport_id=candidate.source_sport_id,
                )
                if fetch_result.endpoint_missing:
                    if respects_stored_availability:
                        odds_not_found_event_ids.add(candidate.event_id)
                    event_result.skipped = True
                    event_result.skip_reason = "oddspapi_odds_endpoint_not_found"
                    summary.events_skipped += 1
                    logger.info(
                        "Oddspapi odds endpoint missing endpoint=%s event_id=%s fixture_id=%s",
                        selected_endpoint,
                        candidate.event_id,
                        candidate.fixture_id,
                    )
                    continue
                odds_response = fetch_result.payload
                if not odds_response:
                    event_result.skipped = True
                    event_result.skip_reason = "no_oddspapi_odds"
                    summary.events_skipped += 1
                    continue
                summary.responses_received += 1
                ingestion_result = self.ingestion_service.save_from_oddspapi_response(
                    odds_response,
                    source=ODDSPAPI_INGESTION_SOURCE,
                    dry_run=dry_run,
                    allowed_market_groups=allowed_market_groups,
                    allowed_market_periods=allowed_market_periods,
                    market_mapping_index=market_mapping_index,
                )
                self._copy_ingestion_stats(event_result, ingestion_result)
                self._accumulate(summary, event_result)
                logger.info(
                    "Oddspapi pre-start response processed endpoint=%s event_id=%s fixture_id=%s "
                    "markets_detected=%s choices_detected=%s bookies_detected=%s "
                    "markets_saved=%s choices_saved=%s snapshots_saved=%s "
                    "unmapped_markets=%s unmapped_outcomes=%s skipped=%s reason=%s",
                    selected_endpoint,
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
                    getattr(ingestion_result, "skipped", False),
                    getattr(ingestion_result, "reason", None),
                )
                if getattr(ingestion_result, "skipped", False):
                    event_result.skipped = True
                    event_result.skip_reason = getattr(ingestion_result, "reason", None) or "ingestion_skipped"
                    summary.events_skipped += 1
                else:
                    summary.events_ingested += 1
            except Exception as exc:
                event_result.error = str(exc)
                summary.events_failed += 1
                logger.warning(
                    "Oddspapi pre-start odds processing failed event_id=%s fixture_id=%s: %s",
                    candidate.event_id,
                    candidate.fixture_id,
                    exc,
                )
        if odds_not_found_event_ids and not dry_run:
            EventSourceMappingRepository.mark_odds_unavailable(
                odds_not_found_event_ids,
                ODDSPAPI_SOURCE,
            )
        return summary
