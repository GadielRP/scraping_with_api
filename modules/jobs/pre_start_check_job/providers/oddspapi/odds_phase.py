"""Public Oddspapi pre-start odds job entrypoint."""

from __future__ import annotations

import logging
from collections import Counter

from infrastructure.persistence.repositories import (
    EventOddsSourceState,
    EventSourceMappingRepository,
)
from infrastructure.settings import Config

from .event_selector import _canonical_event_id, select_oddspapi_pre_start_candidates
from .odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
    OddspapiPreStartOddsEventResult,
    OddspapiPreStartOddsSummary,
)
from .constants import ODDSPAPI_SOURCE
from .settings import ODDSPAPI_PRE_START_SETTINGS

logger = logging.getLogger(__name__)


def _skipped_summary(
    candidates,
    reason: str,
) -> OddspapiPreStartOddsSummary:
    summary = OddspapiPreStartOddsSummary(
        candidates_seen=len(candidates),
        skip_reason=reason,
    )
    for candidate in candidates:
        summary.results.append(
            OddspapiPreStartOddsEventResult(
                event_id=candidate.event_id,
                fixture_id=candidate.fixture_id,
                minutes_until_start=candidate.minutes_until_start,
                skipped=True,
                skip_reason=reason,
            )
        )
    summary.events_skipped = len(summary.results)
    return summary


def _log_summary(summary: OddspapiPreStartOddsSummary) -> None:
    skip_reasons = Counter(
        result.skip_reason
        for result in summary.results
        if result.skipped and result.skip_reason
    )
    logger.info(
        "Oddspapi pre-start odds summary: candidates_seen=%s candidates_with_mapping=%s "
        "requests_attempted=%s responses_received=%s events_ingested=%s events_skipped=%s "
        "events_failed=%s markets_saved=%s choices_saved=%s snapshots_saved=%s "
        "unmapped_markets_detected=%s unmapped_outcomes_detected=%s "
        "skipped_incomplete_markets_detected=%s "
        "http_requests_attempted=%s exchange_outcomes_selected=%s "
        "exchange_historical_requests_attempted=%s "
        "exchange_historical_requests_failed=%s "
        "exchange_outcomes_skipped_budget=%s skip_reasons=%s",
        summary.candidates_seen,
        summary.candidates_with_mapping,
        summary.requests_attempted,
        summary.responses_received,
        summary.events_ingested,
        summary.events_skipped,
        summary.events_failed,
        summary.markets_saved,
        summary.choices_saved,
        summary.snapshots_saved,
        summary.unmapped_markets_detected,
        summary.unmapped_outcomes_detected,
        summary.skipped_incomplete_markets_detected,
        summary.http_requests_attempted,
        summary.exchange_outcomes_selected,
        summary.exchange_historical_requests_attempted,
        summary.exchange_historical_requests_failed,
        summary.exchange_outcomes_skipped_budget,
        dict(skip_reasons),
    )


def _resolve_source_states(
    events_to_process: list[dict],
) -> dict[int, dict[str, EventOddsSourceState]]:
    """Look up Oddspapi source state when the orchestrator didn't already load it."""
    event_ids = [
        event_id
        for event_id in (_canonical_event_id(event) for event in events_to_process or [])
        if event_id is not None
    ]
    return EventSourceMappingRepository.get_odds_source_states(
        event_ids=event_ids,
        sources=[ODDSPAPI_SOURCE],
    )


def run_oddspapi_pre_start_odds(
    events_to_process: list[dict],
    source_states: dict[int, dict[str, EventOddsSourceState]] | None = None,
    *,
    debug_mode: bool = False,
    dry_run: bool = False,
) -> OddspapiPreStartOddsSummary:
    """Ingest mapped Oddspapi odds without affecting the main pre-start job."""
    if not getattr(Config, "ENABLE_ODDSPAPI_PRE_START_ODDS", True):
        summary = OddspapiPreStartOddsSummary(disabled=True, skip_reason="oddspapi_pre_start_disabled")
        _log_summary(summary)
        return summary

    # Selecting here (before resolving source_states) mirrors the timing-only
    # eligibility check; it never needs the mapping lookup below.
    candidates = select_oddspapi_pre_start_candidates(events_to_process, source_states=source_states)
    if not candidates:
        summary = OddspapiPreStartOddsSummary()
        _log_summary(summary)
        return summary

    api_keys = [
        str(value).strip()
        for value in (
            getattr(Config, "ODDSPAPI_KEYS", None)
            or [getattr(Config, "ODDSPAPI_KEY", "")]
        )
        if str(value or "").strip()
    ]
    if not api_keys:
        logger.warning(
            "Oddspapi pre-start odds ingestion skipped because ODDSPAPI_KEY "
            "is not configured"
        )
        summary = _skipped_summary(candidates, "missing_oddspapi_api_key")
        _log_summary(summary)
        return summary

    if source_states is None:
        try:
            source_states = _resolve_source_states(events_to_process)
        except Exception as exc:
            logger.exception("Oddspapi pre-start fixture mapping lookup failed")
            summary = OddspapiPreStartOddsBatchProcessor._failed_worker_summary(candidates, exc)
            _log_summary(summary)
            return summary
        candidates = select_oddspapi_pre_start_candidates(events_to_process, source_states=source_states)

    summary = OddspapiPreStartOddsBatchProcessor().process(
        candidates,
        endpoint=ODDSPAPI_PRE_START_SETTINGS.default_endpoint,
        bookmakers=(
            getattr(Config, "ODDSPAPI_PRE_START_BOOKMAKERS", None)
            or Config.ODDSPAPI_DEFAULT_BOOKMAKERS
        ),
        exchange_bookmakers=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_BOOKMAKERS",
            None,
        ),
        exchange_market_keys=list(
            ODDSPAPI_PRE_START_SETTINGS.exchange_market_keys
        ),
        exchange_main_line_only=(
            ODDSPAPI_PRE_START_SETTINGS.exchange_main_line_only
        ),
        exchange_include_player_props=(
            ODDSPAPI_PRE_START_SETTINGS.exchange_include_player_props
        ),
        exchange_historical_moments=list(
            ODDSPAPI_PRE_START_SETTINGS.opening_historical_moments
        ),
        exchange_max_outcomes_per_event=(
            ODDSPAPI_PRE_START_SETTINGS.exchange_max_outcomes_per_event
        ),
        exchange_max_requests_per_run=(
            ODDSPAPI_PRE_START_SETTINGS.exchange_max_requests_per_run
        ),
        enable_exchange_historical=getattr(
            Config,
            "ENABLE_ODDSPAPI_EXCHANGE_HISTORICAL_REQUESTS",
            True,
        ),
        persist_main_line_only=getattr(
            Config,
            "ODDSPAPI_PRE_START_PERSIST_MAIN_LINE_ONLY",
            ODDSPAPI_PRE_START_SETTINGS.persist_main_line_only,
        ),
        require_active_quotes=getattr(
            Config,
            "ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES",
            ODDSPAPI_PRE_START_SETTINGS.require_active_quotes,
        ),
        minimum_initial_span_minutes=(
            ODDSPAPI_PRE_START_SETTINGS.initial_odds_min_span_minutes
        ),
        dry_run=dry_run,
        allowed_market_keys=ODDSPAPI_PRE_START_SETTINGS.as_list(
            ODDSPAPI_PRE_START_SETTINGS.allowed_market_keys
        ),
        allowed_market_groups=ODDSPAPI_PRE_START_SETTINGS.as_list(
            ODDSPAPI_PRE_START_SETTINGS.allowed_market_groups
        ),
        allowed_market_periods=ODDSPAPI_PRE_START_SETTINGS.as_list(
            ODDSPAPI_PRE_START_SETTINGS.allowed_market_periods
        ),
        max_events=getattr(Config, "ODDSPAPI_PRE_START_MAX_EVENTS_PER_RUN", 0),
        api_keys=api_keys,
        max_workers=getattr(Config, "ODDSPAPI_PRE_START_WORKERS", 1),
        debug_mode=debug_mode,
    )
    _log_summary(summary)
    if debug_mode:
        for result in summary.results:
            logger.debug(
                "Oddspapi pre-start event event_id=%s fixture_id=%s minutes_until_start=%s "
                "requested=%s skipped=%s skip_reason=%s markets_saved=%s snapshots_saved=%s error=%s",
                result.event_id,
                result.fixture_id,
                result.minutes_until_start,
                result.requested,
                result.skipped,
                result.skip_reason,
                result.markets_saved,
                result.snapshots_saved,
                result.error,
            )
    return summary
