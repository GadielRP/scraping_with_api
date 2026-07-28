"""Public Oddspapi pre-start odds job entrypoint."""

from __future__ import annotations

import logging
from collections import Counter

from infrastructure.persistence.repositories import (
    EventOddsSourceState,
    EventSourceMappingRepository,
)
from infrastructure.settings import Config

from .event_selector import select_oddspapi_pre_start_candidates
from .odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
    OddspapiPreStartOddsEventResult,
    OddspapiPreStartOddsSummary,
)
from .constants import ODDSPAPI_HISTORICAL_ODDS_ENDPOINT, ODDSPAPI_SOURCE

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


def run_oddspapi_pre_start_odds_ingestion(
    events_to_process: list[dict],
    *,
    debug_mode: bool = False,
    dry_run: bool = False,
    source_states: dict[int, dict[str, EventOddsSourceState]] | None = None,
) -> OddspapiPreStartOddsSummary:
    """Ingest mapped Oddspapi odds without affecting the main pre-start job."""
    if not getattr(Config, "ENABLE_ODDSPAPI_PRE_START_ODDS", True):
        summary = OddspapiPreStartOddsSummary(disabled=True, skip_reason="oddspapi_pre_start_disabled")
        _log_summary(summary)
        return summary

    candidates = select_oddspapi_pre_start_candidates(
        events_to_process,
        source_states=source_states,
    )
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
            source_states = EventSourceMappingRepository.get_odds_source_states(
                event_ids=[candidate.event_id for candidate in candidates],
                sources=[ODDSPAPI_SOURCE],
            )
        except Exception as exc:
            logger.exception("Oddspapi pre-start fixture mapping lookup failed")
            summary = _skipped_summary(candidates, "oddspapi_mapping_lookup_failed")
            summary.events_skipped = 0
            summary.events_failed = len(candidates)
            for result in summary.results:
                result.skipped = False
                result.skip_reason = None
                result.error = str(exc)
            _log_summary(summary)
            return summary
        candidates = select_oddspapi_pre_start_candidates(
            events_to_process,
            source_states=source_states,
        )

    summary = OddspapiPreStartOddsBatchProcessor().process(
        candidates,
        endpoint=getattr(
            Config,
            "ODDSPAPI_PRE_START_ODDS_ENDPOINT",
            ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
        ),
        bookmakers=(
            getattr(Config, "ODDSPAPI_PRE_START_BOOKMAKERS", None)
            or Config.ODDSPAPI_DEFAULT_BOOKMAKERS
        ),
        exchange_bookmakers=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_BOOKMAKERS",
            None,
        ),
        exchange_market_keys=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_MARKET_KEYS",
            Config.ODDSPAPI_DEFAULT_MARKET_KEYS,
        ),
        exchange_main_line_only=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_MAIN_LINE_ONLY",
            True,
        ),
        exchange_include_player_props=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_INCLUDE_PLAYER_PROPS",
            False,
        ),
        exchange_historical_moments=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_HISTORICAL_MOMENTS",
            [120],
        ),
        exchange_max_outcomes_per_event=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_MAX_OUTCOMES_PER_EVENT",
            8,
        ),
        exchange_max_requests_per_run=getattr(
            Config,
            "ODDSPAPI_PRE_START_EXCHANGE_MAX_REQUESTS_PER_RUN",
            40,
        ),
        minimum_initial_span_minutes=getattr(
            Config,
            "ODDSPAPI_INITIAL_ODDS_MIN_SPAN_MINUTES",
            60.0,
        ),
        dry_run=dry_run,
        allowed_market_keys=getattr(
            Config,
            "ODDSPAPI_PRE_START_MARKET_KEYS",
            None,
        ),
        allowed_market_groups=getattr(
            Config,
            "ODDSPAPI_PRE_START_ALLOWED_MARKET_GROUPS",
            None,
        ),
        allowed_market_periods=getattr(
            Config,
            "ODDSPAPI_PRE_START_ALLOWED_MARKET_PERIODS",
            None,
        ),
        max_events=getattr(Config, "ODDSPAPI_PRE_START_MAX_EVENTS_PER_RUN", 0),
        api_keys=api_keys,
        max_workers=getattr(Config, "ODDSPAPI_PRE_START_WORKERS", 1),
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
