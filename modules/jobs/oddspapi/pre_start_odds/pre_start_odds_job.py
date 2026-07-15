"""Public Oddspapi pre-start odds job entrypoint."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config

from .event_selector import select_oddspapi_pre_start_candidates
from .odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
    OddspapiPreStartOddsEventResult,
    OddspapiPreStartOddsSummary,
)
from .source_mapping_loader import attach_oddspapi_fixture_ids

logger = logging.getLogger(__name__)


def _skipped_summary(candidates, reason: str, *, disabled: bool = False) -> OddspapiPreStartOddsSummary:
    summary = OddspapiPreStartOddsSummary(
        candidates_seen=len(candidates),
        disabled=disabled,
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
    logger.info(
        "Oddspapi pre-start odds summary: candidates_seen=%s candidates_with_mapping=%s "
        "requests_attempted=%s responses_received=%s events_ingested=%s events_skipped=%s "
        "events_failed=%s markets_saved=%s choices_saved=%s snapshots_saved=%s "
        "unmapped_markets_detected=%s unmapped_outcomes_detected=%s",
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
    )


def run_oddspapi_pre_start_odds_ingestion(
    events_to_process: list[dict],
    *,
    debug_mode: bool = False,
    dry_run: bool = False,
) -> OddspapiPreStartOddsSummary:
    """Ingest mapped Oddspapi odds without affecting the main pre-start job."""
    if not getattr(Config, "ENABLE_ODDSPAPI_PRE_START_ODDS", True):
        summary = OddspapiPreStartOddsSummary(disabled=True, skip_reason="oddspapi_pre_start_disabled")
        _log_summary(summary)
        return summary

    candidates = select_oddspapi_pre_start_candidates(events_to_process)
    if not candidates:
        summary = OddspapiPreStartOddsSummary()
        _log_summary(summary)
        return summary
    if not str(getattr(Config, "ODDSPAPI_KEY", "") or "").strip():
        logger.warning("Oddspapi pre-start odds ingestion skipped because ODDSPAPI_KEY is not configured")
        summary = _skipped_summary(candidates, "missing_oddspapi_api_key")
        _log_summary(summary)
        return summary

    try:
        with db_manager.get_session() as session:
            attach_oddspapi_fixture_ids(candidates, session)
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

    summary = OddspapiPreStartOddsBatchProcessor().process(
        candidates,
        bookmakers=(
            getattr(Config, "ODDSPAPI_PRE_START_BOOKMAKERS", None)
            or Config.ODDSPAPI_DEFAULT_BOOKMAKERS
        ),
        dry_run=dry_run,
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
