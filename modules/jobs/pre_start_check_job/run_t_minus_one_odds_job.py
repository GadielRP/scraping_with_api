"""Critical T-1 odds ingestion without unrelated pre-start maintenance."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from infrastructure.settings import Config
from infrastructure.runtime.view_refresh_exclusion import view_refresh_exclusion
from infrastructure.persistence.advisory_lock import exclusive_slot
from shared.temporal import as_utc, utc_now
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    OddsPortalScrapeContext,
)
from modules.jobs.pre_start_check_job.run_pre_start_check_job import (
    run_pre_start_odds_moments,
)

logger = logging.getLogger(__name__)


@view_refresh_exclusion.pre_start()
def run_t_minus_one_odds_job(
    runtime,
    scheduled_at: datetime,
    *,
    debug_mode: bool = False,
):
    """Ingest provider odds for the closing event slot one minute ahead."""
    scheduled_at = as_utc(scheduled_at, field_name="scheduled_at")
    now = utc_now()
    closing_minute = Config.PRE_START_CLOSING_ODDS_MINUTE
    target_start = scheduled_at + timedelta(minutes=closing_minute)
    dispatch_lag_ms = max(0, int((now - scheduled_at).total_seconds() * 1000))
    logger.info(
        "🕛1️⃣ T-1 odds dispatch scheduled_at=%s target_start=%s dispatch_lag_ms=%s",
        scheduled_at.isoformat(),
        target_start.isoformat(),
        dispatch_lag_ms,
    )

    if closing_minute not in Config.PRE_START_ODDS_MOMENTS:
        logger.info(
            "⏭️ Closing moment %s is not configured; skipping dedicated dispatch",
            closing_minute,
        )
        return None

    if now >= target_start:
        logger.error(
            "T-1 odds dispatch missed target_start=%s dispatch_lag_ms=%s",
            target_start.isoformat(),
            dispatch_lag_ms,
        )
        return None

    from modules.competition.tracked_competitions import tracked_competition_ids

    tracked_ids = None
    if Config.TRACKED_COMPETITIONS_ONLY:
        tracked_ids = list(tracked_competition_ids())

    events = runtime.event_repo.get_events_starting_between(
        target_start,
        target_start + timedelta(seconds=1),
        competition_ids=tracked_ids,
    )
    if not events:
        logger.debug("No events found for T-1 target_start=%s", target_start)
        return None

    timings = {event["id"]: closing_minute for event in events}
    with exclusive_slot(736105, int(scheduled_at.timestamp()) // 60) as acquired:
        if not acquired:
            logger.info('Closing slot already executing scheduled_at=%s', scheduled_at.isoformat())
            return None
        return run_pre_start_odds_moments(
            runtime,
            events,
            timings,
            key_moments=(closing_minute,),
            oddsportal_context=OddsPortalScrapeContext({}, set(), {}),
            debug_mode=debug_mode,
            timestamp_correction_enabled=False,
            evaluate_key_moments=False,
        )


__all__ = ["run_t_minus_one_odds_job"]
