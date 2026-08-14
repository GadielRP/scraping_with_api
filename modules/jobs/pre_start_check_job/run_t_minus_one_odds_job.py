"""Critical T-1 odds ingestion without unrelated pre-start maintenance."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.oddsportal_worker import (
    OddsPortalScrapeContext,
)
from modules.jobs.pre_start_check_job.run_pre_start_check_job import (
    run_pre_start_odds_moments,
)

logger = logging.getLogger(__name__)


def run_t_minus_one_odds_job(
    scheduler,
    scheduled_at: datetime,
    *,
    debug_mode: bool = False,
):
    """Start the closing-odds flow for the event slot one minute ahead."""
    now = datetime.now()
    closing_minute = Config.PRE_START_CLOSING_ODDS_MINUTE
    target_start = scheduled_at + timedelta(minutes=closing_minute)
    dispatch_lag_ms = max(0, int((now - scheduled_at).total_seconds() * 1000))
    logger.info(
        "T-1 odds dispatch scheduled_at=%s target_start=%s dispatch_lag_ms=%s",
        scheduled_at.isoformat(),
        target_start.isoformat(),
        dispatch_lag_ms,
    )

    if now >= target_start:
        logger.error(
            "T-1 odds dispatch missed target_start=%s dispatch_lag_ms=%s",
            target_start.isoformat(),
            dispatch_lag_ms,
        )
        return None

    events = scheduler.event_repo.get_events_starting_between(
        target_start,
        target_start + timedelta(seconds=1),
    )
    if not events:
        logger.debug("No events found for T-1 target_start=%s", target_start)
        return None

    timings = {event["id"]: closing_minute for event in events}
    return run_pre_start_odds_moments(
        scheduler,
        events,
        timings,
        key_moments=(closing_minute,),
        oddsportal_context=OddsPortalScrapeContext({}, set(), {}),
        debug_mode=debug_mode,
        timestamp_correction_enabled=False,
    )


__all__ = ["run_t_minus_one_odds_job"]
