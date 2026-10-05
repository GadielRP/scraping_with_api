"""Daily calendar progress, retention and retry coordination."""

from __future__ import annotations

import logging
from datetime import timedelta, timezone

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from infrastructure.settings import Config
from modules.jobs.event_discard_cleanup import run_event_discard_cleanup
from modules.sports.catalog import sofascore_sport_slugs
from shared.temporal import now_in_timezone

from .pipeline import discover_events_for_date

logger = logging.getLogger(__name__)


def resolve_daily_discovery_slot(now=None) -> str | None:
    if now is None:
        now = now_in_timezone(Config.TIMEZONE)

    current_hour = now.hour
    next_utc_day_hour = Config.DAILY_DISCOVERY_NEXT_UTC_DAY_OPEN_HOUR
    current_utc_day_hour = Config.DAILY_DISCOVERY_CURRENT_UTC_DAY_OPEN_HOUR

    # Each configured local-time window identifies the UTC date it queries.
    # Neither window carries over from the previous local calendar day.
    if next_utc_day_hour > current_utc_day_hour:
        if current_utc_day_hour <= current_hour < next_utc_day_hour:
            return "current_utc_day"
        return "next_utc_day" if current_hour >= next_utc_day_hour else None
    else:
        if current_hour >= current_utc_day_hour:
            return "current_utc_day"
        if current_hour >= next_utc_day_hour:
            return "next_utc_day"
        return None


def resolve_daily_discovery_target_date(now, run_slot: str | None) -> str | None:
    """Return the UTC calendar date for a daily-discovery slot.

    ``next_utc_day`` targets the next UTC date; ``current_utc_day`` targets
    the current UTC date. ``now`` should be the scheduled occurrence when this
    is called from the scheduler, so deferred work keeps its original date.
    """
    if run_slot is None:
        return None
    utc_date = now.astimezone(timezone.utc)
    if run_slot == "next_utc_day":
        utc_date += timedelta(days=1)
    elif run_slot != "current_utc_day":
        raise ValueError(f"Unsupported Daily Discovery slot: {run_slot}")
    return utc_date.date().isoformat()


def run_daily_discovery_job(*, target_date=None, run_slot=None) -> dict | None:
    logger.info("Starting daily discovery heartbeat")
    # Run before slot/cache checks so every heartbeat can advance bounded cleanup.
    run_event_discard_cleanup()

    try:
        DailyDiscoveryRepository.cleanup_old_logs(Config.DAILY_DISCOVERY_DAYS_TO_KEEP)
    except Exception as exc:
        logger.warning("Failed to cleanup DailyDiscovery logs: %s", exc)

    now = now_in_timezone(Config.TIMEZONE)
    run_slot = run_slot or resolve_daily_discovery_slot(now)

    if not run_slot:
        logger.info("No Daily Discovery slot is open yet. Skipping.")
        return

    today_str = target_date or resolve_daily_discovery_target_date(now, run_slot)
    logger.info(
        "Daily discovery target_date=%s slot=%s local_now=%s timezone=%s",
        today_str,
        run_slot,
        now.isoformat(),
        Config.TIMEZONE,
    )

    discovery_sports = sofascore_sport_slugs()
    DailyDiscoveryRepository.initialize_sports_for_slot(
        today_str,
        run_slot,
        discovery_sports,
    )
    pending_sports = sofascore_sport_slugs(
        DailyDiscoveryRepository.get_pending_sports(today_str, run_slot)
    )
    if not pending_sports:
        logger.info(
            "Daily discovery slot %s for %s is already completed for all sports.",
            run_slot,
            today_str,
        )
        return

    return discover_events_for_date(date=today_str, sports=pending_sports, run_slot=run_slot)
