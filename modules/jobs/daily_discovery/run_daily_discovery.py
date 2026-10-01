"""Daily discovery job wrappers."""

from __future__ import annotations

import logging

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from modules.jobs.event_discard_cleanup import run_event_discard_cleanup
from modules.sports.catalog import sofascore_sport_slugs
from shared.temporal import now_in_timezone

from .extractor import DailyDiscoveryExtractor

logger = logging.getLogger(__name__)


def resolve_daily_discovery_slot(now=None) -> str | None:
    from infrastructure.settings import Config

    if now is None:
        now = now_in_timezone(Config.TIMEZONE)

    current_hour = now.hour
    am_hour = Config.DAILY_DISCOVERY_AM_OPEN_HOUR
    pm_hour = Config.DAILY_DISCOVERY_PM_OPEN_HOUR

    # Slot names are legacy labels: some deployments open PM before AM.
    # Neither window carries over from the previous local calendar day.
    if am_hour > pm_hour:
        if pm_hour <= current_hour < am_hour:
            return "PM"
        return "AM" if current_hour >= am_hour else None
    else:
        # Standard case (am_hour < pm_hour, e.g. AM=5, PM=16)
        if current_hour >= pm_hour:
            return "PM"
        if current_hour >= am_hour:
            return "AM"
        return None


def run_daily_discovery(sports=None, date_str=None, run_slot=None):
    if date_str is None:
        from infrastructure.settings import Config

        date_str = now_in_timezone(Config.TIMEZONE).strftime("%Y-%m-%d")
    sports = sofascore_sport_slugs(sports)
    return DailyDiscoveryExtractor().discover_events_for_date(date_str, sports=sports, run_slot=run_slot)


def run_daily_discovery_job() -> None:
    logger.info("Starting Job E: Daily discovery heartbeat")
    # Run before slot/cache checks so every heartbeat can advance bounded cleanup.
    run_event_discard_cleanup()

    try:
        from infrastructure.settings import Config

        days_to_keep = getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)
        DailyDiscoveryRepository.cleanup_old_logs(days_to_keep)
    except Exception as exc:
        logger.warning("Failed to cleanup DailyDiscovery logs: %s", exc)

    try:
        now = now_in_timezone(Config.TIMEZONE)
        run_slot = resolve_daily_discovery_slot(now)

        if not run_slot:
            logger.info(
                "No Daily Discovery slot is open yet. Skipping."
            )
            return

        # Resolve both the slot and its target from the same local clock read.
        today_str = now.date().isoformat()
        logger.info(
            "Daily discovery target_date=%s slot=%s local_now=%s timezone=%s",
            today_str, run_slot, now.isoformat(), Config.TIMEZONE,
        )

        discovery_sports = sofascore_sport_slugs()
        initialized = DailyDiscoveryRepository.initialize_sports_for_slot(
            today_str,
            run_slot,
            discovery_sports,
        )
        if not initialized:
            logger.error("Daily discovery could not initialize date=%s slot=%s", today_str, run_slot)
            return

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

        stats = run_daily_discovery(sports=pending_sports, date_str=today_str, run_slot=run_slot)
        if stats:
            logger.info("Daily discovery slot %s completed successfully: %s", run_slot, stats)
        else:
            logger.warning("Daily discovery slot %s completed with no results", run_slot)
    except Exception as exc:
        logger.error("Error in Job E (Daily Discovery): %s", exc)


def run_daily_discovery_retry_job() -> None:
    logger.info("Starting Job E_Retry: Delegating to slot-aware Daily Discovery heartbeat")
    run_daily_discovery_job()
