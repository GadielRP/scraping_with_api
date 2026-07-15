"""Daily discovery job wrappers."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from shared.timezone_utils import get_local_now

from .constants import DEFAULT_DAILY_DISCOVERY_SPORTS
from .extractor import DailyDiscoveryExtractor

logger = logging.getLogger(__name__)


def resolve_daily_discovery_slot(now=None) -> str | None:
    from infrastructure.settings import Config

    if now is None:
        now = get_local_now()

    current_hour = now.hour
    am_hour = Config.DAILY_DISCOVERY_AM_OPEN_HOUR
    pm_hour = Config.DAILY_DISCOVERY_PM_OPEN_HOUR

    # If am_hour is in the evening (e.g. 17) and pm_hour is in the morning (e.g. 8)
    if am_hour > pm_hour:
        if pm_hour <= current_hour < am_hour:
            return "PM"
        return "AM"
    else:
        # Standard case (am_hour < pm_hour, e.g. AM=5, PM=16)
        if current_hour >= pm_hour:
            return "PM"
        if current_hour >= am_hour:
            return "AM"
        return None


def run_daily_discovery(sports=None, date_str=None, run_slot=None):
    if date_str is None:
        date_str = get_local_now().strftime("%Y-%m-%d")
    if sports is None:
        sports = DEFAULT_DAILY_DISCOVERY_SPORTS
    return DailyDiscoveryExtractor().discover_events_for_date(date_str, sports=sports, run_slot=run_slot)


def run_daily_discovery_job() -> None:
    logger.info("Starting Job E: Daily discovery heartbeat")

    try:
        from infrastructure.settings import Config

        days_to_keep = getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)
        DailyDiscoveryRepository.cleanup_old_logs(days_to_keep)
    except Exception as exc:
        logger.warning("Failed to cleanup DailyDiscovery logs: %s", exc)

    try:
        now = get_local_now()
        run_slot = resolve_daily_discovery_slot(now)

        if not run_slot:
            logger.info(
                "No Daily Discovery slot is open yet. Skipping."
            )
            return

        # Calculate target date:
        # AM slot (evening MX / night UTC) targets tomorrow's UTC date.
        # PM slot (morning MX / afternoon UTC) targets today's UTC date.
        utc_now = datetime.now(timezone.utc)
        if run_slot == "AM":
            target_date_obj = utc_now + timedelta(days=1)
        else:
            target_date_obj = utc_now
        today_str = target_date_obj.strftime("%Y-%m-%d")

        DailyDiscoveryRepository.initialize_sports_for_slot(
            today_str,
            run_slot,
            DEFAULT_DAILY_DISCOVERY_SPORTS,
        )

        pending_sports = DailyDiscoveryRepository.get_pending_sports(today_str, run_slot)
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
