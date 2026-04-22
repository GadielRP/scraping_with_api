"""Daily discovery job wrappers."""

from __future__ import annotations

import logging
from datetime import datetime

from infrastructure.persistence.repositories import DailyDiscoveryRepository

from .constants import DEFAULT_DAILY_DISCOVERY_SPORTS
from .extractor import DailyDiscoveryExtractor

logger = logging.getLogger(__name__)


def run_daily_discovery(sports=None):
    today = datetime.now().strftime("%Y-%m-%d")
    if sports is None:
        sports = DEFAULT_DAILY_DISCOVERY_SPORTS
    return DailyDiscoveryExtractor().discover_events_for_date(today, sports=sports)


def run_daily_discovery_job() -> None:
    logger.info("Starting Job E: Daily discovery of today's scheduled events")

    try:
        from infrastructure.settings import Config

        days_to_keep = getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)
        DailyDiscoveryRepository.cleanup_old_logs(days_to_keep)
    except Exception as exc:
        logger.warning("Failed to cleanup DailyDiscovery logs: %s", exc)

    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        DailyDiscoveryRepository.initialize_sports_for_date(today_str, DEFAULT_DAILY_DISCOVERY_SPORTS)

        pending_sports = DailyDiscoveryRepository.get_pending_sports(today_str)
        if not pending_sports:
            logger.info("All sports already completed for daily discovery.")
            return

        stats = run_daily_discovery(sports=pending_sports)
        if stats:
            logger.info("Daily discovery completed successfully: %s", stats)
        else:
            logger.warning("Daily discovery completed with no results")
    except Exception as exc:
        logger.error("Error in Job E (Daily Discovery): %s", exc)


def run_daily_discovery_retry_job() -> None:
    logger.info("Starting Job E_Retry: Checking for failed daily discovery sports")
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        failed_sports = DailyDiscoveryRepository.get_pending_sports(today)
        if not failed_sports:
            logger.info("All sports completed for daily discovery. No retry needed.")
            return

        logger.info("Retrying daily discovery for sports: %s", failed_sports)
        stats = run_daily_discovery(sports=failed_sports)
        if stats:
            logger.info("Daily discovery retry completed: %s", stats)
        else:
            logger.warning("Daily discovery retry completed with no results")
    except Exception as exc:
        logger.error("Error in Job E_Retry: %s", exc)
