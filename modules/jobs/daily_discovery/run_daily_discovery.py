"""Daily discovery job wrappers."""

from __future__ import annotations

import logging
from datetime import datetime

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from today_sport_extractor import run_daily_discovery

logger = logging.getLogger(__name__)


def run_daily_discovery_job() -> None:
    logger.info("Starting Job E: Daily discovery of today's scheduled events")

    try:
        from infrastructure.settings import Config

        days_to_keep = getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)
        DailyDiscoveryRepository.cleanup_old_logs(days_to_keep)
    except Exception as exc:
        logger.warning(f"⚠️ Failed to cleanup DailyDiscovery logs: {exc}")

    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        sports = ["basketball", "tennis", "baseball", "ice-hockey", "american-football", "football", "handball"]
        DailyDiscoveryRepository.initialize_sports_for_date(today_str, sports)

        pending_sports = DailyDiscoveryRepository.get_pending_sports(today_str)
        if not pending_sports:
            logger.info("✅ All sports already completed for daily discovery.")
            return

        stats = run_daily_discovery(sports=pending_sports)
        if stats:
            logger.info(f"✅ Daily discovery completed successfully: {stats}")
        else:
            logger.warning("Daily discovery completed with no results")
    except Exception as exc:
        logger.error(f"Error in Job E (Daily Discovery): {exc}")


def run_daily_discovery_retry_job() -> None:
    logger.info("Starting Job E_Retry: Checking for failed daily discovery sports")
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        failed_sports = DailyDiscoveryRepository.get_pending_sports(today)
        if not failed_sports:
            logger.info("✅ All sports completed for daily discovery. No retry needed.")
            return

        logger.info(f"🔄 Retrying daily discovery for sports: {failed_sports}")
        stats = run_daily_discovery(sports=failed_sports)
        if stats:
            logger.info(f"✅ Daily discovery retry completed: {stats}")
        else:
            logger.warning("Daily discovery retry completed with no results")
    except Exception as exc:
        logger.error(f"Error in Job E_Retry: {exc}")
