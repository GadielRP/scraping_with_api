"""OddsPortal cache cleanup job."""

from __future__ import annotations

import logging

from infrastructure.persistence.repositories import OddsPortalCacheRepository

logger = logging.getLogger(__name__)


def run_clean_league_cache_job() -> None:
    logger.info("Starting Job F: Clean up OddsPortal league cache")
    try:
        OddsPortalCacheRepository.cleanup_old_caches(retention_days=3)
    except Exception as exc:
        logger.error(f"Error in Job F (Clean up OddsPortal league cache): {exc}")
