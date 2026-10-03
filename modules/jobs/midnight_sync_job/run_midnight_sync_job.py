"""Midnight sync job."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.views.view_manager import refresh_materialized_views
from modules.jobs.results_collection_job import run_results_collection_previous_day
from modules.prediction import prediction_logger

logger = logging.getLogger(__name__)


def run_midnight_sync_job() -> None:
    logger.info("Starting Midnight Sync")
    try:
        logger.info("Midnight Sync: starting previous-day results collection")
        result_stats = run_results_collection_previous_day()
        if result_stats['failed']:
            logger.warning("Midnight Sync: results remain pending stats=%s", result_stats)

        logger.info("Midnight Sync: updating prediction logs with actual results")
        stats = prediction_logger.update_predictions_with_results()
        if "error" in stats:
            logger.error("Midnight Sync: prediction log update failed: %s", stats["error"])
        else:
            logger.info(
                "Midnight Sync: prediction logs updated: %s completed, %s cancelled",
                stats["updated"],
                stats["cancelled"],
            )

        logger.info("Midnight Sync: refreshing reporting materialized views")
        refresh_materialized_views(db_manager.engine)
        logger.info("Midnight Sync: reporting materialized views refreshed")
    except Exception as exc:
        logger.exception("Midnight Sync failed: %s", exc)
        raise
