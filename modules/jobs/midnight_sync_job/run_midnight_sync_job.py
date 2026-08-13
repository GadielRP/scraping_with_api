"""Midnight sync job."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import refresh_materialized_views
from modules.jobs.results_collection_job import run_results_collection_previous_day
from modules.prediction import prediction_logger

logger = logging.getLogger(__name__)


def run_midnight_sync_job() -> None:
    logger.info("Starting Midnight Sync")
    try:
        logger.info("Midnight Sync: starting previous-day results collection")
        run_results_collection_previous_day()

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

        logger.info("Midnight Sync: refreshing alert materialized view")
        refresh_materialized_views(db_manager.engine)
        logger.info("Midnight Sync: alert materialized view refreshed")
    except Exception as exc:
        logger.exception("Midnight Sync failed: %s", exc)
