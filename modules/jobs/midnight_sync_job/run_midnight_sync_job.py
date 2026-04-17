"""Midnight sync job."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import refresh_materialized_views
from modules.jobs.results_collection_job import run_results_collection_previous_day
from modules.prediction import prediction_logger

logger = logging.getLogger(__name__)


def run_midnight_sync_job() -> None:
    logger.info("Starting Job D: Midnight results collection")
    try:
        logger.info("📊 Collecting results from finished events...")
        run_results_collection_previous_day()

        logger.info("📊 Updating prediction logs with actual results...")
        stats = prediction_logger.update_predictions_with_results()
        if "error" in stats:
            logger.error(f"Error updating prediction logs: {stats['error']}")
        else:
            logger.info(f"📊 Prediction logs updated: {stats['updated']} completed, {stats['cancelled']} cancelled")

        logger.info("🔄 Refreshing alert materialized views...")
        refresh_materialized_views(db_manager.engine)
        logger.info("✅ Alert data refreshed")
    except Exception as exc:
        logger.error(f"Error in Job D: {exc}")
