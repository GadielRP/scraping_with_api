"""Midnight sync job."""

from __future__ import annotations

import logging
from datetime import timedelta

from modules.jobs.results_collection_job import run_results_collection
from modules.prediction import prediction_logger
from infrastructure.settings import Config
from shared.temporal import now_in_timezone

logger = logging.getLogger(__name__)


def run_midnight_sync_job(target_date=None) -> None:
    logger.info("Starting Midnight Sync")
    target_date = target_date or now_in_timezone(Config.TIMEZONE).date() - timedelta(days=1)
    logger.info("Midnight Sync: starting previous-day results collection")
    result_stats = run_results_collection(target_date, job_name="Results Collection (midnight)")
    if result_stats["failed"]:
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
