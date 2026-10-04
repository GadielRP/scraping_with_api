"""Resumable result collection: bounded reads, provider work, then committed writes."""

from __future__ import annotations

import logging
from datetime import date
from time import monotonic

from infrastructure.persistence.repositories import ResultRepository
from infrastructure.settings import Config
from shared.shutdown import is_shutdown_requested
from .batch_processor import collect_batch
from .contracts import ResultBatchDeferred, result_selection
from infrastructure.settings.job_execution import JobExecutionSettings
from infrastructure.runtime.resource_budget import check_maintenance_capacity
from shared.execution_context import WorkDeferred

logger = logging.getLogger(__name__)


def run_results_collection(target_date=None, *, job_name=None):
    """Collect one local date, or all sufficiently old incomplete events when omitted."""
    if isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)
    job_name = job_name or f"Results Collection ({target_date or 'all finished'})"
    stats = dict(updated=0, deferred=0, failed=0, deleted=0)
    started = monotonic()
    logger.info(
        "Starting %s target_date=%s batch_size=%s",
        job_name,
        target_date,
        Config.EVENT_WRITE_BATCH_SIZE,
    )
    try:
        for batch in ResultRepository.pending_batches(
            result_selection(target_date),
            JobExecutionSettings().event_read_batch_size,
        ):
            check_maintenance_capacity()
            if is_shutdown_requested():
                raise KeyboardInterrupt()
            batch_stats = collect_batch(batch, job_name)
            for key, count in batch_stats.items():
                stats[key] += count
    except WorkDeferred as exc:
        if isinstance(exc, ResultBatchDeferred):
            for key, count in exc.stats.items():
                stats[key] += count
        logger.warning("%s deferred; committed batches retained stats=%s", job_name, stats)
        raise
    except KeyboardInterrupt:
        logger.info("%s interrupted; committed batches retained stats=%s", job_name, stats)
        raise
    except Exception:
        logger.exception("%s failed; committed batches retained stats=%s", job_name, stats)
        raise
    logger.info("%s completed duration_s=%.3f stats=%s", job_name, monotonic() - started, stats)
    return stats
