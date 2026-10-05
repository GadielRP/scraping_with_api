"""Refresh pending materialized views independently of discovery's heartbeat."""

from datetime import timedelta
import logging
from sqlalchemy import text
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.reporting_refresh_repository import (
    ReportingRefreshRepository,
    invalidate_reporting,
)
from infrastructure.persistence.views.view_manager import refresh_reporting_view
from infrastructure.settings.job_execution import JobExecutionSettings
from shared.execution_context import WorkDeferred, check_execution_budget
from shared.temporal import utc_now
from infrastructure.runtime.resource_budget import check_maintenance_capacity
from infrastructure.runtime.view_refresh_exclusion import view_refresh_exclusion

logger = logging.getLogger(__name__)


def run_view_refresh(*, force=False, request=False, critical_pending=None):
    limits = JobExecutionSettings()
    if request:
        with db_manager.get_session() as session:
            invalidate_reporting(session)
    summary = dict(refreshed=0, failed=0, busy=0, pending=0)
    for name, generation, attempts in ReportingRefreshRepository.pending(force=force):
        check_execution_budget()
        check_maintenance_capacity()
        try:
            with view_refresh_exclusion.refresh(critical_pending=critical_pending), db_manager.get_session() as session:
                if (
                    session.get_bind().dialect.name == "postgresql"
                    and not session.execute(
                        text("SELECT pg_try_advisory_xact_lock(736104, :key)"),
                        {"key": 1 if name == "mv_alert_events" else 2},
                    ).scalar()
                ):
                    summary["busy"] += 1
                    continue
                generation = ReportingRefreshRepository.due_generation(session, name, force=force)
                if generation is None:
                    continue
                refresh_reporting_view(session.connection(), name, limits)
                ReportingRefreshRepository.complete(
                    session,
                    name,
                    generation,
                    utc_now() + timedelta(seconds=limits.view_refresh_min_interval_seconds),
                )
            summary["refreshed"] += 1
        except WorkDeferred:
            raise
        except Exception as exc:
            delay = min(limits.view_refresh_retry_seconds * 2 ** min(attempts, 8), 3600)
            ReportingRefreshRepository.failed(
                name, attempts, exc, utc_now() + timedelta(seconds=delay)
            )
            logger.exception(
                "View refresh failed view=%s generation=%s retry_s=%s", name, generation, delay
            )
            summary["failed"] += 1
    summary["pending"] = len(ReportingRefreshRepository.pending(force=True))
    logger.info("View refresh summary: %s", summary)
    return summary
