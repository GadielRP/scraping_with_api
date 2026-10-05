"""Per-view execution with explicit transaction-local resource limits."""

import logging
from time import monotonic
from sqlalchemy import text
from infrastructure.persistence.repositories.reporting_refresh_repository import REPORTING_VIEWS
from shared.runtime_observability import observe_operation

logger = logging.getLogger(__name__)


def refresh_reporting_view(connection, name, limits):
    if name not in REPORTING_VIEWS:
        raise ValueError(f"Unknown reporting view: {name}")
    started = monotonic()
    with observe_operation(f"view_refresh:{name}"):
        connection.execute(text("SELECT set_config('work_mem', '4MB', true)"))
        connection.execute(text("SELECT set_config('max_parallel_workers_per_gather', '0', true)"))
        connection.execute(
            text("SELECT set_config('statement_timeout', :value, true)"),
            {"value": str(limits.view_refresh_timeout_ms)},
        )
        connection.execute(text("SELECT set_config('lock_timeout', '5000', true)"))
        connection.execute(text("SELECT public.refresh_reporting_view(:name)"), {"name": name})
    logger.info("Materialized view refreshed view=%s duration_s=%.3f", name, monotonic() - started)
