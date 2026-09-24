"""Refresh operations for Alembic-managed materialized views."""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

from infrastructure.settings import Config

logger = logging.getLogger(__name__)


def refresh_materialized_views(engine: Engine) -> None:
    """Refresh reporting materialized views, with optional per-view diagnostics."""
    with engine.begin() as connection:
        if Config.global_debug_mode:
            rows = connection.execute(
                text("SELECT * FROM public.refresh_reporting_views_with_diagnostics()")
            ).mappings()
            for row in rows:
                if row["object_kind"] == "materialized_view":
                    logger.info(
                        "View diagnostics: object=%s kind=%s refresh_ms=%.2f "
                        "evaluation_ms=%.2f rows=%d",
                        row["object_name"],
                        row["object_kind"],
                        row["refresh_ms"],
                        row["evaluation_ms"],
                        row["rows_total"],
                    )
                else:
                    # Ordinary SQL views are evaluated when queried; they have no
                    # stored data to refresh. Counting rows measures that evaluation.
                    logger.info(
                        "View diagnostics: object=%s kind=%s evaluation_ms=%.2f rows=%d",
                        row["object_name"],
                        row["object_kind"],
                        row["evaluation_ms"],
                        row["rows_total"],
                    )
        else:
            connection.execute(text("SELECT public.refresh_reporting_views()"))
