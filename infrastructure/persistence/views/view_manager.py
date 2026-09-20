"""Refresh operations for Alembic-managed materialized views."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def refresh_materialized_views(engine: Engine) -> None:
    """Refresh both views through the database's restricted execution grant."""
    with engine.begin() as connection:
        connection.execute(text("SELECT public.refresh_reporting_views()"))
