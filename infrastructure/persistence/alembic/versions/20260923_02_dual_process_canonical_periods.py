"""Update v_dual_process_event_odds view to include Full Time Including Overtime canonical period.

Revision ID: 20260923_02
Revises: 20260923_01
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.views.dual_process_views import (
    build_dual_process_event_odds_view_sql,
)
from infrastructure.settings import Config

revision: str = "20260923_02"
down_revision: Union[str, None] = "20260923_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

CANONICAL_MARKETS = ["Home/Away", "1X2"]
CANONICAL_PERIODS = ["Full Time", "Full Time Including Overtime"]


def upgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name != "postgresql":
        return

    configured_markets = list(Config.MARKETS_DUAL_PROCESS or [])
    configured_periods = list(Config.PERIODS_DUAL_PROCESS or [])

    # Ensure canonical markets and periods are always present
    markets = list(dict.fromkeys(configured_markets + CANONICAL_MARKETS))
    periods = list(dict.fromkeys(configured_periods + CANONICAL_PERIODS))

    connection.exec_driver_sql(
        build_dual_process_event_odds_view_sql(
            markets=markets,
            periods=periods,
        )
    )

    inspector = sa.inspect(connection)
    if "mv_alert_events" in inspector.get_materialized_view_names():
        connection.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_alert_events")


def downgrade() -> None:
    connection = op.get_bind()
    if connection.dialect.name != "postgresql":
        return

    # Revert to legacy single-period 'Full Time'
    connection.exec_driver_sql(
        build_dual_process_event_odds_view_sql(
            markets=["1X2", "Home/Away"],
            periods=["Full Time"],
        )
    )

    inspector = sa.inspect(connection)
    if "mv_alert_events" in inspector.get_materialized_view_names():
        connection.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_alert_events")
