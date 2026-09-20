"""Install reporting views and the alert materialized view under Alembic.

Revision ID: 20260919_02
Revises: 20260919_01
"""

from __future__ import annotations

import os

from alembic import op

from infrastructure.persistence.views.basketball_views import BASKETBALL_RESULTS_VIEW_SQL
from infrastructure.persistence.views.dual_process_views import (
    DUAL_PROCESS_MARKET_INDEXES_SQL,
    EVENT_ALL_ODDS_VIEW_SQL,
    EVENT_ODDS_HISTORY_INDEXES_SQL,
    MV_ALERT_EVENTS_INDEXES_SQL,
    MV_ALERT_EVENTS_SQL,
    build_dual_process_event_odds_view_sql,
)
from infrastructure.persistence.views.season_views import SEASON_EVENTS_WITH_RESULTS_VIEW_SQL
from infrastructure.settings import Config

revision = "20260919_02"
down_revision = "20260919_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()

    # Rebuild the dependent objects together so installations that previously
    # created them at application startup converge on the same Alembic state.
    connection.exec_driver_sql("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events")
    for name in (
        "event_all_odds",
        "v_pre_start_odds_trajectory",
        "v_dual_process_event_odds",
        "basketball_results",
        "season_events_with_results",
    ):
        connection.exec_driver_sql(f"DROP VIEW IF EXISTS {name}")

    for statement in (*DUAL_PROCESS_MARKET_INDEXES_SQL, *EVENT_ODDS_HISTORY_INDEXES_SQL):
        connection.exec_driver_sql(statement)
    connection.exec_driver_sql(
        build_dual_process_event_odds_view_sql(
            Config.MARKETS_DUAL_PROCESS,
            Config.PERIODS_DUAL_PROCESS,
        )
    )
    connection.exec_driver_sql(EVENT_ALL_ODDS_VIEW_SQL)
    connection.exec_driver_sql(BASKETBALL_RESULTS_VIEW_SQL)
    connection.exec_driver_sql(SEASON_EVENTS_WITH_RESULTS_VIEW_SQL)
    connection.exec_driver_sql(MV_ALERT_EVENTS_SQL)
    for statement in MV_ALERT_EVENTS_INDEXES_SQL:
        connection.exec_driver_sql(statement)
    connection.exec_driver_sql(
        """
        CREATE OR REPLACE FUNCTION public.refresh_reporting_views()
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW public.mv_alert_events;
            REFRESH MATERIALIZED VIEW public.mv_p5_price_memory;
        END;
        $$
        """
    )
    connection.exec_driver_sql(
        "REVOKE ALL ON FUNCTION public.refresh_reporting_views() FROM PUBLIC"
    )

    app_role = os.environ.get("APP_DB_ROLE", "sofascore_app")
    role = connection.dialect.identifier_preparer.quote_identifier(app_role)
    connection.exec_driver_sql(f"GRANT CONNECT ON DATABASE {connection.dialect.identifier_preparer.quote_identifier(connection.engine.url.database)} TO {role}")
    connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {role}")
    connection.exec_driver_sql(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {role}"
    )
    connection.exec_driver_sql(
        f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {role}"
    )
    connection.exec_driver_sql(
        f"REVOKE INSERT, UPDATE, DELETE ON public.alembic_version FROM {role}"
    )
    connection.exec_driver_sql(
        f"GRANT EXECUTE ON FUNCTION public.refresh_reporting_views() TO {role}"
    )
    connection.exec_driver_sql(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role}"
    )
    connection.exec_driver_sql(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT USAGE, SELECT ON SEQUENCES TO {role}"
    )


def downgrade() -> None:
    connection = op.get_bind()
    connection.exec_driver_sql(
        "DROP FUNCTION IF EXISTS public.refresh_reporting_views()"
    )
    connection.exec_driver_sql("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events")
    for name in (
        "event_all_odds",
        "v_pre_start_odds_trajectory",
        "v_dual_process_event_odds",
        "basketball_results",
        "season_events_with_results",
    ):
        connection.exec_driver_sql(f"DROP VIEW IF EXISTS {name}")
