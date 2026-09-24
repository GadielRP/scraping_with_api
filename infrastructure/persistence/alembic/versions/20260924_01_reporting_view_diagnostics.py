"""Add per-view refresh and row-count diagnostics for debug runs.

Revision ID: 20260924_01
Revises: 20260923_02
"""

from __future__ import annotations

import os
from typing import Sequence, Union

from alembic import op

revision: str = "20260924_01"
down_revision: Union[str, None] = "20260923_02"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


DIAGNOSTICS_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION public.refresh_reporting_views_with_diagnostics()
RETURNS TABLE (
    object_name text,
    object_kind text,
    refresh_ms double precision,
    evaluation_ms double precision,
    rows_total bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_relation record;
    v_started_at timestamptz;
BEGIN
    -- Keep the same reporting materialized views refreshed by the legacy
    -- function, measuring each independently while retaining its definer grant.
    FOR v_relation IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'm'
          AND c.relname IN ('mv_alert_events', 'mv_p5_price_memory')
        ORDER BY CASE c.relname
            WHEN 'mv_alert_events' THEN 1
            WHEN 'mv_p5_price_memory' THEN 2
            ELSE 3
        END
    LOOP
        v_started_at := clock_timestamp();
        EXECUTE 'REFRESH MATERIALIZED VIEW public.' || quote_ident(v_relation.relname);
        refresh_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000.0;

        v_started_at := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM public.' || quote_ident(v_relation.relname)
            INTO rows_total;
        evaluation_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000.0;

        object_name := v_relation.relname;
        object_kind := 'materialized_view';
        RETURN NEXT;
    END LOOP;

    -- SQL views are not stored and cannot be refreshed. A count query forces
    -- their evaluation and records the resulting row total and elapsed time.
    FOR v_relation IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'v'
        ORDER BY c.relname
    LOOP
        v_started_at := clock_timestamp();
        EXECUTE 'SELECT count(*) FROM public.' || quote_ident(v_relation.relname)
            INTO rows_total;
        evaluation_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000.0;

        object_name := v_relation.relname;
        object_kind := 'view';
        refresh_ms := NULL;
        RETURN NEXT;
    END LOOP;
END;
$$
"""


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    bind.exec_driver_sql(DIAGNOSTICS_FUNCTION_SQL)
    bind.exec_driver_sql(
        "REVOKE ALL ON FUNCTION public.refresh_reporting_views_with_diagnostics() FROM PUBLIC"
    )
    app_role = os.environ.get("APP_DB_ROLE", "sofascore_app")
    role = bind.dialect.identifier_preparer.quote_identifier(app_role)
    bind.exec_driver_sql(
        f"GRANT EXECUTE ON FUNCTION public.refresh_reporting_views_with_diagnostics() TO {role}"
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    bind.exec_driver_sql(
        "REVOKE ALL ON FUNCTION public.refresh_reporting_views_with_diagnostics() FROM PUBLIC"
    )
    bind.exec_driver_sql(
        "DROP FUNCTION IF EXISTS public.refresh_reporting_views_with_diagnostics()"
    )
