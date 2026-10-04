"""Durable reporting generations and per-view privileged concurrent refresh."""

import os
from alembic import op
import sqlalchemy as sa

revision = "20261003_01"
down_revision = "20261002_01"
branch_labels = depends_on = None


def upgrade():
    op.create_table(
        "reporting_refresh_state",
        sa.Column("view_name", sa.String(64), primary_key=True),
        sa.Column("requested_generation", sa.BigInteger(), nullable=False),
        sa.Column("completed_generation", sa.BigInteger(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(1024)),
    )
    op.execute(
        "INSERT INTO public.reporting_refresh_state VALUES ('mv_alert_events',1,0,0,NULL,NULL), ('mv_p5_price_memory',1,0,0,NULL,NULL)"
    )
    op.execute("""CREATE FUNCTION public.refresh_reporting_view(p_name text) RETURNS void
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
        BEGIN
            IF p_name NOT IN ('mv_alert_events', 'mv_p5_price_memory') OR p_name IS NULL THEN
                RAISE EXCEPTION 'Unknown reporting view';
            END IF;
            EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY public.' || quote_ident(p_name);
        END; $$""")
    op.execute("REVOKE ALL ON FUNCTION public.refresh_reporting_view(text) FROM PUBLIC")
    role = op.get_bind().dialect.identifier_preparer.quote_identifier(
        os.environ.get("APP_DB_ROLE", "sofascore_app")
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION public.refresh_reporting_view(text) TO {role}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON public.reporting_refresh_state TO {role}")
    op.execute("DROP FUNCTION IF EXISTS public.refresh_reporting_views_with_diagnostics()")
    op.execute("DROP FUNCTION IF EXISTS public.refresh_reporting_views()")


def downgrade():
    # Restore the previous callable surface without replaying migrations that
    # rebuild materialized views or modifying their data.
    from importlib import import_module

    op.execute("""CREATE FUNCTION public.refresh_reporting_views() RETURNS void
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW public.mv_alert_events;
            REFRESH MATERIALIZED VIEW public.mv_p5_price_memory;
        END; $$""")
    diagnostics = import_module(
        "infrastructure.persistence.alembic.versions.20260924_01_reporting_view_diagnostics"
    )
    op.execute(diagnostics.DIAGNOSTICS_FUNCTION_SQL)
    role = op.get_bind().dialect.identifier_preparer.quote_identifier(
        os.environ.get("APP_DB_ROLE", "sofascore_app")
    )
    for signature in ("refresh_reporting_views()", "refresh_reporting_views_with_diagnostics()"):
        op.execute(f"REVOKE ALL ON FUNCTION public.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION public.{signature} TO {role}")
    op.execute("DROP FUNCTION public.refresh_reporting_view(text)")
    op.drop_table("reporting_refresh_state")
