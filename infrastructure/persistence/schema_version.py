"""Read-only verification of the deployed Alembic schema."""

from __future__ import annotations

import logging
from pathlib import Path

from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def verify_schema_at_head(engine: Engine) -> bool:
    """Require the database revisions to match the migration scripts exactly."""
    try:
        ini_path = Path(__file__).resolve().parents[2] / "alembic.ini"
        if not ini_path.is_file():
            raise FileNotFoundError(f"Alembic configuration is missing: {ini_path}")

        scripts = ScriptDirectory.from_config(AlembicConfig(str(ini_path)))
        expected = set(scripts.get_heads())
        if not expected:
            raise RuntimeError("Alembic has no head revision")

        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        if "alembic_version" not in tables:
            logger.error("Schema is not versioned; run `alembic upgrade head`.")
            return False

        with engine.connect() as connection:
            actual = {
                str(row[0])
                for row in connection.execute(text("SELECT version_num FROM alembic_version"))
            }
        if actual != expected:
            logger.error(
                "Schema revision mismatch: expected=%s actual=%s; run `alembic upgrade head`.",
                sorted(expected),
                sorted(actual),
            )
            return False

        required_columns = {
            "canonical_market_types": {"market_type_id", "requires_line_value"},
            "market_source_mappings": {"market_type_id"},
            "markets": {"market_type_id", "line_value"},
        }
        forbidden_columns = {
            "markets": {"market_name", "market_group", "market_period", "choice_group"},
            "market_source_mappings": {
                "canonical_market_name", "canonical_market_group", "canonical_market_period"
            },
            "pillar_mining_units": {
                "market_name", "market_group", "market_period", "choice_group"
            },
        }
        for table, columns in required_columns.items():
            if table not in tables:
                logger.error("Schema table is missing: %s", table)
                return False
            missing = columns - {item["name"] for item in inspector.get_columns(table)}
            if missing:
                logger.error("Schema columns are missing from %s: %s", table, sorted(missing))
                return False
        for table, columns in forbidden_columns.items():
            if table not in tables:
                continue
            remaining = columns & {item["name"] for item in inspector.get_columns(table)}
            if remaining:
                logger.error("Legacy market columns remain in %s: %s", table, sorted(remaining))
                return False
        if engine.dialect.name == "postgresql":
            event_fks = inspector.get_foreign_keys("event_source_mappings")
            if not any(
                fk["constrained_columns"] == ["event_id"]
                and fk["referred_table"] == "events"
                and fk.get("options", {}).get("ondelete", "").upper() == "CASCADE"
                for fk in event_fks
            ):
                logger.error("Event source mappings are missing the cascading event FK")
                return False
            required_views = {
                "v_dual_process_event_odds",
                "event_all_odds",
                "basketball_results",
                "season_events_with_results",
            }
            missing_views = required_views - set(inspector.get_view_names())
            required_materialized = {"mv_alert_events", "mv_p5_price_memory"}
            missing_materialized = required_materialized - set(
                inspector.get_materialized_view_names()
            )
            if missing_views or missing_materialized:
                logger.error(
                    "Reporting views are missing: views=%s materialized=%s",
                    sorted(missing_views),
                    sorted(missing_materialized),
                )
                return False
            with engine.connect() as connection:
                refresh_function = connection.scalar(
                    text("SELECT to_regprocedure('public.refresh_reporting_views()')")
                )
            if refresh_function is None:
                logger.error("Reporting refresh function is missing")
                return False
        return True
    except Exception:
        logger.exception("Schema verification failed")
        return False
