"""Persistence helpers for durable Oddspapi fixture-discovery executions."""

from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import OddspapiFixtureDiscoveryRun
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class OddspapiFixtureDiscoveryRunRepository:
    """Claim and finish durable executions per UTC target day and sport scope."""

    DEFAULT_SPORT_SCOPE = 'all'

    @staticmethod
    def normalize_sport_scope(sports: dict | None) -> str:
        if not sports:
            return OddspapiFixtureDiscoveryRunRepository.DEFAULT_SPORT_SCOPE
        normalized = sorted(
            {
                str(sport).strip().casefold()
                for sport in sports
                if str(sport).strip()
            }
        )
        return ','.join(normalized) or OddspapiFixtureDiscoveryRunRepository.DEFAULT_SPORT_SCOPE

    @staticmethod
    def ensure_run_scope_schema() -> None:
        """Migrate durable runs to date+sport scope and non-null slot metadata."""
        try:
            inspector = inspect(db_manager.engine)
            if 'oddspapi_fixture_discovery_runs' not in set(inspector.get_table_names()):
                return

            dialect_name = db_manager.engine.dialect.name
            OddspapiFixtureDiscoveryRunRepository._ensure_sport_scope_column(inspector)
            OddspapiFixtureDiscoveryRunRepository._backfill_run_metadata(dialect_name)
            inspector = inspect(db_manager.engine)
            OddspapiFixtureDiscoveryRunRepository._ensure_run_scope_uniqueness(
                inspector,
                dialect_name,
            )
            OddspapiFixtureDiscoveryRunRepository._ensure_run_scope_index()
            logger.info("Oddspapi fixture-discovery run scope schema is ready")
        except Exception:
            logger.exception("Oddspapi fixture-discovery run scope migration failed")

    @staticmethod
    def _ensure_sport_scope_column(inspector) -> None:
        columns = {
            column['name']
            for column in inspector.get_columns('oddspapi_fixture_discovery_runs')
        }
        if 'sport_scope' in columns:
            return
        with db_manager.get_session() as session:
            session.execute(text(
                "ALTER TABLE oddspapi_fixture_discovery_runs "
                "ADD COLUMN sport_scope VARCHAR(255)"
            ))
            session.commit()

    @staticmethod
    def _backfill_run_metadata(dialect_name: str) -> None:
        with db_manager.get_session() as session:
            if dialect_name == 'postgresql':
                session.execute(text(
                    "UPDATE oddspapi_fixture_discovery_runs SET "
                    "sport_scope = COALESCE("
                    "NULLIF(BTRIM(sport_scope), ''), "
                    "NULLIF((SELECT STRING_AGG(LOWER(item->>'sport_slug'), ',' "
                    "ORDER BY LOWER(item->>'sport_slug')) "
                    "FROM JSONB_ARRAY_ELEMENTS("
                    "COALESCE(summary->'sports', '[]'::jsonb)) AS item "
                    "WHERE NULLIF(BTRIM(item->>'sport_slug'), '') IS NOT NULL), ''), "
                    "'all'), "
                    "scheduled_local_date = COALESCE("
                    "scheduled_local_date, TO_CHAR(started_at, 'YYYY-MM-DD')), "
                    "scheduled_time = COALESCE("
                    "scheduled_time, TO_CHAR(started_at, 'HH24:MI'))"
                ))
                session.execute(text(
                    "ALTER TABLE oddspapi_fixture_discovery_runs "
                    "ALTER COLUMN sport_scope SET DEFAULT 'all', "
                    "ALTER COLUMN sport_scope SET NOT NULL, "
                    "ALTER COLUMN scheduled_local_date SET NOT NULL, "
                    "ALTER COLUMN scheduled_time SET NOT NULL"
                ))
            else:
                session.execute(text(
                    "UPDATE oddspapi_fixture_discovery_runs SET "
                    "sport_scope = COALESCE(NULLIF(TRIM(sport_scope), ''), 'all'), "
                    "scheduled_local_date = COALESCE("
                    "scheduled_local_date, STRFTIME('%Y-%m-%d', started_at)), "
                    "scheduled_time = COALESCE("
                    "scheduled_time, STRFTIME('%H:%M', started_at))"
                ))
            session.commit()

    @staticmethod
    def _ensure_run_scope_uniqueness(inspector, dialect_name: str) -> None:
        unique_constraints = {
            tuple(constraint.get('column_names') or ()): constraint.get('name')
            for constraint in inspector.get_unique_constraints(
                'oddspapi_fixture_discovery_runs'
            )
        }
        expected_columns = ('target_date', 'sport_scope')
        if dialect_name == 'postgresql':
            with db_manager.get_session() as session:
                if ('target_date',) in unique_constraints:
                    session.execute(text(
                        "ALTER TABLE oddspapi_fixture_discovery_runs "
                        "DROP CONSTRAINT IF EXISTS "
                        "unique_oddspapi_fixture_discovery_target_date"
                    ))
                if expected_columns not in unique_constraints:
                    session.execute(text(
                        "ALTER TABLE oddspapi_fixture_discovery_runs "
                        "ADD CONSTRAINT unique_oddspapi_fixture_discovery_target_scope "
                        "UNIQUE (target_date, sport_scope)"
                    ))
                session.commit()
            return

        with db_manager.get_session() as session:
            session.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "unique_oddspapi_fixture_discovery_target_scope "
                "ON oddspapi_fixture_discovery_runs (target_date, sport_scope)"
            ))
            session.commit()

    @staticmethod
    def _ensure_run_scope_index() -> None:
        with db_manager.get_session() as session:
            session.execute(text(
                "CREATE INDEX IF NOT EXISTS "
                "idx_oddspapi_fixture_discovery_runs_status_target_scope "
                "ON oddspapi_fixture_discovery_runs "
                "(status, target_date, sport_scope)"
            ))
            session.commit()

    @staticmethod
    def has_success(target_date: str, sport_scope: str = DEFAULT_SPORT_SCOPE) -> bool:
        with db_manager.get_session() as session:
            return (
                session.query(OddspapiFixtureDiscoveryRun.id)
                .filter(
                    OddspapiFixtureDiscoveryRun.target_date == target_date,
                    OddspapiFixtureDiscoveryRun.sport_scope == sport_scope,
                    OddspapiFixtureDiscoveryRun.status == 'success',
                )
                .first()
                is not None
            )

    @staticmethod
    def begin(
        target_date: str,
        *,
        trigger: str,
        sport_scope: str = DEFAULT_SPORT_SCOPE,
        create_mappings: bool = True,
        scheduled_local_date: str | None = None,
        scheduled_time: str | None = None,
    ) -> bool:
        """Atomically claim a target date and sport scope."""
        now = get_local_now()
        scheduled_local_date = scheduled_local_date or now.strftime('%Y-%m-%d')
        scheduled_time = scheduled_time or now.strftime('%H:%M')
        with db_manager.get_session() as session:
            run = None
            if session.bind.dialect.name == 'postgresql':
                inserted_id = session.execute(
                    postgresql_insert(OddspapiFixtureDiscoveryRun)
                    .values(
                        target_date=target_date,
                        sport_scope=sport_scope,
                        scheduled_local_date=scheduled_local_date,
                        scheduled_time=scheduled_time,
                        trigger=trigger,
                        status='running',
                        process_id=os.getpid(),
                        started_at=now,
                        heartbeat_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=['target_date', 'sport_scope'])
                    .returning(OddspapiFixtureDiscoveryRun.id)
                ).scalar_one_or_none()
                if inserted_id is not None:
                    return True
                run = (
                    session.query(OddspapiFixtureDiscoveryRun)
                    .filter(
                        OddspapiFixtureDiscoveryRun.target_date == target_date,
                        OddspapiFixtureDiscoveryRun.sport_scope == sport_scope,
                    )
                    .with_for_update()
                    .one()
                )
            else:
                run = (
                    session.query(OddspapiFixtureDiscoveryRun)
                    .filter(
                        OddspapiFixtureDiscoveryRun.target_date == target_date,
                        OddspapiFixtureDiscoveryRun.sport_scope == sport_scope,
                    )
                    .first()
                )

            successful_dry_run = bool(
                run is not None
                and run.status == 'success'
                and isinstance(run.summary, dict)
                and run.summary.get('create_mappings') is False
            )
            if run is not None and run.status == 'running':
                return False
            if (
                run is not None
                and run.status == 'success'
                and not (create_mappings and successful_dry_run)
            ):
                return False

            if run is None:
                run = OddspapiFixtureDiscoveryRun(
                    target_date=target_date,
                    sport_scope=sport_scope,
                )
                session.add(run)

            run.sport_scope = sport_scope
            run.scheduled_local_date = scheduled_local_date
            run.scheduled_time = scheduled_time
            run.trigger = trigger
            run.status = 'running'
            run.process_id = os.getpid()
            run.started_at = now
            run.heartbeat_at = now
            run.finished_at = None
            run.summary = None
            run.error = None
            return True

    @staticmethod
    def finish_success(
        target_date: str,
        summary: dict[str, Any],
        *,
        sport_scope: str = DEFAULT_SPORT_SCOPE,
    ) -> None:
        now = get_local_now()
        with db_manager.get_session() as session:
            run = (
                session.query(OddspapiFixtureDiscoveryRun)
                .filter(OddspapiFixtureDiscoveryRun.target_date == target_date)
                .filter(OddspapiFixtureDiscoveryRun.sport_scope == sport_scope)
                .one()
            )
            run.status = 'success'
            run.heartbeat_at = now
            run.finished_at = now
            run.summary = summary
            run.error = None

    @staticmethod
    def finish_failed(
        target_date: str,
        error: str,
        *,
        sport_scope: str = DEFAULT_SPORT_SCOPE,
    ) -> None:
        now = get_local_now()
        with db_manager.get_session() as session:
            run = (
                session.query(OddspapiFixtureDiscoveryRun)
                .filter(OddspapiFixtureDiscoveryRun.target_date == target_date)
                .filter(OddspapiFixtureDiscoveryRun.sport_scope == sport_scope)
                .one()
            )
            run.status = 'failed'
            run.heartbeat_at = now
            run.finished_at = now
            run.error = error[:4000]

    @staticmethod
    def mark_running_as_interrupted() -> int:
        """Close rows left running by a process that did not shut down cleanly."""
        now = get_local_now()
        with db_manager.get_session() as session:
            runs = (
                session.query(OddspapiFixtureDiscoveryRun)
                .filter(OddspapiFixtureDiscoveryRun.status == 'running')
                .all()
            )
            for run in runs:
                run.status = 'interrupted'
                run.finished_at = now
                run.heartbeat_at = now
                run.error = (
                    f'Process {run.process_id or "unknown"} ended before the run '
                    'recorded completion'
                )
            return len(runs)


__all__ = ['OddspapiFixtureDiscoveryRunRepository']
