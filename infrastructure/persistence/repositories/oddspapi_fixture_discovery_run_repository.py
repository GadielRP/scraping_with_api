"""Persistence helpers for durable Oddspapi fixture-discovery executions."""

from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import OddspapiFixtureDiscoveryRun
from shared.temporal import utc_now

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
        now = utc_now()
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
        now = utc_now()
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
        now = utc_now()
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
        now = utc_now()
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
