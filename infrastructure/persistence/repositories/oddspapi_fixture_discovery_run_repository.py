"""Persistence helpers for durable Oddspapi fixture-discovery executions."""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import OddspapiFixtureDiscoveryRun
from shared.timezone_utils import get_local_now


class OddspapiFixtureDiscoveryRunRepository:
    """Claim and finish at most one successful execution per UTC target day."""

    @staticmethod
    def has_success(target_date: str) -> bool:
        with db_manager.get_session() as session:
            return (
                session.query(OddspapiFixtureDiscoveryRun.id)
                .filter(
                    OddspapiFixtureDiscoveryRun.target_date == target_date,
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
        scheduled_local_date: str | None = None,
        scheduled_time: str | None = None,
    ) -> bool:
        """Atomically claim *target_date* unless it succeeded or is running."""
        now = get_local_now()
        with db_manager.get_session() as session:
            run = None
            if session.bind.dialect.name == 'postgresql':
                inserted_id = session.execute(
                    postgresql_insert(OddspapiFixtureDiscoveryRun)
                    .values(
                        target_date=target_date,
                        scheduled_local_date=scheduled_local_date,
                        scheduled_time=scheduled_time,
                        trigger=trigger,
                        status='running',
                        process_id=os.getpid(),
                        started_at=now,
                        heartbeat_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=['target_date'])
                    .returning(OddspapiFixtureDiscoveryRun.id)
                ).scalar_one_or_none()
                if inserted_id is not None:
                    return True
                run = (
                    session.query(OddspapiFixtureDiscoveryRun)
                    .filter(
                        OddspapiFixtureDiscoveryRun.target_date == target_date
                    )
                    .with_for_update()
                    .one()
                )
            else:
                run = (
                    session.query(OddspapiFixtureDiscoveryRun)
                    .filter(
                        OddspapiFixtureDiscoveryRun.target_date == target_date
                    )
                    .first()
                )

            if run is not None and run.status in {'success', 'running'}:
                return False

            if run is None:
                run = OddspapiFixtureDiscoveryRun(target_date=target_date)
                session.add(run)

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
    def finish_success(target_date: str, summary: dict[str, Any]) -> None:
        now = get_local_now()
        with db_manager.get_session() as session:
            run = (
                session.query(OddspapiFixtureDiscoveryRun)
                .filter(OddspapiFixtureDiscoveryRun.target_date == target_date)
                .one()
            )
            run.status = 'success'
            run.heartbeat_at = now
            run.finished_at = now
            run.summary = summary
            run.error = None

    @staticmethod
    def finish_failed(target_date: str, error: str) -> None:
        now = get_local_now()
        with db_manager.get_session() as session:
            run = (
                session.query(OddspapiFixtureDiscoveryRun)
                .filter(OddspapiFixtureDiscoveryRun.target_date == target_date)
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
