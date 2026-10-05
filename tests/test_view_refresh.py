"""Durable materialized-view refresh, cooldown and critical-work exclusion."""

from datetime import timedelta
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event
import pytest
from sqlalchemy import text
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.repositories import reporting_refresh_repository as repository
from infrastructure.persistence.repositories.reporting_refresh_repository import (
    ReportingRefreshRepository,
    invalidate_reporting,
)
from infrastructure.persistence.reporting_models import ReportingRefreshState
from shared.temporal import utc_now


@pytest.fixture
def database(tmp_path, monkeypatch):
    db = DatabaseManager(
        os.environ.get("REPORTING_TEST_DATABASE_URL") or f'sqlite:///{tmp_path / "reporting.db"}'
    )
    Base.metadata.create_all(db.engine)
    monkeypatch.setattr(repository, "db_manager", db)
    yield db
    if db.engine.dialect.name == "postgresql":
        with db.engine.begin() as connection:
            connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events"))
            connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_p5_price_memory"))
            connection.execute(text("DROP FUNCTION IF EXISTS public.refresh_reporting_view(text)"))
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()


def test_invalidation_survives_restart_and_new_generation_is_not_lost(database):
    with database.get_session() as session:
        invalidate_reporting(session)
    pending = ReportingRefreshRepository.pending()
    assert len(pending) == 2
    name, generation, _ = pending[0]
    with database.get_session() as session:
        invalidate_reporting(session)
    with database.get_session() as session:
        ReportingRefreshRepository.complete(session, name, generation)
    assert {
        row.view_name: row.requested_generation for row in ReportingRefreshRepository.pending()
    }[name] == 2


def test_rollback_does_not_invalidate_and_backoff_retains_pending(database):
    with pytest.raises(RuntimeError):
        with database.get_session() as session:
            invalidate_reporting(session)
            raise RuntimeError("write failed")
    assert ReportingRefreshRepository.pending() == []
    with database.get_session() as session:
        invalidate_reporting(session)
    for name, _, attempts in ReportingRefreshRepository.pending():
        ReportingRefreshRepository.failed(name, attempts, "busy", utc_now() + timedelta(minutes=1))
    assert ReportingRefreshRepository.pending() == []
    with database.get_session() as session:
        assert session.query(ReportingRefreshState).filter_by(attempts=1).count() == 2


def test_privileged_concurrent_refresh_function_and_allowlist(database, monkeypatch):
    if database.engine.dialect.name != "postgresql":
        pytest.skip("Requires real PostgreSQL concurrent refresh")
    from importlib import import_module
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from infrastructure.persistence.views.view_manager import refresh_reporting_view
    from infrastructure.settings.job_execution import JobExecutionSettings

    migration = import_module(
        "infrastructure.persistence.alembic.versions.20261003_01_reporting_recovery"
    )
    monkeypatch.setenv("APP_DB_ROLE", "execution_reporting_reader")
    with database.engine.begin() as connection:
        connection.execute(
            text(
                "DO $$ BEGIN CREATE ROLE execution_reporting_reader; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
            )
        )
        for name in repository.REPORTING_VIEWS:
            connection.execute(text(f"CREATE MATERIALIZED VIEW {name} AS SELECT 1 AS event_id"))
            connection.execute(text(f"CREATE UNIQUE INDEX {name}_test_unique ON {name}(event_id)"))
        ReportingRefreshState.__table__.drop(connection)
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
        connection.execute(text("SET LOCAL ROLE execution_reporting_reader"))
        for name in repository.REPORTING_VIEWS:
            refresh_reporting_view(connection, name, JobExecutionSettings())
        with pytest.raises(ValueError, match="Unknown reporting"):
            refresh_reporting_view(connection, "events", JobExecutionSettings())
    with database.engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            migration.downgrade()
            assert (
                connection.scalar(
                    text("SELECT to_regprocedure('public.refresh_reporting_views()')")
                )
                is not None
            )
            assert (
                connection.scalar(
                    text("SELECT to_regprocedure('public.refresh_reporting_view(text)')")
                )
                is None
            )
            migration.upgrade()
            assert (
                connection.scalar(
                    text("SELECT to_regprocedure('public.refresh_reporting_views()')")
                )
                is None
            )


def test_refresh_failure_does_not_rollback_successful_other_view(database, monkeypatch):
    from importlib import import_module

    job = import_module("modules.jobs.view_refresh.run_view_refresh")
    monkeypatch.setattr(job, "db_manager", database)

    def refresh(_connection, name, _limits):
        if name == "mv_alert_events":
            raise RuntimeError("first view timed out")

    monkeypatch.setattr(job, "refresh_reporting_view", refresh)
    summary = job.run_view_refresh(request=True)
    assert summary == {"refreshed": 1, "failed": 1, "busy": 0, "pending": 1}
    assert ReportingRefreshRepository.pending() == []
    with database.get_session() as session:
        views = {state.view_name: state for state in session.query(ReportingRefreshState)}
        assert views["mv_alert_events"].completed_generation == 0
        assert views["mv_p5_price_memory"].completed_generation == 1
        assert views["mv_p5_price_memory"].next_attempt_at > utc_now()
        invalidate_reporting(session)
    # Ordinary polling coalesces source writes; an explicit manual refresh can force them.
    assert ReportingRefreshRepository.pending() == []
    assert len(ReportingRefreshRepository.pending(force=True)) == 2


def test_view_refresh_defers_while_pre_start_job_is_active(database, monkeypatch):
    from importlib import import_module
    from shared.execution_context import WorkDeferred

    reporting = import_module("modules.jobs.view_refresh.run_view_refresh")
    pre_start = import_module("modules.jobs.pre_start_check_job.run_pre_start_check_job")
    monkeypatch.setattr(reporting, "db_manager", database)
    monkeypatch.setattr(pre_start, "_tracked_competition_ids", lambda: None)
    started, release = Event(), Event()
    refreshed = []

    def load_events(*_):
        started.set()
        assert release.wait(5)
        raise RuntimeError("stop test pre-start after the guarded load")

    monkeypatch.setattr(pre_start, "_load_upcoming_events", load_events)
    monkeypatch.setattr(reporting, "refresh_reporting_view", lambda *_: refreshed.append(True))
    with database.get_session() as session:
        invalidate_reporting(session)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(pre_start.run_pre_start_check_job, None)
        try:
            assert started.wait(2)
            with pytest.raises(WorkDeferred, match="pre-start work"):
                reporting.run_view_refresh(force=True)
            assert refreshed == []
            assert len(ReportingRefreshRepository.pending()) == 2
            with database.get_session() as session:
                assert all(state.attempts == 0 for state in session.query(ReportingRefreshState))
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="stop test pre-start"):
            future.result(timeout=2)
    assert reporting.run_view_refresh(force=True)["refreshed"] == 2


def test_view_refresh_defers_pending_tick_without_marking_view_failed(database, monkeypatch):
    from importlib import import_module
    from shared.execution_context import WorkDeferred

    reporting = import_module("modules.jobs.view_refresh.run_view_refresh")
    monkeypatch.setattr(reporting, "db_manager", database)
    monkeypatch.setattr(reporting, "refresh_reporting_view", lambda *_: pytest.fail("Refresh overtook pre-start"))
    with database.get_session() as session:
        invalidate_reporting(session)
    with pytest.raises(WorkDeferred):
        reporting.run_view_refresh(critical_pending=lambda: True)
    with database.get_session() as session:
        assert all(row.attempts == row.completed_generation == 0 for row in session.query(ReportingRefreshState))
    assert len(ReportingRefreshRepository.pending()) == 2


def test_automatic_view_refresh_waits_one_hour_after_success(database, monkeypatch):
    from importlib import import_module

    job = import_module("modules.jobs.view_refresh.run_view_refresh")
    monkeypatch.setattr(job, "db_manager", database)
    monkeypatch.setattr(job, "refresh_reporting_view", lambda *_: None)
    now = utc_now()
    monkeypatch.setattr(job, "utc_now", lambda: now)
    assert job.run_view_refresh(request=True)["refreshed"] == 2
    with database.get_session() as session:
        invalidate_reporting(session)
        assert all(
            state.next_attempt_at == now + timedelta(hours=1)
            for state in session.query(ReportingRefreshState)
        )
    monkeypatch.setattr(repository, "utc_now", lambda: now + timedelta(seconds=3599))
    assert ReportingRefreshRepository.pending() == []
    monkeypatch.setattr(repository, "utc_now", lambda: now + timedelta(hours=1))
    assert len(ReportingRefreshRepository.pending()) == 2


def test_pre_start_waits_until_active_refresh_commits(database, monkeypatch):
    from importlib import import_module

    reporting = import_module("modules.jobs.view_refresh.run_view_refresh")
    pre_start = import_module("modules.jobs.pre_start_check_job.run_pre_start_check_job")
    monkeypatch.setattr(reporting, "db_manager", database)
    monkeypatch.setattr(pre_start, "_tracked_competition_ids", lambda: None)
    refreshing, release, pre_start_called, loaded = Event(), Event(), Event(), Event()

    def refresh(*_):
        refreshing.set()
        assert release.wait(5)
        assert not loaded.is_set()

    def load_events(*_):
        loaded.set()
        raise RuntimeError("stop test pre-start after the guarded load")

    def run_pre_start():
        pre_start_called.set()
        pre_start.run_pre_start_check_job(None)

    monkeypatch.setattr(reporting, "refresh_reporting_view", refresh)
    monkeypatch.setattr(pre_start, "_load_upcoming_events", load_events)
    with database.get_session() as session:
        session.add(ReportingRefreshState(view_name="mv_alert_events", requested_generation=1))
    with ThreadPoolExecutor(max_workers=2) as executor:
        refresh_future = executor.submit(reporting.run_view_refresh)
        try:
            assert refreshing.wait(2)
            pre_start_future = executor.submit(run_pre_start)
            assert pre_start_called.wait(2)
            assert not loaded.wait(0.1)
        finally:
            release.set()
        assert refresh_future.result(timeout=2)["refreshed"] == 1
        with pytest.raises(RuntimeError, match="stop test pre-start"):
            pre_start_future.result(timeout=2)
        assert loaded.is_set()
        with database.get_session() as session:
            assert session.get(ReportingRefreshState, "mv_alert_events").completed_generation == 1


def test_midnight_only_collects_results_and_updates_predictions(monkeypatch):
    from datetime import date
    from importlib import import_module

    midnight = import_module("modules.jobs.midnight_sync_job.run_midnight_sync_job")
    reporting = import_module("modules.jobs.view_refresh.run_view_refresh")
    calls = []
    monkeypatch.setattr(reporting, "run_view_refresh", lambda **_: pytest.fail("Unexpected refresh"))
    monkeypatch.setattr(
        midnight, "run_results_collection",
        lambda target, **_: calls.append(("results", target)) or {"failed": 0},
    )
    monkeypatch.setattr(
        midnight.prediction_logger, "update_predictions_with_results",
        lambda: calls.append("predictions") or {"updated": 0, "cancelled": 0},
    )
    target = date(2026, 10, 3)
    midnight.run_midnight_sync_job(target)
    assert calls == [("results", target), "predictions"]
