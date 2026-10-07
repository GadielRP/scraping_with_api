from contextlib import contextmanager
from datetime import datetime
from importlib import import_module
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from infrastructure.persistence.models import DailyDiscoveryLog
from infrastructure.persistence.repositories import DailyDiscoveryRepository
from infrastructure.settings import Config

daily_job = import_module("modules.jobs.daily_discovery.run_daily_discovery")
repository = import_module("infrastructure.persistence.repositories.daily_discovery_repository")


def local(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("America/Mexico_City"))


@pytest.fixture
def progress(monkeypatch):
    engine = create_engine("sqlite://")
    DailyDiscoveryLog.__table__.create(engine)

    @contextmanager
    def session_scope():
        with Session(engine) as session:
            yield session
            session.commit()

    monkeypatch.setattr(repository.db_manager, "get_session", session_scope)
    monkeypatch.setattr(Config, "TIMEZONE", "America/Mexico_City")
    monkeypatch.setattr(daily_job, "run_event_discard_cleanup", lambda: 0)
    monkeypatch.setattr(daily_job, "sofascore_discovery_sport_slugs", lambda sports=None: sports if sports is not None else ["football", "tennis"])
    yield session_scope
    engine.dispose()


@pytest.mark.parametrize("when, expected", [
    ("2026-10-05T16:59", [("2026-10-05", "anticipada"), ("2026-10-05", "actualizacion")]),
    ("2026-10-05T17:01", [("2026-10-05", "anticipada"), ("2026-10-05", "actualizacion")]),
    ("2026-10-05T17:02", [("2026-10-05", "anticipada"), ("2026-10-05", "actualizacion"), ("2026-10-06", "anticipada")]),
    ("2026-10-05T23:48", [("2026-10-06", "anticipada")]),
    ("2026-10-06T00:00", [("2026-10-06", "anticipada")]),
    ("2026-10-06T07:59", [("2026-10-06", "anticipada")]),
    ("2026-10-06T08:01", [("2026-10-06", "anticipada")]),
    ("2026-10-06T08:02", [("2026-10-06", "anticipada"), ("2026-10-06", "actualizacion")]),
    ("2026-12-31T23:48", [("2027-01-01", "anticipada")]),
])
def test_due_passes_keep_the_opening_date_and_expire_past_utc_days(monkeypatch, when, expected):
    monkeypatch.setattr(Config, "TIMEZONE", "America/Mexico_City")
    assert [(target, slot) for _, target, slot in daily_job.daily_discovery_passes(local(when))] == expected


def set_now(monkeypatch, when):
    now = local(when)
    monkeypatch.setattr(daily_job, "now_in_timezone", lambda _: now)
    monkeypatch.setattr(repository, "now_in_timezone", lambda _: now)
    monkeypatch.setattr(repository, "utc_now", lambda: now)


def test_failed_sport_retries_overnight_without_repeating_completed_sport(progress, monkeypatch):
    calls = []

    def discover(date, sports, run_slot):
        calls.append((date, run_slot, sports))
        for sport in sports:
            status = "failed" if len(calls) == 1 and sport == "tennis" else "completed"
            DailyDiscoveryRepository.update_sport_status(date, run_slot, sport, status)
        return {"events_inserted": 1}

    monkeypatch.setattr(daily_job, "discover_events_for_date", discover)
    # Late startup reconstructs the missed 17:02 occurrence, with no rows present.
    for when in ("2026-10-05T23:48", "2026-10-06T00:18", "2026-10-06T07:48", "2026-10-06T08:02", "2026-10-06T08:30"):
        set_now(monkeypatch, when)
        daily_job.run_daily_discovery_job()

    assert calls == [
        ("2026-10-06", "anticipada", ["football", "tennis"]),
        ("2026-10-06", "anticipada", ["tennis"]),
        ("2026-10-06", "actualizacion", ["football", "tennis"]),
    ]
    with progress() as session:
        rows = session.query(DailyDiscoveryLog).order_by(DailyDiscoveryLog.id).all()
        assert [(row.run_slot, row.sport, row.attempts) for row in rows] == [
            ("anticipada", "football", 1), ("anticipada", "tennis", 2),
            ("actualizacion", "football", 1), ("actualizacion", "tennis", 1),
        ]
        assert all(row.status == "completed" for row in rows)
    assert DailyDiscoveryRepository.latest_completed_at("2026-10-06", ["football"]) == local("2026-10-06T08:02")


def test_expired_failures_are_not_replayed(progress, monkeypatch):
    DailyDiscoveryRepository.initialize_sports_for_slot("2026-10-05", "actualizacion", ["football"])
    with progress() as session:
        session.query(DailyDiscoveryLog).update({"status": "failed"})
    set_now(monkeypatch, "2026-10-05T18:00")
    calls = []
    monkeypatch.setattr(daily_job, "discover_events_for_date", lambda **kwargs: calls.append(kwargs) or {})
    daily_job.run_daily_discovery_job()
    assert [(call["date"], call["run_slot"]) for call in calls] == [("2026-10-06", "anticipada")]


def test_calendar_has_two_starts_and_thirty_minute_retries():
    import schedule
    from infrastructure.scheduler.schedules import configure_calendar
    from infrastructure.settings.job_execution import JobExecutionSettings

    clock = schedule.Scheduler()
    configure_calendar(clock, JobExecutionSettings())
    daily = [job for job in clock.jobs if job.job_func.args[1] == "daily"]
    assert sorted(job.at_time.strftime("%H:%M") for job in daily if job.at_time) == ["08:02", "17:02"]
    retry = next(job for job in daily if job.at_time is None)
    assert (retry.interval, retry.unit) == (30, "minutes")


def test_runtime_recovers_sofascore_before_fixture_reconciliation(monkeypatch):
    from types import SimpleNamespace
    from app.runtime import ApplicationRuntime

    calls = []
    monkeypatch.setattr(import_module("modules.jobs.daily_discovery"), "run_daily_discovery_job", lambda: calls.append("sofascore") or {"events_inserted": 1})
    runtime = ApplicationRuntime.__new__(ApplicationRuntime)
    runtime.fixtures = SimpleNamespace(retry_due=lambda: calls.append("fixtures"))
    assert runtime._run_daily_discovery() == {"events_inserted": 1}
    assert calls == ["sofascore", "fixtures"]
