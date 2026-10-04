from datetime import datetime, timezone
from importlib import import_module
from zoneinfo import ZoneInfo

import pytest

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from infrastructure.persistence.models import DailyDiscoveryLog
from infrastructure.settings import Config
from modules.jobs.daily_discovery.run_daily_discovery import resolve_daily_discovery_slot

daily_job = import_module("modules.jobs.daily_discovery.run_daily_discovery")


@pytest.fixture(autouse=True)
def avoid_database_cleanup(monkeypatch):
    """Keep heartbeat unit tests from invoking the real discard-memory cleanup."""
    monkeypatch.setattr(daily_job, "run_event_discard_cleanup", lambda: 0)


def test_daily_discovery_log_uses_slot_scoped_uniqueness():
    column_names = {column.name for column in DailyDiscoveryLog.__table__.columns}
    constraint_names = {constraint.name for constraint in DailyDiscoveryLog.__table__.constraints}

    assert "run_slot" in column_names
    assert "unique_date_slot_sport_discovery" in constraint_names


def test_resolve_daily_discovery_slot_boundaries(monkeypatch):
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_AM_OPEN_HOUR", 5)
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_PM_OPEN_HOUR", 16)

    assert resolve_daily_discovery_slot(datetime(2026, 6, 1, 4, 59)) is None
    assert resolve_daily_discovery_slot(datetime(2026, 6, 1, 5, 0)) == "AM"
    assert resolve_daily_discovery_slot(datetime(2026, 6, 1, 15, 59)) == "AM"
    assert resolve_daily_discovery_slot(datetime(2026, 6, 1, 16, 0)) == "PM"


def test_run_daily_discovery_job_skips_before_am_slot(monkeypatch):
    calls = {
        "cleanup": [],
        "init": [],
        "pending": [],
        "run": [],
    }
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_AM_OPEN_HOUR", 5)
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_PM_OPEN_HOUR", 16)

    monkeypatch.setattr(
        daily_job,
        "now_in_timezone",
        lambda _zone: datetime(2026, 6, 1, 4, 59, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        daily_job.DailyDiscoveryRepository,
        "cleanup_old_logs",
        lambda days: calls["cleanup"].append(days) or 1,
    )
    monkeypatch.setattr(
        daily_job.DailyDiscoveryRepository,
        "initialize_sports_for_slot",
        lambda *args, **kwargs: calls["init"].append((args, kwargs)) or True,
    )
    monkeypatch.setattr(
        daily_job.DailyDiscoveryRepository,
        "get_pending_sports",
        lambda *args, **kwargs: calls["pending"].append((args, kwargs)) or [],
    )
    monkeypatch.setattr(
        daily_job,
        "discover_events_for_date",
        lambda *args, **kwargs: calls["run"].append((args, kwargs)) or {},
    )

    daily_job.run_daily_discovery_job()

    assert calls["cleanup"] == [getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)]
    assert calls["init"] == []
    assert calls["pending"] == []
    assert calls["run"] == []


def test_run_daily_discovery_job_passes_slot_and_pending_sports(monkeypatch):
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_AM_OPEN_HOUR", 5)
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_PM_OPEN_HOUR", 16)
    calls = {
        "cleanup": [],
        "init": [],
        "pending": [],
        "run": [],
    }

    monkeypatch.setattr(
        daily_job,
        "now_in_timezone",
        lambda _zone: datetime(2026, 6, 1, 5, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        daily_job.DailyDiscoveryRepository,
        "cleanup_old_logs",
        lambda days: calls["cleanup"].append(days) or 1,
    )
    monkeypatch.setattr(
        daily_job.DailyDiscoveryRepository,
        "initialize_sports_for_slot",
        lambda *args, **kwargs: calls["init"].append((args, kwargs)) or True,
    )
    monkeypatch.setattr(
        daily_job.DailyDiscoveryRepository,
        "get_pending_sports",
        lambda *args, **kwargs: calls["pending"].append((args, kwargs)) or ["basketball", "tennis"],
    )
    monkeypatch.setattr(
        daily_job,
        "discover_events_for_date",
        lambda *args, **kwargs: calls["run"].append((args, kwargs)) or {"events_inserted": 1},
    )

    daily_job.run_daily_discovery_job()

    assert calls["cleanup"] == [getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)]
    from modules.sports.catalog import sofascore_sport_slugs

    assert calls["init"][0][0] == ("2026-06-01", "AM", sofascore_sport_slugs())
    assert calls["pending"][0][0] == ("2026-06-01", "AM")
    assert calls["run"][0][1] == {
        "sports": ["basketball", "tennis"],
        "date": "2026-06-01",
        "run_slot": "AM",
    }


@pytest.mark.parametrize(
    "hour, slot", [(0, None), (7, None), (8, "PM"), (16, "PM"), (17, "AM"), (23, "AM")]
)
def test_legacy_slot_hours_do_not_wrap_into_the_next_day(monkeypatch, hour, slot):
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_AM_OPEN_HOUR", 17)
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_PM_OPEN_HOUR", 8)
    assert resolve_daily_discovery_slot(datetime(2026, 9, 29, hour)) == slot


@pytest.mark.parametrize("hour, slot", [(8, "PM"), (17, "AM"), (18, "AM"), (21, "AM"), (23, "AM")])
def test_heartbeat_targets_same_mexico_date_across_utc_midnight(monkeypatch, hour, slot):
    from unittest.mock import Mock

    monkeypatch.setattr(Config, "TIMEZONE", "America/Mexico_City")
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_AM_OPEN_HOUR", 17)
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_PM_OPEN_HOUR", 8)
    local_now = datetime(2026, 9, 29, hour, 40, tzinfo=ZoneInfo(Config.TIMEZONE))
    monkeypatch.setattr(daily_job, "now_in_timezone", lambda _: local_now)
    monkeypatch.setattr(DailyDiscoveryRepository, "cleanup_old_logs", lambda _: 0)
    monkeypatch.setattr(DailyDiscoveryRepository, "initialize_sports_for_slot", lambda *args: True)
    pending = Mock(return_value=["football"])
    run = Mock(return_value={"events_inserted": 1})
    monkeypatch.setattr(DailyDiscoveryRepository, "get_pending_sports", pending)
    monkeypatch.setattr(daily_job, "discover_events_for_date", run)

    daily_job.run_daily_discovery_job()

    pending.assert_called_once_with("2026-09-29", slot)
    run.assert_called_once_with(sports=["football"], date="2026-09-29", run_slot=slot)


def test_pending_sports_follow_explicit_slot_status(monkeypatch):
    from contextlib import contextmanager
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from infrastructure.persistence.repositories import daily_discovery_repository as repository

    engine = create_engine("sqlite://")
    DailyDiscoveryLog.__table__.create(engine)
    monkeypatch.setattr(Config, "TIMEZONE", "America/Mexico_City")

    @contextmanager
    def session_scope():
        with Session(engine) as session:
            yield session
            session.commit()

    monkeypatch.setattr(repository.db_manager, "get_session", session_scope)
    with session_scope() as session:
        for sport, slot, status, attempted in [
            ("football", "AM", "completed", datetime(2026, 9, 30, 3, 40, tzinfo=timezone.utc)),
            ("tennis", "AM", "completed", datetime(2026, 10, 1, 23, 0, tzinfo=timezone.utc)),
            ("basketball", "AM", "failed", datetime(2026, 10, 1, 23, 0, tzinfo=timezone.utc)),
            ("football", "PM", "pending", None),
        ]:
            session.add(
                DailyDiscoveryLog(
                    date="2026-10-01",
                    run_slot=slot,
                    sport=sport,
                    status=status,
                    last_attempt_at=attempted,
                )
            )

    assert DailyDiscoveryRepository.get_pending_sports("2026-10-01", "AM") == ["basketball"]
    assert DailyDiscoveryRepository.get_pending_sports("2026-10-01", "PM") == ["football"]
    monkeypatch.setattr(
        repository, "utc_now", lambda: datetime(2026, 10, 1, 23, 0, tzinfo=timezone.utc)
    )
    DailyDiscoveryRepository.update_sport_status("2026-10-01", "AM", "football", "completed")
    assert DailyDiscoveryRepository.get_pending_sports("2026-10-01", "AM") == ["basketball"]
    engine.dispose()


def test_heartbeats_complete_two_passes_per_local_day(monkeypatch):
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_AM_OPEN_HOUR", 17)
    monkeypatch.setattr(Config, "DAILY_DISCOVERY_PM_OPEN_HOUR", 8)
    completed = set()
    calls = []
    monkeypatch.setattr(DailyDiscoveryRepository, "cleanup_old_logs", lambda _: 0)
    monkeypatch.setattr(DailyDiscoveryRepository, "initialize_sports_for_slot", lambda *a: True)
    monkeypatch.setattr(
        DailyDiscoveryRepository,
        "get_pending_sports",
        lambda date, slot: [] if (date, slot) in completed else ["football"],
    )

    def run(**kwargs):
        key = (kwargs["date"], kwargs["run_slot"])
        calls.append(key)
        completed.add(key)
        return {"events_inserted": 1}

    monkeypatch.setattr(daily_job, "discover_events_for_date", run)
    for day in (datetime(2026, 12, 31), datetime(2027, 1, 1)):
        for hour in (0, 7, 8, 12, 17, 18, 21, 23):
            now = day.replace(hour=hour, tzinfo=ZoneInfo("America/Mexico_City"))
            monkeypatch.setattr(daily_job, "now_in_timezone", lambda _, now=now: now)
            daily_job.run_daily_discovery_job()

    assert calls == [
        ("2026-12-31", "PM"),
        ("2026-12-31", "AM"),
        ("2027-01-01", "PM"),
        ("2027-01-01", "AM"),
    ]
