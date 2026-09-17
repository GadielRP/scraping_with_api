from datetime import datetime, timezone
from importlib import import_module

import pytest

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from infrastructure.persistence.models import DailyDiscoveryLog
from infrastructure.settings import Config
from modules.jobs.daily_discovery.extractor import DailyDiscoveryExtractor
from modules.jobs.daily_discovery.run_daily_discovery import resolve_daily_discovery_slot

daily_job = import_module("modules.jobs.daily_discovery.run_daily_discovery")


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
    monkeypatch.setattr(daily_job.DailyDiscoveryRepository, "cleanup_old_logs", lambda days: calls["cleanup"].append(days) or 1)
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
        "run_daily_discovery",
        lambda *args, **kwargs: calls["run"].append((args, kwargs)) or {},
    )

    daily_job.run_daily_discovery_job()

    assert calls["cleanup"] == [getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)]
    assert calls["init"] == []
    assert calls["pending"] == []
    assert calls["run"] == []


def test_run_daily_discovery_job_passes_slot_and_pending_sports(monkeypatch):
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
        daily_job,
        "utc_now",
        lambda: datetime(2026, 5, 31, 23, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(daily_job.DailyDiscoveryRepository, "cleanup_old_logs", lambda days: calls["cleanup"].append(days) or 1)
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
        "run_daily_discovery",
        lambda *args, **kwargs: calls["run"].append((args, kwargs)) or {"events_inserted": 1},
    )

    daily_job.run_daily_discovery_job()

    assert calls["cleanup"] == [getattr(Config, "DAILY_DISCOVERY_DAYS_TO_KEEP", 1)]
    assert calls["init"][0][0] == ("2026-06-01", "AM", daily_job.DEFAULT_DAILY_DISCOVERY_SPORTS)
    assert calls["pending"][0][0] == ("2026-06-01", "AM")
    assert calls["run"][0][1] == {
        "sports": ["basketball", "tennis"],
        "date_str": "2026-06-01",
        "run_slot": "AM",
    }


@pytest.mark.parametrize(
    "odds_response, expected_status",
    [
        (None, "completed"),
        ({"ok": True}, "completed"),
    ],
)
def test_extractor_marks_slot_status_for_missing_or_empty_odds(monkeypatch, odds_response, expected_status):
    status_calls = []

    class FakeApiClient:
        def get_today_sport_events_odds_response(self, date, sport):
            return odds_response

        def get_today_sport_events_response(self, date, sport, *_args):
            return {"events": []}

    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.parse_today_market_odds_response",
        lambda response: {} if response else {},
    )
    monkeypatch.setattr(
        DailyDiscoveryRepository,
        "update_sport_status",
        lambda date, run_slot, sport, status: status_calls.append((date, run_slot, sport, status)) or True,
    )

    extractor = DailyDiscoveryExtractor(api_client=FakeApiClient())
    extractor.discover_events_for_date("2026-06-01", sports=["basketball"], run_slot="PM")

    assert status_calls[-1] == ("2026-06-01", "PM", "basketball", expected_status)
