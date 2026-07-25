from contextlib import nullcontext
from datetime import datetime
from importlib import import_module
from types import SimpleNamespace

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Base,
    OddspapiFixtureDiscoveryRun,
)
from infrastructure.persistence.repositories import (
    OddspapiFixtureDiscoveryRunRepository,
)
from infrastructure.scheduler.job_scheduler import JobScheduler
from infrastructure.settings import Config

scheduler_module = import_module("infrastructure.scheduler.job_scheduler")


def _scheduler_without_setup() -> JobScheduler:
    return JobScheduler.__new__(JobScheduler)


def test_missed_slot_targets_next_utc_day_after_evening_restart(monkeypatch):
    monkeypatch.setattr(Config, "ODDSPAPI_FIXTURE_DISCOVERY_TIMES", ["17:45"])
    monkeypatch.setattr(
        Config,
        "ODDSPAPI_FIXTURE_DISCOVERY_CATCHUP_LOOKBACK_HOURS",
        36,
    )
    monkeypatch.setattr(
        Config,
        "ODDSPAPI_FIXTURE_DISCOVERY_MAX_CATCHUP_RUNS",
        2,
    )

    slots = _scheduler_without_setup()._missed_fixture_discovery_slots(
        now_local=datetime(2026, 7, 24, 17, 52),
    )

    assert slots[-1] == (
        datetime(2026, 7, 24, 17, 45),
        "17:45",
        "2026-07-25",
    )


def test_missed_slot_is_still_recovered_next_morning(monkeypatch):
    monkeypatch.setattr(Config, "ODDSPAPI_FIXTURE_DISCOVERY_TIMES", ["17:45"])
    monkeypatch.setattr(
        Config,
        "ODDSPAPI_FIXTURE_DISCOVERY_CATCHUP_LOOKBACK_HOURS",
        36,
    )
    monkeypatch.setattr(
        Config,
        "ODDSPAPI_FIXTURE_DISCOVERY_MAX_CATCHUP_RUNS",
        2,
    )

    slots = _scheduler_without_setup()._missed_fixture_discovery_slots(
        now_local=datetime(2026, 7, 25, 10, 0),
    )

    assert slots[-1][0] == datetime(2026, 7, 24, 17, 45)
    assert slots[-1][2] == "2026-07-25"


def test_fixture_discovery_records_success(monkeypatch):
    calls = []
    summary = SimpleNamespace(
        total_fixtures_fetched=589,
        total_mappings_created=457,
        sports=[SimpleNamespace(errors=0)],
        to_dict=lambda: {
            "started_at": datetime(2026, 7, 24, 23, 45),
            "total_fixtures_fetched": 589,
        },
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "begin",
        lambda *args, **kwargs: calls.append(("begin", args, kwargs)) or True,
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "finish_success",
        lambda *args, **kwargs: calls.append(("success", args, kwargs)),
    )
    monkeypatch.setattr(
        scheduler_module,
        "run_fixture_discovery_job",
        lambda **kwargs: summary,
    )
    monkeypatch.setattr(
        scheduler_module,
        "observe_operation",
        lambda _name: nullcontext(),
    )
    result = _scheduler_without_setup().job_oddspapi_fixture_discovery(
        target_date="2026-07-25",
        _trigger="catch_up",
        _scheduled_local_date="2026-07-24",
        _scheduled_time="17:45",
    )

    assert result is summary
    assert calls[0][0] == "begin"
    assert calls[0][1] == ("2026-07-25",)
    assert calls[0][2]["trigger"] == "catch_up"
    assert calls[1][0] == "success"
    assert calls[1][1][0] == "2026-07-25"
    assert calls[1][1][1]["started_at"] == "2026-07-24T23:45:00"


def test_fixture_discovery_skips_target_that_already_succeeded(monkeypatch):
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "begin",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(
        scheduler_module,
        "run_fixture_discovery_job",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("completed target must not execute twice")
        ),
    )

    result = _scheduler_without_setup().job_oddspapi_fixture_discovery(
        target_date="2026-07-25"
    )

    assert result is None


def test_fixture_discovery_with_sport_errors_remains_retryable(monkeypatch):
    calls = []
    summary = SimpleNamespace(
        total_fixtures_fetched=10,
        total_mappings_created=4,
        sports=[SimpleNamespace(errors=1)],
        to_dict=lambda: {},
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "begin",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "finish_success",
        lambda *args, **kwargs: calls.append("success"),
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "finish_failed",
        lambda *args, **kwargs: calls.append(("failed", args)),
    )
    monkeypatch.setattr(
        scheduler_module,
        "run_fixture_discovery_job",
        lambda **kwargs: summary,
    )
    monkeypatch.setattr(
        scheduler_module,
        "observe_operation",
        lambda _name: nullcontext(),
    )
    monkeypatch.setattr(
        JobScheduler,
        "_send_fixture_discovery_ops_alert",
        lambda *args, **kwargs: calls.append(("alert", kwargs)),
    )

    result = _scheduler_without_setup().job_oddspapi_fixture_discovery(
        target_date="2026-07-25"
    )

    assert result is summary
    assert calls == [
        (
            "failed",
            ("2026-07-25", "Discovery completed with 1 sport error(s)"),
        ),
        (
            "alert",
            {
                "target_date": "2026-07-25",
                "trigger": "scheduled",
                "detail": "completed with 1 sport error(s)",
            },
        ),
    ]


def test_running_fixture_discovery_cannot_be_claimed_twice():
    Base.metadata.create_all(bind=db_manager.engine)

    assert OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-01",
        trigger="scheduled",
    )
    assert not OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-01",
        trigger="catch_up",
    )

    with db_manager.get_session() as session:
        run = (
            session.query(OddspapiFixtureDiscoveryRun)
            .filter(OddspapiFixtureDiscoveryRun.target_date == "2099-01-01")
            .one()
        )
        assert run.status == "running"
        assert run.trigger == "scheduled"
