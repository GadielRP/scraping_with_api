from contextlib import nullcontext
from datetime import datetime, timezone
from importlib import import_module
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from infrastructure.persistence.database import db_manager, DatabaseManager
from infrastructure.persistence.models import (
    Base,
    OddspapiFixtureDiscoveryRun,
)
from infrastructure.persistence.repositories import (
    OddspapiFixtureDiscoveryRunRepository,
)
from modules.jobs.oddspapi.fixture_discovery.recovery import FixtureDiscoveryService
from infrastructure.settings import Config
from infrastructure.settings import discovery as settings
from dataclasses import replace
from modules.sports.catalog import oddspapi_sport_ids

scheduler_module = import_module("modules.jobs.oddspapi.fixture_discovery.recovery")


@pytest.fixture(autouse=True)
def avoid_daily_progress_database(monkeypatch):
    monkeypatch.setattr(scheduler_module.DailyDiscoveryRepository, "latest_completed_at", lambda *args: None)


def _scheduler_without_setup() -> FixtureDiscoveryService:
    return FixtureDiscoveryService.__new__(FixtureDiscoveryService)


def test_missed_slot_targets_next_utc_day_after_evening_restart(monkeypatch):
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, scheduled_times=["17:45"]))
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, catchup_lookback_hours=36))
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, max_catchup_runs=2))

    slots = _scheduler_without_setup()._missed_fixture_discovery_slots(
        now_local=datetime(2026, 7, 24, 17, 52),
    )

    assert slots[-1] == (
        datetime(2026, 7, 24, 17, 45, tzinfo=ZoneInfo(Config.TIMEZONE)),
        "17:45",
        "2026-07-25",
    )


def test_missed_slot_is_still_recovered_next_morning(monkeypatch):
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, scheduled_times=["17:45"]))
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, catchup_lookback_hours=36))
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, max_catchup_runs=2))

    slots = _scheduler_without_setup()._missed_fixture_discovery_slots(
        now_local=datetime(2026, 7, 25, 10, 0),
    )

    assert slots[-1][0] == datetime(2026, 7, 24, 17, 45, tzinfo=ZoneInfo(Config.TIMEZONE))
    assert slots[-1][2] == "2026-07-25"


@pytest.mark.parametrize("reconciliation_enabled", [True, False])
def test_fixture_discovery_records_success(monkeypatch, reconciliation_enabled):
    monkeypatch.setattr(settings, "ODDSPAPI", replace(
        settings.ODDSPAPI, reconciliation_enabled=reconciliation_enabled,
    ))
    calls = []
    completed_at = datetime(2026, 7, 24, 23, 46, tzinfo=timezone.utc)
    monkeypatch.setattr(scheduler_module.DailyDiscoveryRepository, "latest_completed_at", lambda *args: completed_at)
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
    result = _scheduler_without_setup().run(
        target_date="2026-07-25",
        _trigger="catch_up",
        _scheduled_local_date="2026-07-24",
        _scheduled_time="17:45",
    )

    assert result is summary
    assert calls[0][0] == "begin"
    assert calls[0][1] == ("2026-07-25",)
    assert calls[0][2]["trigger"] == "catch_up"
    assert calls[0][2]["discovery_completed_at"] == (completed_at if reconciliation_enabled else None)
    assert calls[0][2]["sport_scope"] == (
        OddspapiFixtureDiscoveryRunRepository.normalize_sport_scope(oddspapi_sport_ids())
    )
    assert calls[1][0] == "success"
    assert calls[1][1][0] == "2026-07-25"
    assert calls[1][1][1]["started_at"] == "2026-07-24T23:45:00"


def test_fixture_discovery_dry_run_does_not_claim_durable_marker(monkeypatch):
    summary = SimpleNamespace(
        total_fixtures_fetched=0,
        total_mappings_created=0,
        sports=[],
        to_dict=lambda: {},
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "begin",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("dry runs must not claim durable markers")
        ),
    )
    monkeypatch.setattr(scheduler_module, "run_fixture_discovery_job", lambda **kwargs: summary)
    monkeypatch.setattr(scheduler_module, "observe_operation", lambda _name: nullcontext())

    result = _scheduler_without_setup().run(
        target_date="2026-07-25",
        create_mappings=False,
    )

    assert result is summary


def test_fixture_discovery_defaults_when_cli_forwards_none_target_date(monkeypatch):
    calls = []
    summary = SimpleNamespace(
        total_fixtures_fetched=0,
        total_mappings_created=0,
        sports=[],
        to_dict=lambda: {},
    )

    monkeypatch.setattr(
        scheduler_module,
        "now_in_timezone",
        lambda _zone: datetime(2026, 8, 6, 7, 0, tzinfo=ZoneInfo("America/Mexico_City")),
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "begin",
        lambda *args, **kwargs: calls.append((args, kwargs)) or True,
    )
    monkeypatch.setattr(
        scheduler_module.OddspapiFixtureDiscoveryRunRepository,
        "finish_success",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        scheduler_module,
        "run_fixture_discovery_job",
        lambda **kwargs: summary,
    )
    monkeypatch.setattr(scheduler_module, "observe_operation", lambda _name: nullcontext())

    _scheduler_without_setup().run(
        target_date=None,
        sports={"soccer": 10},
    )

    assert calls[0][0] == ("2026-08-06",)
    assert calls[0][1]["sport_scope"] == "soccer"
    assert calls[0][1]["scheduled_local_date"] == "2026-08-06"
    assert calls[0][1]["scheduled_time"] == "07:00"


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

    result = _scheduler_without_setup().run(target_date="2026-07-25")

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
        FixtureDiscoveryService,
        "_send_fixture_discovery_ops_alert",
        lambda *args, **kwargs: calls.append(("alert", kwargs)),
    )

    result = _scheduler_without_setup().run(target_date="2026-07-25")

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


@pytest.fixture
def fixture_database(tmp_path, monkeypatch):
    database = DatabaseManager(f'sqlite:///{tmp_path / "fixture_runs.db"}')
    Base.metadata.create_all(database.engine)
    monkeypatch.setitem(globals(), "db_manager", database)
    module = import_module(
        "infrastructure.persistence.repositories.oddspapi_fixture_discovery_run_repository"
    )
    monkeypatch.setattr(module, "db_manager", database)
    yield database
    database.engine.dispose()


def test_running_fixture_discovery_cannot_be_claimed_twice(fixture_database):

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


def test_commit_run_can_replace_successful_dry_run_marker(fixture_database):

    assert OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-02",
        trigger="scheduled",
        create_mappings=False,
    )
    OddspapiFixtureDiscoveryRunRepository.finish_success(
        "2099-01-02",
        {"create_mappings": False},
    )

    assert OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-02",
        trigger="manual",
        create_mappings=True,
    )


def test_same_target_date_can_be_claimed_for_different_sport_scopes(fixture_database):

    assert OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-03",
        trigger="manual",
        sport_scope="soccer",
    )
    assert OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-03",
        trigger="manual",
        sport_scope="baseball",
    )
    assert not OddspapiFixtureDiscoveryRunRepository.begin(
        "2099-01-03",
        trigger="manual",
        sport_scope="soccer",
    )
    OddspapiFixtureDiscoveryRunRepository.finish_success(
        "2099-01-03",
        {"create_mappings": True},
        sport_scope="soccer",
    )

    with db_manager.get_session() as session:
        runs = (
            session.query(OddspapiFixtureDiscoveryRun)
            .filter(OddspapiFixtureDiscoveryRun.target_date == "2099-01-03")
            .order_by(OddspapiFixtureDiscoveryRun.sport_scope)
            .all()
        )
        assert [run.sport_scope for run in runs] == ["baseball", "soccer"]
        assert [run.status for run in runs] == ["running", "success"]
        assert all(run.scheduled_local_date for run in runs)
        assert all(run.scheduled_time for run in runs)

def test_sport_scope_is_stable_for_multi_sport_runs():
    scope = OddspapiFixtureDiscoveryRunRepository.normalize_sport_scope(
        {"Soccer": 10, "baseball": 13}
    )

    assert scope == "baseball,soccer"


def test_fixture_success_reopens_only_after_new_sofascore_completion(fixture_database, monkeypatch):
    from datetime import timedelta
    module = import_module("infrastructure.persistence.repositories.oddspapi_fixture_discovery_run_repository")
    finished = datetime(2026, 10, 5, 23, 47, tzinfo=timezone.utc)
    monkeypatch.setattr(module, "utc_now", lambda: finished)
    assert OddspapiFixtureDiscoveryRunRepository.begin("2026-10-06", trigger="scheduled")
    OddspapiFixtureDiscoveryRunRepository.finish_success("2026-10-06", {"create_mappings": True})
    assert not OddspapiFixtureDiscoveryRunRepository.begin(
        "2026-10-06", trigger="catch_up", discovery_completed_at=finished - timedelta(minutes=1),
    )
    assert OddspapiFixtureDiscoveryRunRepository.begin(
        "2026-10-06", trigger="catch_up", discovery_completed_at=finished + timedelta(minutes=1),
    )
    assert not OddspapiFixtureDiscoveryRunRepository.begin(
        "2026-10-06", trigger="catch_up", discovery_completed_at=finished + timedelta(minutes=2),
    )


def test_late_manual_fixture_retry_keeps_the_scheduled_date(monkeypatch):
    monkeypatch.setattr(Config, "TIMEZONE", "America/Mexico_City")
    for when in ("2026-10-05T23:48", "2026-10-06T02:00", "2026-10-06T08:00"):
        now = datetime.fromisoformat(when).replace(tzinfo=ZoneInfo(Config.TIMEZONE))
        assert FixtureDiscoveryService.target_date_for_slot(now) == "2026-10-06"


def test_periodic_fixture_recovery_preserves_date_and_does_not_reset_running_markers(monkeypatch):
    service = FixtureDiscoveryService()
    monkeypatch.setattr(scheduler_module, "now_in_timezone", lambda _: datetime(2026, 10, 6, 2, tzinfo=ZoneInfo(Config.TIMEZONE)))
    calls = []
    monkeypatch.setattr(service, "run", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(OddspapiFixtureDiscoveryRunRepository, "mark_running_as_interrupted", lambda: pytest.fail("only startup should reset running markers"))
    service.retry_due()
    assert len(calls) == 1
    assert calls[0]["target_date"] == "2026-10-06"
    assert calls[0]["_scheduled_local_date"] == "2026-10-05"
    assert calls[0]["_scheduled_time"] == "17:47"


def test_deferred_fixture_work_is_retryable_without_failure_alert(monkeypatch):
    from shared.execution_context import WorkDeferred

    service = FixtureDiscoveryService()
    failures = []
    monkeypatch.setattr(scheduler_module.OddspapiFixtureDiscoveryRunRepository, "begin", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_module.OddspapiFixtureDiscoveryRunRepository, "finish_failed", lambda *args, **kwargs: failures.append(args))
    monkeypatch.setattr(service, "_send_fixture_discovery_ops_alert", lambda **kwargs: pytest.fail("deferral is not an operational failure"))
    monkeypatch.setattr(service, "_missed_fixture_discovery_slots", lambda: [(datetime(2026, 10, 5, 17, 47), "17:47", "2026-10-06")])

    def defer(**kwargs):
        raise WorkDeferred("maintenance busy")

    monkeypatch.setattr(scheduler_module, "run_fixture_discovery_job", defer)
    with pytest.raises(WorkDeferred):
        service.retry_due()
    assert failures == [("2026-10-06", "Deferred by maintenance executor")]


@pytest.mark.parametrize("previous_status", ["success", "failed"])
def test_disabled_reconciliation_preserves_failed_run_retries(fixture_database, monkeypatch, previous_status):
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, reconciliation_enabled=False))
    target = "2099-01-04"
    assert OddspapiFixtureDiscoveryRunRepository.begin(target, trigger="scheduled", sport_scope="soccer")
    if previous_status == "success":
        OddspapiFixtureDiscoveryRunRepository.finish_success(target, {}, sport_scope="soccer")
    else:
        OddspapiFixtureDiscoveryRunRepository.finish_failed(target, "HTTP failure", sport_scope="soccer")

    monkeypatch.setattr(scheduler_module.DailyDiscoveryRepository, "latest_completed_at",
                        lambda *args: pytest.fail("disabled reconciliation must not query Daily progress"))
    calls = []
    summary = SimpleNamespace(total_fixtures_fetched=0, total_mappings_created=0, sports=[], to_dict=lambda: {})
    monkeypatch.setattr(scheduler_module, "run_fixture_discovery_job", lambda **kwargs: calls.append(kwargs) or summary)
    monkeypatch.setattr(FixtureDiscoveryService, "refresh_usage", lambda self: False)
    result = FixtureDiscoveryService().run(target_date=target, sports={"soccer": 10}, _trigger="catch_up")
    assert len(calls) == (1 if previous_status == "failed" else 0)
    assert result is (summary if previous_status == "failed" else None)
