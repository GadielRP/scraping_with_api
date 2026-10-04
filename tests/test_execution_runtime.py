from datetime import timedelta
from threading import Event
from time import sleep
import pytest

from infrastructure.scheduler.contracts import Admission, JobRequest
from infrastructure.scheduler.serial_executor import SerialExecutor
from shared.execution_context import ExecutionContext, Priority, execution_scope
from shared.shutdown import is_shutdown_requested
from shared.temporal import utc_now


def request(key, action, priority=Priority.MAINTENANCE, **kwargs):
    return JobRequest(key, action, utc_now(), priority, **kwargs)


def test_lanes_run_independently_and_have_bounded_admission():
    maintenance = SerialExecutor("maintenance-test", 2)
    urgent = SerialExecutor("urgent-test", 2, coalesce_active=True)
    started, release, dispatched, queued = Event(), Event(), Event(), Event()

    def long_job():
        started.set()
        assert release.wait(5)

    try:
        assert maintenance.submit(request("long", long_job)) == Admission.ACCEPTED
        assert started.wait(2)
        assert maintenance.submit(request("long", long_job)) == Admission.COALESCED
        assert maintenance.submit(request("queued", queued.set)) == Admission.ACCEPTED
        assert maintenance.submit(request("overflow", lambda: None)) == Admission.FULL
        urgent.submit(request("urgent", dispatched.set, Priority.PRE_START))
        assert dispatched.wait(2)
        assert not queued.is_set()
        release.set()
        assert queued.wait(2)
    finally:
        release.set()
        maintenance.shutdown()
        urgent.shutdown()


def test_pre_start_coalesces_multiple_ticks_to_latest():
    worker = SerialExecutor("coalesce-test", 2, coalesce_active=True)
    started, release, finished = Event(), Event(), Event()
    seen = []

    def long_job():
        started.set()
        release.wait(5)

    try:
        worker.submit(request("pre-start", long_job))
        assert started.wait(2)
        worker.submit(request("pre-start", lambda: seen.append("old")))
        worker.submit(request("pre-start", lambda: (seen.append("latest"), finished.set())))
        release.set()
        assert finished.wait(2)
        assert seen == ["latest"]
    finally:
        release.set()
        worker.shutdown()


def test_expired_work_is_not_run_and_shutdown_signals_active():
    worker = SerialExecutor("stop-test", 1)
    started = Event()
    stopped = Event()

    def task():
        started.set()
        while not is_shutdown_requested():
            sleep(0.005)
        stopped.set()

    assert (
        worker.submit(request("expired", lambda: None, expires_at=utc_now() - timedelta(seconds=1)))
        == Admission.EXPIRED
    )
    worker.submit(request("active", task))
    assert started.wait(2)
    worker.shutdown()
    assert stopped.is_set()
    assert worker.submit(request("closed", lambda: None)) == Admission.CLOSED


def test_shutdown_grace_is_bounded_for_a_noncooperative_action():
    from time import monotonic

    worker = SerialExecutor("noncooperative-test", 1)
    started, release, finished = Event(), Event(), Event()

    def action():
        started.set()
        release.wait(5)
        finished.set()

    try:
        worker.submit(request("active", action))
        assert started.wait(2)
        before = monotonic()
        assert not worker.shutdown(timeout=0.01)
        assert monotonic() - before < 1
        assert worker.submit(request("new", lambda: None)) == Admission.CLOSED
    finally:
        release.set()
        assert finished.wait(2)
        assert worker.shutdown(timeout=2)


def test_oddsportal_retains_running_cycle_across_empty_and_busy_ticks(monkeypatch):
    from types import SimpleNamespace
    from modules.jobs.pre_start_check_job.runtime import OddsPortalWorkerState
    from modules.jobs.pre_start_check_job import oddsportal_worker as browser_job
    from infrastructure.runtime.reporting_exclusion import reporting_exclusion
    from shared.execution_context import WorkDeferred

    state = OddsPortalWorkerState()
    runtime = SimpleNamespace(oddsportal=state)
    started, release, next_cycle = Event(), Event(), Event()
    monkeypatch.setattr(browser_job.Config, "ODDSPORTAL_SCRAPING_ENABLED", True)

    def action(*args, **kwargs):
        started.set()
        release.wait(5)

    monkeypatch.setattr(browser_job, "run_oddsportal_scrape_cycle", action)
    try:
        first = browser_job.start_oddsportal_scrape_thread(runtime, [{}], {}, {})
        assert started.wait(2)
        assert browser_job.start_oddsportal_scrape_thread(runtime, [], {}, {}) is None
        assert state.active_thread is first
        with pytest.raises(WorkDeferred):
            with reporting_exclusion.refresh():
                pytest.fail("Reporting must not overlap the background browser cycle")
        blocked_state = browser_job.create_oddsportal_scrape_state([{"event_id": 1}])
        assert browser_job.start_oddsportal_scrape_thread(runtime, [{}], blocked_state, {}) is None
        assert blocked_state[1]["done_event"].is_set()
        assert state.active_thread is first
        release.set()
        first.join(timeout=2)
        assert not first.is_alive()
        with reporting_exclusion.refresh():
            pass
        second = state.launch(next_cycle.set)
        assert next_cycle.wait(2)
        second.join(timeout=2)
        assert state.close(timeout=2)
        assert state.launch(lambda: None) is None
    finally:
        release.set()
        state.close(timeout=2)


def test_execution_settings_are_configured_in_python_not_environment(monkeypatch):
    from infrastructure.settings.job_execution import JobExecutionSettings

    monkeypatch.setenv("JOB_REPORTING_MIN_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("JOB_REPORTING_POLL_SECONDS", "1")
    settings = JobExecutionSettings()
    assert settings.reporting_min_interval_seconds == 1800
    assert settings.reporting_poll_seconds == 60
    with pytest.raises(ValueError, match="reporting_min_interval_seconds must be positive"):
        JobExecutionSettings(reporting_min_interval_seconds=0)


def test_operation_registry_tracks_simultaneous_jobs():
    from shared.runtime_observability import observe_operation, _OPERATIONS

    with execution_scope(ExecutionContext("first", Priority.MAINTENANCE)), observe_operation(
        "first"
    ):
        with observe_operation("second"):
            assert {item["name"] for item in _OPERATIONS.values()} == {"first", "second"}
        assert len(_OPERATIONS) == 1
    assert not _OPERATIONS


def test_clock_preserves_expired_closing_occurrence(monkeypatch):
    from infrastructure.scheduler.job_scheduler import JobScheduler
    from infrastructure.scheduler.schedules import configure_calendar
    from app.runtime import ApplicationRuntime
    from infrastructure.settings import Config
    from infrastructure.settings.job_execution import JobExecutionSettings

    scheduler = JobScheduler({})
    captured = []
    scheduler.dispatch = captured.append
    monkeypatch.setattr(Config, "ENABLE_PRE_START_T_MINUS_ONE_JOB", True)
    actions = dict.fromkeys(
        (
            "discovery",
            "discovery2",
            "midnight",
            "daily",
            "fixtures",
            "reporting",
            "league_cache",
            "account_usage",
            "pre_start",
            "closing",
        ),
        lambda **_: None,
    )
    runtime = ApplicationRuntime.__new__(ApplicationRuntime)
    runtime.scheduler = scheduler
    runtime.jobs = actions
    runtime.settings = JobExecutionSettings()
    configure_calendar(scheduler.clock, runtime.settings, runtime.dispatch_scheduled)
    closing = next(job for job in scheduler.clock.jobs if job.job_func.args[1] == "closing")
    original = utc_now() - timedelta(minutes=5)
    closing.next_run = original.astimezone().replace(tzinfo=None)
    closing.run()
    assert captured[-1].scheduled_at == original
    assert captured[-1].expires_at < utc_now()


def test_fanout_bounds_submission_and_propagates_priority():
    from shared.concurrency import bounded_results
    from shared.execution_context import current_execution

    context = ExecutionContext("bounded", Priority.MAINTENANCE)
    submitted, consumed, identities = [], [], []

    def inputs():
        for index in range(100):
            assert len(submitted) - len(consumed) <= 3
            submitted.append(index)
            yield index

    def action(index):
        identities.append((current_execution().run_id, current_execution().priority))
        return index

    with execution_scope(context):
        for result in bounded_results(action, inputs(), 3):
            consumed.append(result)
    assert sorted(consumed) == list(range(100))
    assert set(identities) == {(context.run_id, Priority.MAINTENANCE)}


def test_missing_odds_cooldown_preserves_new_critical_attempts():
    from modules.jobs.pre_start_check_job.budget import MissingOddsCooldown

    cooldown = MissingOddsCooldown(600, 2)
    cooldown.missing(101, 30)
    assert not cooldown.allows(101, 30, (30, 5, 1))
    assert cooldown.allows(101, 5, (30, 5, 1))
    cooldown.missing(101, 5)
    assert not cooldown.allows(101, 4, (30, 5, 1))
    assert cooldown.allows(101, 1, (30, 5, 1))
    cooldown.available(101)
    assert cooldown.allows(101, 4, (30, 5, 1))
    for event_id in (101, 102, 103):
        cooldown.missing(event_id, 30)
    assert cooldown.allows(101, 30, (30,))
    assert not cooldown.allows(103, 30, (30,))


def test_deferred_maintenance_retains_original_date_and_bounded_retry():
    from infrastructure.scheduler.job_scheduler import JobScheduler

    worker = SerialExecutor("retry-test", 1)
    scheduler = JobScheduler({Priority.MAINTENANCE: worker})
    old_date = request("midnight:2026-10-02", lambda: None)
    try:
        scheduler.defer(old_date)
        assert scheduler._retry[old_date.key][0] is old_date
        scheduler.defer(request("overflow", lambda: None))
        assert len(scheduler._retry) == 1
    finally:
        scheduler.stop()


def test_provider_request_priority_and_diagnostics_are_thread_local(monkeypatch):
    from time import monotonic
    from infrastructure.settings import Config
    from modules.sofascore.client import SofaScoreAPI

    client = SofaScoreAPI()
    monkeypatch.setattr(Config, "REQUEST_DELAY_SECONDS", 0.05)
    client.last_request_time = monotonic()
    seen = []
    worker = SerialExecutor("http-background-test", 1)
    closing = SerialExecutor("http-closing-test", 1)
    finished = Event()

    def call(name, evidence):
        client.set_challenge_evidence_enabled(evidence)
        client._rate_limit()
        seen.append((name, client.challenge_evidence_enabled))
        if len(seen) == 2:
            finished.set()

    try:
        worker.submit(request("background", lambda: call("background", True)))
        closing.submit(request("closing", lambda: call("closing", False), Priority.CLOSING))
        assert finished.wait(3)
        assert seen == [("closing", False), ("background", True)]
    finally:
        worker.shutdown()
        closing.shutdown()
