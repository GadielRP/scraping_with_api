"""Scheduler responsiveness, bounded admission, and refresh recovery."""
from threading import Event
from threading import Condition, Thread
from time import monotonic, sleep
from unittest.mock import Mock

import pytest
import schedule

from infrastructure.scheduler.maintenance_executor import MaintenanceExecutor
from importlib import import_module
scheduler_module = import_module('infrastructure.scheduler.job_scheduler')
from infrastructure.scheduler.job_scheduler import JobScheduler
from modules.sofascore.client import SofaScoreAPI
from infrastructure.settings import Config
from shared.shutdown import background_work, is_shutdown_requested


def test_worker_does_not_block_scheduler_and_suppresses_duplicates():
    worker = MaintenanceExecutor(capacity=2)
    started, release, urgent, second = Event(), Event(), Event(), Event()
    def long_job():
        started.set()
        assert release.wait(5)
    try:
        dispatcher = JobScheduler.__new__(JobScheduler)
        dispatcher._maintenance = worker
        scheduler = schedule.Scheduler()
        scheduler.every().second.do(dispatcher._background(long_job))
        scheduler.every().second.do(urgent.set)
        scheduler.run_all()
        assert urgent.is_set() and started.wait(2)
        assert not worker.submit('long_job', long_job)
        assert worker.submit('daily', second.set)
        assert not worker.submit('another', lambda: None)
        assert not second.is_set()
        release.set()
        assert second.wait(2)
    finally:
        release.set()
        worker.shutdown()
    assert not worker.submit('closed', lambda: None)


def test_failed_work_releases_slot():
    worker = MaintenanceExecutor(capacity=1)
    try:
        def failure():
            raise RuntimeError('failure')
        assert worker.submit('failed', failure)
    finally:
        worker.shutdown()
    assert not worker._pending


def test_daily_skips_refresh_without_writes_and_retries_failed_refresh(monkeypatch):
    scheduler = JobScheduler.__new__(JobScheduler)
    scheduler._reporting_refresh_pending = False
    daily = Mock(return_value=None)
    refresh = Mock(side_effect=[RuntimeError('busy'), None])
    monkeypatch.setattr(scheduler_module, 'run_daily_discovery_job', daily)
    monkeypatch.setattr(scheduler_module, 'refresh_materialized_views', refresh)
    scheduler.job_daily_discovery()
    refresh.assert_not_called()
    daily.return_value = {'events_persisted': 2}
    with pytest.raises(RuntimeError, match='busy'):
        scheduler.job_daily_discovery()
    assert scheduler._reporting_refresh_pending
    daily.return_value = None
    scheduler.job_daily_discovery()
    assert refresh.call_count == 2
    assert not scheduler._reporting_refresh_pending


def test_shutdown_signals_active_work():
    worker = MaintenanceExecutor()
    started, stopped = Event(), Event()
    def task():
        started.set()
        while not is_shutdown_requested():
            sleep(0.01)
        stopped.set()
    worker.submit('active', task)
    assert started.wait(2)
    worker.shutdown()
    assert stopped.is_set()


def test_foreground_requests_precede_background_waiters(monkeypatch):
    client = SofaScoreAPI.__new__(SofaScoreAPI)
    client._rate_limit_condition = Condition()
    client._foreground_waiters = 0
    client.last_request_time = monotonic()
    monkeypatch.setattr(Config, 'REQUEST_DELAY_SECONDS', 0.2)
    order = []
    started = Event()
    def background():
        with background_work(Event()):
            started.set()
            client._rate_limit()
            order.append('background')
    def foreground():
        client._rate_limit()
        order.append('foreground')
    low = Thread(target=background)
    high = Thread(target=foreground)
    low.start()
    assert started.wait(2)
    high.start()
    high.join(2)
    low.join(2)
    assert order == ['foreground', 'background']
