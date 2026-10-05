"""Application composition. Manual commands do not construct scheduler workers."""

from functools import partial
from datetime import timedelta
from time import monotonic
from infrastructure.persistence.repositories import EventRepository
from infrastructure.settings import Config
from infrastructure.settings.job_execution import JobExecutionSettings
from modules.jobs.pre_start_check_job.runtime import PreStartRuntime
from modules.jobs.pre_start_check_job.run_pre_start_check_job import run_pre_start_check_job
from modules.jobs.pre_start_check_job.run_t_minus_one_odds_job import run_t_minus_one_odds_job
from modules.jobs.oddspapi.fixture_discovery.recovery import FixtureDiscoveryService
from shared.execution_context import ExecutionContext, Priority, execution_scope
from shared.runtime_observability import observe_operation


class ApplicationRuntime:
    def __init__(self):
        self.settings = JobExecutionSettings()
        self.pre_start = PreStartRuntime(EventRepository(), self.settings)
        self.fixtures = FixtureDiscoveryService()
        self.scheduler = None
        from modules.jobs.clean_league_cache import run_clean_league_cache_job
        from modules.jobs.daily_discovery import run_daily_discovery_job
        from modules.jobs.discover_dropping_odds import run_discover_dropping_odds
        from modules.jobs.discover_secondary_sources import run_discover_secondary_sources
        from modules.jobs.midnight_sync_job import run_midnight_sync_job
        from modules.jobs.view_refresh.run_view_refresh import run_view_refresh
        from modules.jobs.results_collection_job import run_results_collection

        self.jobs = dict(
            discovery=run_discover_dropping_odds,
            discovery2=run_discover_secondary_sources,
            midnight=run_midnight_sync_job,
            daily=run_daily_discovery_job,
            results=run_results_collection,
            results_all=run_results_collection,
            results_date=run_results_collection,
            view_refresh=run_view_refresh,
            fixtures=self.fixtures.run,
            account_usage=self.fixtures.refresh_usage,
            league_cache=run_clean_league_cache_job,
            pre_start=partial(run_pre_start_check_job, self.pre_start, Config.global_debug_mode),
            closing=partial(
                run_t_minus_one_odds_job, self.pre_start, debug_mode=Config.global_debug_mode
            ),
        )

    def run(self, name, **kwargs):
        if name == "fixtures":
            kwargs.setdefault("_trigger", "manual")
        elif name == "results":
            from shared.temporal import now_in_timezone

            kwargs.setdefault(
                "target_date", now_in_timezone(Config.TIMEZONE).date() - timedelta(days=1)
            )
        priority = {
            "pre_start": Priority.PRE_START,
            "closing": Priority.CLOSING,
        }.get(name, Priority.MAINTENANCE)
        deadline = (
            monotonic() + self.settings.pre_start_budget_seconds
            if priority == Priority.PRE_START
            else None
        )
        try:
            with execution_scope(
                ExecutionContext(name, priority, deadline=deadline)
            ), observe_operation(name):
                return self.jobs[name](**kwargs)
        finally:
            if self.scheduler is None:
                self.close()

    def dispatch_scheduled(self, name, scheduled_at, priority, closing=False):
        """Bind occurrence identity and deadline before submitting to the assigned worker."""
        from infrastructure.scheduler.contracts import JobRequest
        from shared.temporal import in_timezone
        from modules.jobs.daily_discovery import (
            resolve_daily_discovery_slot,
            resolve_daily_discovery_target_date,
        )

        local = in_timezone(scheduled_at, Config.TIMEZONE)
        action = self.jobs[name]
        key = name
        expires_at = None
        if closing:
            action = partial(action, scheduled_at=scheduled_at)
            key = f"{name}:{scheduled_at.isoformat()}"
            expires_at = scheduled_at + timedelta(minutes=Config.PRE_START_CLOSING_ODDS_MINUTE)
        elif name == "midnight":
            target = local.date() - timedelta(days=1)
            action, key = partial(action, target_date=target), f"midnight:{target}"
        elif name == "daily":
            slot = resolve_daily_discovery_slot(local)
            target = resolve_daily_discovery_target_date(local, slot)
            action = partial(action, target_date=target, run_slot=slot)
            key = f"daily:{local.date()}:{slot}"
        elif name == "fixtures":
            target = self.fixtures.target_date_for_slot(local)
            action = partial(
                action,
                target_date=target,
                _scheduled_local_date=local.date().isoformat(),
                _scheduled_time=local.strftime("%H:%M"),
            )
            key = f"fixtures:{target}"
        return self.scheduler.dispatch(
            JobRequest(
                key,
                action,
                scheduled_at,
                priority,
                expires_at,
                self.settings.pre_start_budget_seconds if priority == Priority.PRE_START else None,
            )
        )

    def start(self):
        if self.scheduler is not None:
            raise RuntimeError("Application runtime has already started")
        from infrastructure.scheduler import JobScheduler
        from infrastructure.scheduler.serial_executor import SerialExecutor
        from infrastructure.scheduler.schedules import configure_calendar
        from infrastructure.scheduler.contracts import JobRequest
        from shared.temporal import utc_now

        self.scheduler = JobScheduler(
            {
                Priority.PRE_START: SerialExecutor("pre-start", 2, coalesce_active=True),
                Priority.CLOSING: SerialExecutor("closing", 2),
                Priority.MAINTENANCE: SerialExecutor(
                    "maintenance", self.settings.maintenance_capacity
                ),
            }
        )
        configure_calendar(self.scheduler.clock, self.settings, self.dispatch_scheduled)
        self.jobs["view_refresh"] = partial(
            self.jobs["view_refresh"], critical_pending=self.scheduler.critical_work_pending
        )
        from infrastructure.persistence.transient.run_directory import clean_abandoned_runs

        self.scheduler.dispatch(
            JobRequest("temporary_cleanup", clean_abandoned_runs, utc_now(), Priority.MAINTENANCE)
        )
        self.scheduler.dispatch(
            JobRequest("fixture_recovery", self.fixtures.recover, utc_now(), Priority.MAINTENANCE)
        )
        self.scheduler.start()

    def close(self):
        deadline = monotonic() + self.settings.shutdown_grace_seconds
        self.pre_start.oddsportal.stop_admission()
        if self.scheduler:
            self.scheduler.stop(timeout=max(0, deadline - monotonic()))
        self.pre_start.oddsportal.close(timeout=max(0, deadline - monotonic()))
