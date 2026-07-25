"""Infrastructure scheduler for the SofaScore odds system."""

from __future__ import annotations

import json
import logging
import schedule
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from infrastructure.persistence.repositories import (
    EventRepository,
    OddspapiFixtureDiscoveryRunRepository,
    ResultRepository,
)
from infrastructure.settings import Config
from modules.jobs.clean_league_cache import run_clean_league_cache_job
from modules.jobs.daily_discovery import run_daily_discovery_job, run_daily_discovery_retry_job
from modules.jobs.discover_dropping_odds import run_discover_dropping_odds
from modules.jobs.discover_secondary_sources import run_discover_secondary_sources
from modules.jobs.midnight_sync_job import run_midnight_sync_job
from modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery import run_fixture_discovery_job
from modules.jobs.pre_start_check_job.run_pre_start_check_job import run_pre_start_check_job
from modules.jobs.results_collection_job import (
    run_results_collection_all_finished,
    run_results_collection_for_date,
    run_results_collection_previous_day,
)
from shared.runtime_observability import observe_operation
from shared.timezone_utils import TIMEZONE, get_local_now

logger = logging.getLogger(__name__)


class JobScheduler:
    """Schedule and trigger background jobs."""

    def __init__(self):
        self.running = False
        self.thread = None
        self.event_repo = EventRepository()
        self.result_repo = ResultRepository()
        self.recently_rescheduled = set()
        self.last_cleanup_time = time.time()
        self._active_op_thread = None
        self._setup_jobs()

    def _setup_jobs(self):
        """Register all scheduled jobs."""
        for time_str in Config.DISCOVERY_TIMES:
            schedule.every().day.at(time_str).do(self.job_discovery)

        for time_str in Config.DISCOVERY2_TIMES:
            schedule.every().day.at(time_str).do(self.job_discovery2)

        self._setup_pre_start_jobs()

        schedule.every().day.at("04:00").do(self.job_midnight_sync)
        schedule.every(3).days.at("05:00").do(self.job_clean_league_cache)

        daily_discovery_fixed_times = getattr(Config, "DAILY_DISCOVERY_FIXED_TIMES", ["18:10"])
        for time_str in daily_discovery_fixed_times:
            schedule.every().day.at(time_str).do(self.job_daily_discovery)

        daily_discovery_interval = getattr(
            Config,
            "DAILY_DISCOVERY_CHECK_INTERVAL_MINUTES",
            getattr(Config, "DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES", 240),
        )
        schedule.every(daily_discovery_interval).minutes.do(self.job_daily_discovery)

        oddspapi_fixture_discovery_times = getattr(
            Config,
            "ODDSPAPI_FIXTURE_DISCOVERY_TIMES",
            ["17:47"],
        )
        for time_str in oddspapi_fixture_discovery_times:
            schedule.every().day.at(time_str).do(self.job_oddspapi_fixture_discovery)

        logger.info("Jobs scheduled:")
        logger.info(f"  - Discovery: daily at {', '.join(Config.DISCOVERY_TIMES)}")
        logger.info(f"  - Discovery 2: daily at {', '.join(Config.DISCOVERY2_TIMES)}")
        logger.info(
            f"  - Pre-start check: every {Config.POLL_INTERVAL_MINUTES} minutes (includes tennis timestamp checks + NBA 4th quarter checks)"
        )
        logger.info("  - Midnight sync: daily at 04:00")
        logger.info(
            "  - Daily discovery: fixed trigger(s) at %s; retry heartbeat every %s minutes; AM opens at %s:00, PM opens at %s:00",
            ", ".join(daily_discovery_fixed_times),
            daily_discovery_interval,
            Config.DAILY_DISCOVERY_AM_OPEN_HOUR,
            Config.DAILY_DISCOVERY_PM_OPEN_HOUR,
        )
        logger.info(
            "  - Oddspapi fixture discovery: daily at %s (UTC calendar day)",
            ", ".join(oddspapi_fixture_discovery_times),
        )
        logger.info("  - League cache cleanup: every 3 days at 05:00")

    def _setup_pre_start_jobs(self):
        interval_minutes = Config.POLL_INTERVAL_MINUTES
        for minute in range(0, 60, interval_minutes):
            schedule.every().hour.at(f":{minute:02d}").do(self.job_pre_start_check)
        logger.info(
            f"  - Pre-start check scheduled every {interval_minutes} minutes at exact minute marks (upcoming events + tennis/NBA in-game checks)"
        )

    def _cleanup_recently_rescheduled(self):
        current_time = time.time()
        if current_time - self.last_cleanup_time > 600:
            self.recently_rescheduled.clear()
            self.last_cleanup_time = current_time
            logger.debug("Cleaned up recently_rescheduled tracking set")

    def start(self):
        """Start the scheduler loop."""
        if self.running:
            logger.warning("Scheduler is already running")
            return

        self.running = True
        self._recover_missed_oddspapi_fixture_discovery_runs()

        # Do startup work before the scheduler thread begins. The old order
        # allowed an immediate pre-start check and a due scheduled check to
        # overlap, multiplying memory use during restarts.
        logger.info("Running immediate pre-start check for any games starting soon...")
        self.job_pre_start_check()

        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        logger.info("Job scheduler started")

    def stop(self):
        """Stop the scheduler loop."""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("Job scheduler stopped")

    def _run_scheduler(self):
        logger.info("Scheduler loop started - monitoring for pending jobs...")
        last_check = time.time()

        while self.running:
            try:
                due_jobs = [job for job in schedule.jobs if job.should_run]
                if due_jobs:
                    now = datetime.now()
                    logger.info(
                        "Scheduler dispatching %s due job(s): %s",
                        len(due_jobs),
                        ", ".join(
                            f"{job.job_func.__name__}"
                            f"(late_s={max(0, int((now - job.next_run).total_seconds()))})"
                            for job in due_jobs
                        ),
                    )
                    dispatch_started = time.monotonic()
                    schedule.run_pending()
                    logger.info(
                        "Scheduler completed due batch jobs=%s duration_s=%.1f",
                        ", ".join(job.job_func.__name__ for job in due_jobs),
                        time.monotonic() - dispatch_started,
                    )

                current_time = time.time()
                if current_time - last_check >= 30:
                    logger.debug(
                        f"Scheduler heartbeat - {len(schedule.jobs)} jobs scheduled, next run in {schedule.idle_seconds()} seconds"
                    )
                    last_check = current_time

                time.sleep(1)
            except Exception as exc:
                logger.exception(f"Error in scheduler loop: {exc}")
                time.sleep(5)

    def job_discovery(self):
        logger.info("Starting Job A: Event Discovery with Odds Processing")
        try:
            run_discover_dropping_odds()
        except Exception as exc:
            logger.error(f"Error in Job A: {exc}")

    def job_discovery2(self):
        logger.info("Starting Job B: Event Discovery from streaks, team streaks, h2h and winning odds events")
        try:
            run_discover_secondary_sources()
        except Exception as exc:
            logger.error(f"Error in Job B: {exc}")

    def job_pre_start_check(self):
        with observe_operation("pre_start_check"):
            debug_mode = Config.global_debug_mode
            try:
                if debug_mode:
                    logger.info(f"Global debug mode set for pre start check to {debug_mode}")
                run_pre_start_check_job(self, debug_mode)
            except Exception as exc:
                logger.exception(f"Error in Job C: {exc}")

    def job_results_collection(self):
        logger.info("Starting Job E: Results collection for finished events")
        try:
            run_results_collection_previous_day()
        except Exception as exc:
            logger.error(f"Error in Job E: {exc}")

    def job_results_collection_all_finished(self):
        logger.info("Starting Job E2: Comprehensive results collection")
        try:
            run_results_collection_all_finished()
        except Exception as exc:
            logger.error(f"Error in Job E2: {exc}")

    def job_results_collection_for_date(self, target_date):
        logger.info(f"Starting results collection for date: {target_date}")
        try:
            run_results_collection_for_date(target_date)
        except Exception as exc:
            logger.error(f"Error in results collection for {target_date}: {exc}")

    def job_midnight_sync(self):
        logger.info("Starting Job D: Midnight results collection")
        try:
            run_midnight_sync_job()
        except Exception as exc:
            logger.error(f"Error in Job D: {exc}")

    def job_daily_discovery(self):
        logger.info("Starting Job E: Daily discovery heartbeat")
        try:
            run_daily_discovery_job()
        except Exception as exc:
            logger.error(f"Error in Job E (Daily Discovery): {exc}")

    def job_oddspapi_fixture_discovery(self, **kwargs):
        trigger = kwargs.pop("_trigger", "scheduled")
        scheduled_local_date = kwargs.pop("_scheduled_local_date", None)
        scheduled_time = kwargs.pop("_scheduled_time", None)

        # If target_date is not explicitly passed, compute it dynamically.
        # Since this job runs late in the MX evening (23:45 UTC), we target the upcoming UTC day
        # (tomorrow UTC) to avoid trying to resolve matches that have already started.
        if "target_date" not in kwargs:
            utc_now = datetime.now(timezone.utc)
            # If running after 12:00 UTC, target tomorrow's UTC calendar day
            if utc_now.hour >= 12:
                target = utc_now + timedelta(days=1)
            else:
                target = utc_now
            kwargs["target_date"] = target.strftime("%Y-%m-%d")

        target_date_str = kwargs.get("target_date")
        tracked_run = False
        try:
            tracked_run = OddspapiFixtureDiscoveryRunRepository.begin(
                target_date_str,
                trigger=trigger,
                scheduled_local_date=scheduled_local_date,
                scheduled_time=scheduled_time,
            )
            if not tracked_run:
                logger.info(
                    "Skipping Oddspapi fixture discovery for UTC day %s: "
                    "a successful or currently running durable run already exists",
                    target_date_str,
                )
                return None
        except Exception as exc:
            # Discovery is more important than observability. Run fail-open if
            # the marker cannot be written, while making the durability loss loud.
            logger.exception(
                "Could not claim durable Oddspapi fixture-discovery run for %s; "
                "continuing without a marker: %s",
                target_date_str,
                exc,
            )

        logger.info(
            "Starting Oddspapi fixture discovery for UTC day: %s trigger=%s "
            "scheduled_local_date=%s scheduled_time=%s",
            target_date_str,
            trigger,
            scheduled_local_date,
            scheduled_time,
        )
        try:
            with observe_operation(
                f"oddspapi_fixture_discovery:{target_date_str}:{trigger}"
            ):
                summary = run_fixture_discovery_job(**kwargs)
            errors = sum(sport.errors for sport in summary.sports)
            logger.info(
                "Oddspapi fixture discovery completed fixtures=%s mappings_created=%s errors=%s",
                summary.total_fixtures_fetched,
                summary.total_mappings_created,
                errors,
            )
            if tracked_run and errors == 0:
                summary_payload = json.loads(
                    json.dumps(
                        summary.to_dict(),
                        default=lambda value: value.isoformat(),
                    )
                )
                OddspapiFixtureDiscoveryRunRepository.finish_success(
                    target_date_str,
                    summary_payload,
                )
            elif tracked_run:
                OddspapiFixtureDiscoveryRunRepository.finish_failed(
                    target_date_str,
                    f"Discovery completed with {errors} sport error(s)",
                )
                logger.warning(
                    "Oddspapi fixture discovery target %s was not marked successful "
                    "because %s sport error(s) were reported",
                    target_date_str,
                    errors,
                )
                self._send_fixture_discovery_ops_alert(
                    target_date=target_date_str,
                    trigger=trigger,
                    detail=f"completed with {errors} sport error(s)",
                )
            return summary
        except Exception as exc:
            if tracked_run:
                try:
                    OddspapiFixtureDiscoveryRunRepository.finish_failed(
                        target_date_str,
                        repr(exc),
                    )
                except Exception:
                    logger.exception(
                        "Could not mark failed Oddspapi fixture-discovery run for %s",
                        target_date_str,
                    )
            self._send_fixture_discovery_ops_alert(
                target_date=target_date_str,
                trigger=trigger,
                detail=f"failed: {type(exc).__name__}: {exc}",
            )
            logger.exception(f"Error in Oddspapi fixture discovery: {exc}")
            raise

    @staticmethod
    def _send_fixture_discovery_ops_alert(
        *,
        target_date: str,
        trigger: str,
        detail: str,
    ) -> None:
        """Best-effort alert using the already configured Telegram transport."""
        try:
            from modules.alerts import pre_start_notifier

            message = (
                "🚨 Oddspapi fixture discovery requires attention\n"
                f"UTC target: {target_date}\n"
                f"Trigger: {trigger}\n"
                f"Detail: {detail}"
            )
            if not pre_start_notifier.send_telegram_message(message):
                logger.warning(
                    "Oddspapi fixture-discovery ops alert was not delivered "
                    "target_date=%s trigger=%s",
                    target_date,
                    trigger,
                )
        except Exception:
            logger.exception(
                "Could not send Oddspapi fixture-discovery ops alert "
                "target_date=%s trigger=%s",
                target_date,
                trigger,
            )

    @staticmethod
    def _target_date_for_local_slot(slot_local: datetime) -> str:
        slot_utc = TIMEZONE.localize(slot_local).astimezone(timezone.utc)
        target = slot_utc + timedelta(days=1) if slot_utc.hour >= 12 else slot_utc
        return target.strftime("%Y-%m-%d")

    def _missed_fixture_discovery_slots(
        self,
        *,
        now_local: datetime | None = None,
    ) -> list[tuple[datetime, str, str]]:
        now_local = now_local or get_local_now()
        lookback_hours = max(
            0,
            Config.ODDSPAPI_FIXTURE_DISCOVERY_CATCHUP_LOOKBACK_HOURS,
        )
        cutoff = now_local - timedelta(hours=lookback_hours)
        day_count = lookback_hours // 24 + 2
        slots: list[tuple[datetime, str, str]] = []
        seen_targets: set[str] = set()

        for days_ago in range(day_count, -1, -1):
            local_date = (now_local - timedelta(days=days_ago)).date()
            for configured_time in Config.ODDSPAPI_FIXTURE_DISCOVERY_TIMES:
                try:
                    slot_time = datetime.strptime(configured_time, "%H:%M").time()
                except ValueError:
                    logger.error(
                        "Ignoring invalid ODDSPAPI_FIXTURE_DISCOVERY_TIMES value: %s",
                        configured_time,
                    )
                    continue
                occurrence = datetime.combine(local_date, slot_time)
                if occurrence < cutoff or occurrence > now_local:
                    continue
                target_date = self._target_date_for_local_slot(occurrence)
                if target_date in seen_targets:
                    continue
                seen_targets.add(target_date)
                slots.append((occurrence, configured_time, target_date))

        slots.sort(key=lambda item: item[0])
        max_runs = max(0, Config.ODDSPAPI_FIXTURE_DISCOVERY_MAX_CATCHUP_RUNS)
        return slots[-max_runs:] if max_runs else []

    def _recover_missed_oddspapi_fixture_discovery_runs(self) -> None:
        try:
            interrupted = (
                OddspapiFixtureDiscoveryRunRepository.mark_running_as_interrupted()
            )
            if interrupted:
                logger.critical(
                    "Recovered %s Oddspapi fixture-discovery run marker(s) left "
                    "running by an unclean process exit",
                    interrupted,
                )
        except Exception:
            logger.exception(
                "Could not mark interrupted Oddspapi fixture-discovery runs"
            )

        for occurrence, configured_time, target_date in self._missed_fixture_discovery_slots():
            try:
                if OddspapiFixtureDiscoveryRunRepository.has_success(target_date):
                    continue
            except Exception:
                logger.exception(
                    "Could not check prior Oddspapi fixture-discovery success "
                    "for %s; attempting catch-up fail-open",
                    target_date,
                )

            logger.warning(
                "Catch-up Oddspapi fixture discovery for missed slot "
                "local_date=%s time=%s target_utc_date=%s",
                occurrence.strftime("%Y-%m-%d"),
                configured_time,
                target_date,
            )
            try:
                self.job_oddspapi_fixture_discovery(
                    target_date=target_date,
                    _trigger="catch_up",
                    _scheduled_local_date=occurrence.strftime("%Y-%m-%d"),
                    _scheduled_time=configured_time,
                )
            except Exception:
                logger.exception(
                    "Catch-up Oddspapi fixture discovery failed for target UTC day %s",
                    target_date,
                )

    def job_clean_league_cache(self):
        logger.info("Starting Job F: Clean up OddsPortal league cache")
        try:
            run_clean_league_cache_job()
        except Exception as exc:
            logger.error(f"Error in Job F (Clean up OddsPortal league cache): {exc}")

    def job_daily_discovery_retry(self):
        logger.info("Starting Job E_Retry: Delegating to slot-aware daily discovery heartbeat")
        try:
            run_daily_discovery_retry_job()
        except Exception as exc:
            logger.error(f"Error in Job E_Retry: {exc}")

    def run_job_discovery_now(self):
        logger.info("Running Job A immediately")
        self.job_discovery()

    def run_job_discovery2_now(self):
        logger.info("Running Job B immediately")
        self.job_discovery2()

    def run_job_pre_start_check_now(self):
        logger.info("Running Job C immediately")
        self.job_pre_start_check()

        if getattr(self, "_active_op_thread", None) and self._active_op_thread.is_alive():
            logger.info("⏳ Waiting for OddsPortal background worker to finish before exiting...")
            self._active_op_thread.join()
            logger.info("✅ OddsPortal background worker finished.")

    def run_job_midnight_sync_now(self):
        logger.info("Running Job D immediately")
        self.job_midnight_sync()

    def run_job_results_collection_now(self):
        logger.info("Running Job E immediately")
        self.job_results_collection()

    def run_job_results_collection_for_date_now(self, target_date):
        logger.info(f"Running results collection for {target_date} immediately")
        self.job_results_collection_for_date(target_date)

    def run_job_results_collection_all_now(self):
        logger.info("Running Job E2 immediately")
        self.job_results_collection_all_finished()

    def run_job_daily_discovery_now(self):
        logger.info("Running Job E (Daily Discovery) immediately")
        self.job_daily_discovery()

    def run_job_oddspapi_fixture_discovery_now(self, **kwargs):
        logger.info("Running Oddspapi fixture discovery immediately")
        return self.job_oddspapi_fixture_discovery(**kwargs)

    def get_scheduled_jobs(self) -> List[Dict]:
        jobs = []
        for job in schedule.jobs:
            job_info = {
                "function": job.job_func.__name__,
                "interval": str(job.interval),
                "unit": job.unit,
                "at_time": job.at_time,
                "next_run": job.next_run,
            }

            if job.job_func.__name__ == "job_discovery":
                job_info["display"] = (
                    f"Discovery: Daily at {job.at_time}" if job.at_time else f"Discovery: Every {job.interval} {job.unit}"
                )
            elif job.job_func.__name__ == "job_pre_start_check":
                job_info["display"] = (
                    f"Pre-start check (+ NBA 4th quarter): Every 5 minutes at {job.at_time}"
                    if job.at_time
                    else f"Pre-start check: Every {job.interval} {job.unit}"
                )
                if job.at_time:
                    job_info["next_run"] = self._calculate_next_pre_start_time(job.at_time)
            elif job.job_func.__name__ == "job_midnight_sync":
                job_info["display"] = (
                    f"Midnight sync: Daily at {job.at_time}" if job.at_time else f"Midnight sync: Every {job.interval} {job.unit}"
                )
            elif job.job_func.__name__ == "job_daily_discovery":
                job_info["display"] = (
                    f"Daily discovery heartbeat: Every {job.interval} {job.unit}"
                )
            elif job.job_func.__name__ == "job_oddspapi_fixture_discovery":
                job_info["display"] = (
                    f"Oddspapi fixture discovery: Daily at {job.at_time}"
                    if job.at_time
                    else f"Oddspapi fixture discovery: Every {job.interval} {job.unit}"
                )
            else:
                job_info["display"] = f"{job.job_func.__name__}: Every {job.interval} {job.unit}"

            jobs.append(job_info)

        return jobs

    def _calculate_next_pre_start_time(self, at_time) -> datetime:
        now = datetime.now()

        if isinstance(at_time, str):
            target_minute = int(at_time.split(":")[1])
        elif hasattr(at_time, "minute"):
            target_minute = at_time.minute
        else:
            return now + timedelta(minutes=5)

        next_time = now.replace(minute=target_minute, second=0, microsecond=0)
        if next_time <= now:
            next_time = next_time + timedelta(hours=1)
        return next_time


job_scheduler = JobScheduler()
