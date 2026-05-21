"""Infrastructure scheduler for the SofaScore odds system."""

from __future__ import annotations

import logging
import schedule
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List

from infrastructure.persistence.repositories import EventRepository, ResultRepository
from infrastructure.settings import Config
from modules.jobs.clean_league_cache import run_clean_league_cache_job
from modules.jobs.daily_discovery import run_daily_discovery_job, run_daily_discovery_retry_job
from modules.jobs.discover_dropping_odds import run_discover_dropping_odds
from modules.jobs.discover_secondary_sources import run_discover_secondary_sources
from modules.jobs.midnight_sync_job import run_midnight_sync_job
from modules.jobs.pre_start_check_job.run_pre_start_check_job import run_pre_start_check_job
from modules.jobs.results_collection_job import (
    run_results_collection_all_finished,
    run_results_collection_for_date,
    run_results_collection_previous_day,
)

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
        schedule.every().day.at("05:21").do(self.job_daily_discovery)
        schedule.every(3).days.at("05:00").do(self.job_clean_league_cache)

        retry_interval = getattr(Config, "DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES", 60)
        schedule.every(retry_interval).minutes.do(self.job_daily_discovery_retry)

        logger.info("Jobs scheduled:")
        logger.info(f"  - Discovery: daily at {', '.join(Config.DISCOVERY_TIMES)}")
        logger.info(f"  - Discovery 2: daily at {', '.join(Config.DISCOVERY2_TIMES)}")
        logger.info(
            f"  - Pre-start check: every {Config.POLL_INTERVAL_MINUTES} minutes (includes tennis timestamp checks + NBA 4th quarter checks)"
        )
        logger.info("  - Midnight sync: daily at 04:00")
        logger.info("  - Daily discovery: daily at 05:21")
        logger.info("  - League cache cleanup: every 3 days at 05:00")
        logger.info(f"  - Daily discovery retry: every {retry_interval} minutes")

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
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        logger.info("Job scheduler started")

        logger.info("Running immediate pre-start check for any games starting soon...")
        self.job_pre_start_check()

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
                pending_jobs = schedule.run_pending()
                if pending_jobs:
                    logger.info(f"Executed {len(pending_jobs)} pending jobs")
                    for job in pending_jobs:
                        logger.info(f"  - Executed: {job.job_func.__name__}")

                current_time = time.time()
                if current_time - last_check >= 30:
                    logger.debug(
                        f"Scheduler heartbeat - {len(schedule.jobs)} jobs scheduled, next run in {schedule.idle_seconds()} seconds"
                    )
                    last_check = current_time

                time.sleep(1)
            except Exception as exc:
                logger.error(f"Error in scheduler loop: {exc}")
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
        
        debug_mode = Config.global_debug_mode
        try:
            if debug_mode:
                logger.info(f"Global debug mode set for pre start check to {debug_mode}")
            run_pre_start_check_job(self, debug_mode)
        except Exception as exc:
            logger.error(f"Error in Job C: {exc}")

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
        logger.info("Starting Job E: Daily discovery of today's scheduled events with odds")
        try:
            run_daily_discovery_job()
        except Exception as exc:
            logger.error(f"Error in Job E (Daily Discovery): {exc}")

    def job_clean_league_cache(self):
        logger.info("Starting Job F: Clean up OddsPortal league cache")
        try:
            run_clean_league_cache_job()
        except Exception as exc:
            logger.error(f"Error in Job F (Clean up OddsPortal league cache): {exc}")

    def job_daily_discovery_retry(self):
        logger.info("Starting Job E_Retry: Checking for failed daily discovery sports")
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
                    f"Daily discovery: Daily at {job.at_time}" if job.at_time else f"Daily discovery: Every {job.interval} {job.unit}"
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
