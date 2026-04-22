import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timedelta

from infrastructure.settings import Config

from .logging_setup import setup_logging


def run_discovery():
    """Run event discovery job."""
    logger = logging.getLogger(__name__)
    logger.info("Running event discovery...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_discovery_now()


def run_discovery2():
    """Run event discovery 2 job."""
    logger = logging.getLogger(__name__)
    logger.info("Running event discovery 2 (streaks, h2h, winning odds)...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_discovery2_now()


def run_pre_start_check():
    """Run pre-start check job."""
    logger = logging.getLogger(__name__)
    logger.info("Running pre-start check...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_pre_start_check_now()


def run_midnight_sync():
    """Run midnight results collection job."""
    logger = logging.getLogger(__name__)
    logger.info("Running midnight results collection...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_midnight_sync_now()


def run_results_collection():
    """Run results collection job."""
    logger = logging.getLogger(__name__)
    logger.info("Running results collection...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_results_collection_now()


def run_results_collection_all():
    """Run comprehensive results collection for all finished events."""
    logger = logging.getLogger(__name__)
    logger.info("Running comprehensive results collection...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_results_collection_all_now()


def run_results_for_date(date_str: str):
    """Run results collection for a specific date (yyyy-mm-dd)."""
    logger = logging.getLogger(__name__)

    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid date format: '{date_str}'. Use yyyy-mm-dd format.")
        sys.exit(1)

    logger.info(f"Running results collection for date: {target_date}")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_results_collection_for_date_now(target_date)


def run_daily_discovery():
    """Run daily discovery job."""
    logger = logging.getLogger(__name__)
    logger.info("Running daily discovery (today's scheduled events with odds)...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.run_job_daily_discovery_now()


def start_scheduler():
    """Start the job scheduler."""
    logger = logging.getLogger(__name__)
    logger.info("Starting job scheduler...")

    from infrastructure.scheduler import job_scheduler

    job_scheduler.start()

    print("\nSofaScore Odds System Started Successfully!")
    print("=" * 50)
    print("Scheduled Jobs:")
    print(f"  - Discovery (dropping odds): Daily at {', '.join(Config.DISCOVERY_TIMES)}")
    print(
        f"  - Discovery 2 (streaks, top team streaks, h2h, winning odds): Daily at {', '.join(Config.DISCOVERY2_TIMES)}"
    )

    interval_minutes = Config.POLL_INTERVAL_MINUTES
    intervals_per_hour = 60 // interval_minutes
    _fixed_times = [f"{i * interval_minutes:02d}" for i in range(intervals_per_hour)]

    print(
        "  - Pre-start check: Every 5 minutes at clock intervals "
        "(00, 05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)"
    )
    print("    - Scans for games starting within 30 minutes")
    print("    - Fetches final odds only when games are starting soon")

    print("  - Results collection: Daily at 00:05 (collect results from finished games)")
    print("  - Daily discovery: Daily at 05:21 (today's scheduled events with odds)")
    print("\nNext Discovery Run:")

    now = datetime.now()
    next_discovery = None

    for time_str in Config.DISCOVERY_TIMES:
        hour, minute = map(int, time_str.split(":"))
        next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_time <= now:
            next_time += timedelta(days=1)
        if next_discovery is None or next_time < next_discovery:
            next_discovery = next_time

    if next_discovery:
        print(f"  - {next_discovery.strftime('%Y-%m-%d %H:%M:%S')}")

    print("\nSystem is running in background. Press Ctrl+C to stop.")
    print("=" * 50)

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal, stopping scheduler...")
        job_scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        job_scheduler.stop()


def _build_parser():
    parser = argparse.ArgumentParser(description="SofaScore Odds Alert System")
    parser.add_argument(
        "command",
        choices=[
            "start",
            "discovery",
            "discovery2",
            "pre-start",
            "midnight",
            "results",
            "results-date",
            "results-all",
            "daily-discovery",
            "backfill-results",
            "status",
            "events",
            "alerts",
            "refresh-alerts",
        ],
        help="Command to run",
    )
    parser.add_argument("--limit", type=int, default=10, help="Limit for events display")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in yyyy-mm-dd format (for results-date command)",
    )
    return parser


def _run_command(args):
    from .commands import (
        refresh_alert_data,
        run_alerts,
        run_backfill_results,
        show_events,
        show_status,
    )
    from .initialize import initialize_system

    if args.command == "results-date" and not args.date:
        logging.getLogger(__name__).error(
            "--date argument is required for results-date command (format: yyyy-mm-dd)"
        )
        sys.exit(1)

    if not initialize_system():
        logging.getLogger(__name__).error("Failed to initialize system")
        sys.exit(1)

    if args.command == "start":
        start_scheduler()
    elif args.command == "discovery":
        run_discovery()
    elif args.command == "discovery2":
        run_discovery2()
    elif args.command == "pre-start":
        run_pre_start_check()
    elif args.command == "midnight":
        run_midnight_sync()
    elif args.command == "results":
        run_results_collection()
    elif args.command == "results-date":
        run_results_for_date(args.date)
    elif args.command == "results-all":
        run_results_collection_all()
    elif args.command == "daily-discovery":
        run_daily_discovery()
    elif args.command == "backfill-results":
        run_backfill_results(args.limit)
    elif args.command == "status":
        show_status()
    elif args.command == "events":
        show_events(args.limit)
    elif args.command == "alerts":
        run_alerts()
    elif args.command == "refresh-alerts":
        refresh_alert_data()


def main():
    """Main entry point for the CLI."""
    setup_logging()

    logger = logging.getLogger(__name__)
    logger.info(
        f"OddsPortal config: parallel_browsers={Config.ODDSPORTAL_PARALLEL_BROWSERS} block_resources={Config.ODDSPORTAL_BLOCK_RESOURCES} previous_cycle_timeout_s={Config.ODDSPORTAL_PREVIOUS_CYCLE_TIMEOUT} "
        f"alert_wait_timeout_s={getattr(Config, 'ODDSPORTAL_ALERT_WAIT_TIMEOUT', 180)} proxy_enabled={Config.PROXY_ENABLED} proxy_endpoint_set={bool(getattr(Config, 'PROXY_ENDPOINT', ''))}"
    )
    logger.info(f"Time corrections config: enabled={Config.ENABLE_TIMESTAMP_CORRECTION}")

    parser = _build_parser()
    args = parser.parse_args()

    logger.info(f"Starting SofaScore Odds System with command: {args.command}")

    try:
        _run_command(args)
    except Exception as exc:
        logger.error(f"Error running command {args.command}: {exc}")
        sys.exit(1)


__all__ = [
    "main",
    "run_daily_discovery",
    "run_discovery",
    "run_discovery2",
    "run_midnight_sync",
    "run_pre_start_check",
    "run_results_collection",
    "run_results_collection_all",
    "run_results_for_date",
    "start_scheduler",
]
