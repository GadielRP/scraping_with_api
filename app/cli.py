import argparse
import json
import logging
import signal
import sys
import time
from datetime import date

from infrastructure.settings import Config
from modules.oddsportal.scraping_settings import ODDSPORTAL_SCRAPING_SETTINGS
from shared.shutdown import clear_shutdown_request, request_shutdown, is_shutdown_requested
from shared.runtime_observability import (
    mark_clean_shutdown,
    start_runtime_observability,
)

from .logging_setup import setup_logging


def _run_oddspapi_fixture_discovery(args):
    """Run Oddspapi fixture discovery for a UTC day from the application CLI."""
    logger = logging.getLogger(__name__)
    logger.info("Running Oddspapi fixture discovery command...")

    from app.runtime import ApplicationRuntime
    from modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery import _resolve_sports

    sports = _resolve_sports(args.sports)
    summary = ApplicationRuntime().run(
        "fixtures",
        target_date=args.date,
        lookahead_days=args.lookahead_days,
        sports=sports,
        create_mappings=bool(args.commit and not args.dry_run),
        persist_queue=bool(args.persist_queue and args.commit and not args.dry_run),
        max_fixtures_per_sport=args.max_fixtures_per_sport,
    )
    if summary is None:
        logger.info(
            "Oddspapi fixture discovery skipped because the target UTC day "
            "already has a successful or currently running durable run"
        )
        return
    if args.log_json:
        print(json.dumps(summary.to_dict(), default=lambda value: value.isoformat()))
    else:
        print(
            "Oddspapi fixture discovery complete: "
            f"fixtures={summary.total_fixtures_fetched} "
            f"mappings_created={summary.total_mappings_created}"
        )


def _start_scheduler():
    from app.runtime import ApplicationRuntime

    runtime = ApplicationRuntime()
    try:
        runtime.start()
        print("Scheduler running. Press Ctrl+C to stop.")
        while not is_shutdown_requested():
            time.sleep(1)
    finally:
        runtime.close()


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
            "oddspapi-fixture-discovery",
            "backfill-results",
            "status",
            "events",
            "refresh-alerts",
        ],
        help="Command to run",
    )
    parser.add_argument("--limit", type=int, default=10, help="Limit for events display")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Calendar date yyyy-mm-dd (local for results-date; UTC for Oddspapi discovery)",
    )
    parser.add_argument(
        "--sports",
        type=str,
        default=None,
        help="Comma-separated Oddspapi sport slugs",
    )
    parser.add_argument(
        "--lookahead-days",
        type=int,
        default=1,
        help="Oddspapi discovery days starting at the UTC day boundary",
    )
    discovery_mode = parser.add_mutually_exclusive_group()
    discovery_mode.add_argument("--dry-run", action="store_true")
    discovery_mode.add_argument("--commit", action="store_true")
    parser.add_argument("--persist-queue", action="store_true")
    parser.add_argument("--max-fixtures-per-sport", type=int, default=None)
    parser.add_argument("--log-json", action="store_true")
    return parser


def _run_command(args):
    from app.runtime import ApplicationRuntime
    from .commands import run_backfill_results, show_events, show_status
    from .initialize import initialize_system

    target_date = None
    if args.command == "results-date":
        if not args.date:
            raise ValueError("results-date requires --date yyyy-mm-dd")
        target_date = date.fromisoformat(args.date)
    if not initialize_system():
        raise RuntimeError("Failed to initialize system")

    jobs = {
        "discovery": "discovery",
        "discovery2": "discovery2",
        "pre-start": "pre_start",
        "midnight": "midnight",
        "results": "results",
        "results-all": "results_all",
        "daily-discovery": "daily",
    }
    if args.command in jobs:
        ApplicationRuntime().run(jobs[args.command])
    elif args.command == "results-date":
        ApplicationRuntime().run("results_date", target_date=target_date)
    elif args.command == "start":
        _start_scheduler()
    elif args.command == "oddspapi-fixture-discovery":
        _run_oddspapi_fixture_discovery(args)
    elif args.command == "backfill-results":
        run_backfill_results(args.limit)
    elif args.command == "status":
        show_status()
    elif args.command == "events":
        show_events(args.limit)
    elif args.command == "refresh-alerts":
        summary = ApplicationRuntime().run("view_refresh", force=True, request=True)
        if summary["failed"]:
            raise RuntimeError(f"View refresh incomplete: {summary}")
        logging.getLogger(__name__).info("Alert data refresh summary=%s", summary)


def main():
    """Main entry point for the CLI."""
    setup_logging()
    start_runtime_observability()
    clear_shutdown_request()

    logger = logging.getLogger(__name__)
    logger.info(
        "OddsPortal config: enabled=%s parallel_browsers=%s "
        "block_resources=%s language=%s domain=%s "
        "alert_wait_timeout_s=%s "
        "proxy_enabled=%s proxy_endpoint_set=%s",
        Config.ODDSPORTAL_SCRAPING_ENABLED,
        Config.ODDSPORTAL_PARALLEL_BROWSERS,
        ODDSPORTAL_SCRAPING_SETTINGS.browser.block_resources,
        ODDSPORTAL_SCRAPING_SETTINGS.ui_language,
        ODDSPORTAL_SCRAPING_SETTINGS.domain,
        Config.ODDSPORTAL_ALERT_WAIT_TIMEOUT,
        Config.PROXY_ENABLED,
        bool(getattr(Config, "PROXY_ENDPOINT", "")),
        extra={"oddsportal": True},
    )
    logger.info(f"Time corrections config: enabled={Config.ENABLE_TIMESTAMP_CORRECTION}")

    parser = _build_parser()
    args = parser.parse_args()

    logger.info(f"Starting SofaScore Odds System with command: {args.command}")

    def _handle_shutdown_signal(signum, frame):
        request_shutdown()
        logger.info("Shutdown signal received (%s). Stopping command: %s", signum, args.command)

    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)

    completed_cleanly = False
    try:
        _run_command(args)
        completed_cleanly = True
    except KeyboardInterrupt:
        logger.info("Shutdown requested via Ctrl+C. Stopping command: %s", args.command)
        sys.exit(130)
    except Exception as exc:
        logger.error(f"Error running command {args.command}: {exc}")
        sys.exit(1)
    finally:
        if completed_cleanly or is_shutdown_requested():
            mark_clean_shutdown()
        if is_shutdown_requested():
            sys.exit(130)
