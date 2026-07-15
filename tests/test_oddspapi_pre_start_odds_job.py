"""Manual integration harness for one event's Oddspapi pre-start odds flow.

This intentionally exercises only the persisted Oddspapi mapping, `/v4/odds`
request, market mapping, bookmaker resolution, and market/snapshot ingestion.
It does not call SofaScore, refresh materialized views, or run alerts/pillars.

Usage:
    python -m tests.test_oddspapi_pre_start_odds_job 12345
    python -m tests.test_oddspapi_pre_start_odds_job 12345 --dry-run
    python -m tests.test_oddspapi_pre_start_odds_job
"""

from __future__ import annotations

import argparse
import logging
import sys

from app.initialize import initialize_system
from app.logging_setup import setup_logging
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Market, MarketChoice, MarketChoiceSnapshot
from infrastructure.persistence.repositories import EventRepository, EventSourceMappingRepository
from infrastructure.settings import Config
from modules.jobs.oddspapi.pre_start_odds.pre_start_odds_job import (
    run_oddspapi_pre_start_odds_ingestion,
)
from modules.jobs.pre_start_check_job.timing import minutes_until_start

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

setup_logging()
logger = logging.getLogger("tests.test_oddspapi_pre_start_odds_job")


def _snapshot_count(event_id: int) -> int:
    """Count persisted Oddspapi snapshots for reporting, without modifying data."""
    with db_manager.get_session() as session:
        return (
            session.query(MarketChoiceSnapshot)
            .join(MarketChoice, MarketChoiceSnapshot.choice_id == MarketChoice.choice_id)
            .join(Market, MarketChoice.market_id == Market.market_id)
            .filter(
                Market.event_id == event_id,
                MarketChoiceSnapshot.source == "oddspapi",
            )
            .count()
        )


def _event_payload(event) -> dict:
    """Build only the fields required by the Oddspapi pre-start entrypoint."""
    return {
        "event_id": event.id,
        "event_data": {
            "id": event.id,
            "slug": event.slug,
            "sport": event.sport,
            "home_team": event.home_team,
            "away_team": event.away_team,
            "start_time_utc": event.start_time_utc,
        },
        # The production orchestrator computes this flag through its shared
        # timing logic. This focused harness forces eligibility so it can test
        # one event's Oddspapi fetch and ingestion independent of wall-clock time.
        "should_extract_odds": True,
        "minutes_until_start": minutes_until_start(event.start_time_utc),
        "metadata_snapshot": None,
    }


def run_for_event(event_id: int, *, dry_run: bool = False) -> int:
    """Run the real Oddspapi pre-start flow for one canonical ``events.id``."""
    if not initialize_system():
        logger.error("System initialization failed.")
        return 1

    event = EventRepository.get_event_by_id(event_id)
    if event is None:
        logger.error("No canonical event found for events.id=%s", event_id)
        return 1

    fixture_id = EventSourceMappingRepository.get_source_event_id(event_id, "oddspapi")
    logger.info("=" * 90)
    logger.info("Oddspapi pre-start odds integration test")
    logger.info("events.id=%s | event=%s vs %s | start=%s | minutes_until_start=%s", event.id, event.home_team, event.away_team, event.start_time_utc, minutes_until_start(event.start_time_utc))
    logger.info("existing Oddspapi fixture mapping=%s", fixture_id or "<missing>")
    logger.info(
        "mode=%s | bookmakers=%s",
        "dry-run" if dry_run else "commit",
        getattr(Config, "ODDSPAPI_PRE_START_BOOKMAKERS", None)
        or Config.ODDSPAPI_DEFAULT_BOOKMAKERS,
    )

    if not getattr(Config, "ENABLE_ODDSPAPI_PRE_START_ODDS", True):
        logger.error("ENABLE_ODDSPAPI_PRE_START_ODDS=false; enable it to run this integration test.")
        return 1
    if not str(Config.ODDSPAPI_KEY or "").strip():
        logger.error("ODDSPAPI_KEY is not configured; no HTTP request will be made.")
        return 1
    if not fixture_id:
        logger.error(
            "Missing event_source_mappings row for events.id=%s source=oddspapi. "
            "Run Oddspapi fixture discovery first.",
            event_id,
        )
        return 1

    snapshots_before = _snapshot_count(event_id)
    summary = run_oddspapi_pre_start_odds_ingestion(
        [_event_payload(event)],
        debug_mode=True,
        dry_run=dry_run,
    )
    snapshots_after = _snapshot_count(event_id)

    logger.info(
        "Result: candidates=%s mapped=%s requests=%s responses=%s ingested=%s "
        "skipped=%s failed=%s markets_saved=%s choices_saved=%s snapshots_saved=%s",
        summary.candidates_seen,
        summary.candidates_with_mapping,
        summary.requests_attempted,
        summary.responses_received,
        summary.events_ingested,
        summary.events_skipped,
        summary.events_failed,
        summary.markets_saved,
        summary.choices_saved,
        summary.snapshots_saved,
    )
    for result in summary.results:
        logger.info(
            "Event result: event_id=%s fixture_id=%s requested=%s skipped=%s "
            "reason=%s error=%s markets_saved=%s snapshots_saved=%s",
            result.event_id,
            result.fixture_id,
            result.requested,
            result.skipped,
            result.skip_reason,
            result.error,
            result.markets_saved,
            result.snapshots_saved,
        )
    logger.info(
        "Persisted Oddspapi snapshots: before=%s after=%s delta=%s",
        snapshots_before,
        snapshots_after,
        snapshots_after - snapshots_before,
    )

    if summary.events_failed or summary.events_ingested != 1:
        logger.error("Oddspapi pre-start odds integration test did not ingest the event successfully.")
        return 1
    return 0


def _parse_event_id(value: str | None) -> int:
    if value is None:
        value = input("Canonical events.id to test: ").strip()
    try:
        event_id = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("events.id must be a positive integer") from exc
    if event_id <= 0:
        raise argparse.ArgumentTypeError("events.id must be a positive integer")
    return event_id


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch and ingest Oddspapi odds for one canonical events.id only.",
    )
    parser.add_argument(
        "event_id",
        nargs="?",
        help="Canonical events.id. If omitted, the command prompts for it.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse the response without persisting markets or snapshots.",
    )
    args = parser.parse_args()
    try:
        event_id = _parse_event_id(args.event_id)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    return run_for_event(event_id, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
