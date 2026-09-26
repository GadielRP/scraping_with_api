#!/usr/bin/env python3
"""Backfill markets for mapped events that currently have no market rows.

Examples:
    python scripts/backfill/maintenance/backfill_missing_sofascore_markets.py
    python scripts/backfill/maintenance/backfill_missing_sofascore_markets.py --source sofascore --limit 100
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import exists, func

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, EventSourceMapping
from infrastructure.persistence.odds_models import Market
from infrastructure.persistence.repositories import EventOddsSourceState
from modules.jobs.pre_start_check_job.odds_source_state import SOFASCORE_SOURCE
from modules.jobs.pre_start_check_job.providers.sofascore.odds_phase import (
    run_sofascore_pre_start_odds,
)

logger = logging.getLogger("backfill_missing_sofascore_markets")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch SofaScore odds for events with a true provider mapping and "
            "no rows in markets."
        )
    )
    parser.add_argument(
        "--source",
        default=SOFASCORE_SOURCE,
        help=f"event_source_mappings.source to select (default: {SOFASCORE_SOURCE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process at most this many events (useful for a small first run).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of candidates sent through the existing ingestion phase per batch.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be greater than zero")
    if args.batch_size < 1:
        parser.error("--batch-size must be greater than zero")
    args.source = args.source.strip().lower()
    if not args.source:
        parser.error("--source cannot be empty")
    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    has_market = exists().where(Market.event_id == Event.id)
    with db_manager.get_session() as session:
        # Select and limit distinct canonical events in SQL before loading ORM
        # objects. One event can have multiple mappings for the same source.
        candidate_mappings = (
            session.query(
                Event.id.label("event_id"),
                func.min(EventSourceMapping.mapping_id).label("mapping_id"),
            )
            .join(EventSourceMapping, EventSourceMapping.event_id == Event.id)
            .filter(
                EventSourceMapping.source == args.source,
                EventSourceMapping.has_odds.is_(True),
                ~has_market,
            )
            .group_by(Event.id)
            .order_by(Event.id)
        )
        if args.limit is not None:
            candidate_mappings = candidate_mappings.limit(args.limit)
        candidate_mappings = candidate_mappings.subquery()

        rows = (
            session.query(Event, EventSourceMapping)
            .join(candidate_mappings, candidate_mappings.c.event_id == Event.id)
            .join(
                EventSourceMapping,
                EventSourceMapping.mapping_id == candidate_mappings.c.mapping_id,
            )
            .order_by(Event.id)
            .all()
        )

    # The SQL query already chose one stable mapping per canonical event.
    selected: dict[int, tuple[Event, EventSourceMapping]] = {}
    for event, mapping in rows:
        selected.setdefault(event.id, (event, mapping))

    logger.info(
        "Selected %s events with source=%s and no markets",
        len(selected),
        args.source,
    )
    if not selected:
        return 0

    pending = list(selected.values())
    totals = {
        "requests_attempted": 0,
        "events_ingested": 0,
        "events_skipped": 0,
        "events_failed": 0,
        "missing_endpoints": 0,
        "markets_saved": 0,
    }

    for offset in range(0, len(pending), args.batch_size):
        batch = pending[offset : offset + args.batch_size]
        candidates: list[dict] = []
        source_states: dict[int, dict[str, EventOddsSourceState]] = {}
        invalid_source_ids = 0

        for event, mapping in batch:
            event_id = int(event.id)
            source_event_id = str(mapping.source_event_id)
            try:
                sofascore_event_id = int(source_event_id)
            except (TypeError, ValueError):
                invalid_source_ids += 1
                logger.warning(
                    "Skipping event %s: source=%s event id %r is not numeric",
                    event_id,
                    args.source,
                    source_event_id,
                )
                continue

            event_data = {
                "id": event_id,
                "slug": event.slug,
                "starts_at": event.starts_at,
                "sport": event.sport,
                "competition_id": event.competition_id,
                "home_team": event.home_team,
                "away_team": event.away_team,
            }
            candidates.append(
                {
                    "event_id": event_id,
                    "event_data": event_data,
                    "minutes_until_start": None,
                    "should_extract_odds": True,
                    "original_start_time": event.starts_at,
                    "metadata_snapshot": None,
                    "sofascore_event_id": sofascore_event_id,
                }
            )
            # Keep the mapping source configurable through the existing
            # SofaScore phase and its existing availability bookkeeping.
            source_states[event_id] = {
                args.source: EventOddsSourceState(
                    event_id=event_id,
                    source=args.source,
                    source_event_id=source_event_id,
                    has_odds=True,
                    source_sport_id=mapping.source_sport_id,
                )
            }

        summary = run_sofascore_pre_start_odds(
            candidates,
            source_states,
            source=args.source,
        )
        for name in totals:
            totals[name] += getattr(summary, name)
        logger.info(
            "Batch %s-%s/%s: requests=%s ingested=%s markets_saved=%s skipped=%s failed=%s invalid_source_ids=%s",
            offset + 1,
            offset + len(batch),
            len(pending),
            summary.requests_attempted,
            summary.events_ingested,
            summary.markets_saved,
            summary.events_skipped,
            summary.events_failed,
            invalid_source_ids,
        )

    logger.info("Backfill finished: %s", totals)
    return 0 if totals["events_failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
