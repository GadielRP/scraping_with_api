#!/usr/bin/env python3
"""Backfill missing SofaScore results for mature, source-mapped events.

Examples:
    python -m scripts.backfill.maintenance.backfill_missing_sofascore_results
    python -m scripts.backfill.maintenance.backfill_missing_sofascore_results --limit 250 --dry-run
    python -m scripts.backfill.maintenance.backfill_missing_sofascore_results --min-age-days 0 --after-event-id 120000

The SQL candidate query is capped by ``--limit`` before ORM event rows are
loaded. Use ``--after-event-id`` to continue through a large candidate set
without repeatedly selecting the same unresolved events.
"""

from __future__ import annotations

import argparse
import logging
from modules.events.discards.contracts import DeletionBatch
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import and_, exists, func, or_

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, EventSourceMapping, Result
from infrastructure.persistence.repositories import EventRepository, ResultRepository
from modules.observations import sport_observation_service
from modules.sofascore import api_client
from shared.temporal import utc_now

logger = logging.getLogger("backfill_missing_sofascore_results")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum candidate events selected by the initial SQL query (default: 1000).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Maximum events processed and persisted in each batch (default: 100).",
    )
    parser.add_argument(
        "--min-age-days",
        type=int,
        default=30,
        help=(
            "Only consider events that started at least this many days ago, in addition "
            "to the sport-specific finish-time buffer (default: 30). Set to 0 to include "
            "all events that should have finished."
        ),
    )
    parser.add_argument(
        "--after-event-id",
        type=int,
        default=0,
        help="Only select event IDs greater than this value; use the logged cursor to continue.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Call SofaScore and report would-be upserts/deletions without persisting them.",
    )
    parser.add_argument(
        "--show-raw-score-status",
        action="store_true",
        help=(
            "For responses without a parsed result, log the raw SofaScore status, "
            "homeScore, awayScore, and winnerCode fields."
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be greater than zero")
    if args.batch_size < 1:
        parser.error("--batch-size must be greater than zero")
    if args.min_age_days < 0:
        parser.error("--min-age-days cannot be negative")
    if args.after_event_id < 0:
        parser.error("--after-event-id cannot be negative")
    return args


def _finished_event_filter(now):
    """Mirror EventRepository.get_all_finished_events' sport-specific buffer."""
    return or_(
        and_(
            Event.sport.in_(["Football", "Futsal"]),
            Event.starts_at < now - timedelta(hours=2, minutes=30),
        ),
        and_(
            Event.sport.in_(["Tennis", "Baseball"]),
            Event.starts_at < now - timedelta(hours=4),
        ),
        and_(Event.sport == "Basketball", Event.starts_at < now - timedelta(hours=3)),
        and_(
            ~Event.sport.in_(["Football", "Futsal", "Tennis", "Baseball", "Basketball"]),
            Event.starts_at < now - timedelta(hours=3),
        ),
    )


def _select_candidates(*, limit: int, min_age_days: int, after_event_id: int):
    """Select at most ``limit`` rows in SQL before loading ORM event objects."""
    now = utc_now()
    mature_event = _finished_event_filter(now)
    age_cutoff = now - timedelta(days=min_age_days)
    has_complete_result = exists().where(
        and_(
            Result.event_id == Event.id,
            Result.home_score.is_not(None),
            Result.away_score.is_not(None),
        )
    )

    with db_manager.get_session() as session:
        # One canonical event may have multiple SofaScore mappings. Pick one
        # stable mapping per event, then cap this SQL result before ORM loading.
        candidate_ids = (
            session.query(
                Event.id.label("event_id"),
                func.min(EventSourceMapping.mapping_id).label("mapping_id"),
            )
            .join(EventSourceMapping, EventSourceMapping.event_id == Event.id)
            .filter(
                EventSourceMapping.source == "sofascore",
                Event.id > after_event_id,
                Event.starts_at <= age_cutoff,
                mature_event,
                ~has_complete_result,
            )
            .group_by(Event.id)
            .order_by(Event.id)
            .limit(limit)
            .subquery()
        )
        rows = (
            session.query(Event, EventSourceMapping.source_event_id)
            .join(candidate_ids, candidate_ids.c.event_id == Event.id)
            .join(
                EventSourceMapping,
                EventSourceMapping.mapping_id == candidate_ids.c.mapping_id,
            )
            .order_by(Event.id)
            .all()
        )

    return rows


def _process_batch(
    rows: list[tuple[Event, str]],
    *,
    dry_run: bool,
    show_raw_score_status: bool = False,
) -> dict[str, int]:
    stats = {
        "results_saved": 0,
        "results_would_save": 0,
        "events_deleted": 0,
        "events_would_delete": 0,
        "no_result_response": 0,
        "errors": 0,
    }
    deferred_deletion_ids = DeletionBatch(origin="backfill_missing_sofascore_results")
    results_to_upsert: list[tuple[int, dict[str, Any]]] = []
    observations_to_process: list[tuple[Event, dict[str, Any]]] = []

    for event, source_event_id in rows:
        try:
            result_data = api_client.get_event_results(
                int(source_event_id),
                canonical_event_id=event.id,
                deferred_deletion_event_ids=deferred_deletion_ids,
                on_not_started="delete",
                update_event_info=not dry_run,
                log_result_diagnostics=show_raw_score_status,
            )
            if not result_data:
                if event.id not in deferred_deletion_ids:
                    stats["no_result_response"] += 1
                continue

            results_to_upsert.append((event.id, result_data))
            observations_to_process.append((event, result_data))
        except (TypeError, ValueError) as exc:
            logger.error(
                "Invalid SofaScore event ID for canonical event %s (%r): %s",
                event.id,
                source_event_id,
                exc,
            )
            stats["errors"] += 1
        except Exception:
            logger.exception("Failed to backfill result for event %s", event.id)
            stats["errors"] += 1

    if results_to_upsert:
        if dry_run:
            stats["results_would_save"] = len(results_to_upsert)
        else:
            saved_count = len(ResultRepository.batch_upsert_results(results_to_upsert))
            stats["results_saved"] = saved_count
            if saved_count != len(results_to_upsert):
                stats["errors"] += len(results_to_upsert) - saved_count
            if saved_count:
                saved_ids = {event_id for event_id, _ in results_to_upsert}
                for event, result_data in observations_to_process:
                    if event.id not in saved_ids:
                        continue
                    try:
                        sport_observation_service.process_result_observations(
                            event,
                            result_data,
                        )
                    except Exception:
                        logger.exception(
                            "Failed to process result observations for event %s",
                            event.id,
                        )

    if deferred_deletion_ids:
        if dry_run:
            stats["events_would_delete"] = len(deferred_deletion_ids)
        else:
            stats["events_deleted"] = int(
                EventRepository.batch_delete_events(deferred_deletion_ids) or 0
            )
            if stats["events_deleted"] < len(deferred_deletion_ids):
                stats["errors"] += len(deferred_deletion_ids) - stats["events_deleted"]

    return stats


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    rows = _select_candidates(
        limit=args.limit,
        min_age_days=args.min_age_days,
        after_event_id=args.after_event_id,
    )
    logger.info(
        "Selected %s missing-result events (SQL limit=%s, min_age_days=%s, after_event_id=%s)",
        len(rows),
        args.limit,
        args.min_age_days,
        args.after_event_id,
    )
    if not rows:
        return 0

    logger.info("Highest selected event_id cursor: %s", max(event.id for event, _ in rows))
    totals = {
        "results_saved": 0,
        "results_would_save": 0,
        "events_deleted": 0,
        "events_would_delete": 0,
        "no_result_response": 0,
        "errors": 0,
    }
    for offset in range(0, len(rows), args.batch_size):
        batch = rows[offset : offset + args.batch_size]
        batch_stats = _process_batch(
            batch,
            dry_run=args.dry_run,
            show_raw_score_status=args.show_raw_score_status,
        )
        for key, value in batch_stats.items():
            totals[key] += value
        logger.info(
            "Batch %s-%s/%s: %s",
            offset + 1,
            offset + len(batch),
            len(rows),
            batch_stats,
        )

    logger.info("Backfill finished (dry_run=%s): %s", args.dry_run, totals)
    return 1 if totals["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
