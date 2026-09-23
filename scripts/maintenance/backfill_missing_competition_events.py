"""
Script to update events with missing competition_id (Event.competition_id IS NULL).

Uses existing infrastructure (api_client, EventRepository, ResultRepository, event_identity)
to:
1. Fetch fresh event payloads from SofaScore.
2. Normalize participants and competition, and upsert the event.
3. Batch update results for finished events.
4. Batch delete events that are canceled/postponed.
5. Batch delete events whose /event endpoint returns 404 and have NO persisted results in the results table.

Usage:
    python -m scripts.maintenance.backfill_missing_competition_events
    python -m scripts.maintenance.backfill_missing_competition_events --limit 100 --sleep 0.2
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Dict, List, Tuple

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, Result
from infrastructure.persistence.repositories import (
    EventRepository,
    ResultRepository,
)
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_id
from modules.sofascore.exceptions import (
    SofaScoreChallengeException,
    SofaScoreNotFoundException,
    SofaScoreRateLimitException,
)

logger = logging.getLogger(__name__)


def get_events_with_missing_competition(limit: int | None = None) -> list[int]:
    """Retrieve canonical event IDs where competition_id is NULL."""
    with db_manager.get_session() as session:
        query = (
            session.query(Event.id)
            .filter(Event.competition_id.is_(None))
            .order_by(Event.id)
        )
        if limit is not None and limit > 0:
            query = query.limit(limit)
        return [row[0] for row in query.all()]


def describe_event_status(raw_event: dict) -> str:
    status = raw_event.get("status") or {}
    code = status.get("code")
    status_type = status.get("type")
    description = status.get("description")
    return f"code={code}, type={status_type}, description={description}"


def get_events_with_persisted_results(event_ids: set[int]) -> set[int]:
    """Return set of event IDs that already have valid scores/results in results table."""
    if not event_ids:
        return set()
    with db_manager.get_session() as session:
        rows = (
            session.query(Result.event_id)
            .filter(
                Result.event_id.in_(list(event_ids)),
                (
                    Result.home_score.is_not(None)
                    | Result.away_score.is_not(None)
                    | Result.winner.is_not(None)
                ),
            )
            .all()
        )
        return {row[0] for row in rows if row[0]}


def update_missing_competition_events(
    limit: int | None = None,
    sleep_seconds: float = 0.2,
    batch_flush_size: int = 100,
) -> dict[str, int]:
    """Fetch and update events whose competition_id is currently NULL."""
    event_ids = get_events_with_missing_competition(limit=limit)
    total_candidates = len(event_ids)

    logger.info("Found %s event(s) with missing competition_id", total_candidates)
    if not event_ids:
        return {
            "total": 0,
            "processed": 0,
            "results_upserted": 0,
            "canceled_deleted": 0,
            "not_found_404_deleted": 0,
            "not_found_404_preserved": 0,
            "skipped": 0,
            "failed": 0,
        }

    processed_count = 0
    skipped_count = 0
    failed_count = 0
    total_results_upserted = 0

    results_to_upsert: List[Tuple[int, Dict]] = []
    canceled_ids_to_delete: set[int] = set()
    not_found_404_ids: set[int] = set()

    for idx, event_id in enumerate(event_ids, start=1):
        try:
            sofascore_event_id = resolve_sofascore_event_id(event_id)

            try:
                response = api_client.request_json(f"/event/{sofascore_event_id}")
            except SofaScoreNotFoundException:
                logger.warning(
                    "[%s/%s] HTTP 404 from SofaScore for event_id=%s (sofascore_id=%s)",
                    idx,
                    total_candidates,
                    event_id,
                    sofascore_event_id,
                )
                not_found_404_ids.add(event_id)
                continue
            except (SofaScoreRateLimitException, SofaScoreChallengeException) as exc:
                logger.warning(
                    "[%s/%s] Rate limit/challenge for event_id=%s: %s",
                    idx,
                    total_candidates,
                    event_id,
                    exc,
                )
                skipped_count += 1
                continue

            if not response or "event" not in response:
                logger.warning(
                    "[%s/%s] No 'event' payload returned for event_id=%s (sofascore_id=%s)",
                    idx,
                    total_candidates,
                    event_id,
                    sofascore_event_id,
                )
                skipped_count += 1
                continue

            raw_event = response["event"]
            event_data = api_client.normalize_event_payload(
                raw_event, discovery_source="scraping_on_command"
            )

            # Preserve discovery_source if existing
            event_payload = dict(event_data.get("event", event_data))
            event_payload.pop("discovery_source", None)
            event_data["event"] = event_payload

            # Upsert normalized event (links competition_id, participants, and mappings)
            updated_event = EventRepository.upsert_event(event_data)
            if updated_event:
                processed_count += 1
                logger.info(
                    "[%s/%s] Updated event %s (competition_id=%s): %s vs %s",
                    idx,
                    total_candidates,
                    event_id,
                    updated_event.competition_id,
                    event_payload.get("homeTeam"),
                    event_payload.get("awayTeam"),
                )
            else:
                failed_count += 1
                logger.warning(
                    "[%s/%s] Failed to upsert event %s",
                    idx,
                    total_candidates,
                    event_id,
                )
                continue

            # Extract results
            result_data = api_client.extract_results_from_response({"event": raw_event})
            if result_data and result_data.get("_canceled"):
                canceled_ids_to_delete.add(event_id)
                logger.info(
                    "Queued canceled event %s for deletion. status=%s",
                    event_id,
                    describe_event_status(raw_event),
                )
            elif result_data:
                results_to_upsert.append((event_id, result_data))

            # Periodic batch flush of results to prevent holding too many in memory
            if len(results_to_upsert) >= batch_flush_size:
                upserted = ResultRepository.batch_upsert_results(results_to_upsert)
                total_results_upserted += upserted
                logger.info(
                    "Flushed batch of %s results (total so far: %s)",
                    upserted,
                    total_results_upserted,
                )
                results_to_upsert.clear()

        except Exception as exc:
            failed_count += 1
            logger.error("Error processing event %s: %s", event_id, exc)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    # Final flush of results
    if results_to_upsert:
        upserted = ResultRepository.batch_upsert_results(results_to_upsert)
        total_results_upserted += upserted
        logger.info("Batch upserted final %s results", upserted)
        results_to_upsert.clear()

    # Determine 404 events to delete vs preserve (preserve if they have persisted results)
    persisted_result_event_ids = get_events_with_persisted_results(not_found_404_ids)
    events_404_to_delete = not_found_404_ids - persisted_result_event_ids
    events_404_preserved = not_found_404_ids & persisted_result_event_ids

    if events_404_preserved:
        logger.info(
            "Preserved %s 404 event(s) because they have persisted results: %s",
            len(events_404_preserved),
            sorted(events_404_preserved),
        )

    # Batch delete all eligible events (canceled + 404s without results)
    all_ids_to_delete = sorted(canceled_ids_to_delete | events_404_to_delete)
    total_deleted = 0
    if all_ids_to_delete:
        total_deleted = EventRepository.batch_delete_events(all_ids_to_delete)
        logger.info(
            "Batch deleted %s event(s) (canceled=%s, 404_without_results=%s)",
            total_deleted,
            len(canceled_ids_to_delete),
            len(events_404_to_delete),
        )

    logger.info("=" * 80)
    logger.info("Update Missing Competition Events Complete!")
    logger.info("Total Candidates: %s", total_candidates)
    logger.info("Processed & Upserted: %s", processed_count)
    logger.info("Results Upserted: %s", total_results_upserted)
    logger.info("Canceled Events Queued: %s", len(canceled_ids_to_delete))
    logger.info("404 Events Without Results (Deleted): %s", len(events_404_to_delete))
    logger.info("404 Events With Results (Preserved): %s", len(events_404_preserved))
    logger.info("Total Events Deleted in Batch: %s", total_deleted)
    logger.info("Skipped: %s", skipped_count)
    logger.info("Failed: %s", failed_count)
    logger.info("=" * 80)

    return {
        "total": total_candidates,
        "processed": processed_count,
        "results_upserted": total_results_upserted,
        "canceled_deleted": len(canceled_ids_to_delete),
        "not_found_404_deleted": len(events_404_to_delete),
        "not_found_404_preserved": len(events_404_preserved),
        "total_deleted": total_deleted,
        "skipped": skipped_count,
        "failed": failed_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update events that have missing competition_id from SofaScore API and clean up 404s/canceled"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of events to process (default: all)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep delay in seconds between API calls (default: 0.2)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for intermediate results flush (default: 100)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    update_missing_competition_events(
        limit=args.limit,
        sleep_seconds=args.sleep,
        batch_flush_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
