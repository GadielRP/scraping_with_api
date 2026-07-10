"""
Backfill normalized event entity links from SofaScore /event/{event_id}.

This refetches authoritative event payloads instead of guessing participant or
competition IDs from legacy display text.
"""

from __future__ import annotations

import argparse
import logging
import time

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories import EventRepository
from modules.sofascore import api_client, normalize_event_payload
from modules.sofascore.event_identity import resolve_sofascore_event_id

logger = logging.getLogger(__name__)


def iter_event_ids(limit: int | None = None, only_missing: bool = True) -> list[int]:
    with db_manager.get_session() as session:
        query = session.query(Event.id).order_by(Event.id)
        if only_missing:
            query = query.filter(
                (Event.home_participant_id.is_(None))
                | (Event.away_participant_id.is_(None))
                | (Event.competition_id.is_(None))
            )
        if limit:
            query = query.limit(limit)
        return [row[0] for row in query.all()]


def backfill(limit: int | None = None, sleep_seconds: float = 0.5, only_missing: bool = True) -> None:
    event_ids = iter_event_ids(limit=limit, only_missing=only_missing)
    logger.info("Backfilling normalized entities for %s event(s)", len(event_ids))

    processed = 0
    skipped = 0
    failed = 0

    for event_id in event_ids:
        try:
            sofascore_event_id = resolve_sofascore_event_id(event_id)
            response = api_client._request_json(f"/event/{sofascore_event_id}", no_retry_on_404=True)
            event_response = (response or {}).get("event")
            if not event_response:
                skipped += 1
                logger.warning("No SofaScore event payload for %s", event_id)
                continue

            event_data = normalize_event_payload(event_response, discovery_source="backfill")
            updated_event = EventRepository.upsert_event(event_data)
            if updated_event:
                processed += 1
                logger.info("Backfilled event %s", event_id)
            else:
                failed += 1
                logger.warning("Failed to backfill event %s", event_id)
        except Exception as exc:
            failed += 1
            logger.error("Error backfilling event %s: %s", event_id, exc)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    logger.info("Backfill complete: processed=%s skipped=%s failed=%s", processed, skipped, failed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill event participant/competition links from SofaScore")
    parser.add_argument("--limit", type=int, default=None, help="Maximum events to process")
    parser.add_argument("--sleep", type=float, default=0.5, help="Delay between SofaScore requests")
    parser.add_argument("--all", action="store_true", help="Process all events, not just rows missing links")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    backfill(limit=args.limit, sleep_seconds=args.sleep, only_missing=not args.all)


if __name__ == "__main__":
    main()
