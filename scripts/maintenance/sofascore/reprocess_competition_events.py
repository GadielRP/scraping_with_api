"""Refetch and reprocess persisted SofaScore events for one competition.

Example:
    python scripts/reprocess_competition_events.py --competition_name "Premier League"
"""

import argparse
import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Event, EventSourceMapping
from infrastructure.persistence.repositories import EventRepository, ResultRepository
from infrastructure.persistence.schema_version import verify_schema_at_head
from modules.competition.competition_ids import COMPETITION_NAMES
from modules.sofascore import api_client
from modules.sofascore.results_parser import parse_event_result


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def initialize_database() -> bool:
    """Require a reachable database at the deployed Alembic revision."""
    if not db_manager.test_connection():
        logger.error("Database connection failed - cannot proceed")
        return False
    if not verify_schema_at_head(db_manager.engine):
        logger.error("Alembic schema verification failed")
        return False
    return True


def get_competition_events(
    competition_identity,
    *,
    season_id: int | None = None,
    limit: int | None = None,
) -> list[tuple[int, str]]:
    """Return canonical and SofaScore event IDs for the selected identity."""
    filters = [
        Competition.source == competition_identity.source,
        Competition.source_unique_tournament_id
        == competition_identity.source_unique_tournament_id,
    ]
    if competition_identity.source_tournament_id is not None:
        filters.append(
            Competition.source_tournament_id == competition_identity.source_tournament_id
        )

    with db_manager.get_session() as session:
        query = (
            session.query(Event.id, EventSourceMapping.source_event_id)
            .join(Competition, Competition.competition_id == Event.competition_id)
            .join(EventSourceMapping, EventSourceMapping.event_id == Event.id)
            .filter(
                *filters,
                EventSourceMapping.source == competition_identity.source,
            )
            .order_by(Event.id)
        )
        if season_id is not None:
            query = query.filter(Event.season_id == season_id)
        if limit is not None:
            query = query.limit(limit)
        rows = query.all()

    # Keep one fetch per source ID if malformed historical data has duplicate mappings.
    unique_events = {
        str(source_event_id): int(event_id)
        for event_id, source_event_id in rows
        if source_event_id is not None
    }
    return sorted(
        ((event_id, source_event_id) for source_event_id, event_id in unique_events.items()),
        key=lambda item: item[0],
    )


def reprocess_competition(
    competition_name: str,
    competition_identity,
    *,
    season_id: int | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    events = get_competition_events(
        competition_identity,
        season_id=season_id,
        limit=limit,
    )
    logger.info(
        "Reprocessing %s events for %s (unique tournament %s, tournament %s, season %s, limit %s)",
        len(events),
        competition_name,
        competition_identity.source_unique_tournament_id,
        competition_identity.source_tournament_id,
        season_id,
        limit,
    )

    fetched = normalized = persisted = results_persisted = failed = 0
    processed_canonical_event_ids: list[int] = []
    for canonical_event_id, source_event_id in events:
        try:
            response = api_client.request_json_or_none(f"/event/{source_event_id}")
            if not response or not isinstance(response.get("event"), dict):
                failed += 1
                logger.warning("No event payload returned for SofaScore event %s", source_event_id)
                continue

            raw_event = response["event"]
            fetched += 1
            event_data = api_client.normalize_event_payload(
                raw_event,
                discovery_source="scraping_on_command",
            )
            write_result = EventRepository.batch_upsert_events(
                [event_data], "sofascore", "direct", 1.0
            )
            event = write_result.events.get(str(source_event_id))
            if event is None:
                failed += 1
                logger.warning(
                    "Event %s was fetched and normalized but not persisted",
                    source_event_id,
                )
                continue

            normalized += 1
            persisted += 1
            processed_canonical_event_ids.append(event.id)
            parsed = parse_event_result({"event": raw_event})
            if parsed.kind == "finished" and parsed.result:
                confirmed = ResultRepository.batch_upsert_results(
                    [(event.id, parsed.result)]
                )
                results_persisted += len(confirmed)

        except Exception:
            failed += 1
            logger.exception(
                "Failed to reprocess canonical event %s (SofaScore %s)",
                canonical_event_id,
                source_event_id,
            )

    stats = {
        "selected": len(events),
        "fetched": fetched,
        "normalized": normalized,
        "persisted": persisted,
        "results_persisted": results_persisted,
        "failed": failed,
    }
    logger.info("Reprocessing complete for %s: %s", competition_name, stats)
    logger.info("Processed canonical Event.id values: %s", processed_canonical_event_ids)
    return stats


def main() -> int:
    name_to_identity = {
        name.casefold(): identity for identity, name in COMPETITION_NAMES.items()
    }
    parser = argparse.ArgumentParser(
        description="Refetch, normalize, and repersist saved events for a SofaScore competition."
    )
    parser.add_argument(
        "--competition_name",
        required=True,
        choices=sorted(COMPETITION_NAMES.values()),
        help="Competition name from modules/competition/competition_ids.py",
    )
    parser.add_argument(
        "--skip-db-init",
        action="store_true",
        help="Skip database connection and schema checks",
    )
    parser.add_argument(
        "--season_id",
        type=int,
        help="Process only events assigned to this SofaScore season ID",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process at most this many events, ordered by canonical event ID",
    )
    args = parser.parse_args()

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be a positive integer")

    if not args.skip_db_init and not initialize_database():
        return 1

    identity = name_to_identity[args.competition_name.casefold()]
    stats = reprocess_competition(
        args.competition_name,
        identity,
        season_id=args.season_id,
        limit=args.limit,
    )
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
