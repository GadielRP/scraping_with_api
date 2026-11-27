import argparse
import json
import logging
import sys

from app.logging_setup import setup_logging
from infrastructure.persistence.repositories import EventRepository
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.sofascore import api_client, normalize_event_payload
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Get event details by event ID")
    parser.add_argument(
        "event_id",
        type=int,
        help="SofaScore event ID by default, or canonical event ID with --source canonical",
    )
    parser.add_argument(
        "--source",
        choices=["canonical", "sofascore"],
        default="sofascore",
        help="Interpret event_id as a SofaScore or canonical ID",
    )
    parser.add_argument("--persist", action="store_true", help="Normalize and persist the event")
    add_debug_file_argument(parser)
    args = parser.parse_args()
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        identity = resolve_sofascore_event_identity(
            args.event_id,
            input_id_type=args.source,
        )
        response = api_client.get_event_details(identity.sofascore_event_id)
        logger.info(f"Event details response for event {args.event_id} (source={args.source}):\n")
        if args.persist and response:
            event_data = normalize_event_payload(response)
            persisted_event = EventRepository.upsert_event(event_data)
            if persisted_event:
                logger.info(
                    "Persisted SofaScore event %s as canonical event %s",
                    identity.sofascore_event_id,
                    persisted_event.id,
                )
            else:
                logger.warning("Could not persist SofaScore event %s", identity.sofascore_event_id)

        debug_path = None
        if args.save_debug and isinstance(response, dict):
            event = response.get("event") if isinstance(response.get("event"), dict) else response
            tournament = event.get("tournament") or {}
            unique_tournament = tournament.get("uniqueTournament") or {}
            category = tournament.get("category") or {}
            sport = (category.get("sport") or {}).get("name")
            debug_path = SofaScoreDebugResponseWriter.save_response(
                response_type="events",
                event_id=identity.canonical_event_id or identity.sofascore_event_id,
                payload=response,
                competition_slug=(
                    unique_tournament.get("slug")
                    or tournament.get("slug")
                    or unique_tournament.get("name")
                    or tournament.get("name")
                ),
                sport=sport,
                home_participant=(event.get("homeTeam") or {}).get("name"),
                away_participant=(event.get("awayTeam") or {}).get("name"),
                event_label=event.get("slug"),
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_event_details", response)

        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting event details for event {args.event_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
