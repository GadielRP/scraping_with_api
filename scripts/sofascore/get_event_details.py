import argparse
import logging
from app.logging_setup import setup_logging
from infrastructure.persistence.repositories import EventRepository
from modules.sofascore import api_client, normalize_event_payload
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)
import sys

if __name__ == "__main__":
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
            event_data = normalize_event_payload(response, discovery_source="scraping_on_command")
            persisted_event = EventRepository.upsert_event(event_data)
            if persisted_event:
                logger.info(
                    "Persisted SofaScore event %s as canonical event %s",
                    identity.sofascore_event_id,
                    persisted_event.id,
                )
            else:
                logger.warning("Could not persist SofaScore event %s", identity.sofascore_event_id)
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting event details for event {args.event_id}: {e}")
        sys.exit(1)
