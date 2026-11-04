import argparse
import logging
from app.logging_setup import setup_logging
from infrastructure.persistence.repositories import EventRepository
from modules.sofascore import api_client, normalize_event_payload
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get event details by event ID")
    parser.add_argument("event_id", type=int, help="The SofaScore ID of the event")
    parser.add_argument("--persist", action="store_true", help="Normalize and persist the event")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_event_details(args.event_id)
        logger.info(f"Event details response for event {args.event_id}:\n")
        if args.persist and response:
            event_data = normalize_event_payload(response, discovery_source="scraping_on_command")
            persisted_event = EventRepository.upsert_event(event_data)
            if persisted_event:
                logger.info(
                    "Persisted SofaScore event %s as canonical event %s",
                    args.event_id,
                    persisted_event.id,
                )
            else:
                logger.warning("Could not persist SofaScore event %s", args.event_id)
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting event details for event {args.event_id}: {e}")
        sys.exit(1)
