from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get event details by event ID")
    parser.add_argument("event_id", type=int, help="The SofaScore ID of the event")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_event_details(args.event_id)
        logger.info(f"Event details response for event {args.event_id}:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting event details for event {args.event_id}: {e}")
        sys.exit(1)
