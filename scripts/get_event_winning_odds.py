from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get winning odds response by event ID")
    parser.add_argument("event_id", type=int, help="The SofaScore ID of the event")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_winning_odds_response(args.event_id)
        logger.info(f"Winning odds response for event {args.event_id}:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting winning odds for event {args.event_id}: {e}")
        sys.exit(1)
