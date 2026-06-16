from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get event odds by event ID")
    parser.add_argument("event_id", type=int, help="The ID of the event to get odds for")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        odds_response = api_client.get_event_final_odds(args.event_id)
        logger.info(f"Odds for event {args.event_id}:\n")
        # pretty print dictionary with indentation
        import json
        print(json.dumps(odds_response, indent=4))
    except Exception as e:
        logger.error(f"Error getting event odds: {e}")
        sys.exit(1)
