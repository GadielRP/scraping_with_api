from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get dropping odds events and odds")
    parser.add_argument("--sport", type=str, help="The slug of the sport (e.g. football, tennis). If not specified, returns all.")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_dropping_odds_with_odds_and_events_response(sport=args.sport)
        logger.info(f"Dropping odds response (sport: {args.sport or 'all'}):\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting dropping odds: {e}")
        sys.exit(1)
