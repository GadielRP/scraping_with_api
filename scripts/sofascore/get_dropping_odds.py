from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get dropping odds events and odds")
    parser.add_argument("--sport", type=str, help="The slug of the sport (e.g. football, tennis). If not specified, returns all.")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_dropping_odds_with_odds_and_events_response(sport=args.sport)
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        logger.info(f"Dropping odds response (sport: {args.sport or 'all'}):\n")
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting dropping odds: {e}")
        sys.exit(1)
