from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get live events per sport by sport slug")
    parser.add_argument("sport_slug", type=str, help="The slug of the sport (e.g. football, tennis)")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_live_events_response_per_sport(args.sport_slug)
        logger.info(f"Live events response for sport {args.sport_slug}:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting live events for sport {args.sport_slug}: {e}")
        sys.exit(1)
