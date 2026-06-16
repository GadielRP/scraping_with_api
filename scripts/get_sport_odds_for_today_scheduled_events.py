from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

from shared.timezone_utils import get_local_now

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get odds for a specific sport by sport slug")
    parser.add_argument("sport_slug", type=str, help="The slug of the sport to get odds for (e.g. football, tennis)")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format (default: today's local date)")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    date_str = args.date or get_local_now().strftime("%Y-%m-%d")
    sport_slug = args.sport_slug

    try:
        odds_response = api_client.get_today_sport_events_odds_response(date_str, sport_slug)
        logger.info(f"Odds response for sport {sport_slug} on {date_str}:\n")
        import json
        print(json.dumps(odds_response, indent=4))
    except Exception as e:
        logger.error(f"Error getting event odds: {e}")
        sys.exit(1)



