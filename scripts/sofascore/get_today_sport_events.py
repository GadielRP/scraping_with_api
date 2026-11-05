from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

from infrastructure.settings import Config
from shared.temporal import now_in_timezone
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get scheduled events for today for a specific sport by sport slug")
    parser.add_argument("sport_slug", type=str, help="The slug of the sport (e.g. football, tennis)")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format (default: today's local date)")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    date_str = args.date or now_in_timezone(Config.TIMEZONE).strftime("%Y-%m-%d")
    sport_slug = args.sport_slug

    try:
        response = api_client.get_today_sport_events_response(date_str, sport_slug)
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        logger.info(f"Scheduled events response for sport {sport_slug} on {date_str}:\n")
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting scheduled events for sport {sport_slug} on {date_str}: {e}")
        sys.exit(1)
