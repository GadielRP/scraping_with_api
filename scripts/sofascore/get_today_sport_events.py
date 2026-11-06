import argparse
import json
import logging
import sys

from app.logging_setup import setup_logging
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.sofascore import api_client
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)
from shared.temporal import now_in_timezone


def main() -> None:
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
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            safe_sport = SofaScoreDebugResponseWriter._filename_token(sport_slug, fallback="sport")
            safe_date = SofaScoreDebugResponseWriter._filename_token(date_str, fallback="date")
            filename = f"{safe_sport}_{safe_date}_events.json"
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="today_events",
                payload=response,
                filename=filename,
                subfolder=safe_sport,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_today_sport_events", response)

        logger.info(f"Scheduled events response for sport {sport_slug} on {date_str}:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting scheduled events for sport {sport_slug} on {date_str}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
