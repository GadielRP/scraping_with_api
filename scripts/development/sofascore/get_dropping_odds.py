import argparse
import json
import logging
import sys

from app.logging_setup import setup_logging
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.sofascore import api_client
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Get dropping odds events and odds")
    parser.add_argument("--sport", type=str, help="The slug of the sport (e.g. football, tennis). If not specified, returns all.")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_dropping_odds_with_odds_and_events_response(sport=args.sport)
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            subfolder = SofaScoreDebugResponseWriter._filename_token(args.sport, fallback="all") if args.sport else None
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="dropping_odds",
                payload=response,
                subfolder=subfolder,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_dropping_odds", response)

        logger.info(f"Dropping odds response (sport: {args.sport or 'all'}):\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting dropping odds: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
