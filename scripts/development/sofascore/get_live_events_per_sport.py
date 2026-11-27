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
    parser = argparse.ArgumentParser(description="Get live events per sport by sport slug")
    parser.add_argument("sport_slug", type=str, help="The slug of the sport (e.g. football, tennis)")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_live_events_response_per_sport(args.sport_slug)
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            safe_sport = SofaScoreDebugResponseWriter._filename_token(args.sport_slug, fallback="sport")
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="live_events",
                payload=response,
                subfolder=safe_sport,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_live_events_per_sport", response)

        logger.info(f"Live events response for sport {args.sport_slug}:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting live events for sport {args.sport_slug}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
