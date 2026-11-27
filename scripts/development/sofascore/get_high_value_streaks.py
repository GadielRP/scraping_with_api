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
    parser = argparse.ArgumentParser(description="Get high value streaks events")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_high_value_streaks_events()
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="high_value_streaks",
                payload=response,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_high_value_streaks", response)

        logger.info("High value streaks response:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting high value streaks: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
