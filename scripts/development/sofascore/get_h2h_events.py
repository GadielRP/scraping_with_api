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
    parser = argparse.ArgumentParser(description="Get H2H events by event custom ID")
    parser.add_argument("custom_id", type=str, help="The SofaScore custom ID of the event (e.g. 'nEbsGGb')")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_h2h_events_for_event(args.custom_id)
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            safe_id = SofaScoreDebugResponseWriter._filename_token(args.custom_id, fallback="custom_id")
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="h2h_events",
                payload=response,
                filename=f"{safe_id}_h2h_events.json",
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_h2h_events", response)

        logger.info(f"H2H events response for custom ID {args.custom_id}:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting H2H events for custom ID {args.custom_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
