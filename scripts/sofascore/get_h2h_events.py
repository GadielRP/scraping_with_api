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
    parser = argparse.ArgumentParser(description="Get H2H events by event custom ID")
    parser.add_argument("custom_id", type=str, help="The SofaScore custom ID of the event (e.g. 'nEbsGGb')")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_h2h_events_for_event(args.custom_id)
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        logger.info(f"H2H events response for custom ID {args.custom_id}:\n")
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting H2H events for custom ID {args.custom_id}: {e}")
        sys.exit(1)
