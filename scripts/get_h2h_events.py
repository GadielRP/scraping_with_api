from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get H2H events by event custom ID")
    parser.add_argument("custom_id", type=str, help="The SofaScore custom ID of the event (e.g. 'nEbsGGb')")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_h2h_events_for_event(args.custom_id)
        logger.info(f"H2H events response for custom ID {args.custom_id}:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting H2H events for custom ID {args.custom_id}: {e}")
        sys.exit(1)
