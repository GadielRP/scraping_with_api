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
    parser = argparse.ArgumentParser(description="Get nearest event for a team by team ID")
    parser.add_argument("team_id", type=int, help="The SofaScore ID of the team")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_nearest_event_for_team(args.team_id)
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        logger.info(f"Nearest event response for team {args.team_id}:\n")
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting nearest event for team {args.team_id}: {e}")
        sys.exit(1)
