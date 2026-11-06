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
    parser = argparse.ArgumentParser(description="Get nearest event for a team by team ID")
    parser.add_argument("team_id", type=int, help="The SofaScore ID of the team")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_nearest_event_for_team(args.team_id)
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            filename = f"team_{args.team_id}_nearest_event.json"
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="team_nearest_event",
                payload=response,
                filename=filename,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_nearest_event_by_team_id", response)

        logger.info(f"Nearest event response for team {args.team_id}:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting nearest event for team {args.team_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
