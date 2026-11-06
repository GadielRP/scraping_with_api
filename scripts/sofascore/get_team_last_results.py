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
    parser = argparse.ArgumentParser(description="Get team last results")
    parser.add_argument("team_id", type=int, help="The SofaScore ID of the team")
    parser.add_argument("--singles", action="store_true", help="Set if querying tennis singles results")
    parser.add_argument("--doubles", action="store_true", help="Set if querying tennis doubles results")
    parser.add_argument("--index", type=int, default=0, help="The batch fetch index (defaults to 0)")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_team_last_results_response(
            args.team_id,
            is_tennis_singles=args.singles,
            is_tennis_doubles=args.doubles,
            fetch_index=args.index
        )
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            filename = f"team_{args.team_id}_index_{args.index}_results.json"
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="team_last_results",
                payload=response,
                filename=filename,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_team_last_results", response)

        logger.info(f"Last results response for team {args.team_id} (singles: {args.singles}, doubles: {args.doubles}, index: {args.index}):\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting last results for team {args.team_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
