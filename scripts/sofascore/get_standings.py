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
    parser = argparse.ArgumentParser(description="Get standings by season ID and unique tournament ID")
    parser.add_argument("season_id", type=int, help="The SofaScore season ID")
    parser.add_argument("unique_tournament_id", type=int, help="The SofaScore unique tournament ID")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_standings_response(args.season_id, args.unique_tournament_id)
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            filename = f"season_{args.season_id}_tournament_{args.unique_tournament_id}_standings.json"
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="standings",
                payload=response,
                filename=filename,
            )
        elif args.save_debug:
            debug_path = save_response_for_debug("get_standings", response)

        logger.info(f"Standings response for season {args.season_id}, tournament {args.unique_tournament_id}:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting standings for season {args.season_id}, tournament {args.unique_tournament_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
