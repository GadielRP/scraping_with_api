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
    parser = argparse.ArgumentParser(description="Get standings by season ID and unique tournament ID")
    parser.add_argument("season_id", type=int, help="The SofaScore season ID")
    parser.add_argument("unique_tournament_id", type=int, help="The SofaScore unique tournament ID")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_standings_response(args.season_id, args.unique_tournament_id)
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        logger.info(f"Standings response for season {args.season_id}, tournament {args.unique_tournament_id}:\n")
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting standings for season {args.season_id}, tournament {args.unique_tournament_id}: {e}")
        sys.exit(1)
