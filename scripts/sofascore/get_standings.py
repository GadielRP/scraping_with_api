from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get standings by season ID and unique tournament ID")
    parser.add_argument("season_id", type=int, help="The SofaScore season ID")
    parser.add_argument("unique_tournament_id", type=int, help="The SofaScore unique tournament ID")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_standings_response(args.season_id, args.unique_tournament_id)
        logger.info(f"Standings response for season {args.season_id}, tournament {args.unique_tournament_id}:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting standings for season {args.season_id}, tournament {args.unique_tournament_id}: {e}")
        sys.exit(1)
