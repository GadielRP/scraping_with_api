from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get team last results")
    parser.add_argument("team_id", type=int, help="The SofaScore ID of the team")
    parser.add_argument("--singles", action="store_true", help="Set if querying tennis singles results")
    parser.add_argument("--doubles", action="store_true", help="Set if querying tennis doubles results")
    parser.add_argument("--index", type=int, default=0, help="The batch fetch index (defaults to 0)")
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
        logger.info(f"Last results response for team {args.team_id} (singles: {args.singles}, doubles: {args.doubles}, index: {args.index}):\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting last results for team {args.team_id}: {e}")
        sys.exit(1)
