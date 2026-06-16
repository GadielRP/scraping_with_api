from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get nearest event for a team by team ID")
    parser.add_argument("team_id", type=int, help="The SofaScore ID of the team")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_nearest_event_for_team(args.team_id)
        logger.info(f"Nearest event response for team {args.team_id}:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting nearest event for team {args.team_id}: {e}")
        sys.exit(1)
