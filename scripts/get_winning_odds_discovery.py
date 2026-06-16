from modules.sofascore import api_client
import argparse
import logging
from app.logging_setup import setup_logging
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get winning odds discovery events")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_winning_odds_events()
        logger.info("Winning odds discovery events response:\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting winning odds discovery events: {e}")
        sys.exit(1)
