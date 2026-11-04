import argparse
import logging
from app.logging_setup import setup_logging
import sys
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get winning odds response by event ID")
    parser.add_argument("event_id", type=int, help="SofaScore event ID by default, or canonical event ID with --source canonical")
    parser.add_argument("--source", choices=["canonical", "sofascore"], default="sofascore", help="Interpret event_id as a SofaScore or canonical ID")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        identity = resolve_sofascore_event_identity(
            args.event_id,
            input_id_type=args.source,
        )
        response = api_client.get_winning_odds_response(identity.sofascore_event_id)
        logger.info(f"Winning odds response for event {args.event_id} (source={args.source}):\n")
        import json
        print(json.dumps(response, indent=4))
    except Exception as e:
        logger.error(f"Error getting winning odds for event {args.event_id}: {e}")
        sys.exit(1)
