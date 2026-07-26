import argparse
import logging
from app.logging_setup import setup_logging
import sys
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_id
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher
from infrastructure.persistence.repositories import EventSourceMappingRepository

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get event odds by event ID")
    parser.add_argument("event_id", type=int, help="Canonical event ID by default, or SofaScore event ID if --source sofascore is used")
    parser.add_argument("--source", choices=["canonical", "sofascore"], default="canonical", help="Interpret event_id as canonical or SofaScore source ID")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        sofascore_event_id = args.event_id if args.source == "sofascore" else resolve_sofascore_event_id(args.event_id)
        fetch_result = SofaScoreOddsFetcher(api_client).fetch_odds(sofascore_event_id)
        if fetch_result.endpoint_missing:
            if args.source == "canonical":
                EventSourceMappingRepository.mark_odds_unavailable(
                    [args.event_id],
                    "sofascore",
                )
            logger.info(
                "SofaScore odds endpoint not found for event %s (source=%s)",
                args.event_id,
                args.source,
            )
            sys.exit(0)

        odds_response = fetch_result.payload
        logger.info(f"Odds for event {args.event_id} (source={args.source}):\n")
        # pretty print dictionary with indentation
        import json
        print(json.dumps(odds_response, indent=4))
    except Exception as e:
        logger.error(f"Error getting event odds: {e}")
        sys.exit(1)
