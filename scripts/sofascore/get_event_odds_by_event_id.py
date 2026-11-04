import argparse
import logging
from app.logging_setup import setup_logging
import sys
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher
from infrastructure.persistence.repositories import EventSourceMappingRepository

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get event odds by event ID")
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
        fetch_result = SofaScoreOddsFetcher(api_client).fetch_odds(identity.sofascore_event_id)
        if fetch_result.endpoint_missing:
            if args.source == "canonical":
                EventSourceMappingRepository.mark_odds_unavailable(
                    [identity.canonical_event_id],
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
