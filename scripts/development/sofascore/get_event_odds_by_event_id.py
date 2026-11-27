import argparse
import json
import logging
import sys

from app.logging_setup import setup_logging
from infrastructure.persistence.repositories import EventSourceMappingRepository
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)


def _save_odds_debug_file(identity, payload, save_debug: bool):
    if not save_debug or not isinstance(payload, dict):
        return None
    try:
        event = api_client.get_event_details(identity.sofascore_event_id) or {}
        tournament = event.get("tournament") or {}
        unique_tournament = tournament.get("uniqueTournament") or {}
        category = tournament.get("category") or {}
        sport = (category.get("sport") or {}).get("name")
        return SofaScoreDebugResponseWriter.save_response(
            response_type="odds",
            event_id=identity.canonical_event_id or identity.sofascore_event_id,
            payload=payload,
            competition_slug=(
                unique_tournament.get("slug")
                or tournament.get("slug")
                or unique_tournament.get("name")
                or tournament.get("name")
            ),
            sport=sport,
            home_participant=(event.get("homeTeam") or {}).get("name"),
            away_participant=(event.get("awayTeam") or {}).get("name"),
            event_label=event.get("slug"),
        )
    except Exception:
        return save_response_for_debug("get_event_odds_by_event_id", payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Get event odds by event ID")
    parser.add_argument("event_id", type=int, help="SofaScore event ID by default, or canonical event ID with --source canonical")
    parser.add_argument("--source", choices=["canonical", "sofascore"], default="sofascore", help="Interpret event_id as a SofaScore or canonical ID")
    add_debug_file_argument(parser)
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
            debug_path = _save_odds_debug_file(identity, fetch_result.payload, args.save_debug)
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
            log_debug_file(debug_path)
            sys.exit(0)

        odds_response = fetch_result.payload
        debug_path = _save_odds_debug_file(identity, odds_response, args.save_debug)
        logger.info(f"Odds for event {args.event_id} (source={args.source}):\n")
        print(json.dumps(odds_response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting event odds: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
