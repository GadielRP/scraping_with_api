import argparse
import json
import logging
import sys

from app.logging_setup import setup_logging
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Get event results or metadata snapshot by event ID")
    parser.add_argument("event_id", type=int, help="SofaScore event ID by default, or canonical event ID with --source canonical")
    parser.add_argument("--source", choices=["canonical", "sofascore"], default="sofascore", help="Interpret event_id as a SofaScore or canonical ID")
    parser.add_argument("--update-time", action="store_true", help="Check and update starting time")
    parser.add_argument("--update-court-type", action="store_true", help="Update tennis court/ground type and rankings")
    parser.add_argument("--return-snapshot", action="store_true", help="Return metadata snapshot instead of parsed results")
    parser.add_argument("--minutes-until-start", type=int, default=0, help="Minutes until event starts (passed to get_event_results)")
    parser.add_argument("--no-event-info-update", action="store_false", dest="update_event_info", help="Disable updating event information in DB")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        identity = resolve_sofascore_event_identity(
            args.event_id,
            input_id_type=args.source,
        )
        response = api_client.get_event_results(
            identity.sofascore_event_id,
            canonical_event_id=identity.canonical_event_id,
            update_time=args.update_time,
            update_court_type=args.update_court_type,
            minutes_until_start=args.minutes_until_start,
            update_event_info=args.update_event_info,
            return_snapshot=args.return_snapshot
        )
        debug_path = None
        if args.save_debug and isinstance(response, dict):
            event = api_client.get_event_details(identity.sofascore_event_id) or {}
            tournament = event.get("tournament") or {}
            unique_tournament = tournament.get("uniqueTournament") or {}
            category = tournament.get("category") or {}
            sport = (category.get("sport") or {}).get("name")
            debug_path = SofaScoreDebugResponseWriter.save_response(
                response_type="results",
                event_id=identity.canonical_event_id or identity.sofascore_event_id,
                payload=response,
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
        elif args.save_debug:
            debug_path = save_response_for_debug("get_event_results", response)

        logger.info(f"Event results/snapshot response for event {args.event_id} (source={args.source}):\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting event results for event {args.event_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
