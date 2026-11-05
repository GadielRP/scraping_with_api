import argparse
import logging
from app.logging_setup import setup_logging
import sys
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)

if __name__ == "__main__":
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
        debug_path = (
            save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
            if args.save_debug
            else None
        )
        logger.info(f"Event results/snapshot response for event {args.event_id} (source={args.source}):\n")
        import json
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting event results for event {args.event_id}: {e}")
        sys.exit(1)
