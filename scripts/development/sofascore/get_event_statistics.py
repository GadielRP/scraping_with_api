import argparse
import json

from modules.discipline.contracts import DisciplineEndpoint
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)
from scripts.sofascore.discipline import persist_discipline


def main() -> None:
    parser = argparse.ArgumentParser(description="Get statistics for a SofaScore event")
    parser.add_argument(
        "event_id",
        type=int,
        help="SofaScore event ID by default, or canonical event ID with --source canonical",
    )
    parser.add_argument(
        "--source",
        choices=["canonical", "sofascore"],
        default="sofascore",
        help="Interpret event_id as a SofaScore or canonical ID",
    )
    parser.add_argument("--persist", action="store_true", help="Persist discipline totals for a finished event")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    identity = resolve_sofascore_event_identity(
        args.event_id,
        input_id_type=args.source,
    )
    response = (persist_discipline(identity, DisciplineEndpoint.STATISTICS, api_client)
                if args.persist else api_client.get_event_statistics(identity.sofascore_event_id))
    debug_path = None
    if args.save_debug and isinstance(response, dict):
        event = api_client.get_event_details(identity.sofascore_event_id) or {}
        tournament = event.get("tournament") or {}
        unique_tournament = tournament.get("uniqueTournament") or {}
        category = tournament.get("category") or {}
        debug_path = SofaScoreDebugResponseWriter.save_response(
            response_type="statistics",
            event_id=identity.canonical_event_id or identity.sofascore_event_id,
            payload=response,
            competition_slug=(
                unique_tournament.get("slug")
                or tournament.get("slug")
                or unique_tournament.get("name")
                or tournament.get("name")
            ),
            sport=(category.get("sport") or {}).get("name"),
            home_participant=(event.get("homeTeam") or {}).get("name"),
            away_participant=(event.get("awayTeam") or {}).get("name"),
            event_label=event.get("slug"),
        )
    elif args.save_debug:
        debug_path = save_response_for_debug(__name__.rsplit(".", 1)[-1], response)
    print(json.dumps(response, indent=4))
    log_debug_file(debug_path)


if __name__ == "__main__":
    main()
