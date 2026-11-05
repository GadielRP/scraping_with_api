import argparse
import json
import sys

from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_identity
from modules.sofascore.exceptions import SofaScoreNotFoundException
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Get event comments by event ID")
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
    args = parser.parse_args()

    identity = resolve_sofascore_event_identity(
        args.event_id,
        input_id_type=args.source,
    )
    try:
        response = api_client.get_event_comments(identity.sofascore_event_id)
    except SofaScoreNotFoundException as exc:
        print(
            f"SofaScore did not find the comments resource: "
            f"endpoint={exc.endpoint} event_id={identity.sofascore_event_id} "
            f"(HTTP {exc.status_code}).",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    if isinstance(response, dict):
        event = api_client.get_event_details(identity.sofascore_event_id) or {}
        tournament = event.get("tournament") or {}
        unique_tournament = tournament.get("uniqueTournament") or {}
        category = tournament.get("category") or {}
        sport = (category.get("sport") or {}).get("name")
        SofaScoreDebugResponseWriter.save_response(
            response_type="comments",
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
    print(json.dumps(response, indent=4))


if __name__ == "__main__":
    main()
