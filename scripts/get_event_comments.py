import argparse
import json

from modules.sofascore import api_client
from modules.jobs.pre_start_check_job.providers.sofascore.comments_debug_response_writer import (
    SofaScoreCommentsDebugResponseWriter,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Get event comments by event ID")
    parser.add_argument("event_id", type=int, help="The SofaScore ID of the event")
    args = parser.parse_args()

    response = api_client.get_event_comments(args.event_id)
    if isinstance(response, dict):
        event = api_client.get_event_details(args.event_id) or {}
        tournament = event.get("tournament") or {}
        unique_tournament = tournament.get("uniqueTournament") or {}
        category = tournament.get("category") or {}
        sport = (category.get("sport") or {}).get("name")
        SofaScoreCommentsDebugResponseWriter.save(
            event_id=args.event_id,
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
