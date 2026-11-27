import argparse
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path

from app.logging_setup import setup_logging
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.sofascore import api_client
from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
)


def _clean_snake_case(value: object, *, fallback: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    token = re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_")
    return token or fallback


def _standings_debug_subfolder(standings: object, season_year: object) -> Path:
    if not isinstance(standings, list):
        raise ValueError("Standings response does not contain a standings list")
    for table in standings:
        if not isinstance(table, dict):
            continue
        tournament = table.get("tournament") or {}
        sport = (tournament.get("category") or {}).get("sport") or {}
        sport_name = sport.get("name")
        tournament_slug = tournament.get("slug")
        if sport_name and tournament_slug:
            return (
                Path(_clean_snake_case(sport_name, fallback="unknown_sport"))
                / _clean_snake_case(tournament_slug, fallback="unknown_tournament")
                / _clean_snake_case(season_year, fallback="unknown_season")
            )
    raise ValueError("Standings response is missing sport or tournament slug metadata")


def _get_season_year(season_response: object, season_id: int) -> str:
    seasons = season_response.get("seasons") if isinstance(season_response, dict) else None
    if isinstance(seasons, list):
        for season in seasons:
            if isinstance(season, dict) and str(season.get("id")) == str(season_id):
                year = season.get("year")
                if year:
                    return str(year)
    raise ValueError(f"Could not find year for SofaScore season {season_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Get standings by season ID and unique tournament ID")
    parser.add_argument("season_id", type=int, help="The SofaScore season ID")
    parser.add_argument("unique_tournament_id", type=int, help="The SofaScore unique tournament ID")
    add_debug_file_argument(parser)
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        response = api_client.get_standings_response(args.season_id, args.unique_tournament_id)
        debug_path = None
        if args.save_debug and response is not None:
            seasons = api_client.request_json(
                f"/unique-tournament/{args.unique_tournament_id}/seasons"
            )
            season_year = _get_season_year(seasons, args.season_id)
            debug_path = SofaScoreDebugResponseWriter.save_general_response(
                response_type="standings",
                payload=response,
                subfolder=_standings_debug_subfolder(response, season_year),
                output_directory=(
                    SofaScoreDebugResponseWriter.OUTPUT_ROOT / "sofascore" / "standings"
                ),
            )

        logger.info(f"Standings response for season {args.season_id}, tournament {args.unique_tournament_id}:\n")
        print(json.dumps(response, indent=4))
        log_debug_file(debug_path)
    except Exception as e:
        logger.error(f"Error getting standings for season {args.season_id}, tournament {args.unique_tournament_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
