"""Standings helpers for SofaScore."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def get_standings_response(client, season_id: int, unique_tournament_id: int) -> Optional[Dict]:
    response = client._request_json(f"/tournament/{unique_tournament_id}/season/{season_id}/standings/total")
    logger.info(f"fetching Sofascore's standing api endpoint /tournament/{unique_tournament_id}/season/{season_id}/standings/total")
    if not response or "standings" not in response:
        logger.error("No standings found for season %s and tournament %s", season_id, unique_tournament_id)
        return None
    return response["standings"]


def process_standings_response(
    standings: Optional[List[Dict]],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
) -> Tuple[Optional[Dict], Optional[Dict]]:
    if not standings or not isinstance(standings, list):
        logger.debug("process_standings_response called with empty standings data")
        return None, None

    total_table = next((table for table in standings), None)
    remaining_tables = [table for table in standings if table is not total_table]
    tables_to_scan = []
    if total_table:
        tables_to_scan.append(total_table)
    tables_to_scan.extend(remaining_tables or standings)

    def _build_team_snapshot(row: Dict) -> Dict:
        team = row.get("team", {})
        goals_for = row.get("scoresFor")
        goals_against = row.get("scoresAgainst")
        goal_diff_numeric = None
        if goals_for is not None and goals_against is not None:
            try:
                goal_diff_numeric = goals_for - goals_against
            except TypeError:
                goal_diff_numeric = None

        return {
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "position": row.get("position"),
            "matches": row.get("matches"),
            "wins": row.get("wins"),
            "draws": row.get("draws"),
            "losses": row.get("losses"),
            "points": row.get("points"),
            "goals_for": goals_for,
            "goals_against": goals_against,
            "goal_diff": goal_diff_numeric,
            "goal_diff_formatted": row.get("scoreDiffFormatted"),
        }

    home_snapshot = None
    away_snapshot = None

    for table in tables_to_scan:
        for row in table.get("rows", []):
            team = row.get("team", {})
            team_id = team.get("id")
            if team_id is None:
                continue

            if home_team_id is not None and team_id == home_team_id and not home_snapshot:
                home_snapshot = _build_team_snapshot(row)
            elif away_team_id is not None and team_id == away_team_id and not away_snapshot:
                away_snapshot = _build_team_snapshot(row)

            if home_snapshot and away_snapshot:
                break
        if home_snapshot and away_snapshot:
            break

    if home_team_id and not home_snapshot:
        logger.debug("Home team id %s not found in standings response", home_team_id)
    if away_team_id and not away_snapshot:
        logger.debug("Away team id %s not found in standings response", away_team_id)

    return home_snapshot, away_snapshot
