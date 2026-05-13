"""Standings helpers for SofaScore."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedStandingsCompetitionMetadata:
    number_of_teams: Optional[int]
    standings_grouping: Optional[str]
    table_count: int
    unique_team_ids: List[int]
    unique_team_count: int
    rows_count_by_table: List[int]
    bucket_values: List[str]
    valid: bool
    reason: Optional[str]


def get_standings_response(client, season_id: int, unique_tournament_id: int) -> Optional[Dict]:
    response = client._request_json(f"/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/total")
    logger.info(f"✈️ fetching Sofascore's standing api endpoint /unique-tournament/{unique_tournament_id}/season/{season_id}/standings/total")
    if not response or "standings" not in response:
        logger.error("No standings found for season %s and uniquetournament %s", season_id, unique_tournament_id)
        return None
    return response["standings"]


def parse_competition_metadata_from_standings(standings) -> ParsedStandingsCompetitionMetadata:
    """Extract competition-level metadata from a SofaScore standings response.

    This parser intentionally does not use row["matches"] because matches
    reflects games played to date, not the total regular-season schedule.
    """
    if not isinstance(standings, list):
        return ParsedStandingsCompetitionMetadata(
            number_of_teams=None,
            standings_grouping=None,
            table_count=0,
            unique_team_ids=[],
            unique_team_count=0,
            rows_count_by_table=[],
            bucket_values=[],
            valid=False,
            reason="standings_not_list",
        )

    if not standings:
        return ParsedStandingsCompetitionMetadata(
            number_of_teams=None,
            standings_grouping=None,
            table_count=0,
            unique_team_ids=[],
            unique_team_count=0,
            rows_count_by_table=[],
            bucket_values=[],
            valid=False,
            reason="standings_empty",
        )

    unique_team_ids = set()
    rows_count_by_table: List[int] = []
    bucket_values = set()
    table_count = 0

    for table in standings:
        if not isinstance(table, dict):
            rows_count_by_table.append(0)
            continue

        bucket = table.get("bucket")
        if bucket is not None:
            bucket_values.add(str(bucket))

        valid_rows_count = 0
        rows = table.get("rows")
        if not isinstance(rows, list):
            rows_count_by_table.append(0)
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            team = row.get("team")
            if not isinstance(team, dict):
                continue
            team_id = team.get("id")
            if team_id is None:
                continue
            try:
                team_id = int(team_id)
            except (TypeError, ValueError):
                continue
            unique_team_ids.add(team_id)
            valid_rows_count += 1

        rows_count_by_table.append(valid_rows_count)
        if valid_rows_count > 0:
            table_count += 1

    unique_team_ids_list = sorted(unique_team_ids)
    unique_team_count = len(unique_team_ids_list)
    if unique_team_count <= 1:
        return ParsedStandingsCompetitionMetadata(
            number_of_teams=None,
            standings_grouping=None,
            table_count=table_count,
            unique_team_ids=unique_team_ids_list,
            unique_team_count=unique_team_count,
            rows_count_by_table=rows_count_by_table,
            bucket_values=sorted(bucket_values),
            valid=False,
            reason="insufficient_unique_teams",
        )

    standings_grouping = "single_table" if table_count == 1 else "split_tables"
    return ParsedStandingsCompetitionMetadata(
        number_of_teams=unique_team_count,
        standings_grouping=standings_grouping,
        table_count=table_count,
        unique_team_ids=unique_team_ids_list,
        unique_team_count=unique_team_count,
        rows_count_by_table=rows_count_by_table,
        bucket_values=sorted(bucket_values),
        valid=True,
        reason=None,
    )


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
