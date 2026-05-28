"""Historical standings calculation engine.

This layer owns the DB-backed standings computation and the cache used by
historical form retrieval.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config

from modules.competition.league_config import (
    get_canonical_season_id,
    get_collected_season_bundle,
    get_grouping_method,
    get_included_season_ids,
    get_standings_method,
)
from .constants import get_team_group
from .standings_rules import (
    assign_positions_with_ties,
    build_display_sort_key,
    normalize_result_subtype,
    sort_football_h2h_items,
)

logger = logging.getLogger(__name__)


def _create_team_stats(group: Optional[str]) -> Dict[str, object]:
    return {
        "wins": 0,
        "losses": 0,
        "draws": 0,
        "ties": 0,
        "ot_losses": 0,
        "regulation_wins": 0,
        "ot_so_wins": 0,
        "goals_for": 0,
        "goals_against": 0,
        "games_played": 0,
        "group": group,
        "points": 0,
        "pct": None,
    }


def _finalize_team_stats(stats: Dict[str, object], standings_method: str) -> None:
    stats["goal_diff"] = stats["goals_for"] - stats["goals_against"]

    if standings_method in {"football_3_1_0", "football_3_1_0_h2h"}:
        stats["points"] = (stats["wins"] * 3) + stats["draws"]
        stats["pct"] = None
    elif standings_method == "win_pct":
        stats["points"] = stats["wins"]
        stats["pct"] = stats["wins"] / stats["games_played"] if stats["games_played"] > 0 else 0.0
    elif standings_method == "win_pct_half_tie":
        standing_points = stats["wins"] + (0.5 * stats["ties"])
        stats["points"] = standing_points
        stats["pct"] = standing_points / stats["games_played"] if stats["games_played"] > 0 else 0.0
    elif standings_method == "nhl_2_1_0_otl":
        stats["points"] = (stats["wins"] * 2) + stats["ot_losses"]
        stats["pct"] = stats["points"] / (stats["games_played"] * 2) if stats["games_played"] > 0 else 0.0
    elif standings_method == "hockey_3_2_1_0":
        stats["wins"] = stats["regulation_wins"] + stats["ot_so_wins"]
        stats["points"] = (
            (stats["regulation_wins"] * 3)
            + (stats["ot_so_wins"] * 2)
            + stats["ot_losses"]
        )
        stats["pct"] = stats["points"] / (stats["games_played"] * 3) if stats["games_played"] > 0 else 0.0
    else:
        stats["points"] = stats["wins"]
        stats["pct"] = stats["wins"] / stats["games_played"] if stats["games_played"] > 0 else 0.0


def _build_team_standing_payload(
    stats: Dict[str, object],
    position_meta: Dict[str, object],
    standings_method: str,
    group_name: Optional[str] = None,
) -> Dict[str, object]:
    payload = dict(stats)
    position = position_meta.get("position")

    # Engine-level aliases kept for downstream callers that still expect them.
    payload["position"] = position
    payload["rank"] = position
    payload["games_played"] = stats.get("games_played")
    payload["gp"] = stats.get("games_played")
    payload["goal_diff"] = stats.get("goal_diff")
    payload["diff"] = stats.get("goal_diff")
    payload["method"] = standings_method
    payload["standings_method"] = standings_method
    payload["group"] = group_name
    payload["conference"] = group_name
    payload["is_primary_tie"] = position_meta.get("is_primary_tie")
    payload["primary_rank_key"] = position_meta.get("primary_rank_key")
    return payload


def _apply_game_result(
    team_stats: Dict[str, Dict[str, object]],
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    winner: str,
    result_subtype: str,
    standings_method: str,
) -> None:
    team_stats[home_team]["goals_for"] += home_score
    team_stats[home_team]["goals_against"] += away_score
    team_stats[home_team]["games_played"] += 1

    team_stats[away_team]["goals_for"] += away_score
    team_stats[away_team]["goals_against"] += home_score
    team_stats[away_team]["games_played"] += 1

    if standings_method in {"football_3_1_0", "football_3_1_0_h2h"}:
        if winner == "1":
            team_stats[home_team]["wins"] += 1
            team_stats[away_team]["losses"] += 1
        elif winner == "2":
            team_stats[away_team]["wins"] += 1
            team_stats[home_team]["losses"] += 1
        elif winner == "X":
            team_stats[home_team]["draws"] += 1
            team_stats[away_team]["draws"] += 1

    elif standings_method == "win_pct":
        if winner == "1":
            team_stats[home_team]["wins"] += 1
            team_stats[away_team]["losses"] += 1
        elif winner == "2":
            team_stats[away_team]["wins"] += 1
            team_stats[home_team]["losses"] += 1
        elif winner == "X":
            team_stats[home_team]["draws"] += 1
            team_stats[away_team]["draws"] += 1

    elif standings_method == "win_pct_half_tie":
        if winner == "1":
            team_stats[home_team]["wins"] += 1
            team_stats[away_team]["losses"] += 1
        elif winner == "2":
            team_stats[away_team]["wins"] += 1
            team_stats[home_team]["losses"] += 1
        elif winner == "X":
            team_stats[home_team]["ties"] += 1
            team_stats[away_team]["ties"] += 1
            team_stats[home_team]["draws"] += 1
            team_stats[away_team]["draws"] += 1

    elif standings_method == "nhl_2_1_0_otl":
        if winner == "1":
            team_stats[home_team]["wins"] += 1
            team_stats[away_team]["losses"] += 1
            if result_subtype in {"OT", "SO"}:
                team_stats[home_team]["ot_so_wins"] += 1
                team_stats[away_team]["ot_losses"] += 1
            else:
                team_stats[home_team]["regulation_wins"] += 1
        elif winner == "2":
            team_stats[away_team]["wins"] += 1
            team_stats[home_team]["losses"] += 1
            if result_subtype in {"OT", "SO"}:
                team_stats[away_team]["ot_so_wins"] += 1
                team_stats[home_team]["ot_losses"] += 1
            else:
                team_stats[away_team]["regulation_wins"] += 1
        elif winner == "X":
            team_stats[home_team]["ties"] += 1
            team_stats[away_team]["ties"] += 1
            team_stats[home_team]["draws"] += 1
            team_stats[away_team]["draws"] += 1

    elif standings_method == "hockey_3_2_1_0":
        if winner == "1":
            team_stats[away_team]["losses"] += 1
            if result_subtype in {"OT", "SO"}:
                team_stats[home_team]["ot_so_wins"] += 1
                team_stats[away_team]["ot_losses"] += 1
            else:
                team_stats[home_team]["regulation_wins"] += 1
        elif winner == "2":
            team_stats[home_team]["losses"] += 1
            if result_subtype in {"OT", "SO"}:
                team_stats[away_team]["ot_so_wins"] += 1
                team_stats[home_team]["ot_losses"] += 1
            else:
                team_stats[away_team]["regulation_wins"] += 1
        elif winner == "X":
            team_stats[home_team]["ties"] += 1
            team_stats[away_team]["ties"] += 1
            team_stats[home_team]["draws"] += 1
            team_stats[away_team]["draws"] += 1

    else:
        if winner == "1":
            team_stats[home_team]["wins"] += 1
            team_stats[away_team]["losses"] += 1
        elif winner == "2":
            team_stats[away_team]["wins"] += 1
            team_stats[home_team]["losses"] += 1
        elif winner == "X":
            team_stats[home_team]["draws"] += 1
            team_stats[away_team]["draws"] += 1


def _build_standings_payload(
    team_stats: Dict[str, Dict[str, object]],
    standings_method: str,
    grouping_method: str,
    match_records: Optional[List[Dict[str, object]]] = None,
) -> Dict[str, Dict]:
    standings: Dict[str, Dict] = {}

    if grouping_method == "league_wide":
        if standings_method == "football_3_1_0_h2h":
            sorted_teams, rank_key_by_team = sort_football_h2h_items(
                team_stats.items(),
                match_records or [],
            )
            positions = assign_positions_with_ties(sorted_teams, standings_method, rank_key_by_team)
        else:
            sorted_teams = sorted(
                team_stats.items(),
                key=lambda item: build_display_sort_key(item[0], item[1], standings_method),
                reverse=True,
            )
            positions = assign_positions_with_ties(sorted_teams, standings_method)

        for team_name, stats in sorted_teams:
            pos_meta = positions[team_name]
            standings[team_name] = _build_team_standing_payload(stats, pos_meta, standings_method)
        return standings

    grouped_teams: Dict[str, List[Tuple[str, Dict[str, object]]]] = {}
    for team_name, stats in team_stats.items():
        group_name = stats.get("group") or "UNKNOWN"
        grouped_teams.setdefault(group_name, []).append((team_name, stats))

    for group_name, teams in grouped_teams.items():
        if standings_method == "football_3_1_0_h2h":
            sorted_teams, rank_key_by_team = sort_football_h2h_items(
                teams,
                match_records or [],
            )
            positions = assign_positions_with_ties(sorted_teams, standings_method, rank_key_by_team)
        else:
            sorted_teams = sorted(
                teams,
                key=lambda item: build_display_sort_key(item[0], item[1], standings_method),
                reverse=True,
            )
            positions = assign_positions_with_ties(sorted_teams, standings_method)

        for team_name, stats in sorted_teams:
            pos_meta = positions[team_name]
            standings[team_name] = _build_team_standing_payload(stats, pos_meta, standings_method, group_name)

    return standings


def _build_league_totals_context(
    *,
    season_id: int,
    canonical_season_id: int,
    cutoff_timestamp: float,
    sport: Optional[str],
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    match_records: List[Dict[str, object]],
    team_stats: Dict[str, Dict[str, object]],
    standings: Dict[str, Dict],
) -> Dict[str, Any]:
    teams: Dict[str, Dict[str, Any]] = {}

    for team_name, stats in team_stats.items():
        games_played = int(stats.get("games_played", 0) or 0)
        goals_for = int(stats.get("goals_for", 0) or 0)
        goals_against = int(stats.get("goals_against", 0) or 0)
        teams[team_name] = {
            "team_name": team_name,
            "games_played": games_played,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "team_total_per_game": (
                (goals_for + goals_against) / games_played if games_played > 0 else None
            ),
            "game_totals": [],
            "results": [],
            "standing": standings.get(team_name, {}),
        }

    matches: List[Dict[str, Any]] = []
    for record in match_records:
        game_total = int(record["home_score"]) + int(record["away_score"])
        start_timestamp = int(record["startTimestamp"])
        match_payload = {
            "event_id": int(record["event_id"]),
            "startTimestamp": start_timestamp,
            "home_team": record["home_team"],
            "away_team": record["away_team"],
            "home_score": int(record["home_score"]),
            "away_score": int(record["away_score"]),
            "winner": record["winner"],
            "result_subtype": record.get("result_subtype"),
            "game_total": game_total,
        }
        matches.append(match_payload)

        home_team_name = record["home_team"]
        away_team_name = record["away_team"]
        winner = record["winner"]

        if winner == "X":
            home_result_code = "X"
            away_result_code = "X"
        elif winner == "1":
            home_result_code = "1"
            away_result_code = "2"
        elif winner == "2":
            home_result_code = "2"
            away_result_code = "1"
        else:
            home_result_code = "X"
            away_result_code = "X"

        home_entry = teams.get(home_team_name)
        if home_entry is not None:
            home_entry["game_totals"].append(game_total)
            home_entry["results"].append({
                "event_id": int(record["event_id"]),
                "startTimestamp": start_timestamp,
                "team_role": "home",
                "opponent_name": away_team_name,
                "team_score": int(record["home_score"]),
                "opponent_score": int(record["away_score"]),
                "game_total": game_total,
                "team_result_code": home_result_code,
            })

        away_entry = teams.get(away_team_name)
        if away_entry is not None:
            away_entry["game_totals"].append(game_total)
            away_entry["results"].append({
                "event_id": int(record["event_id"]),
                "startTimestamp": start_timestamp,
                "team_role": "away",
                "opponent_name": home_team_name,
                "team_score": int(record["away_score"]),
                "opponent_score": int(record["home_score"]),
                "game_total": game_total,
                "team_result_code": away_result_code,
            })

    return {
        "source": "db_standings_calculator",
        "season_id": season_id,
        "canonical_season_id": canonical_season_id,
        "cutoff_timestamp": cutoff_timestamp,
        "cutoff_rule": "start_time_utc < cutoff_dt",
        "sport": sport,
        "source_unique_tournament_id": source_unique_tournament_id,
        "source_tournament_id": source_tournament_id,
        "match_count": len(match_records),
        "team_count": len(teams),
        "matches": matches,
        "teams": teams,
    }


def _serialize_match_records(match_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return a public-safe copy of match records without datetime objects."""
    serialized_records: List[Dict[str, Any]] = []
    for record in match_records:
        serialized_records.append({
            "event_id": record["event_id"],
            "startTimestamp": record["startTimestamp"],
            "home_team": record["home_team"],
            "away_team": record["away_team"],
            "home_score": record["home_score"],
            "away_score": record["away_score"],
            "winner": record["winner"],
            "result_subtype": record.get("result_subtype"),
            "game_total": record["game_total"],
        })
    return serialized_records


class HistoricalStandingsCalculator:
    """Compute standings at any point in time for a collected season."""

    def __init__(self):
        self._cache: Dict[Tuple[int, float, str, Optional[int], Optional[int]], Dict[str, Any]] = {}

    def _get_cache_key(
        self,
        canonical_season_id: int,
        cutoff_timestamp: float,
        sport: Optional[str],
        source_unique_tournament_id: Optional[int],
        source_tournament_id: Optional[int],
    ) -> Tuple[int, float, str, Optional[int], Optional[int]]:
        return (
            canonical_season_id,
            cutoff_timestamp,
            sport or "",
            source_unique_tournament_id,
            source_tournament_id,
        )

    def _fetch_match_records_before_cutoff(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        source_unique_tournament_id: Optional[int],
        source_tournament_id: Optional[int],
    ) -> List[Dict[str, Any]]:
        cutoff_dt = datetime.fromtimestamp(cutoff_timestamp)
        all_season_ids = get_included_season_ids(
            source_unique_tournament_id,
            source_tournament_id,
            season_id,
        )
        collected_bundle = get_collected_season_bundle(
            source_unique_tournament_id,
            source_tournament_id,
            season_id,
        )
        included_competition_identities = (
            collected_bundle.included_competition_identities if collected_bundle else ()
        )
        tournament_ids = tuple(
            identity.source_tournament_id
            for identity in included_competition_identities
            if identity.source_tournament_id is not None
        )

        query_sql = """
            SELECT
                event_id,
                start_time_utc,
                home_team,
                away_team,
                home_score,
                away_score,
                winner,
                result_subtype
            FROM season_events_with_results
            WHERE season_id = ANY(:season_ids)
              AND round = 'regular_season'
              AND start_time_utc < :cutoff_dt
        """
        query_params: Dict[str, Any] = {
            "season_ids": list(all_season_ids),
            "cutoff_dt": cutoff_dt,
        }
        if source_unique_tournament_id is not None:
            query_sql += " AND source_unique_tournament_id = :source_unique_tournament_id"
            query_params["source_unique_tournament_id"] = source_unique_tournament_id
        if tournament_ids:
            query_sql += " AND source_tournament_id = ANY(:source_tournament_ids)"
            query_params["source_tournament_ids"] = list(tournament_ids)
        query_sql += "\n            ORDER BY start_time_utc\n            "

        query = text(query_sql)

        match_records: List[Dict[str, Any]] = []
        with db_manager.get_session() as session:
            result = session.execute(query, query_params)
            all_rows = result.fetchall()

            for row in all_rows:
                home_score = int(row.home_score)
                away_score = int(row.away_score)
                winner = row.winner
                result_subtype = normalize_result_subtype(getattr(row, "result_subtype", None), winner)
                start_time_utc = row.start_time_utc

                match_records.append({
                    "event_id": int(row.event_id),
                    "start_time_utc": start_time_utc,
                    "startTimestamp": int(start_time_utc.timestamp()),
                    "home_team": row.home_team,
                    "away_team": row.away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "winner": winner,
                    "result_subtype": result_subtype,
                    "game_total": home_score + away_score,
                })

        return match_records

    def calculate_standings_at(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str = None,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Dict]:
        bundle = self.calculate_standings_bundle_at(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            send_debug_standings=send_debug_standings,
        )
        return bundle.get("standings", {})

    def calculate_standings_bundle_at(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str = None,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Any]:
        canonical_season_id = get_canonical_season_id(
            source_unique_tournament_id,
            source_tournament_id,
            season_id,
        )
        cache_key = self._get_cache_key(
            canonical_season_id,
            cutoff_timestamp,
            sport,
            source_unique_tournament_id,
            source_tournament_id,
        )
        if cache_key in self._cache:
            cached_bundle = self._cache[cache_key]
            normalized_bundle = {
                **cached_bundle,
                "status": cached_bundle.get("status", "ACTIVE"),
                "error": cached_bundle.get("error"),
                "match_records": _serialize_match_records(cached_bundle.get("match_records", [])),
            }
            if normalized_bundle != cached_bundle:
                self._cache[cache_key] = normalized_bundle
            return normalized_bundle

        try:
            bundle = self._calculate_standings_bundle_internal(
                season_id=season_id,
                cutoff_timestamp=cutoff_timestamp,
                sport=sport,
                source_unique_tournament_id=source_unique_tournament_id,
                source_tournament_id=source_tournament_id,
                send_debug_standings=send_debug_standings,
            )
            bundle = {
                **bundle,
                "match_records": _serialize_match_records(bundle.get("match_records", [])),
            }
            self._cache[cache_key] = bundle
            return bundle
        except Exception as exc:
            logger.error("Error computing standings for season %s: %s", season_id, exc)
            return {
                "standings": {},
                "match_records": [],
                "league_totals_context": None,
                "status": "ERROR",
                "error": str(exc),
            }

    def compute_standings(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str = None,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Dict]:
        """Backward-compatible alias for older callers."""
        return self.calculate_standings_at(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            send_debug_standings=send_debug_standings,
        )

    def _calculate_standings_bundle_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Any]:
        standings_method = get_standings_method(
            source_unique_tournament_id,
            source_tournament_id,
            sport,
        )
        grouping_method = get_grouping_method(
            source_unique_tournament_id,
            source_tournament_id,
        )
        group_by_conference = getattr(Config, "MATCHUP_STANDINGS_GROUP_BY_CONFERENCE", True)
        if not group_by_conference:
            grouping_method = "league_wide"

        canonical_season_id = get_canonical_season_id(
            source_unique_tournament_id,
            source_tournament_id,
            season_id,
        )
        match_records = self._fetch_match_records_before_cutoff(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
        )
        team_stats: Dict[str, Dict[str, object]] = {}
        warned_unknown_group_teams = set()
        for row in match_records:
            home_team = row["home_team"]
            away_team = row["away_team"]
            winner = row["winner"]
            result_subtype = row.get("result_subtype")

            for team in (home_team, away_team):
                if team not in team_stats:
                    group = None
                    if grouping_method != "league_wide":
                        group = get_team_group(team, grouping_method)
                        if not group:
                            if team not in warned_unknown_group_teams:
                                logger.warning(
                                    "Standings group mapping missing for season %s team '%s'; keeping in UNKNOWN group",
                                    season_id,
                                    team,
                                )
                                warned_unknown_group_teams.add(team)
                            group = "UNKNOWN"

                    team_stats[team] = _create_team_stats(group)

            _apply_game_result(
                team_stats=team_stats,
                home_team=home_team,
                away_team=away_team,
                home_score=row["home_score"],
                away_score=row["away_score"],
                winner=winner,
                result_subtype=result_subtype,
                standings_method=standings_method,
            )

        for stats in team_stats.values():
            _finalize_team_stats(stats, standings_method)

        standings = _build_standings_payload(team_stats, standings_method, grouping_method, match_records)
        league_totals_context = _build_league_totals_context(
            season_id=season_id,
            canonical_season_id=canonical_season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            match_records=match_records,
            team_stats=team_stats,
            standings=standings,
        )

        return {
            "standings": standings,
            "match_records": match_records,
            "league_totals_context": league_totals_context,
            "status": "ACTIVE",
            "error": None,
        }

    def _calculate_standings_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Dict]:
        bundle = self._calculate_standings_bundle_internal(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            send_debug_standings=send_debug_standings,
        )
        return bundle.get("standings", {})

    def _compute_standings_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        send_debug_standings: bool = False,
    ) -> Dict[str, Dict]:
        """Backward-compatible alias kept for older tests and callers."""
        return self._calculate_standings_internal(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            send_debug_standings=send_debug_standings,
        )

    def clear_cache(self):
        """Clear the internal cache."""
        self._cache.clear()


standings_calculator = HistoricalStandingsCalculator()
standings_simulator = standings_calculator
StandingsSimulator = HistoricalStandingsCalculator


__all__ = [
    "HistoricalStandingsCalculator",
    "StandingsSimulator",
    "standings_calculator",
    "standings_simulator",
]
