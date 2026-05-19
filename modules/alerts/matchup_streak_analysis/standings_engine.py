"""Historical standings calculation engine.

This layer owns the DB-backed standings computation and the cache used by
historical form retrieval.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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

    if standings_method == "football_3_1_0":
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

    if standings_method == "football_3_1_0":
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
) -> Dict[str, Dict]:
    standings: Dict[str, Dict] = {}

    if grouping_method == "league_wide":
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


class HistoricalStandingsCalculator:
    """Compute standings at any point in time for a collected season."""

    def __init__(self):
        self._cache: Dict[Tuple[int, float, str, Optional[int], Optional[int]], Dict[str, Dict]] = {}

    def calculate_standings_at(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str = None,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Dict]:
        canonical_season_id = get_canonical_season_id(
            source_unique_tournament_id,
            source_tournament_id,
            season_id,
        )
        cache_key = (
            canonical_season_id,
            cutoff_timestamp,
            sport or "",
            source_unique_tournament_id,
            source_tournament_id,
        )
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            standings = self._calculate_standings_internal(
                season_id=season_id,
                cutoff_timestamp=cutoff_timestamp,
                sport=sport,
                source_unique_tournament_id=source_unique_tournament_id,
                source_tournament_id=source_tournament_id,
                send_debug_standings=send_debug_standings,
            )
            self._cache[cache_key] = standings
            return standings
        except Exception as exc:
            logger.error("Error computing standings for season %s: %s", season_id, exc)
            return {}

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

    def _calculate_standings_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Dict]:
        cutoff_dt = datetime.fromtimestamp(cutoff_timestamp)
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
        canonical_season_id = get_canonical_season_id(
            source_unique_tournament_id,
            source_tournament_id,
            season_id,
        )

        query_sql = """
            SELECT
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
        query_params = {
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

        team_stats: Dict[str, Dict[str, object]] = {}
        warned_unknown_group_teams = set()

        with db_manager.get_session() as session:
            result = session.execute(query, query_params)
            all_rows = result.fetchall()

            # if send_debug_standings:
            #     logger.info(
            #         "STANDINGS DEBUG: Found %s events. Input season: %s, Canonical: %s, Bundle: %s before %s",
            #         len(all_rows),
            #         season_id,
            #         canonical_season_id,
            #         all_season_ids,
            #         cutoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
            #     )

            for row in all_rows:
                home_team = row.home_team
                away_team = row.away_team
                winner = row.winner
                result_subtype = normalize_result_subtype(getattr(row, "result_subtype", None), winner)

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
                    home_score=row.home_score,
                    away_score=row.away_score,
                    winner=winner,
                    result_subtype=result_subtype,
                    standings_method=standings_method,
                )

        for stats in team_stats.values():
            _finalize_team_stats(stats, standings_method)

        return _build_standings_payload(team_stats, standings_method, grouping_method)

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
