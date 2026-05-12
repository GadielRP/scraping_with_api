"""DB-backed historical form service for matchup streak analysis."""

import logging
import os
from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy import text

from .constants import get_all_season_ids, get_standings_method
from .historical_form_reporting import (
    format_standings_table_for_telegram,
    get_round_cutoff_timestamps,
    send_debug_telegram,
)
from .standings_engine import standings_calculator

logger = logging.getLogger(__name__)


def _normalize_standing_snapshot(raw_standing: Dict, standings_method: str = None) -> Dict:
    """Return a normalized standings snapshot with stable aliases."""
    standing = dict(raw_standing or {})

    rank = standing.get("rank", standing.get("position"))
    gp = standing.get("gp", standing.get("games_played"))
    diff = standing.get("diff", standing.get("goal_diff"))

    standing["rank"] = rank
    standing["position"] = standing.get("position", rank)
    standing["gp"] = gp
    standing["games_played"] = standing.get("games_played", gp)
    standing["diff"] = diff
    standing["goal_diff"] = standing.get("goal_diff", diff)
    standing["points"] = standing.get("points")
    standing["wins"] = standing.get("wins")
    standing["draws"] = standing.get("draws")
    standing["losses"] = standing.get("losses")
    standing["pct"] = standing.get("pct")
    standing["goals_for"] = standing.get("goals_for")
    standing["goals_against"] = standing.get("goals_against")
    standing["method"] = standing.get("method") or standings_method
    standing["standings_method"] = standing.get("standings_method") or standings_method
    return standing


class HistoricalFormService:
    """Fetch historical form from the local database for collected seasons."""

    def __init__(self, standings_calculator_instance=None):
        self.standings_calculator = standings_calculator_instance or standings_calculator

    def _calculate_current_win_streak(self, results: List[Dict]) -> int:
        if not results:
            return 0

        streak_count = 0
        for result in results:
            if result.get("winner") == "1":
                streak_count += 1
            else:
                break
        return streak_count

    def get_team_form_from_db(
        self,
        team_name: str,
        season_id: int,
        sport: str,
        exclude_event_id: int = None,
        current_event_timestamp: float = None,
        send_debug_standings: bool = True,
    ) -> Tuple[List[Dict], int]:
        try:
            from infrastructure.persistence.database import db_manager

            standings_method = get_standings_method(season_id, sport)

            query = text(
                """
                SELECT
                    event_id,
                    home_team,
                    away_team,
                    home_score,
                    away_score,
                    winner,
                    start_time_utc
                FROM season_events_with_results
                WHERE season_id = ANY(:season_ids)
                  AND round = 'regular_season'
                  AND (home_team = :team_name OR away_team = :team_name)
                ORDER BY start_time_utc DESC
                """
            )

            results = []
            all_season_ids = get_all_season_ids(season_id)

            with db_manager.get_session() as session:
                db_result = session.execute(query, {"season_ids": all_season_ids, "team_name": team_name})
                all_rows = db_result.fetchall()
                logger.info("DB query returned %s events for %s in seasons %s", len(all_rows), team_name, all_season_ids)

                for row in all_rows:
                    event_id = row.event_id
                    if exclude_event_id and event_id == exclude_event_id:
                        continue

                    if current_event_timestamp:
                        event_ts = row.start_time_utc.timestamp()
                        if event_ts >= current_event_timestamp:
                            continue

                    is_team_home = row.home_team == team_name
                    opponent_name = row.away_team if is_team_home else row.home_team

                    if row.winner == "1":
                        team_result = "1" if is_team_home else "2"
                    elif row.winner == "2":
                        team_result = "1" if not is_team_home else "2"
                    else:
                        team_result = "X"

                    if is_team_home:
                        team_score = row.home_score
                        opponent_score = row.away_score
                        team_role = "home"
                        opponent_role = "away"
                    else:
                        team_score = row.away_score
                        opponent_score = row.home_score
                        team_role = "away"
                        opponent_role = "home"

                    game_timestamp = row.start_time_utc.timestamp()
                    standings = self.standings_calculator.calculate_standings_at(
                        season_id,
                        game_timestamp,
                        sport,
                        send_debug_standings,
                    )

                    team_standing = _normalize_standing_snapshot(
                        standings.get(team_name, {}),
                        standings_method,
                    )
                    opponent_standing = _normalize_standing_snapshot(
                        standings.get(opponent_name, {}),
                        standings_method,
                    )

                    team_result_code = team_result
                    team_result = "W" if team_result_code == "1" else "L" if team_result_code == "2" else "D"

                    results.append(
                        {
                            "event_id": event_id,
                            "team_name": team_name,
                            "team_role": team_role,
                            "opponent_name": opponent_name,
                            "opponent_role": opponent_role,
                            "team_score": team_score,
                            "opponent_score": opponent_score,
                            "team_result_code": team_result_code,
                            "team_result": team_result,
                            "winner": team_result_code,
                            "home_score": team_score,
                            "away_score": opponent_score,
                            "startTimestamp": int(game_timestamp),
                            "role": team_role,
                            "opponent_ranking": opponent_standing.get("rank") or 0,
                            "own_ranking": team_standing.get("rank") or 0,
                            "standings_position": team_standing.get("rank"),
                            "standings_points": team_standing.get("points"),
                            "opponent_standings_position": opponent_standing.get("rank"),
                            "opponent_standings_points": opponent_standing.get("points"),
                            "team_standing": team_standing,
                            "opponent_standing": opponent_standing,
                            "winner_code": team_result_code,
                        }
                    )

            win_streak = self._calculate_current_win_streak(results)
            logger.info(
                "DB-based form: %s - %s games from season %s, win streak: %s",
                team_name,
                len(results),
                season_id,
                win_streak,
            )

            if send_debug_standings:
                personal_chat_id = os.getenv("PERSONAL_CHAT_ID", "")
                if personal_chat_id:
                    rounds_to_check = [5, 10, 15, 20, 25]
                    round_cutoffs = get_round_cutoff_timestamps(season_id, rounds_to_check)

                    for round_num in sorted(round_cutoffs.keys()):
                        cutoff_ts = round_cutoffs[round_num]
                        cutoff_date = datetime.fromtimestamp(cutoff_ts).strftime("%Y-%m-%d")

                        standings = self.standings_calculator.calculate_standings_at(season_id, cutoff_ts, sport)
                        title = f"Round {round_num} Standings ({cutoff_date})"
                        message = format_standings_table_for_telegram(
                            standings,
                            title,
                            standings_method=get_standings_method(season_id, sport),
                        )
                        send_debug_telegram(message, personal_chat_id)

                    if current_event_timestamp:
                        current_standings = self.standings_calculator.calculate_standings_at(
                            season_id,
                            current_event_timestamp,
                            sport,
                        )
                        current_date = datetime.fromtimestamp(current_event_timestamp).strftime("%Y-%m-%d %H:%M")
                        title = f"CURRENT Standings (at {current_date})"
                        message = format_standings_table_for_telegram(
                            current_standings,
                            title,
                            standings_method=get_standings_method(season_id, sport),
                        )
                        send_debug_telegram(message, personal_chat_id)

                else:
                    logger.warning("PERSONAL_CHAT_ID not configured - skipping debug standings")

            return results, win_streak

        except Exception as exc:
            logger.error("Error getting team form from DB for %s: %s", team_name, exc)
            return [], 0


historical_form_service = HistoricalFormService()
historical_form_processor = historical_form_service
HistoricalFormProcessor = HistoricalFormService


__all__ = [
    "HistoricalFormProcessor",
    "HistoricalFormService",
    "historical_form_processor",
    "historical_form_service",
]
