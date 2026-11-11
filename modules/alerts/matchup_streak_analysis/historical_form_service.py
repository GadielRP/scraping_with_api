"""DB-backed historical form service for matchup streak analysis.

Responsibilities:
  - Fetch a team's season games from the local database.
  - Attach standings snapshots (own/opponent ranking at each game cutoff)
    using a single-pass standings timeline instead of one league-wide
    recompute per game.
  - Optionally send a current-standings debug report to Telegram.

Standings math itself lives in standings_engine; Telegram formatting lives in
historical_form_reporting.
"""

import logging
import os
from collections import OrderedDict
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from infrastructure.settings import Config
from shared.temporal import from_unix_timestamp, in_timezone
from infrastructure.persistence.repositories.competition_repository import get_competition_profile_for_local_id as get_competition_profile
from modules.competition.rule_terms import GroupingMethod
from .historical_form_reporting import (
    format_fair_play_table_for_telegram,
    format_standings_table_for_telegram,
    send_debug_telegram,
)
from .standings_engine import standings_calculator

logger = logging.getLogger(__name__)
_MAX_RECENT_GLOBAL_DEBUG_TABLES = 256


def _normalize_standing_snapshot(raw_standing: Dict, standings_method: str = None) -> Dict:
    """Return a normalized standings snapshot with canonical fields."""
    standing = dict(raw_standing or {})

    rank = standing.get("rank", standing.get("position"))
    gp = standing.get("gp", standing.get("games_played"))
    diff = standing.get("diff", standing.get("goal_diff"))

    standing["rank"] = rank
    standing["gp"] = gp
    standing["diff"] = diff
    standing["points"] = standing.get("points")
    standing["wins"] = standing.get("wins")
    standing["draws"] = standing.get("draws")
    standing["losses"] = standing.get("losses")
    standing["pct"] = standing.get("pct")
    standing["goals_for"] = standing.get("goals_for")
    standing["goals_against"] = standing.get("goals_against")
    standing["standings_method"] = standing.get("standings_method") or standings_method
    return standing


class HistoricalFormService:
    """Fetch historical form from the local database for collected seasons."""

    def __init__(self, standings_calculator_instance=None):
        self.standings_calculator = standings_calculator_instance or standings_calculator
        self._global_debug_table_lock = Lock()
        self._sent_global_debug_tables: OrderedDict[Tuple, None] = OrderedDict()

    def _claim_global_debug_table(self, key: Tuple) -> bool:
        """Claim one global debug table, deduplicating concurrent team lookups."""
        with self._global_debug_table_lock:
            if key in self._sent_global_debug_tables:
                self._sent_global_debug_tables.move_to_end(key)
                return False
            self._sent_global_debug_tables[key] = None
            if len(self._sent_global_debug_tables) > _MAX_RECENT_GLOBAL_DEBUG_TABLES:
                self._sent_global_debug_tables.popitem(last=False)
            return True

    def _calculate_current_win_streak(self, results: List[Dict]) -> int:
        if not results:
            return 0

        streak_count = 0
        for result in results:
            if result.get("team_result_code") == "1":
                streak_count += 1
            else:
                break
        return streak_count

    @staticmethod
    def _fetch_team_game_rows(
        team_name: str,
        season_id: int,
        source_unique_tournament_id: Optional[int],
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
    ) -> List:
        """Fetch the team's regular-season games, most recent first."""
        # Imported lazily to avoid import cycles at module load time.
        from infrastructure.persistence.database import db_manager

        query_sql = """
            SELECT
                event_id,
                home_team,
                away_team,
                home_score,
                away_score,
                winner,
                starts_at
            FROM season_events_with_results
            WHERE round = 'regular_season'
        """
        query_params: Dict[str, Any] = {
            "team_name": team_name,
        }
        if competition_id is not None:
            query_sql += " AND competition_id = :competition_id"
            query_params["competition_id"] = int(competition_id)
        elif source_unique_tournament_id is not None:
            query_sql += " AND source_unique_tournament_id = :source_unique_tournament_id"
            query_params["source_unique_tournament_id"] = source_unique_tournament_id

        if season_year is not None:
            query_sql += " AND season_year = :season_year"
            query_params["season_year"] = int(season_year)
        else:
            query_sql += " AND season_id = :season_id"
            query_params["season_id"] = int(season_id)

        query_sql += " AND (home_team = :team_name OR away_team = :team_name) ORDER BY starts_at DESC"

        with db_manager.get_session() as session:
            return session.execute(text(query_sql), query_params).fetchall()

    @staticmethod
    def _build_form_result(
        row,
        team_name: str,
        standings_by_cutoff: Dict[float, Dict[str, Dict]],
        standings_method: str,
    ) -> Dict:
        """Transform one DB row into the canonical form-result dict."""
        is_team_home = row.home_team == team_name
        opponent_name = row.away_team if is_team_home else row.home_team

        if row.winner == "1":
            team_result_code = "1" if is_team_home else "2"
        elif row.winner == "2":
            team_result_code = "1" if not is_team_home else "2"
        else:
            team_result_code = "X"

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

        game_timestamp = row.starts_at.timestamp()
        standings = standings_by_cutoff.get(game_timestamp) or {}
        team_standing = _normalize_standing_snapshot(
            standings.get(team_name, {}),
            standings_method,
        )
        opponent_standing = _normalize_standing_snapshot(
            standings.get(opponent_name, {}),
            standings_method,
        )

        team_result = "W" if team_result_code == "1" else "L" if team_result_code == "2" else "D"

        return {
            "event_id": row.event_id,
            "team_name": team_name,
            "team_role": team_role,
            "opponent_name": opponent_name,
            "opponent_role": opponent_role,
            "team_score": team_score,
            "opponent_score": opponent_score,
            "team_result_code": team_result_code,
            "team_result": team_result,
            "startTimestamp": int(game_timestamp),
            "standings_as_of_timestamp": game_timestamp,
            "opponent_ranking": opponent_standing.get("rank"),
            "own_ranking": team_standing.get("rank"),
            "team_standing": team_standing,
            "opponent_standing": opponent_standing,
        }

    def _send_current_standings_debug(
        self,
        current_event_timestamp: Optional[float],
        current_standings: Dict[str, Dict],
        deduplicate_global: bool,
        debug_scope: Tuple,
    ) -> None:
        """Send the current standings table to the personal debug chat."""
        # Argentina's zone and annual tables are sent together after annual
        # standings preparation, where that calculation can be reused.
        if self._is_argentina_zones_profile(debug_scope):
            return
        personal_chat_id = os.getenv("PERSONAL_CHAT_ID", "")
        if not personal_chat_id:
            logger.warning("PERSONAL_CHAT_ID not configured - skipping debug standings")
            return
        if not current_event_timestamp:
            return
        current_date = in_timezone(
            from_unix_timestamp(current_event_timestamp),
            Config.TIMEZONE,
        ).strftime("%Y-%m-%d %H:%M")
        title = f"CURRENT Standings (at {current_date})"
        message = format_standings_table_for_telegram(
            current_standings,
            title,
        )
        standings_key = (*debug_scope, "standings")
        if not deduplicate_global or self._claim_global_debug_table(standings_key):
            send_debug_telegram(message, personal_chat_id)
        fair_play_message = format_fair_play_table_for_telegram(
            current_standings, f"CURRENT (at {current_date})",
        )
        fair_play_key = (*debug_scope, "fair_play")
        if fair_play_message and (
            not deduplicate_global or self._claim_global_debug_table(fair_play_key)
        ):
            send_debug_telegram(fair_play_message, personal_chat_id)

    @staticmethod
    def _is_argentina_zones_profile(debug_scope: Tuple) -> bool:
        """Identify Argentina editions from the provider IDs in the debug scope."""
        from modules.competition import competition_ids

        if len(debug_scope) < 6:
            return False
        source_unique_tournament_id = debug_scope[3]
        source_tournament_id = debug_scope[4]
        return (
            source_unique_tournament_id
            == competition_ids.ARGENTINA_APERTURA.source_unique_tournament_id
            and source_tournament_id
            in {
                competition_ids.ARGENTINA_APERTURA.source_tournament_id,
                competition_ids.ARGENTINA_CLAUSURA.source_tournament_id,
            }
        )

    def get_team_form_from_db(
        self,
        team_name: str,
        season_id: int,
        sport: str,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
        exclude_event_id: int = None,
        current_event_timestamp: float = None,
        send_debug_standings: bool = True,
    ) -> Tuple[List[Dict], int]:
        try:
            profile = get_competition_profile(competition_id)
            if profile is None:
                raise ValueError(f"No competition profile configured for competition_id={competition_id}")
            standings_method = profile.standings.name
            grouping_method = profile.grouping_method
            logger.info(
                "DB historical form scope for %s: competition_id=%s season_id=%s season_year=%s source_unique_tournament_id=%s source_tournament_id=%s standings_method=%s grouping_method=%s",
                team_name,
                competition_id,
                season_id,
                season_year,
                source_unique_tournament_id,
                source_tournament_id,
                standings_method,
                grouping_method,
            )

            all_rows = self._fetch_team_game_rows(
                team_name,
                season_id,
                source_unique_tournament_id,
                competition_id=competition_id,
                season_year=season_year,
            )
            logger.info(
                "DB query returned %s events for %s in competition %s (year=%s)",
                len(all_rows),
                team_name,
                competition_id or source_unique_tournament_id,
                season_year,
            )

            applicable_rows = [
                row
                for row in all_rows
                if not (exclude_event_id and row.event_id == exclude_event_id)
                and not (
                    current_event_timestamp
                    and row.starts_at.timestamp() >= current_event_timestamp
                )
            ]

            # Single-pass standings walk: one league fetch covers every game
            # cutoff, instead of one league-wide recompute per game.
            standings_by_cutoff = self.standings_calculator.calculate_standings_timeline(
                season_id,
                [
                    *(row.starts_at.timestamp() for row in applicable_rows),
                    *([current_event_timestamp] if send_debug_standings and current_event_timestamp else []),
                ],
                source_unique_tournament_id=source_unique_tournament_id,
                competition_id=competition_id,
                season_year=season_year,
            )

            results = [
                self._build_form_result(row, team_name, standings_by_cutoff, standings_method)
                for row in applicable_rows
            ]

            win_streak = self._calculate_current_win_streak(results)
            logger.info(
                "DB-based form: %s - %s games from season %s, win streak: %s",
                team_name,
                len(results),
                season_id,
                win_streak,
            )

            if send_debug_standings:
                self._send_current_standings_debug(
                    current_event_timestamp=current_event_timestamp,
                    current_standings=standings_by_cutoff.get(current_event_timestamp, {}),
                    deduplicate_global=grouping_method == GroupingMethod.LEAGUE_WIDE,
                    debug_scope=(
                        competition_id,
                        season_id,
                        season_year,
                        source_unique_tournament_id,
                        source_tournament_id,
                        current_event_timestamp,
                    ),
                )

            return results, win_streak

        except Exception as exc:
            logger.error("Error getting team form from DB for %s: %s", team_name, exc)
            return [], 0


historical_form_service = HistoricalFormService()


__all__ = [
    "HistoricalFormService",
    "historical_form_service",
]
