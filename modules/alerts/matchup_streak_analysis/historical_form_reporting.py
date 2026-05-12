"""Optional reporting helpers for DB-backed historical form analysis."""

import logging
import os
from typing import Dict, List

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config

logger = logging.getLogger(__name__)


def format_standings_table_for_telegram(
    standings: Dict[str, Dict],
    title: str,
    standings_method: str = None,
) -> str:
    """Format standings data into a Telegram-friendly table."""
    message = f"STANDINGS: <b>{title}</b>\n\n"

    sorted_standings = sorted(standings.items(), key=lambda item: item[1].get("position", 999))

    for team_name, stats in sorted_standings:
        pos = stats.get("rank", stats.get("position", "?"))
        pts = stats.get("points", 0)
        wins = stats.get("wins", 0)
        draws = stats.get("draws", 0)
        losses = stats.get("losses", 0)
        gd = stats.get("diff", stats.get("goal_diff", 0))
        gd_str = f"+{gd}" if gd > 0 else str(gd)
        games = stats.get("gp", stats.get("games_played", 0))
        pct = stats.get("pct")
        method = standings_method or ""
        ot_losses = stats.get("ot_losses", 0)
        ties = stats.get("ties", 0)

        if pct is not None and method == "win_pct":
            message += f"#{pos} {team_name}: .{int(pct * 1000):03d} ({wins}W-{losses}L) GP:{games} DIFF:{gd_str}\n"
        elif pct is not None and method == "win_pct_half_tie":
            message += f"#{pos} {team_name}: .{int(pct * 1000):03d} ({wins}W-{losses}L-{ties}T) GP:{games} DIFF:{gd_str}\n"
        elif method in {"nhl_2_1_0_otl", "hockey_3_2_1_0"} and ot_losses > 0:
            message += f"#{pos} {team_name}: {pts}pts ({wins}W-{losses}L-{ot_losses}OTL) GP:{games} DIFF:{gd_str}\n"
        else:
            message += f"#{pos} {team_name}: {pts}pts ({wins}W-{draws}D-{losses}L, GP:{games}) DIFF:{gd_str}\n"

    return message


def send_debug_telegram(message: str, chat_id: str) -> None:
    """Send a debug message to Telegram if the bot is configured."""
    import requests

    if not Config.TELEGRAM_BOT_TOKEN or not chat_id:
        logger.debug("Telegram not configured for debug messages")
        return

    try:
        url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Debug standings sent to personal chat")
        else:
            logger.warning("Failed to send debug standings: %s", response.status_code)
    except Exception as exc:
        logger.error("Error sending debug Telegram: %s", exc)


def get_round_cutoff_timestamps(season_id: int, rounds: List[int]) -> Dict[int, float]:
    """Find the timestamp when all teams in the season have completed N games."""
    from .constants import get_all_season_ids

    query = text(
        """
        SELECT
            home_team,
            away_team,
            start_time_utc
        FROM season_events_with_results
        WHERE season_id = ANY(:season_ids)
          AND round = 'regular_season'
        ORDER BY start_time_utc ASC
        """
    )

    team_game_counts: Dict[str, int] = {}
    round_cutoffs: Dict[int, float] = {}

    with db_manager.get_session() as session:
        result = session.execute(query, {"season_ids": get_all_season_ids(season_id)})
        all_rows = result.fetchall()

        logger.info("ROUND CUTOFF: scanning %s events in season %s for rounds %s", len(all_rows), season_id, rounds)

        for row in all_rows:
            home_team = row.home_team
            away_team = row.away_team
            event_ts = row.start_time_utc.timestamp()

            team_game_counts[home_team] = team_game_counts.get(home_team, 0) + 1
            team_game_counts[away_team] = team_game_counts.get(away_team, 0) + 1

            if team_game_counts:
                min_games = min(team_game_counts.values())
                for round_num in rounds:
                    if round_num not in round_cutoffs and min_games >= round_num:
                        round_cutoffs[round_num] = event_ts + 1
                        logger.info(
                            "ROUND CUTOFF: Round %s completed at %s (%s teams)",
                            round_num,
                            row.start_time_utc,
                            len(team_game_counts),
                        )

            if len(round_cutoffs) == len(rounds):
                break

    return round_cutoffs


def debug_standings_enabled() -> bool:
    """Return whether Telegram debug output can be attempted."""
    return bool(Config.TELEGRAM_BOT_TOKEN and os.getenv("PERSONAL_CHAT_ID", ""))
