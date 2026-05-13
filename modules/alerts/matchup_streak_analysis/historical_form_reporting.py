"""Optional reporting helpers for DB-backed historical form analysis."""

import logging
import os
from typing import Dict, List, Optional

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config

logger = logging.getLogger(__name__)

_GROUP_ORDER = {
    "Eastern": 0,
    "Western": 1,
    "AFC": 2,
    "NFC": 3,
    "AL": 4,
    "NL": 5,
}


def _format_standing_line(team_name: str, stats: Dict, standings_method: str = None) -> str:
    """Format a single standings row using canonical fields."""
    pos = stats.get("rank") or "?"
    pts = stats.get("points", 0)
    wins = stats.get("wins", 0)
    draws = stats.get("draws", 0)
    losses = stats.get("losses", 0)
    gd = stats.get("diff", 0)
    gd_str = f"+{gd}" if gd > 0 else str(gd)
    games = stats.get("gp", 0)
    pct = stats.get("pct")
    method = standings_method or ""
    ot_losses = stats.get("ot_losses", 0)
    ties = stats.get("ties", 0)

    if pct is not None and method == "win_pct":
        return f"#{pos} {team_name}: .{int(pct * 1000):03d} ({wins}W-{losses}L) GP:{games} DIFF:{gd_str}"
    if pct is not None and method == "win_pct_half_tie":
        return f"#{pos} {team_name}: .{int(pct * 1000):03d} ({wins}W-{losses}L-{ties}T) GP:{games} DIFF:{gd_str}"
    if method in {"nhl_2_1_0_otl", "hockey_3_2_1_0"} and ot_losses > 0:
        return f"#{pos} {team_name}: {pts}pts ({wins}W-{losses}L-{ot_losses}OTL) GP:{games} DIFF:{gd_str}"
    return f"#{pos} {team_name}: {pts}pts ({wins}W-{draws}D-{losses}L, GP:{games}) DIFF:{gd_str}"


def _group_sort_key(group_name: str) -> tuple:
    if group_name == "UNKNOWN":
        return (2, "", group_name)
    if group_name in _GROUP_ORDER:
        return (0, _GROUP_ORDER[group_name], group_name)
    return (1, group_name.lower(), group_name)


def format_standings_table_for_telegram(
    standings: Dict[str, Dict],
    title: str,
    standings_method: str = None,
) -> str:
    """Format standings data into a Telegram-friendly table."""
    message = f"STANDINGS: <b>{title}</b>\n\n"
    has_groups = any((stats.get("group") or stats.get("conference")) for stats in standings.values())

    if not has_groups:
        sorted_standings = sorted(standings.items(), key=lambda item: item[1].get("rank") or 999)
        for team_name, stats in sorted_standings:
            message += _format_standing_line(team_name, stats, standings_method) + "\n"
        return message

    grouped_standings: Dict[str, List[tuple]] = {}
    for team_name, stats in standings.items():
        group_name = stats.get("group") or stats.get("conference") or "UNKNOWN"
        grouped_standings.setdefault(group_name, []).append((team_name, stats))

    for group_name in sorted(grouped_standings.keys(), key=_group_sort_key):
        message += f"<b>{group_name}</b>\n"
        sorted_group = sorted(
            grouped_standings[group_name],
            key=lambda item: item[1].get("rank") or 999,
        )
        for team_name, stats in sorted_group:
            message += _format_standing_line(team_name, stats, standings_method) + "\n"
        message += "\n"

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


def get_round_cutoff_timestamps(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    season_id: int,
    rounds: List[int],
) -> Dict[int, float]:
    """Find the timestamp when all teams in the season have completed N games."""
    from modules.competition.league_config import get_included_season_ids
    from modules.competition.league_config import get_collected_season_bundle

    if source_unique_tournament_id is None:
        return {}

    included_season_ids = get_included_season_ids(
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
            home_team,
            away_team,
            start_time_utc
        FROM season_events_with_results
        WHERE season_id = ANY(:season_ids)
          AND round = 'regular_season'
          AND source_unique_tournament_id = :source_unique_tournament_id
    """
    query_params = {
        "season_ids": list(included_season_ids),
        "source_unique_tournament_id": source_unique_tournament_id,
    }
    if tournament_ids:
        query_sql += " AND source_tournament_id = ANY(:source_tournament_ids)"
        query_params["source_tournament_ids"] = list(tournament_ids)
    query_sql += " ORDER BY start_time_utc ASC"
    query = text(query_sql)

    team_game_counts: Dict[str, int] = {}
    round_cutoffs: Dict[int, float] = {}

    with db_manager.get_session() as session:
        result = session.execute(query, query_params)
        all_rows = result.fetchall()

        logger.info(
            "ROUND CUTOFF: scanning %s events in season %s for rounds %s (included_season_ids=%s, source_unique_tournament_id=%s, source_tournament_id=%s)",
            len(all_rows),
            season_id,
            rounds,
            included_season_ids,
            source_unique_tournament_id,
            source_tournament_id,
        )

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
