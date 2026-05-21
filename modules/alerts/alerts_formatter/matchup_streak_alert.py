"""Formatting helpers for matchup streak alerts."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _format_game_date(timestamp: int) -> str:
    """Format timestamp to date string (MM/DD/YYYY)."""
    if timestamp == 0:
        return ""
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return ""


def _calculate_h2h_tennis_total_points(match: Dict, is_home: bool) -> int:
    """Calculate total tennis points from all periods for an H2H match."""
    prefix = "hist_home" if is_home else "hist_away"
    period1 = match.get(f"{prefix}_period1", 0) or 0
    period2 = match.get(f"{prefix}_period2", 0) or 0
    period3 = match.get(f"{prefix}_period3", 0) or 0
    return period1 + period2 + period3


def _calculate_ranking_prediction(streak, home_total_games, away_total_games) -> Optional[Dict]:
    """Calculate ranking prediction based on final real rankings and historical form points."""
    home_ranking = streak.home_team_final_real_ranking
    away_ranking = streak.away_team_final_real_ranking

    if streak.sport not in ["Tennis", "Tennis Doubles"]:
        return None

    if home_ranking == 0 or away_ranking == 0:
        return None

    if home_ranking < away_ranking:
        best_ranking = home_ranking
        worst_ranking = away_ranking
        best_team_name = streak.home_team_name
        worst_team_name = streak.away_team_name
        best_batches = streak.home_team_batches
        worst_batches = streak.away_team_batches
        best_total_games = home_total_games
        worst_total_games = away_total_games
    else:
        best_ranking = away_ranking
        worst_ranking = home_ranking
        best_team_name = streak.away_team_name
        worst_team_name = streak.home_team_name
        best_batches = streak.away_team_batches
        worst_batches = streak.home_team_batches
        best_total_games = away_total_games
        worst_total_games = home_total_games

    ranking_advantage = abs(best_ranking - worst_ranking)

    best_total_points = 0
    for batch in best_batches:
        best_total_points += batch.get("batch_home_net_points", 0) + batch.get("batch_away_net_points", 0)

    worst_total_points = 0
    for batch in worst_batches:
        worst_total_points += batch.get("batch_home_net_points", 0) + batch.get("batch_away_net_points", 0)

    best_total_points_per_game = best_total_points / best_total_games if best_total_games > 0 else 0
    worst_total_points_per_game = worst_total_points / worst_total_games if worst_total_games > 0 else 0
    prediction_diff = best_total_points - worst_total_points

    return {
        "ranking_advantage": ranking_advantage,
        "best_ranking": best_ranking,
        "worst_ranking": worst_ranking,
        "best_total_points_per_game": best_total_points_per_game,
        "worst_total_points_per_game": worst_total_points_per_game,
        "best_team_name": best_team_name,
        "worst_team_name": worst_team_name,
        "best_total_points": best_total_points,
        "worst_total_points": worst_total_points,
        "prediction_diff": prediction_diff,
        "best_total_games": best_total_games,
        "worst_total_games": worst_total_games,
    }


def _format_signed_metric(value: Any) -> str:
    """Format numeric metrics with a signed prefix when possible."""
    if value is None:
        return "N/A"
    try:
        numeric = float(value)
        if numeric.is_integer():
            numeric = int(numeric)
        return f"{numeric:+d}" if isinstance(numeric, int) else f"{numeric:+g}"
    except (TypeError, ValueError):
        return str(value)


def _format_compact_standing(
    standing: Optional[Dict],
) -> str:
    """Format a canonical standing snapshot with rank, points, GP and DIFF."""
    standing = standing if isinstance(standing, dict) else {}
    rank = standing.get("rank")
    points = standing.get("points")
    gp = standing.get("gp")
    diff = standing.get("diff")

    parts: List[str] = []
    if rank is not None:
        parts.append(f"#{rank}")

    # point diff set as diff, removed for character limit purposes
    # if diff is not None:
    #     parts.append(f"DIFF:{_format_signed_metric(diff)}")
    return " ".join(parts)


def create_matchup_streak_message(streak) -> str:
    """Create Matchup streak alert message for Telegram."""
    try:
        away_total_games = (
            len(streak.away_team_results)
            if hasattr(streak, "away_team_results")
            else (streak.away_team_wins + streak.away_team_losses + streak.away_team_draws)
        )
        home_total_games = (
            len(streak.home_team_results)
            if hasattr(streak, "home_team_results")
            else (streak.home_team_wins + streak.home_team_losses + streak.home_team_draws)
        )

        message = f"📊 <b>{streak.discovery_source.title().replace('_', ' ')} Matchup Streak Analysis Alert</b>\n"
        message += f"🏆 <b>{streak.event_id} {streak.participants}</b>\n"
        if streak.sport == "Football":
            message += "⚽ "
        elif streak.sport == "Basketball":
            message += "🏀 "
        elif streak.sport == "Tennis":
            message += f"⚜️ H~{streak.sofascores_snapshot_home_team_ranking} vs A~{streak.sofascores_snapshot_away_team_ranking}\n🎾 "
        elif streak.sport == "Hockey":
            message += "🏒 "
        elif streak.sport == "Baseball":
            message += "⚾ "
        elif streak.sport == "Handball":
            message += f"🤾 {streak.sport})"
        elif streak.sport == "Rugby":
            message += "🏉 "
        elif streak.sport == "American Football":
            message += "🏈 "
        elif streak.sport == "Volleyball":
            message += "🏐 "
        else:
            message += f"🏟️ {streak.sport}"

        message += f"({streak.competition_name})\n"

        if streak.minutes_until_start == 0:
            message += "\n🕔 Event is startig now!"
        elif streak.minutes_until_start < 0:
            message += "\n🕔 Event is Live!"
        else:
            message += f"\n🕔 {streak.minutes_until_start} minutes\n"

        if streak.one_open is not None and streak.one_final is not None:
            odds_display = f"1: {streak.one_open}→{streak.one_final}"
            if streak.x_open is not None and streak.x_final is not None:
                odds_display += f", X: {streak.x_open}→{streak.x_final}"
            odds_display += f", 2: {streak.two_open}→{streak.two_final}"
            message += f"💰 {odds_display}\n"
        elif streak.one_final is not None:
            odds_display = f"1: {streak.one_final}"
            if streak.x_final is not None:
                odds_display += f", X: {streak.x_final}"
            odds_display += f", 2: {streak.two_final}"
            message += f"💰 {odds_display}\n"

        overall_streak_lines = []
        if getattr(streak, "home_current_win_streak", 0):
            overall_streak_lines.append(
                f"{streak.home_team_name}: {streak.home_current_win_streak} consecutive wins"
            )
        if getattr(streak, "away_current_win_streak", 0):
            overall_streak_lines.append(
                f"{streak.away_team_name}: {streak.away_current_win_streak} consecutive wins"
            )
        if overall_streak_lines:
            message += "\n🎯 General Win Streaks:\n"
            for line in overall_streak_lines:
                message += f"{line}\n"

        home_standing = getattr(streak, "home_team_standing", None)
        away_standing = getattr(streak, "away_team_standing", None)
        if home_standing or away_standing:
            message += "\n🏆 Standings Snapshot:\n"

            def _format_standing_line(team_name: str, standing: Dict) -> str:
                position = standing.get("rank", standing.get("position"))
                matches = standing.get("gp", standing.get("games_played", standing.get("matches")))
                wins = standing.get("wins")
                h2h_matchup_draws = standing.get("h2h_matchup_draws")
                losses = standing.get("losses")
                points = standing.get("points")
                goal_diff = standing.get("diff", standing.get("goal_diff"))
                goal_diff_display = _format_signed_metric(goal_diff)

                parts = [f"{team_name}: ", f"#{position}" if position is not None else "#N/A"]
                if points is not None:
                    parts.append(f", {points} pts")
                record_parts = []
                if wins is not None:
                    record_parts.append(f"{wins}W")
                if h2h_matchup_draws is not None:
                    record_parts.append(f"{h2h_matchup_draws}D")
                if losses is not None:
                    record_parts.append(f"{losses}L")
                if record_parts:
                    parts.append(f" ({'-'.join(record_parts)})")
                if matches is not None:
                    parts.append(f", GP:{matches}")
                # point diff set as diff, removed for character limit purposes
                # if goal_diff is not None:
                #     parts.append(f", DIFF:{goal_diff_display}")
                return "".join(parts)

            if home_standing:
                message += f"{_format_standing_line(streak.home_team_name, home_standing)}\n"
            else:
                message += f"{streak.home_team_name}: No standings data\n"

            if away_standing:
                message += f"{_format_standing_line(streak.away_team_name, away_standing)}\n"
            else:
                message += f"{streak.away_team_name}: No standings data\n"

        message += "\n📈 H2H (Last 2 Years):\n"
        message += f"Total Matches: {streak.h2h_matchup_matches_analyzed}\n"

        if hasattr(streak, "h2h_matchup_matches") and streak.h2h_matchup_matches:
            if streak.h2h_matchup_home_wins > 0:
                home_team_home_net = 0
                home_team_away_net = 0
                for m in streak.h2h_matchup_matches:
                    if m.get("winner") == "1":
                        hist_home = m.get("hist_home")
                        if streak.sport in ["Tennis", "Tennis Doubles"] and "hist_home_period1" in m:
                            hs = _calculate_h2h_tennis_total_points(m, is_home=True)
                            as_ = _calculate_h2h_tennis_total_points(m, is_home=False)
                        else:
                            hs = m.get("hist_home_score", 0)
                            as_ = m.get("hist_away_score", 0)

                        if hist_home == streak.home_team_name:
                            home_team_home_net += hs - as_
                        else:
                            home_team_away_net += as_ - hs

                home_net_str = f"+{home_team_home_net}" if home_team_home_net >= 0 else str(home_team_home_net)
                away_net_str = f"+{home_team_away_net}" if home_team_away_net >= 0 else str(home_team_away_net)
                message += (
                    f"\n{streak.home_team_name}: {streak.h2h_matchup_home_wins} wins "
                    f"({streak.h2h_matchup_home_win_rate}%) [H:{home_net_str}, A:{away_net_str}]\n"
                )
                for match in streak.h2h_matchup_matches:
                    if match.get("winner") == "1":
                        hist_home = match.get("hist_home", "Unknown")
                        hist_away = match.get("hist_away", "Unknown")
                        hist_home_score = match.get("hist_home_score", 0)
                        hist_away_score = match.get("hist_away_score", 0)
                        hist_home_penalties = match.get("hist_home_penalties", 0)
                        hist_away_penalties = match.get("hist_away_penalties", 0)
                        match_timestamp = match.get("startTimestamp", 0)
                        match_date = _format_game_date(match_timestamp)
                        date_prefix = f"{match_date} " if match_date else ""
                        if hist_home_penalties or hist_away_penalties:
                            message += (
                                f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} "
                                f"{hist_away} (P:{hist_home_penalties}-{hist_away_penalties})\n"
                            )
                        else:
                            message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away}\n"

            if streak.h2h_matchup_away_wins > 0:
                away_team_home_net = 0
                away_team_away_net = 0
                for m in streak.h2h_matchup_matches:
                    if m.get("winner") == "2":
                        hist_home = m.get("hist_home")
                        if streak.sport in ["Tennis", "Tennis Doubles"] and "hist_home_period1" in m:
                            hs = _calculate_h2h_tennis_total_points(m, is_home=True)
                            as_ = _calculate_h2h_tennis_total_points(m, is_home=False)
                        else:
                            hs = m.get("hist_home_score", 0)
                            as_ = m.get("hist_away_score", 0)

                        if hist_home == streak.away_team_name:
                            away_team_home_net += hs - as_
                        else:
                            away_team_away_net += as_ - hs

                home_net_str = f"+{away_team_home_net}" if away_team_home_net >= 0 else str(away_team_home_net)
                away_net_str = f"+{away_team_away_net}" if away_team_away_net >= 0 else str(away_team_away_net)
                message += (
                    f"\n{streak.away_team_name}: {streak.h2h_matchup_away_wins} wins "
                    f"({streak.h2h_matchup_away_win_rate}%) [H:{home_net_str}, A:{away_net_str}]\n"
                )
                for match in streak.h2h_matchup_matches:
                    if match.get("winner") == "2":
                        hist_home = match.get("hist_home", "Unknown")
                        hist_away = match.get("hist_away", "Unknown")
                        hist_home_score = match.get("hist_home_score", 0)
                        hist_away_score = match.get("hist_away_score", 0)
                        match_timestamp = match.get("startTimestamp", 0)
                        match_date = _format_game_date(match_timestamp)
                        date_prefix = f"{match_date} " if match_date else ""
                        message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away}\n"

            if streak.h2h_matchup_draws > 0:
                message += f"\nDraws: {streak.h2h_matchup_draws} ({streak.h2h_matchup_draw_rate}%)\n"
                for match in streak.h2h_matchup_matches:
                    if match.get("winner") == "X":
                        hist_home = match.get("hist_home", "Unknown")
                        hist_away = match.get("hist_away", "Unknown")
                        hist_home_score = match.get("hist_home_score", 0)
                        hist_away_score = match.get("hist_away_score", 0)
                        match_timestamp = match.get("startTimestamp", 0)
                        match_date = _format_game_date(match_timestamp)
                        date_prefix = f"{match_date} " if match_date else ""
                        message += f"{date_prefix}{hist_home} {hist_home_score}-{hist_away_score} {hist_away}\n"
        else:
            message += f"{streak.home_team_name}: {streak.h2h_matchup_home_wins} wins ({streak.h2h_matchup_home_win_rate}%)\n"
            message += f"{streak.away_team_name}: {streak.h2h_matchup_away_wins} wins ({streak.h2h_matchup_away_win_rate}%)\n"
            if streak.h2h_matchup_draws > 0:
                message += f"Draws: {streak.h2h_matchup_draws} ({streak.h2h_matchup_draw_rate}%)\n"

        message += "\n"

        if hasattr(streak, "home_team_wins") and hasattr(streak, "away_team_wins"):
            message += "📊 Season Form:\n"
            message += (
                f"{streak.home_team_name}: {streak.home_team_wins}W-"
                f"{streak.home_team_losses}L-{streak.home_team_draws}D ({home_total_games} games)\n"
            )
            message += (
                f"{streak.away_team_name}: {streak.away_team_wins}W-"
                f"{streak.away_team_losses}L-{streak.away_team_draws}D ({away_total_games} games)\n\n"
            )

            if hasattr(streak, "home_team_batches") and hasattr(streak, "away_team_batches"):
                message += "📈 Historical Form:\n"

                def _append_batches(team_name: str, batches: List[Dict], is_tennis: bool, final_real_ranking: int) -> None:
                    nonlocal message
                    if not batches:
                        message += f"<b>{team_name}</b>: No recent form data\n"
                        return

                    final_ranking_str = f" (~{final_real_ranking})" if final_real_ranking > 0 else ""
                    message += f"<b>{team_name}{final_ranking_str}</b>:\n"

                    cumulative_home_net = 0
                    cumulative_away_net = 0
                    for i, batch in enumerate(batches):
                        game_count = (i + 1) * 5
                        batch_summary = (
                            f"{game_count}: {batch['batch_wins']}W-{batch['batch_losses']}L-"
                            f"{batch['batch_draws']}D"
                        )
                        if batch["batch_net_points"] > 0:
                            batch_summary += f"(+{batch['batch_net_points']})"
                        elif batch["batch_net_points"] < 0:
                            batch_summary += f"({batch['batch_net_points']})"
                        else:
                            batch_summary += " (0)"

                        home_net = batch.get("batch_home_net_points", 0)
                        away_net = batch.get("batch_away_net_points", 0)
                        home_net_str = f"+{home_net}" if home_net >= 0 else str(home_net)
                        away_net_str = f"+{away_net}" if away_net >= 0 else str(away_net)
                        batch_summary += f" [H:{home_net_str}, A:{away_net_str}]"

                        if is_tennis:
                            real_ranking = batch.get("batch_real_ranking", 0)
                            if real_ranking > 0:
                                batch_summary += f" [~{real_ranking}]"

                        message += f"{batch_summary}\n"

                        for game in batch["games"]:
                            game_date = _format_game_date(game.get("startTimestamp", 0))
                            date_prefix = f"{game_date} " if game_date else ""

                            game_net_score = game.get("net_score", 0)
                            game_role = game["team_role"]
                            if game_role == "home":
                                cumulative_home_net += game_net_score
                            else:
                                cumulative_away_net += game_net_score

                            team_standing_display = _format_compact_standing(game["team_standing"])
                            opponent_standing_display = _format_compact_standing(game["opponent_standing"])

                            if is_tennis:
                                team_prefix = f"[{team_standing_display}] " if team_standing_display else ""
                                opponent_suffix = f" [{opponent_standing_display}]" if opponent_standing_display else ""
                                message += (
                                    f"{date_prefix}{team_prefix}{game['result']} vs "
                                    f"{game['opponent']}{opponent_suffix} "
                                    f"({game['team_score']}-{game['opponent_score']})\n"
                                )
                            else:
                                role_indicator = "🏠" if game_role == "home" else "✈️"
                                team_standings_str = f"[{team_standing_display}] " if team_standing_display else ""
                                opponent_standings_str = f" [{opponent_standing_display}]" if opponent_standing_display else ""

                                message += (
                                    f"{date_prefix}{role_indicator}{team_standings_str}{game['result']} vs "
                                    f"{game['opponent']} ({game['team_score']}-{game['opponent_score']})"
                                    f"{opponent_standings_str}\n"
                                )

                        if i < len(batches) - 1:
                            message += "\n"

                if streak.sport in ["Tennis", "Tennis Doubles"]:
                    _append_batches(
                        streak.home_team_name,
                        streak.home_team_batches,
                        True,
                        streak.home_team_final_real_ranking,
                    )
                else:
                    _append_batches(streak.home_team_name, streak.home_team_batches, False, 0)

                message += "\n"

                if streak.sport in ["Tennis", "Tennis Doubles"]:
                    _append_batches(
                        streak.away_team_name,
                        streak.away_team_batches,
                        True,
                        streak.away_team_final_real_ranking,
                    )
                else:
                    _append_batches(streak.away_team_name, streak.away_team_batches, False, 0)

                message += "\n"

        ranking_prediction = _calculate_ranking_prediction(streak, home_total_games, away_total_games)
        if ranking_prediction:
            message += "🎯 Ranking Prediction:\n"
            message += f"Ranking Advantage: {ranking_prediction['ranking_advantage']}\n"
            message += f"<b>{ranking_prediction['best_team_name']}</b> (~{ranking_prediction['best_ranking']}):\n"
            message += f"Total Points: {ranking_prediction['best_total_points']}\n\n"
            message += f"<b>{ranking_prediction['worst_team_name']}</b> (~{ranking_prediction['worst_ranking']}):\n"
            message += f"Total Points: {ranking_prediction['worst_total_points']}\n\n"

            prediction_diff = ranking_prediction["prediction_diff"]
            if prediction_diff > 0:
                message += f"🏆 Prediction: {ranking_prediction['best_team_name']} wins by {prediction_diff} points\n"
            elif prediction_diff < 0:
                message += f"🏆 Prediction: {ranking_prediction['worst_team_name']} wins by {abs(prediction_diff)} points\n"
            else:
                message += "🏆 Prediction: Tie (0 point difference)\n"
            message += "\n"

        return message
    except Exception as e:
        logger.error("Error creating matchup streak analysis message: %s", e)
        return f"❌ Error creating matchup streak analysis message for event {streak.event_id}: {str(e)}"


def send_matchup_streak_alerts(notifier: Any, streak_reports: List) -> bool:
    """Send Matchup streak alerts via Telegram."""
    if not streak_reports:
        return True

    success_count = 0

    try:
        from infrastructure.settings import Config
        from modules.oddsportal.oddsportal_config import SEASON_ODDSPORTAL_MAP
    except ImportError:
        Config = None

    for streak in streak_reports:
        if Config and Config.FILTER_ALERTS_BY_OP_SEASON:
            if streak.season_id not in SEASON_ODDSPORTAL_MAP:
                logger.info(
                    "🚫 Skipping Matchup streak alert for event %s (season %s) due to OP season filter.",
                    streak.event_id, streak.season_id
                )
                continue

        try:
            message = create_matchup_streak_message(streak)
            sent = notifier.send_telegram_message(message)

            if sent:
                success_count += 1
                logger.info("Matchup streak alert sent for event %s", streak.event_id)
            else:
                logger.warning("Failed to send Matchup streak alert for event %s", streak.event_id)
        except Exception as e:
            logger.error("Error sending Matchup streak alert for event %s: %s", streak.event_id, e)
            continue

    logger.info("Sent %s/%s Matchup streak alerts successfully", success_count, len(streak_reports))
    return success_count > 0


__all__ = [
    "_calculate_h2h_tennis_total_points",
    "_calculate_ranking_prediction",
    "_format_game_date",
    "create_matchup_streak_message",
    "send_matchup_streak_alerts",
]
