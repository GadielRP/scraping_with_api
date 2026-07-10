"""Pure SofaScore results parsing helpers."""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _build_sets_string(score_data: Dict, sport: str) -> Optional[str]:
    if not score_data:
        return None

    if sport and sport.lower() == "cricket":
        innings_data = score_data.get("innings")
        if not innings_data:
            return None

        innings_parts = []
        inning_num = 1
        while True:
            inning_key = f"inning{inning_num}"
            inning_data = innings_data.get(inning_key)
            if inning_data is None:
                break

            inning_score = inning_data.get("score")
            if inning_score is not None:
                innings_parts.append(str(int(inning_score)))
            inning_num += 1

        return "-".join(innings_parts) if innings_parts else None

    period_parts = []
    period_num = 1
    while True:
        period_key = f"period{period_num}"
        period_value = score_data.get(period_key)
        if period_value is None and period_key not in score_data:
            break
        if period_value is not None:
            period_parts.append(str(int(period_value)))
        period_num += 1

    if not period_parts:
        return None

    sets_string = "-".join(period_parts)
    overtime = score_data.get("overtime")
    if overtime is not None:
        sets_string += f"-({int(overtime)})"

    penalties = score_data.get("penalties")
    if penalties is not None:
        sets_string += f"+{int(penalties)}"

    return sets_string


def extract_results_from_response(
    response: Dict,
    extract_tennis_points: bool = False,
    for_streaks: bool = False,
) -> Optional[Dict]:
    try:
        if not response or "event" not in response:
            logger.warning("No event data found in results response")
            return None

        event_data = response["event"]
        sport = event_data.get("tournament", {}).get("category", {}).get("sport", {}).get("name")
        event_id = event_data.get("id")
        status = event_data.get("status", {})
        status_code = status.get("code")
        status_type = status.get("type", "").lower()
        status_description = status.get("description", "")

        finished_status_codes = {100, 110, 92, 120, 130, 140}
        canceled_status_codes = {60, 70, 80, 90, 91}

        if extract_tennis_points and status_code == 92:
            logger.info("Tennis player retired - status: %s, event_id: %s", status_description, event_id)
            return None

        if status_code in canceled_status_codes:
            logger.info("Event canceled/postponed - status: %s, event_id: %s", status_description, event_id)
            return {
                "_canceled": True,
                "status_code": status_code,
                "status_description": status_description,
            }

        if status_code not in finished_status_codes or status_type != "finished":
            logger.info("Event not finished yet - status: %s, event_id: %s", status_description, event_id)
            return None

        home_score_data = event_data.get("homeScore", {})
        away_score_data = event_data.get("awayScore", {})
        if not home_score_data or not away_score_data:
            logger.warning("Score data not found in response")
            return None

        home_penalties = None
        away_penalties = None

        if sport == "Football":
            if for_streaks:
                home_score = home_score_data.get("display")
                away_score = away_score_data.get("display")
                if status_code == 120:
                    logger.info("Penalty shootout detected - using display scores")
                    home_penalties = home_score_data.get("penalties")
                    away_penalties = away_score_data.get("penalties")
            else:
                home_score = home_score_data.get("normaltime")
                away_score = away_score_data.get("normaltime")
                # Fallback for amateur leagues that only provide 'display'/'current'
                if home_score is None and away_score is None:
                    home_score = home_score_data.get("display", home_score_data.get("current"))
                    away_score = away_score_data.get("display", away_score_data.get("current"))
        else:
            home_score = (
                home_score_data.get("display")
                if home_score_data.get("display") is not None
                else home_score_data.get("current")
                if home_score_data.get("current") is not None
                else home_score_data.get("normaltime")
                if home_score_data.get("normaltime") is not None
                else home_score_data.get("overtime")
                if home_score_data.get("overtime") is not None
                else home_score_data.get("penalties")
                if home_score_data.get("penalties") is not None
                else None
            )
            away_score = (
                away_score_data.get("display")
                if away_score_data.get("display") is not None
                else away_score_data.get("current")
                if away_score_data.get("current") is not None
                else away_score_data.get("normaltime")
                if away_score_data.get("normaltime") is not None
                else away_score_data.get("overtime")
                if away_score_data.get("overtime") is not None
                else away_score_data.get("penalties")
                if away_score_data.get("penalties") is not None
                else None
            )

        if home_score is None and "point" in home_score_data:
            try:
                home_score = int(home_score_data["point"])
            except (TypeError, ValueError):
                pass

        if away_score is None and "point" in away_score_data:
            try:
                away_score = int(away_score_data["point"])
            except (TypeError, ValueError):
                pass

        if home_score is None and away_score is None:
            if home_score_data.get("current") == 0 and away_score_data.get("current") == 0:
                home_score = 0
                away_score = 0
            else:
                logger.warning("Could not extract valid scores from response")
                return None

        if home_score is None or away_score is None:
            logger.warning("Could not extract valid scores from response")
            return None

        winner = None
        winner_code = event_data.get("winnerCode")

        if status_code == 120 and for_streaks and sport == "Football":
            if home_penalties is not None and away_penalties is not None:
                if home_penalties > away_penalties:
                    winner = "1"
                elif home_penalties < away_penalties:
                    winner = "2"

        if status_code == 120 and sport == "Football":
            if home_score == away_score:
                winner = "X"
            elif home_score > away_score:
                winner = "1"
            else:
                winner = "2"
        else:
            if winner_code == 1:
                winner = "1"
            elif winner_code == 2:
                winner = "2"
            elif winner_code == 3:
                winner = "X"
            elif home_score == away_score:
                winner = "X"
            elif home_score > away_score:
                winner = "1"
            else:
                winner = "2"

        result_data = {
            "home_score": int(home_score),
            "away_score": int(away_score),
            "winner": winner,
            "home_sets": _build_sets_string(home_score_data, sport),
            "away_sets": _build_sets_string(away_score_data, sport),
        }

        if extract_tennis_points:
            result_data.update(
                {
                    "home_period1": home_score_data.get("period1"),
                    "home_period2": home_score_data.get("period2"),
                    "home_period3": home_score_data.get("period3"),
                    "away_period1": away_score_data.get("period1"),
                    "away_period2": away_score_data.get("period2"),
                    "away_period3": away_score_data.get("period3"),
                }
            )

        if for_streaks and (home_penalties or away_penalties) and sport == "Football":
            result_data["home_penalties"] = home_penalties
            result_data["away_penalties"] = away_penalties

        return result_data
    except Exception as exc:
        logger.error("Error extracting results from response: %s", exc)
        return None
