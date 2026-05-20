"""Shared helpers for extracting score series from result payloads."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple


def _coerce_float(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_score_for_against(game: Dict) -> Optional[Tuple[float, float]]:
    """Extract ``(for, against)`` scores from a single game payload."""
    team_score = game.get("team_score")
    opponent_score = game.get("opponent_score")
    if team_score is not None and opponent_score is not None:
        gf = _coerce_float(team_score)
        ga = _coerce_float(opponent_score)
        if gf is not None and ga is not None:
            return gf, ga

    score_for = game.get("score_for")
    score_against = game.get("score_against")
    if score_for is not None and score_against is not None:
        gf = _coerce_float(score_for)
        ga = _coerce_float(score_against)
        if gf is not None and ga is not None:
            return gf, ga

    return None


def extract_gf_ga_series(results: List[Dict]) -> List[Tuple[float, float]]:
    """Build a GF/GA series from a list of game result payloads."""
    series: List[Tuple[float, float]] = []
    for game in results:
        score = extract_score_for_against(game)
        if score is not None:
            series.append(score)
    return series
