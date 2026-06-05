"""M4 - Quality-Adjusted Immediate State Engine.

PURPOSE
-------
M4 measures how teams are arriving RIGHT NOW, adjusting recent results by
the quality of the rivals faced. It answers the question:

    "How is each team performing lately, considering WHO they played against?"

A win vs the league leader counts more than a win vs the last-place team.
A loss vs the top rival penalises less than a loss vs a weak opponent.

WHAT M4 MEASURES
----------------
- Adjusted recent form: recent results and score margins weighted by
  opponent strength.
- Quality-corrected momentum: eliminates "fake momentum" from soft schedules.

WHAT M4 DOES NOT MEASURE
-------------------------
- Long-term structural strength              → M1
- Offensive production profile               → M2
- Head-to-head historical matchup            → M3
- Competitive schedule cost (fixture burden) → M5
- Long-term structural trajectory / drift    → M6
- Expectation vs opponent level              → M7

KEY PARAMETERS (SEALED)
-----------------------
- WINDOW_SIZE = 5           Last 5 valid matches per team (most recent first).
- RESULT_SCORE_WEIGHT = 0.60  Weight of adjusted win/draw/loss score.
- GD_SCORE_WEIGHT     = 0.40  Weight of adjusted points-difference score.

CORE FORMULAS
-------------
1. OPP_STRENGTH (opponent strength by league rank):
       OPP_STRENGTH = 1 - (rank - 1) / (n_liga - 1)
       Range: 0.0 (last place) → 1.0 (first place).

2. RESULT_VALUE (numeric result encoding):
       WIN  → +1.0
       DRAW →  0.0
       LOSS → -1.0

3. ADJUSTED_RESULT (quality-adjusted result per match):
       ADJUSTED_RESULT = RESULT_VALUE × OPP_STRENGTH

4. ADJUSTED_GD (quality-adjusted points difference per match):
       ADJUSTED_GD = points_difference × OPP_STRENGTH

5. HOME/AWAY sums over the window:
       home_adjusted_result = Σ ADJUSTED_RESULT  (home team, last 5)
       away_adjusted_result = Σ ADJUSTED_RESULT  (away team, last 5)
       home_adjusted_gd     = Σ ADJUSTED_GD      (home team, last 5)
       away_adjusted_gd     = Σ ADJUSTED_GD      (away team, last 5)

6. RESULT_SCORE (relative result score, normalised):
       RESULT_SCORE = (home_adjusted_result - away_adjusted_result)
                      / (|home_adjusted_result| + |away_adjusted_result|)
       Range: [-1.0, +1.0]. Positive → home advantage.

7. GD_SCORE (relative points-difference score, normalised):
       GD_SCORE = (home_adjusted_gd - away_adjusted_gd)
                  / (|home_adjusted_gd| + |away_adjusted_gd|)
       Range: [-1.0, +1.0]. Positive → home advantage.

8. M4_EDGE_RAW (weighted aggregation):
       M4_EDGE_RAW = RESULT_SCORE_WEIGHT × RESULT_SCORE
                   + GD_SCORE_WEIGHT     × GD_SCORE

9. M4_EDGE (final, clamped):
       M4_EDGE = clamp(M4_EDGE_RAW, -1.0, 1.0)

OUTPUT
------
- value     : M4_EDGE  (float in [-1.0, +1.0], positive = home advantage)
- bias      : HOME | NEUTRAL | AWAY
- strength  : IGNORE | LOW | MEDIUM | HIGH | EXTREME  (based on |M4_EDGE|)
- status    : ACTIVE (5 valid matches each) | DEGRADED (3–4) | INSUFFICIENT_DATA
- components: RESULT_SCORE and GD_SCORE with their individual edge, weight,
              weighted_edge, bias, and strength.

STRENGTH THRESHOLDS
-------------------
    |edge| < 0.05              → IGNORE
    0.05 ≤ |edge| < 0.15      → LOW
    0.15 ≤ |edge| < 0.30      → MEDIUM
    0.30 ≤ |edge| < 0.60      → HIGH
    |edge| ≥ 0.60             → EXTREME

DATA REQUIREMENTS
-----------------
Each match record must supply:
    opponent_ranking  : int   (league rank of the opponent, 1 = strongest)
    team_result       : str   (W/WIN/1, D/DRAW/X/0, L/LOSS/2/-1)
    team_score        : float (points scored by the team)
    opponent_score    : float (points scored by the opponent)
    startTimestamp    : int   (Unix timestamp, used for recency ordering)

League size (n_liga) is resolved from, in priority order:
    1. streak_analysis.current_standings  (number of entries)
    2. event_context.competition.number_of_teams
    3. max opponent rank inferred from match records
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

WINDOW_SIZE = 5
RESULT_SCORE_WEIGHT = 0.60
GD_SCORE_WEIGHT = 0.40
ENGINE_PROFILE = "quality_adjusted_immediate_state_v1"


@dataclass(frozen=True)
class ParsedImmediateMatch:
    event_id: Optional[int]
    start_timestamp: Optional[int]
    team_name: str
    opponent_name: str
    opponent_rank: int
    team_result: str
    result_value: float
    team_score: float
    opponent_score: float
    goal_difference: float
    opponent_strength: float
    adjusted_result: float
    adjusted_gd: float


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M4_IMMEDIATE_STATE_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M4_IMMEDIATE_STATE_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M4_IMMEDIATE_STATE_ENGINE DEBUG | %s", name)
    logger.info("M4_IMMEDIATE_STATE_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("M4_IMMEDIATE_STATE_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("M4_IMMEDIATE_STATE_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M4_IMMEDIATE_STATE_ENGINE DEBUG |   Lectura: %s", meaning)


def _fmt(value: Any, decimals: int = 6) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.{decimals}f}"
        return str(value)
    if isinstance(value, dict):
        items = list(value.items())
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items[:4])
        if len(items) > 4:
            preview += ", ..."
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence[:5])
        if len(sequence) > 5:
            preview += ", ..."
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _coerce_int(value: Any) -> Optional[int]:
    coerced_float = _coerce_float(value)
    if coerced_float is None:
        return None
    if not coerced_float.is_integer():
        return None
    return int(coerced_float)


def _normalize_result(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if not normalized:
        return None
    if normalized in {"W", "WIN", "1", "+1"}:
        return "WIN"
    if normalized in {"D", "DRAW", "X", "0"}:
        return "DRAW"
    if normalized in {"L", "LOSS", "2", "-1"}:
        return "LOSS"
    return None


def _result_value(normalized_result: str) -> float:
    if normalized_result == "WIN":
        return 1.0
    if normalized_result == "DRAW":
        return 0.0
    if normalized_result == "LOSS":
        return -1.0
    raise ValueError(f"unsupported_normalized_result={normalized_result!r}")


def _opponent_strength(rank: int, n_liga: int) -> float:
    if n_liga <= 1:
        return 0.0
    raw_strength = 1.0 - ((float(rank) - 1.0) / float(n_liga - 1))
    return clamp(raw_strength, 0.0, 1.0)


def _sort_matches_recent_first(matches: List[dict]) -> List[dict]:
    indexed_matches: List[Tuple[int, dict, Optional[int]]] = []
    for index, match in enumerate(matches):
        start_timestamp = _coerce_int(match.get("startTimestamp")) if isinstance(match, dict) else None
        indexed_matches.append((index, match, start_timestamp))

    if not any(timestamp is not None for _, _, timestamp in indexed_matches):
        return list(matches)

    ordered_matches = sorted(
        indexed_matches,
        key=lambda item: (
            0 if item[2] is not None else 1,
            -(item[2] or 0),
            item[0],
        ),
    )
    return [match for _, match, _ in ordered_matches]


def _extract_opponent_name(match: Dict[str, Any]) -> str:
    for key in ("opponent_name", "opponent", "opponent_team_name", "rival_name", "rival_team_name"):
        value = match.get(key)
        if isinstance(value, dict):
            for nested_key in ("name", "teamName", "team_name", "short_name"):
                nested_value = value.get(nested_key)
                if nested_value is not None and str(nested_value).strip():
                    return str(nested_value).strip()
            continue
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _extract_match_timestamp(match: Dict[str, Any]) -> Optional[int]:
    for key in ("startTimestamp", "start_timestamp"):
        timestamp = _coerce_int(match.get(key))
        if timestamp is not None:
            return timestamp
    return None


def _extract_event_id(match: Dict[str, Any]) -> Optional[int]:
    for key in ("event_id", "eventId", "id"):
        event_id = _coerce_int(match.get(key))
        if event_id is not None:
            return event_id
    return None


def _extract_candidate_ranks(match: Dict[str, Any]) -> List[int]:
    candidate_ranks: List[int] = []
    for key in (
        "opponent_ranking",
        "opponent_rank",
        "opponent_current_rank",
        "opponent_standing_rank",
        "opponent_position",
        "own_ranking",
        "own_rank",
        "team_ranking",
        "team_current_rank",
        "rank",
        "position",
        "standing_rank",
    ):
        rank = _coerce_int(match.get(key))
        if rank is not None and rank > 0:
            candidate_ranks.append(rank)

    for nested_key in ("team_standing", "opponent_standing"):
        nested = match.get(nested_key)
        if not isinstance(nested, dict):
            continue
        for key in ("rank", "position", "current_rank", "standing_rank"):
            rank = _coerce_int(nested.get(key))
            if rank is not None and rank > 0:
                candidate_ranks.append(rank)
    return candidate_ranks


def _resolve_n_liga(
    streak_analysis: Any,
    event_context: EventContext,
    all_matches: List[dict],
) -> Tuple[Optional[int], str]:
    current_standings = getattr(streak_analysis, "current_standings", None)
    if isinstance(current_standings, dict) and len(current_standings) > 0:
        return len(current_standings), "streak_analysis.current_standings.len"

    competition = getattr(event_context, "competition", None)
    number_of_teams = _coerce_int(getattr(competition, "number_of_teams", None))
    if number_of_teams is not None and number_of_teams > 1:
        return number_of_teams, "event_context.competition.number_of_teams"

    inferred_ranks: List[int] = []
    for match in all_matches:
        if isinstance(match, dict):
            inferred_ranks.extend(_extract_candidate_ranks(match))

    if inferred_ranks:
        inferred_n_liga = max(inferred_ranks)
        if inferred_n_liga > 1:
            return inferred_n_liga, "matches.inferred_ranking"

    return None, "unresolved"


def _parse_match(
    match: dict,
    team_name: str,
    n_liga: int,
    debug_mode: bool = False,
) -> Tuple[Optional[ParsedImmediateMatch], Optional[dict]]:
    if not isinstance(match, dict):
        invalid_match = {
            "event_id": None,
            "opponent_name": None,
            "reason": "invalid_match_type",
            "raw_fields": {
                "opponent_ranking": None,
                "team_result": None,
                "team_score": None,
                "opponent_score": None,
            },
        }
        if debug_mode:
            _debug_line("    Encuentro omitido: Tipo de partido no es diccionario (invalid_match_type)")
        return None, invalid_match

    opponent_name = _extract_opponent_name(match)
    event_id = _extract_event_id(match)
    raw_opponent_ranking = match.get("opponent_ranking")
    raw_team_result = match.get("team_result", match.get("team_result_code"))
    raw_team_score = match.get("team_score")
    raw_opponent_score = match.get("opponent_score")

    if n_liga is None or n_liga <= 1:
        invalid_match = {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "invalid_n_liga",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
        if debug_mode:
            _debug_line("    Encuentro omitido: n_liga inválido (n_liga=%s)", _fmt(n_liga))
        return None, invalid_match

    opponent_rank = _coerce_int(raw_opponent_ranking)
    if opponent_rank is None:
        invalid_match = {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_opponent_ranking",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
        if debug_mode:
            _debug_line(
                "    Encuentro omitido: event_id=%s, rival=%s | Razón: missing_opponent_ranking | ranking_crudo=%s",
                _fmt(event_id),
                _fmt(opponent_name),
                _fmt(raw_opponent_ranking),
            )
        return None, invalid_match
    if opponent_rank < 1 or opponent_rank > n_liga:
        invalid_match = {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "invalid_opponent_rank",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
        if debug_mode:
            _debug_line(
                "    Encuentro omitido: event_id=%s, rival=%s | Razón: invalid_opponent_rank | ranking=%s (n_liga=%s)",
                _fmt(event_id),
                _fmt(opponent_name),
                _fmt(opponent_rank),
                _fmt(n_liga),
            )
        return None, invalid_match

    normalized_result = _normalize_result(raw_team_result)
    if normalized_result is None:
        invalid_match = {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_or_uninterpretable_team_result",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
        if debug_mode:
            _debug_line(
                "    Encuentro omitido: event_id=%s, rival=%s | Razón: missing_or_uninterpretable_team_result | resultado_crudo=%s",
                _fmt(event_id),
                _fmt(opponent_name),
                _fmt(raw_team_result),
            )
        return None, invalid_match

    team_score = _coerce_float(raw_team_score)
    if team_score is None:
        invalid_match = {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_team_score",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
        if debug_mode:
            _debug_line(
                "    Encuentro omitido: event_id=%s, rival=%s | Razón: missing_team_score | marcador_equipo_crudo=%s",
                _fmt(event_id),
                _fmt(opponent_name),
                _fmt(raw_team_score),
            )
        return None, invalid_match

    opponent_score = _coerce_float(raw_opponent_score)
    if opponent_score is None:
        invalid_match = {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_opponent_score",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
        if debug_mode:
            _debug_line(
                "    Encuentro omitido: event_id=%s, rival=%s | Razón: missing_opponent_score | marcador_rival_crudo=%s",
                _fmt(event_id),
                _fmt(opponent_name),
                _fmt(raw_opponent_score),
            )
        return None, invalid_match

    result_value = _result_value(normalized_result)
    goal_difference = team_score - opponent_score
    opponent_strength = _opponent_strength(opponent_rank, n_liga)
    adjusted_result = result_value * opponent_strength
    adjusted_gd = goal_difference * opponent_strength

    parsed_match = ParsedImmediateMatch(
        event_id=event_id,
        start_timestamp=_extract_match_timestamp(match),
        team_name=str(team_name or ""),
        opponent_name=opponent_name,
        opponent_rank=opponent_rank,
        team_result=normalized_result,
        result_value=result_value,
        team_score=team_score,
        opponent_score=opponent_score,
        goal_difference=goal_difference,
        opponent_strength=opponent_strength,
        adjusted_result=adjusted_result,
        adjusted_gd=adjusted_gd,
    )

    if debug_mode:
        start_ts = parsed_match.start_timestamp
        date_str = "N/A"
        if start_ts is not None:
            date_str = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        _debug_line(
            "    Encuentro parseado: event_id=%s, rival=%s, rank=%s, fuerza_rival=%s, resultado=%s, "
            "valor_resultado=%s, diferencia_puntos (gd)=%s, resultado_ajustado=%s, diferencia_puntos_ajustada (adjusted_gd)=%s | fecha=%s",
            _fmt(event_id),
            _fmt(opponent_name),
            _fmt(opponent_rank),
            _fmt(opponent_strength),
            _fmt(normalized_result),
            _fmt(result_value),
            _fmt(goal_difference),
            _fmt(adjusted_result),
            _fmt(adjusted_gd),
            date_str,
        )

    return parsed_match, None


def _serialize_parsed_match(match: ParsedImmediateMatch) -> Dict[str, Any]:
    return {
        "event_id": match.event_id,
        "start_timestamp": match.start_timestamp,
        "team_name": match.team_name,
        "opponent_name": match.opponent_name,
        "opponent_rank": match.opponent_rank,
        "team_result": match.team_result,
        "result_value": match.result_value,
        "team_score": match.team_score,
        "opponent_score": match.opponent_score,
        "goal_difference": match.goal_difference,
        "opponent_strength": match.opponent_strength,
        "adjusted_result": match.adjusted_result,
        "adjusted_gd": match.adjusted_gd,
    }


def _build_team_window(
    matches: List[dict],
    team_name: str,
    n_liga: int,
    window_size: int,
    debug_mode: bool = False,
) -> Tuple[List[ParsedImmediateMatch], List[dict]]:
    parsed_matches: List[ParsedImmediateMatch] = []
    invalid_matches: List[dict] = []

    for match in _sort_matches_recent_first(matches):
        parsed_match, invalid_match = _parse_match(match, team_name, n_liga, debug_mode)
        if parsed_match is not None:
            parsed_matches.append(parsed_match)
            if len(parsed_matches) >= window_size:
                if debug_mode:
                    _debug_line("    Límite de ventana alcanzado (window_size=%s)", _fmt(window_size))
                break
            continue
        if invalid_match is not None:
            invalid_matches.append(invalid_match)

    return parsed_matches, invalid_matches


def _relative_score(home_value: float, away_value: float) -> Tuple[float, float]:
    denominator = abs(home_value) + abs(away_value)
    if denominator == 0:
        return 0.0, 0.0
    return (home_value - away_value) / denominator, denominator


def _component(name: str, edge: float, weight: float, raw: dict) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _build_insufficient_result(
    *,
    pillar_id: str,
    module_id: str,
    module_name: str,
    event_id: int,
    participants: str,
    raw: Dict[str, Any],
) -> ModuleResult:
    raw = dict(raw)
    raw.setdefault("m4_edge_raw", 0.0)
    raw.setdefault("m4_edge", 0.0)
    raw.setdefault("m4_abs_edge", 0.0)
    raw.setdefault("m4_bias", calculate_bias(0.0))
    raw.setdefault("m4_strength", classify_strength(0.0))
    raw.setdefault("m4_status", "INSUFFICIENT_DATA")
    raw.setdefault("m4_status_reason", "missing_team_results")
    raw.setdefault("engine_profile", ENGINE_PROFILE)

    return ModuleResult(
        pillar_id=pillar_id,
        module_id=module_id,
        module_name=module_name,
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=classify_strength(0.0),
        components=[],
        raw=raw,
    )


def calculate_quality_adjusted_immediate_state_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    home_results: List[Dict[str, Any]] = list(getattr(streak_analysis, "home_team_results", None) or [])
    away_results: List[Dict[str, Any]] = list(getattr(streak_analysis, "away_team_results", None) or [])
    home_team = getattr(streak_analysis, "home_team_name", None) or getattr(event_context.home, "name", None) or ""
    away_team = getattr(streak_analysis, "away_team_name", None) or getattr(event_context.away, "name", None) or ""
    event_id = getattr(streak_analysis, "event_id", getattr(event_context, "event_id", 0)) or 0
    participants = getattr(streak_analysis, "participants", None) or getattr(event_context, "participants_label", None) or f"{home_team} vs {away_team}"

    if debug_mode:
        _debug_section("INICIO M4 IMMEDIATE STATE ENGINE")
        _debug_section("CONFIGURACIÓN GLOBAL / CONSTANTES")
        _debug_line("WINDOW_SIZE: %s", _fmt(WINDOW_SIZE))
        _debug_line("RESULT_SCORE_WEIGHT: %s", _fmt(RESULT_SCORE_WEIGHT))
        _debug_line("GD_SCORE_WEIGHT (PD_SCORE_WEIGHT): %s", _fmt(GD_SCORE_WEIGHT))
        _debug_line("ENGINE_PROFILE: %s", ENGINE_PROFILE)
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", _fmt(participants))
        _debug_line("Home Team: %s", _fmt(home_team))
        _debug_line("Away Team: %s", _fmt(away_team))
        _debug_line("Home Results (Total): %s", _fmt(len(home_results)))
        _debug_line("Away Results (Total): %s", _fmt(len(away_results)))

    if not home_results or not away_results:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": 0,
            "away_valid_matches": 0,
            "home_window": [],
            "away_window": [],
            "n_liga": None,
            "n_liga_source": "missing_team_results",
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": "missing_team_results",
            "invalid_home_matches": [],
            "invalid_away_matches": [],
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            _debug_line("Estado: INACTIVE (INSUFFICIENT_DATA)")
            _debug_line("Razón: missing_team_results")
            _debug_section("FIN M4 IMMEDIATE STATE ENGINE")
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    all_matches: List[dict] = [
        match for match in home_results + away_results if isinstance(match, dict)
    ]
    if debug_mode:
        _debug_line("Resolviendo tamaño de la liga (n_liga)...")
    n_liga, n_liga_source = _resolve_n_liga(streak_analysis, event_context, all_matches)
    if debug_mode:
        _debug_line("  Fuente utilizada: %s", n_liga_source)
        _debug_line("  Valor resuelto (n_liga): %s", _fmt(n_liga))

    if n_liga is None or n_liga <= 1:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": 0,
            "away_valid_matches": 0,
            "home_window": [],
            "away_window": [],
            "n_liga": n_liga,
            "n_liga_source": n_liga_source if n_liga is not None else "missing_league_size",
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": "missing_league_size",
            "invalid_home_matches": [],
            "invalid_away_matches": [],
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            _debug_line("Estado: INACTIVE (INSUFFICIENT_DATA)")
            _debug_line("Razón: missing_league_size")
            _debug_section("FIN M4 IMMEDIATE STATE ENGINE")
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    if debug_mode:
        _debug_section("PROCESAMIENTO DE PARTIDOS - LOCAL (%s)" % home_team)
    home_window, invalid_home_matches = _build_team_window(home_results, str(home_team), n_liga, WINDOW_SIZE, debug_mode)
    
    if debug_mode:
        _debug_section("PROCESAMIENTO DE PARTIDOS - VISITANTE (%s)" % away_team)
    away_window, invalid_away_matches = _build_team_window(away_results, str(away_team), n_liga, WINDOW_SIZE, debug_mode)

    home_valid_matches = len(home_window)
    away_valid_matches = len(away_window)

    if debug_mode:
        _debug_section("RESUMEN DE PARTIDOS PARSEADOS")
        _debug_line("Home Valid Matches Count: %s", _fmt(home_valid_matches))
        _debug_line("Away Valid Matches Count: %s", _fmt(away_valid_matches))
        _debug_line("Invalid Home Matches Count: %s", _fmt(len(invalid_home_matches)))
        _debug_line("Invalid Away Matches Count: %s", _fmt(len(invalid_away_matches)))

    if home_valid_matches == 0 or away_valid_matches == 0:
        status_reason = "invalid_match_data" if (invalid_home_matches or invalid_away_matches) else "not_enough_valid_matches"
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": home_valid_matches,
            "away_valid_matches": away_valid_matches,
            "home_window": [_serialize_parsed_match(match) for match in home_window],
            "away_window": [_serialize_parsed_match(match) for match in away_window],
            "n_liga": n_liga,
            "n_liga_source": n_liga_source,
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": status_reason,
            "invalid_home_matches": invalid_home_matches,
            "invalid_away_matches": invalid_away_matches,
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            _debug_line("Estado: INACTIVE (INSUFFICIENT_DATA)")
            _debug_line("Razón: %s", status_reason)
            _debug_section("FIN M4 IMMEDIATE STATE ENGINE")
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    if home_valid_matches < 3 or away_valid_matches < 3:
        status_reason = "not_enough_valid_matches"
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": home_valid_matches,
            "away_valid_matches": away_valid_matches,
            "home_window": [_serialize_parsed_match(match) for match in home_window],
            "away_window": [_serialize_parsed_match(match) for match in away_window],
            "n_liga": n_liga,
            "n_liga_source": n_liga_source,
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": status_reason,
            "invalid_home_matches": invalid_home_matches,
            "invalid_away_matches": invalid_away_matches,
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            _debug_line("Estado: INACTIVE (INSUFFICIENT_DATA)")
            _debug_line("Razón: %s", status_reason)
            _debug_section("FIN M4 IMMEDIATE STATE ENGINE")
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    home_adjusted_result = sum(match.adjusted_result for match in home_window)
    away_adjusted_result = sum(match.adjusted_result for match in away_window)
    home_adjusted_gd = sum(match.adjusted_gd for match in home_window)
    away_adjusted_gd = sum(match.adjusted_gd for match in away_window)

    if debug_mode:
        _debug_section("CÁLCULO DE SUMAS AJUSTADAS POR CALIDAD DEL RIVAL")
        _debug_line("Suma ajustada del resultado Local (home_adjusted_result): %s", _fmt(home_adjusted_result))
        _debug_line("Suma ajustada del resultado Visitante (away_adjusted_result): %s", _fmt(away_adjusted_result))
        _debug_line("Suma ajustada de diferencia de puntos Local (home_adjusted_gd): %s", _fmt(home_adjusted_gd))
        _debug_line("Suma ajustada de diferencia de puntos Visitante (away_adjusted_gd): %s", _fmt(away_adjusted_gd))

    result_score, result_denominator = _relative_score(home_adjusted_result, away_adjusted_result)
    gd_score, gd_denominator = _relative_score(home_adjusted_gd, away_adjusted_gd)

    if debug_mode:
        _debug_section("RESULT SCORE LAYER")
        if result_denominator == 0:
            _debug_formula(
                "RESULT_SCORE",
                "(home_adjusted_result - away_adjusted_result) / (abs(home_adjusted_result) + abs(away_adjusted_result))",
                f"({_fmt(home_adjusted_result)} - {_fmt(away_adjusted_result)}) / 0.0",
                "0.0",
                "Denominador es cero, result_score es 0.0"
            )
        else:
            _debug_formula(
                "RESULT_SCORE",
                "(home_adjusted_result - away_adjusted_result) / (abs(home_adjusted_result) + abs(away_adjusted_result))",
                f"({_fmt(home_adjusted_result)} - {_fmt(away_adjusted_result)}) / {_fmt(result_denominator)}",
                _fmt(result_score),
                "Puntaje de resultado relativo ajustado"
            )

        _debug_section("POINTS DIFFERENCE (GD) SCORE LAYER")
        if gd_denominator == 0:
            _debug_formula(
                "GD_SCORE",
                "(home_adjusted_gd - away_adjusted_gd) / (abs(home_adjusted_gd) + abs(away_adjusted_gd))",
                f"({_fmt(home_adjusted_gd)} - {_fmt(away_adjusted_gd)}) / 0.0",
                "0.0",
                "Denominador es cero, gd_score es 0.0"
            )
        else:
            _debug_formula(
                "GD_SCORE",
                "(home_adjusted_gd - away_adjusted_gd) / (abs(home_adjusted_gd) + abs(away_adjusted_gd))",
                f"({_fmt(home_adjusted_gd)} - {_fmt(away_adjusted_gd)}) / {_fmt(gd_denominator)}",
                _fmt(gd_score),
                "Puntaje de diferencia de puntos relativo ajustado"
            )

    m4_edge_raw = (RESULT_SCORE_WEIGHT * result_score) + (GD_SCORE_WEIGHT * gd_score)
    m4_edge = clamp(m4_edge_raw, -1.0, 1.0)
    m4_bias = calculate_bias(m4_edge)
    m4_strength = classify_strength(m4_edge)

    m4_status = "ACTIVE" if home_valid_matches >= WINDOW_SIZE and away_valid_matches >= WINDOW_SIZE else "DEGRADED"
    m4_status_reason = "active" if m4_status == "ACTIVE" else "partial_window"

    if debug_mode:
        _debug_section("AGREGACIÓN DE CAPAS (SUMA PONDERADA)")
        _debug_line("Parámetros y variables de entrada para la agregación:")
        _debug_line("  RESULT_SCORE_WEIGHT: %s", _fmt(RESULT_SCORE_WEIGHT))
        _debug_line("  GD_SCORE_WEIGHT (PD_SCORE_WEIGHT): %s", _fmt(GD_SCORE_WEIGHT))
        _debug_line("  result_score: %s", _fmt(result_score))
        _debug_line("  gd_score (pd_score): %s", _fmt(gd_score))

        _debug_formula(
            "M4_EDGE_RAW (UNCLAMPED)",
            "RESULT_SCORE_WEIGHT * result_score + GD_SCORE_WEIGHT * gd_score",
            f"{_fmt(RESULT_SCORE_WEIGHT)} * {_fmt(result_score)} + {_fmt(GD_SCORE_WEIGHT)} * {_fmt(gd_score)}",
            _fmt(m4_edge_raw),
            "Suma ponderada de puntaje de resultado y de diferencia de puntos"
        )
        _debug_formula(
            "M4_EDGE",
            "clamp(m4_edge_raw, -1.0, 1.0)",
            f"clamp({_fmt(m4_edge_raw)})",
            _fmt(m4_edge),
            "M4 edge final (clamped)"
        )
        _debug_line("M4 Bias: %s", m4_bias)
        _debug_line("M4 Strength: %s", m4_strength)
        _debug_line("M4 Status: %s (%s)", m4_status, m4_status_reason)

    components = [
        _component(
            "RESULT_SCORE",
            result_score,
            RESULT_SCORE_WEIGHT,
            {
                "home_adjusted_result": home_adjusted_result,
                "away_adjusted_result": away_adjusted_result,
                "denominator": result_denominator,
                "formula": "(home_adjusted_result - away_adjusted_result) / (abs(home_adjusted_result) + abs(away_adjusted_result))",
                "reason": "ZERO_DENOMINATOR" if result_denominator == 0 else "active",
            },
        ),
        _component(
            "GD_SCORE",
            gd_score,
            GD_SCORE_WEIGHT,
            {
                "home_adjusted_gd": home_adjusted_gd,
                "away_adjusted_gd": away_adjusted_gd,
                "denominator": gd_denominator,
                "formula": "(home_adjusted_gd - away_adjusted_gd) / (abs(home_adjusted_gd) + abs(away_adjusted_gd))",
                "reason": "ZERO_DENOMINATOR" if gd_denominator == 0 else "active",
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "window_size": WINDOW_SIZE,
        "home_valid_matches": home_valid_matches,
        "away_valid_matches": away_valid_matches,
        "home_window": [_serialize_parsed_match(match) for match in home_window],
        "away_window": [_serialize_parsed_match(match) for match in away_window],
        "n_liga": n_liga,
        "n_liga_source": n_liga_source,
        "home_adjusted_result": home_adjusted_result,
        "away_adjusted_result": away_adjusted_result,
        "home_adjusted_gd": home_adjusted_gd,
        "away_adjusted_gd": away_adjusted_gd,
        "result_score": result_score,
        "gd_score": gd_score,
        "result_score_weight": RESULT_SCORE_WEIGHT,
        "gd_score_weight": GD_SCORE_WEIGHT,
        "m4_edge_raw": m4_edge_raw,
        "m4_edge": m4_edge,
        "m4_abs_edge": abs(m4_edge),
        "m4_bias": m4_bias,
        "m4_strength": m4_strength,
        "m4_status": m4_status,
        "m4_status_reason": m4_status_reason,
        "invalid_home_matches": invalid_home_matches,
        "invalid_away_matches": invalid_away_matches,
        "engine_profile": ENGINE_PROFILE,
    }

    if debug_mode:
        _debug_section("M4 OUTPUT FINAL")
        _debug_line("value: %s", _fmt(m4_edge))
        _debug_line("bias: %s", _fmt(m4_bias))
        _debug_line("strength: %s", _fmt(m4_strength))
        _debug_line("components count: %s", _fmt(len(components)))
        for comp in components:
            _debug_line(
                "  Component %s | edge=%s | weight=%s | weighted_edge=%s | bias=%s | strength=%s",
                comp.name,
                _fmt(comp.edge),
                _fmt(comp.weight),
                _fmt(comp.weighted_edge),
                comp.bias,
                comp.strength,
            )
        _debug_section("FIN M4 IMMEDIATE STATE ENGINE")

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M4",
        module_name="Quality-Adjusted Immediate State Engine",
        event_id=event_id,
        participants=participants,
        value=m4_edge,
        bias=m4_bias,
        strength=m4_strength,
        components=components,
        raw=raw,
    )
