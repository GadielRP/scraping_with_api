"""M3 - Matchup Engine / sample-aware direct matchup profile."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from modules.pillars.common import (
    DEFAULT_STRENGTH_MAX_LABEL,
    DEFAULT_STRENGTH_THRESHOLDS,
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

M3_STRENGTH_THRESHOLDS = DEFAULT_STRENGTH_THRESHOLDS
M3_STRENGTH_MAX_LABEL = DEFAULT_STRENGTH_MAX_LABEL
M3_STRENGTH_PROFILE = "M3_matchup_engine_sample_aware_v1"

WIN_MATCHUP_WEIGHT = 0.70
POINTS_MATCHUP_WEIGHT = 0.30


@dataclass(frozen=True)
class ParsedH2HMatch:
    home_points: float
    away_points: float
    diff: float
    result_value_home: float
    current_home_was_home: bool
    start_timestamp: Optional[int]
    raw: Dict[str, Any]


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M3_MATCHUP_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M3_MATCHUP_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M3_MATCHUP_ENGINE DEBUG | %s", name)
    logger.info("M3_MATCHUP_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("M3_MATCHUP_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("M3_MATCHUP_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M3_MATCHUP_ENGINE DEBUG |   Lectura: %s", meaning)


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


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _win_from_diff(diff: float) -> float:
    if diff > 0:
        return 1.0
    if diff < 0:
        return 0.0
    return 0.5


def _infer_current_home_was_home(
    match: Dict[str, Any],
    home_name: str,
    away_name: str,
    debug_mode: bool = False,
) -> Optional[bool]:
    role = _norm(match.get("upcoming_home_role"))
    if role == "home":
        if debug_mode:
            _debug_line("    ¿El local del encuentro actual jugó como local en este H2H?: 'upcoming_home_role' es 'home' -> Sí (True)")
        return True
    if role == "away":
        if debug_mode:
            _debug_line("    ¿El local del encuentro actual jugó como local en este H2H?: 'upcoming_home_role' es 'away' -> No (False)")
        return False

    hist_home = _norm(match.get("hist_home"))
    hist_away = _norm(match.get("hist_away"))
    if hist_home == home_name and hist_away == away_name:
        if debug_mode:
            _debug_line(f"    ¿El local del encuentro actual jugó como local en este H2H?: hist_home ({hist_home}) coincide con el local actual ({home_name}) -> Sí (True)")
        return True
    if hist_home == away_name and hist_away == home_name:
        if debug_mode:
            _debug_line(f"    ¿El local del encuentro actual jugó como local en este H2H?: hist_home ({hist_home}) coincide con el visitante actual ({away_name}) -> No (False)")
        return False
    if debug_mode:
        _debug_line(f"    Inferencia de localía fallida en H2H: hist_home={hist_home}, hist_away={hist_away}, local actual={home_name}, visitante actual={away_name} -> No coincide (None)")
    return None


def _classify_sample_confidence(sample_factor: float) -> str:
    if sample_factor <= 0:
        return "NONE"
    if sample_factor < 0.25:
        return "VERY LOW"
    if sample_factor < 0.50:
        return "LOW"
    if sample_factor < 0.75:
        return "MEDIUM"
    if sample_factor <= 0.90:
        return "HIGH"
    return "VERY HIGH"


def _parse_h2h_match(
    match: Dict[str, Any],
    home_name: str,
    away_name: str,
    debug_mode: bool = False,
) -> Optional[ParsedH2HMatch]:
    current_home_was_home = _infer_current_home_was_home(match, home_name, away_name, debug_mode)

    home_score = _coerce_float(match.get("home_score"))
    away_score = _coerce_float(match.get("away_score"))
    if home_score is not None and away_score is not None:
        diff = home_score - away_score
        res = ParsedH2HMatch(
            home_points=home_score,
            away_points=away_score,
            diff=diff,
            result_value_home=_win_from_diff(diff),
            current_home_was_home=bool(current_home_was_home) if current_home_was_home is not None else False,
            start_timestamp=_coerce_int(match.get("startTimestamp")),
            raw=match,
        )
        if debug_mode:
            start_ts = res.start_timestamp
            date_str = "N/A"
            if start_ts is not None:
                date_str = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
            hist_home = match.get("hist_home")
            hist_away = match.get("hist_away")
            if not hist_home or not hist_away:
                if res.current_home_was_home:
                    hist_home = hist_home or home_name
                    hist_away = hist_away or away_name
                else:
                    hist_home = hist_home or away_name
                    hist_away = hist_away or home_name
            _debug_line(
                "    Encuentro H2H parseado de home_score/away_score: "
                "home_points=%s, away_points=%s, diff=%s, "
                "result_value_home=%s, current_home_was_home=%s | "
                "hist_home=%s, hist_away=%s, fecha=%s",
                _fmt(res.home_points),
                _fmt(res.away_points),
                _fmt(res.diff),
                _fmt(res.result_value_home),
                _fmt(res.current_home_was_home),
                hist_home,
                hist_away,
                date_str,
            )
        return res

    hist_home_score = _coerce_float(match.get("hist_home_score"))
    hist_away_score = _coerce_float(match.get("hist_away_score"))
    hist_home = _norm(match.get("hist_home"))
    hist_away = _norm(match.get("hist_away"))
    if hist_home_score is None or hist_away_score is None:
        if debug_mode:
            _debug_line("    Encuentro H2H omitido: marcadores nulos/inválidos (home_score=%s, away_score=%s, hist_home_score=%s, hist_away_score=%s)", _fmt(home_score), _fmt(away_score), _fmt(hist_home_score), _fmt(hist_away_score))
        return None
    if hist_home == home_name and hist_away == away_name:
        home_points = hist_home_score
        away_points = hist_away_score
        current_home_was_home = True
    elif hist_home == away_name and hist_away == home_name:
        home_points = hist_away_score
        away_points = hist_home_score
        current_home_was_home = False
    else:
        if debug_mode:
            _debug_line(
                "    Encuentro H2H omitido: nombres de equipos no coinciden (hist_home=%s, "
                "hist_away=%s, home_name=%s, away_name=%s)",
                _fmt(hist_home),
                _fmt(hist_away),
                _fmt(home_name),
                _fmt(away_name),
            )
        return None

    diff = home_points - away_points
    res = ParsedH2HMatch(
        home_points=home_points,
        away_points=away_points,
        diff=diff,
        result_value_home=_win_from_diff(diff),
        current_home_was_home=current_home_was_home,
        start_timestamp=_coerce_int(match.get("startTimestamp")),
        raw=match,
    )
    if debug_mode:
        start_ts = res.start_timestamp
        date_str = "N/A"
        if start_ts is not None:
            date_str = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        hist_home_val = match.get("hist_home")
        hist_away_val = match.get("hist_away")
        if not hist_home_val or not hist_away_val:
            if res.current_home_was_home:
                hist_home_val = hist_home_val or home_name
                hist_away_val = hist_away_val or away_name
            else:
                hist_home_val = hist_home_val or away_name
                hist_away_val = hist_away_val or home_name
        _debug_line(
            "    Encuentro H2H parseado de hist_home_score/hist_away_score: "
            "home_points=%s, away_points=%s, diff=%s, "
            "result_value_home=%s, current_home_was_home=%s | "
            "hist_home=%s, hist_away=%s, fecha=%s",
            _fmt(res.home_points),
            _fmt(res.away_points),
            _fmt(res.diff),
            _fmt(res.result_value_home),
            _fmt(res.current_home_was_home),
            hist_home_val,
            hist_away_val,
            date_str,
        )
    return res


def _component(name: str, edge: float, weight: float, raw: Dict[str, Any]) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(
            edge,
            thresholds=DEFAULT_STRENGTH_THRESHOLDS,
            max_label=DEFAULT_STRENGTH_MAX_LABEL,
        ),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _serialize_parsed_match(match: ParsedH2HMatch) -> Dict[str, Any]:
    return {
        "home_points": match.home_points,
        "away_points": match.away_points,
        "diff": match.diff,
        "result_value_home": match.result_value_home,
        "current_home_was_home": match.current_home_was_home,
        "start_timestamp": match.start_timestamp,
    }


def _build_inactive_result(
    *,
    event_id: int,
    participants: str,
    home_team: str,
    away_team: str,
    total_h2h: int,
    analyzed_total_h2h: Optional[int],
    parsed_h2h: List[Dict[str, Any]],
) -> ModuleResult:
    raw_edge = 0.0
    sample_factor = 0.0
    sample_confidence = _classify_sample_confidence(sample_factor)
    strength = classify_strength(
        0.0,
        thresholds=DEFAULT_STRENGTH_THRESHOLDS,
        max_label=DEFAULT_STRENGTH_MAX_LABEL,
    )
    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M3",
        module_name="Matchup Engine",
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=strength,
        components=[],
        raw={
            "home_team": home_team,
            "away_team": away_team,
            "total_h2h": total_h2h,
            "h2h_matchup_matches_analyzed": analyzed_total_h2h,
            "parsed_h2h_count": 0,
            "h2h_home_wins": 0,
            "h2h_away_wins": 0,
            "h2h_draws": 0,
            "h2h_pf_home": 0.0,
            "h2h_pf_away": 0.0,
            "win_rate_home": 0.0,
            "win_rate_away": 0.0,
            "win_matchup_edge_raw": 0.0,
            "points_matchup_edge_raw": 0.0,
            "win_matchup_weight": WIN_MATCHUP_WEIGHT,
            "points_matchup_weight": POINTS_MATCHUP_WEIGHT,
            "m3_raw_edge": raw_edge,
            "m3_edge_raw": raw_edge,
            "m3_raw_strength": strength,
            "m3_sample_factor": sample_factor,
            "m3_sample_confidence": sample_confidence,
            "m3_edge": 0.0,
            "m3_abs_edge": 0.0,
            "m3_strength": strength,
            "m3_bias": calculate_bias(0.0),
            "m3_status": "INACTIVE",
            "m3_status_reason": "NO_VALID_H2H_SAMPLE",
            "parsed_h2h": parsed_h2h,
            "strength_threshold_profile": M3_STRENGTH_PROFILE,
            "strength_thresholds": DEFAULT_STRENGTH_THRESHOLDS,
            "strength_max_label": DEFAULT_STRENGTH_MAX_LABEL,
        },
    )


def calculate_direct_matchup_profile(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    """Calculate M3 - Matchup Engine for an event."""
    home_team = getattr(streak_analysis, "home_team_name", None) or event_context.home.name
    away_team = getattr(streak_analysis, "away_team_name", None) or event_context.away.name
    participants = getattr(streak_analysis, "participants", None) or event_context.participants_label
    event_id = getattr(streak_analysis, "event_id", 0)
    h2h_matchup_matches = getattr(streak_analysis, "h2h_matchup_matches", []) or []
    analyzed_total_h2h = getattr(streak_analysis, "h2h_matchup_matches_analyzed", None)

    if debug_mode:
        _debug_section("INICIO M3 MATCHUP ENGINE")
        _debug_line("Configuración Global / Constantes:")
        _debug_line("  M3_STRENGTH_PROFILE: %s", _fmt(M3_STRENGTH_PROFILE))
        _debug_line("  M3_STRENGTH_THRESHOLDS: %s", _fmt(M3_STRENGTH_THRESHOLDS))
        _debug_line("  M3_STRENGTH_MAX_LABEL: %s", _fmt(M3_STRENGTH_MAX_LABEL))
        _debug_line("  WIN_MATCHUP_WEIGHT: %s", _fmt(WIN_MATCHUP_WEIGHT))
        _debug_line("  POINTS_MATCHUP_WEIGHT: %s", _fmt(POINTS_MATCHUP_WEIGHT))
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", _fmt(participants))
        _debug_line("Equipo Local (home_team): %s", _fmt(home_team))
        _debug_line("Equipo Visitante (away_team): %s", _fmt(away_team))
        _debug_line("H2H raw matches count: %s", _fmt(len(h2h_matchup_matches)))
        _debug_line("H2H matches analyzed in streak: %s", _fmt(analyzed_total_h2h))
        _debug_line("Comenzando parseo de encuentros H2H...")

    parsed_matches = []
    for match in h2h_matchup_matches:
        parsed = _parse_h2h_match(match, _norm(home_team), _norm(away_team), debug_mode)
        if parsed is not None:
            parsed_matches.append(parsed)

    if any(match.start_timestamp is not None for match in parsed_matches):
        parsed_matches.sort(key=lambda match: match.start_timestamp or 0, reverse=True)
        if debug_mode:
            _debug_line("Encuentros H2H ordenados cronológicamente descendente (por start_timestamp)")

    if debug_mode and parsed_matches:
        _debug_line("Listado de encuentros H2H parseados:")
        for idx, pm in enumerate(parsed_matches, 1):
            start_ts = pm.start_timestamp
            date_str = "N/A"
            if start_ts is not None:
                date_str = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
            
            hist_home = pm.raw.get("hist_home")
            hist_away = pm.raw.get("hist_away")
            if not hist_home or not hist_away:
                if pm.current_home_was_home:
                    hist_home = hist_home or home_team
                    hist_away = hist_away or away_team
                else:
                    hist_home = hist_home or away_team
                    hist_away = hist_away or home_team

            if pm.current_home_was_home:
                hist_home_score_val = pm.home_points
                hist_away_score_val = pm.away_points
            else:
                hist_home_score_val = pm.away_points
                hist_away_score_val = pm.home_points

            _debug_line(
                "  [%d] Home: %s, Away: %s | Puntos: %s - %s | Fecha: %s (timestamp: %s)",
                idx,
                hist_home,
                hist_away,
                _fmt(hist_home_score_val),
                _fmt(hist_away_score_val),
                date_str,
                _fmt(start_ts),
            )

    parsed_h2h = [_serialize_parsed_match(match) for match in parsed_matches]
    total_h2h = len(h2h_matchup_matches)
    h2h_matches = len(parsed_matches)

    if debug_mode:
        _debug_line("Activación de M3: %s (encuentros parseados válidos = %s)", "ACTIVE" if h2h_matches >= 1 else "INACTIVE", _fmt(h2h_matches))

    if h2h_matches == 0:
        if debug_mode:
            _debug_line("  => M3 INACTIVE: NO_VALID_H2H_SAMPLE")
            _debug_formula(
                "M3_RAW_EDGE",
                "0.0",
                "0.0",
                "0.0",
                "Sin encuentros válidos para calcular el edge crudo"
            )
            _debug_formula(
                "M3_SAMPLE_FACTOR",
                "0.0",
                "0.0",
                "0.0",
                "Muestra de encuentros igual a cero"
            )
            _debug_line("M3_SAMPLE_CONFIDENCE: NONE")
            _debug_formula(
                "M3_EDGE",
                "m3_raw_edge * m3_sample_factor",
                "0.0 * 0.0",
                "0.0",
                "M3 matchup edge final clamped"
            )
            _debug_line("M3_STRENGTH: IGNORE")
            _debug_line("M3_BIAS: NEUTRAL")

        res = _build_inactive_result(
            event_id=event_id,
            participants=participants,
            home_team=home_team,
            away_team=away_team,
            total_h2h=total_h2h,
            analyzed_total_h2h=analyzed_total_h2h,
            parsed_h2h=parsed_h2h,
        )

        if debug_mode:
            _debug_section("M3 OUTPUT FINAL (INACTIVE)")
            _debug_line("value: %s", _fmt(res.value))
            _debug_line("bias: %s", _fmt(res.bias))
            _debug_line("strength: %s", _fmt(res.strength))
            _debug_line("status: INACTIVE")
            _debug_line("status_reason: NO_VALID_H2H_SAMPLE")
            _debug_section("FIN M3 MATCHUP ENGINE")
        return res

    home_wins = sum(1 for match in parsed_matches if match.diff > 0)
    away_wins = sum(1 for match in parsed_matches if match.diff < 0)
    draws = sum(1 for match in parsed_matches if match.diff == 0)

    if debug_mode:
        _debug_line("Recuento de resultados en H2H:")
        _debug_line("  Victorias/Éxitos locales (home_wins): %s", _fmt(home_wins))
        _debug_line("  Victorias/Éxitos visitantes (away_wins): %s", _fmt(away_wins))
        _debug_line("  Empates (draws): %s", _fmt(draws))
        _debug_line("  Total de encuentros parseados válidos (h2h_matches): %s", _fmt(h2h_matches))

    win_rate_home = (home_wins + 0.5 * draws) / float(h2h_matches)
    win_rate_away = (away_wins + 0.5 * draws) / float(h2h_matches)
    win_matchup_edge_raw = clamp(win_rate_home - win_rate_away)

    if debug_mode:
        _debug_section("WIN MATCHUP LAYER")
        _debug_formula(
            "WIN_RATE_HOME",
            "(home_wins + 0.5 * draws) / h2h_matches",
            f"({home_wins} + 0.5 * {draws}) / {h2h_matches}",
            _fmt(win_rate_home),
            "Tasa de éxito para local en H2H"
        )
        _debug_formula(
            "WIN_RATE_AWAY",
            "(away_wins + 0.5 * draws) / h2h_matches",
            f"({away_wins} + 0.5 * {draws}) / {h2h_matches}",
            _fmt(win_rate_away),
            "Tasa de éxito para visitante en H2H"
        )
        _debug_formula(
            "WIN_MATCHUP_EDGE_RAW (UNCLAMPED)",
            "win_rate_home - win_rate_away",
            f"{_fmt(win_rate_home)} - {_fmt(win_rate_away)}",
            _fmt(win_rate_home - win_rate_away),
            "Diferencia neta de tasas de éxito en H2H"
        )
        _debug_formula(
            "WIN_MATCHUP_EDGE_RAW",
            "clamp(win_rate_home - win_rate_away)",
            f"clamp({_fmt(win_rate_home - win_rate_away)})",
            _fmt(win_matchup_edge_raw),
            "Win matchup edge final (clamped)"
        )
        win_bias = calculate_bias(win_matchup_edge_raw)
        win_strength = classify_strength(
            win_matchup_edge_raw,
            thresholds=DEFAULT_STRENGTH_THRESHOLDS,
            max_label=DEFAULT_STRENGTH_MAX_LABEL,
        )
        _debug_line("Win Matchup Bias: %s", win_bias)
        _debug_line("Win Matchup Strength: %s", win_strength)

    h2h_pf_home = sum(match.home_points for match in parsed_matches)
    h2h_pf_away = sum(match.away_points for match in parsed_matches)
    points_total = h2h_pf_home + h2h_pf_away
    points_matchup_reason = "active"
    if points_total > 0:
        points_matchup_edge_raw = clamp((h2h_pf_home - h2h_pf_away) / points_total)
    else:
        points_matchup_edge_raw = 0.0
        points_matchup_reason = "NO_POINTS_TOTAL"

    if debug_mode:
        _debug_section("POINTS MATCHUP LAYER")
        _debug_line("Puntos anotados local (h2h_pf_home): %s", _fmt(h2h_pf_home))
        _debug_line("Puntos anotados visitante (h2h_pf_away): %s", _fmt(h2h_pf_away))
        _debug_line("Puntos totales en H2H (points_total): %s", _fmt(points_total))
        _debug_line("Razón del cálculo de puntos: %s", points_matchup_reason)
        if points_total > 0:
            _debug_formula(
                "POINTS_MATCHUP_EDGE_RAW (UNCLAMPED)",
                "(h2h_pf_home - h2h_pf_away) / points_total",
                f"({_fmt(h2h_pf_home)} - {_fmt(h2h_pf_away)}) / {_fmt(points_total)}",
                _fmt((h2h_pf_home - h2h_pf_away) / points_total),
                "Diferencia de puntos normalizada en H2H"
            )
            _debug_formula(
                "POINTS_MATCHUP_EDGE_RAW",
                "clamp((h2h_pf_home - h2h_pf_away) / points_total)",
                f"clamp({_fmt((h2h_pf_home - h2h_pf_away) / points_total)})",
                _fmt(points_matchup_edge_raw),
                "Points matchup edge final (clamped)"
            )
        else:
            _debug_formula(
                "POINTS_MATCHUP_EDGE_RAW",
                "0.0",
                "0.0",
                "0.0",
                "No hubo puntos en total en el H2H"
            )
        points_bias = calculate_bias(points_matchup_edge_raw)
        points_strength = classify_strength(
            points_matchup_edge_raw,
            thresholds=DEFAULT_STRENGTH_THRESHOLDS,
            max_label=DEFAULT_STRENGTH_MAX_LABEL,
        )
        _debug_line("Points Matchup Bias: %s", points_bias)
        _debug_line("Points Matchup Strength: %s", points_strength)

    m3_raw_edge = clamp(
        WIN_MATCHUP_WEIGHT * win_matchup_edge_raw
        + POINTS_MATCHUP_WEIGHT * points_matchup_edge_raw
    )
    m3_sample_factor = min(h2h_matches / 5.0, 1.0)
    m3_sample_confidence = _classify_sample_confidence(m3_sample_factor)
    m3_edge = clamp(m3_raw_edge * m3_sample_factor)

    m3_raw_strength = classify_strength(
        m3_raw_edge,
        thresholds=DEFAULT_STRENGTH_THRESHOLDS,
        max_label=DEFAULT_STRENGTH_MAX_LABEL,
    )
    m3_strength = classify_strength(
        m3_edge,
        thresholds=DEFAULT_STRENGTH_THRESHOLDS,
        max_label=DEFAULT_STRENGTH_MAX_LABEL,
    )
    m3_bias = calculate_bias(m3_edge)

    if debug_mode:
        _debug_section("AGGREGATION AND SAMPLE DISCOUNT")
        _debug_line("Parámetros y variables de entrada para la agregación:")
        _debug_line("  WIN_MATCHUP_WEIGHT: %s", _fmt(WIN_MATCHUP_WEIGHT))
        _debug_line("  POINTS_MATCHUP_WEIGHT: %s", _fmt(POINTS_MATCHUP_WEIGHT))
        _debug_line("  win_matchup_edge_raw: %s", _fmt(win_matchup_edge_raw))
        _debug_line("  points_matchup_edge_raw: %s", _fmt(points_matchup_edge_raw))
        _debug_line("  h2h_matches: %s", _fmt(h2h_matches))

        _debug_formula(
            "M3_RAW_EDGE (UNCLAMPED)",
            "WIN_MATCHUP_WEIGHT * win_matchup_edge_raw + POINTS_MATCHUP_WEIGHT * points_matchup_edge_raw",
            f"{_fmt(WIN_MATCHUP_WEIGHT)} * {_fmt(win_matchup_edge_raw)} + {_fmt(POINTS_MATCHUP_WEIGHT)} * {_fmt(points_matchup_edge_raw)}",
            _fmt(WIN_MATCHUP_WEIGHT * win_matchup_edge_raw + POINTS_MATCHUP_WEIGHT * points_matchup_edge_raw),
            "Suma ponderada de win y points matchup edges"
        )
        _debug_formula(
            "M3_RAW_EDGE",
            "clamp(WIN_MATCHUP_WEIGHT * win_matchup_edge_raw + POINTS_MATCHUP_WEIGHT * points_matchup_edge_raw)",
            f"clamp({_fmt(WIN_MATCHUP_WEIGHT * win_matchup_edge_raw + POINTS_MATCHUP_WEIGHT * points_matchup_edge_raw)})",
            _fmt(m3_raw_edge),
            "M3 raw edge final (clamped)"
        )
        _debug_line("M3 Raw Strength: %s", m3_raw_strength)
        
        _debug_formula(
            "M3_SAMPLE_FACTOR",
            "min(h2h_matches / 5.0, 1.0)",
            f"min({h2h_matches} / 5.0, 1.0)",
            _fmt(m3_sample_factor),
            "Factor de descuento por tamaño de muestra (máximo 5 encuentros)"
        )
        _debug_line("M3 Sample Confidence: %s", m3_sample_confidence)
        
        _debug_formula(
            "M3_EDGE (UNCLAMPED)",
            "m3_raw_edge * m3_sample_factor",
            f"{_fmt(m3_raw_edge)} * {_fmt(m3_sample_factor)}",
            _fmt(m3_raw_edge * m3_sample_factor),
            "M3 matchup edge descontado"
        )
        _debug_formula(
            "M3_EDGE",
            "clamp(m3_raw_edge * m3_sample_factor)",
            f"clamp({_fmt(m3_raw_edge * m3_sample_factor)})",
            _fmt(m3_edge),
            "M3 matchup edge final (clamped)"
        )
        _debug_line("M3 Bias: %s", m3_bias)
        _debug_line("M3 Strength: %s", m3_strength)

    components = [
        _component(
            "WIN_MATCHUP_EDGE_RAW",
            win_matchup_edge_raw,
            WIN_MATCHUP_WEIGHT,
            {
                "home_wins": home_wins,
                "away_wins": away_wins,
                "draws": draws,
                "win_rate_home": win_rate_home,
                "win_rate_away": win_rate_away,
            },
        ),
        _component(
            "POINTS_MATCHUP_EDGE_RAW",
            points_matchup_edge_raw,
            POINTS_MATCHUP_WEIGHT,
            {
                "h2h_pf_home": h2h_pf_home,
                "h2h_pf_away": h2h_pf_away,
                "points_total": points_total,
                "reason": points_matchup_reason,
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "total_h2h": total_h2h,
        "h2h_matchup_matches_analyzed": analyzed_total_h2h,
        "parsed_h2h_count": h2h_matches,
        "h2h_home_wins": home_wins,
        "h2h_away_wins": away_wins,
        "h2h_draws": draws,
        "h2h_pf_home": h2h_pf_home,
        "h2h_pf_away": h2h_pf_away,
        "win_rate_home": win_rate_home,
        "win_rate_away": win_rate_away,
        "win_matchup_edge_raw": win_matchup_edge_raw,
        "points_matchup_edge_raw": points_matchup_edge_raw,
        "win_matchup_weight": WIN_MATCHUP_WEIGHT,
        "points_matchup_weight": POINTS_MATCHUP_WEIGHT,
        "m3_raw_edge": m3_raw_edge,
        "m3_edge_raw": m3_raw_edge,
        "m3_raw_strength": m3_raw_strength,
        "m3_sample_factor": m3_sample_factor,
        "m3_sample_confidence": m3_sample_confidence,
        "m3_edge": m3_edge,
        "m3_abs_edge": abs(m3_edge),
        "m3_strength": m3_strength,
        "m3_bias": m3_bias,
        "m3_status": "ACTIVE",
        "m3_status_reason": "active",
        "parsed_h2h": parsed_h2h,
        "strength_threshold_profile": M3_STRENGTH_PROFILE,
        "strength_thresholds": DEFAULT_STRENGTH_THRESHOLDS,
        "strength_max_label": DEFAULT_STRENGTH_MAX_LABEL,
    }

    if debug_mode:
        _debug_section("M3 OUTPUT FINAL")
        _debug_line("value: %s", _fmt(m3_edge))
        _debug_line("bias: %s", _fmt(m3_bias))
        _debug_line("strength: %s", _fmt(m3_strength))
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
        _debug_section("FIN M3 MATCHUP ENGINE")

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M3",
        module_name="Matchup Engine",
        event_id=event_id,
        participants=participants,
        value=m3_edge,
        bias=m3_bias,
        strength=m3_strength,
        components=components,
        raw=raw,
    )
