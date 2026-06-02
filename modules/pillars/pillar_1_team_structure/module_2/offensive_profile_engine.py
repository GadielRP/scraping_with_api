"""M2 - Offensive Profile Engine v2.0."""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M2_OFFENSIVE_PROFILE_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG | %s", name)
    logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG |   Lectura: %s", meaning)


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


_ENGINE_VERSION = "offensive_profile_engine_v2.0"
_STRENGTH_THRESHOLD_PROFILE = "common.DEFAULT_STRENGTH_THRESHOLDS"
_COMPONENT_WEIGHTS = {
    "SCORING_CONSISTENCY_EDGE": 0.35,
    "OFFENSIVE_CEILING_EDGE": 0.30,
    "BLANK_RATE_EDGE": 0.20,
    "EXPLOSION_FREQUENCY_EDGE": 0.15,
}
_EXPLOSION_THRESHOLD = 2.0


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_text(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text is not None:
            return text
    return ""


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        coerced = float(value)
        return coerced if math.isfinite(coerced) else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            coerced = float(stripped)
        except ValueError:
            return None
        return coerced if math.isfinite(coerced) else None
    return None


def _extract_game_gf_trace(game: Dict[str, Any]) -> tuple[Optional[float], Optional[str], Any, Optional[str]]:
    for key in ("team_score", "score_for", "goals_for", "gf"):
        raw_value = game.get(key)
        gf = _coerce_float(raw_value)
        if gf is not None:
            return gf, key, raw_value, None

    for key in ("team_score", "score_for", "goals_for", "gf"):
        if key in game:
            raw_value = game.get(key)
            if raw_value is None:
                return None, key, raw_value, "null_gf_value"
            return None, key, raw_value, "invalid_gf_value"

    return None, None, None, "missing_gf_keys"


def _extract_gf_series(results: Any, side_label: str, debug_mode: bool = False) -> List[float]:
    if not isinstance(results, (list, tuple)):
        if debug_mode:
            _debug_line("Results for %s is not a list/tuple: %r", side_label, results)
        return []

    series: List[float] = []
    log_lines = []

    for index, result in enumerate(results):
        if not isinstance(result, dict):
            if debug_mode:
                log_lines.append(f"  Game {len(results) - index} skipped: non-dict result raw={result!r}")
            continue
        gf, source_key, raw_value, reason = _extract_game_gf_trace(result)
        if gf is None:
            if debug_mode:
                opponent = result.get("opponent_name") or result.get("opponent") or "opponent"
                game_date = ""
                if "startTimestamp" in result:
                    import datetime
                    game_date = f" [{datetime.datetime.fromtimestamp(result['startTimestamp']).strftime('%Y-%m-%d')}]"
                log_lines.append(f"  Game {len(results) - index}{game_date} vs {opponent}: GF could not be extracted (reason={reason}, raw={raw_value!r})")
            continue
        if debug_mode:
            opponent = result.get("opponent_name") or result.get("opponent") or "opponent"
            game_date = ""
            if "startTimestamp" in result:
                import datetime
                game_date = f" [{datetime.datetime.fromtimestamp(result['startTimestamp']).strftime('%Y-%m-%d')}]"
            log_lines.append(f"  Game {len(results) - index}{game_date} vs {opponent}: gf={gf} (extracted from key '{source_key}' with raw value {raw_value!r})")
        series.append(gf)

    if debug_mode:
        _debug_line("Extracting GF values from results for %s (n=%s matches, Orden Cronológico):", side_label, len(results))
        for line in reversed(log_lines):
            logger.info("M2_OFFENSIVE_PROFILE_ENGINE DEBUG | " + line)

    return series


def _population_std_trace(values: List[float]) -> tuple[float, float, List[float], float]:
    if not values:
        return 0.0, 0.0, [], 0.0
    mean = sum(values) / len(values)
    squared_diffs = [(value - mean) ** 2 for value in values]
    variance = sum(squared_diffs) / len(values)
    return math.sqrt(variance), mean, squared_diffs, variance


def _average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _top_n_average(values: List[float], n: int = 5) -> tuple[List[float], float, int]:
    if not values:
        return [], 0.0, 0
    top_values = sorted(values, reverse=True)[: min(n, len(values))]
    return top_values, _average(top_values), len(top_values)


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return count / float(total)


def _component(name: str, edge: float, weight: float, raw: Dict[str, Any]) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _m2_bias_label(edge: float) -> str:
    if edge == 0:
        return "NEUTRAL"
    if edge > 0 and abs(edge) < 0.05:
        return "SLIGHT_HOME"
    if edge < 0 and abs(edge) < 0.05:
        return "SLIGHT_AWAY"
    if edge > 0:
        return "HOME"
    return "AWAY"


def _inactive_result(
    *,
    event_id: int,
    participants: str,
    home_team: str,
    away_team: str,
    home_gf_series: List[float],
    away_gf_series: List[float],
    m2_status_reason: str,
) -> ModuleResult:
    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "home_gp": len(home_gf_series),
        "away_gp": len(away_gf_series),
        "home_game_gf": home_gf_series,
        "away_game_gf": away_gf_series,
        "std_gf_home": 0.0,
        "std_gf_away": 0.0,
        "scoring_consistency_edge": 0.0,
        "top_n_home": 0,
        "top_n_away": 0,
        "top_gf_home": [],
        "top_gf_away": [],
        "offensive_ceiling_home": 0.0,
        "offensive_ceiling_away": 0.0,
        "offensive_ceiling_edge": 0.0,
        "blanks_home": 0,
        "blanks_away": 0,
        "blank_rate_home": 0.0,
        "blank_rate_away": 0.0,
        "blank_rate_edge": 0.0,
        "explosion_threshold": _EXPLOSION_THRESHOLD,
        "explosion_games_home": 0,
        "explosion_games_away": 0,
        "explosion_rate_home": 0.0,
        "explosion_rate_away": 0.0,
        "explosion_frequency_edge": 0.0,
        "component_weights": dict(_COMPONENT_WEIGHTS),
        "weighted_edges": {
            "SCORING_CONSISTENCY_EDGE": 0.0,
            "OFFENSIVE_CEILING_EDGE": 0.0,
            "BLANK_RATE_EDGE": 0.0,
            "EXPLOSION_FREQUENCY_EDGE": 0.0,
        },
        "m2_edge_raw": 0.0,
        "m2_edge": 0.0,
        "m2_abs_edge": 0.0,
        "m2_bias": calculate_bias(0.0),
        "m2_bias_label": _m2_bias_label(0.0),
        "m2_strength": classify_strength(0.0),
        "m2_status": "INSUFFICIENT_DATA",
        "m2_status_reason": m2_status_reason,
        "engine_version": _ENGINE_VERSION,
        "strength_threshold_profile": _STRENGTH_THRESHOLD_PROFILE,
    }
    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M2",
        module_name="Offensive Profile Engine",
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=classify_strength(0.0),
        components=[],
        raw=raw,
    )


def calculate_performance_profile(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = _first_text(
        getattr(streak_analysis, "participants", None),
        getattr(event_context, "participants_label", None) if event_context is not None else None,
    )
    home_team = _first_text(
        getattr(streak_analysis, "home_team_name", None),
        getattr(getattr(event_context, "home", None), "name", None) if event_context is not None else None,
    )
    away_team = _first_text(
        getattr(streak_analysis, "away_team_name", None),
        getattr(getattr(event_context, "away", None), "name", None) if event_context is not None else None,
    )

    if debug_mode:
        _debug_section("INICIO")
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", participants or "N/A")
        _debug_line("Home team: %s", home_team or "N/A")
        _debug_line("Away team: %s", away_team or "N/A")
        _debug_line("Engine version: %s", _ENGINE_VERSION)

    home_results = getattr(streak_analysis, "home_team_results", None) or []
    away_results = getattr(streak_analysis, "away_team_results", None) or []

    home_gf_series = _extract_gf_series(home_results, "HOME", debug_mode=debug_mode)
    away_gf_series = _extract_gf_series(away_results, "AWAY", debug_mode=debug_mode)

    home_gp = len(home_gf_series)
    away_gp = len(away_gf_series)
    home_sum = sum(home_gf_series)
    away_sum = sum(away_gf_series)
    home_zero_indexes = [index for index, gf in enumerate(home_gf_series) if gf == 0]
    away_zero_indexes = [index for index, gf in enumerate(away_gf_series) if gf == 0]
    home_explosion_indexes = [index for index, gf in enumerate(home_gf_series) if gf >= _EXPLOSION_THRESHOLD]
    away_explosion_indexes = [index for index, gf in enumerate(away_gf_series) if gf >= _EXPLOSION_THRESHOLD]

    if debug_mode:
        _debug_section("INPUTS BASE")
        _debug_line("HOME GF series: count=%s, sum=%s, series=%s", home_gp, home_sum, _fmt(home_gf_series))
        _debug_line("AWAY GF series: count=%s, sum=%s, series=%s", away_gp, away_sum, _fmt(away_gf_series))
        _debug_line("Blanks (0 goals): HOME count=%s, indexes=%s | AWAY count=%s, indexes=%s",
                    len(home_zero_indexes), home_zero_indexes, len(away_zero_indexes), away_zero_indexes)
        _debug_line("Explosions (>=%s goals): HOME count=%s, indexes=%s | AWAY count=%s, indexes=%s",
                    _EXPLOSION_THRESHOLD, len(home_explosion_indexes), home_explosion_indexes,
                    len(away_explosion_indexes), away_explosion_indexes)

    if not home_gf_series or not away_gf_series:
        m2_status_reason = "missing_game_gf_series"
        if debug_mode:
            _debug_line("=> ABORT: missing_game_gf_series")
            _debug_section("OUTPUT FINAL")
            _debug_line("m2_status = INSUFFICIENT_DATA (Reason: %s)", m2_status_reason)
        return _inactive_result(
            event_id=event_id,
            participants=participants,
            home_team=home_team,
            away_team=away_team,
            home_gf_series=home_gf_series,
            away_gf_series=away_gf_series,
            m2_status_reason=m2_status_reason,
        )

    std_gf_home, mean_home, home_squared_diffs, variance_home = _population_std_trace(home_gf_series)
    std_gf_away, mean_away, away_squared_diffs, variance_away = _population_std_trace(away_gf_series)
    scoring_consistency_edge = std_gf_away - std_gf_home

    if debug_mode:
        _debug_section("SCORING_CONSISTENCY_EDGE")
        _debug_formula(
            "STD_GF_HOME",
            "STD_GF = sqrt( sum( (GF_i - mean)^2 ) / N )",
            f"mean = {mean_home:.6f} | sum((GF_i - mean)^2) = {sum(home_squared_diffs):.6f} | N = {home_gp}",
            f"{std_gf_home:.6f}"
        )
        _debug_formula(
            "STD_GF_AWAY",
            "STD_GF = sqrt( sum( (GF_i - mean)^2 ) / N )",
            f"mean = {mean_away:.6f} | sum((GF_i - mean)^2) = {sum(away_squared_diffs):.6f} | N = {away_gp}",
            f"{std_gf_away:.6f}"
        )
        _debug_formula(
            "SCORING_CONSISTENCY_EDGE_RAW",
            "SCORING_CONSISTENCY_EDGE = std_gf_away - std_gf_home",
            f"{std_gf_away:.6f} - {std_gf_home:.6f}",
            f"{scoring_consistency_edge:+.12f}",
            "Diferencia de consistencia anotadora (menor desviación estándar favorece al equipo)."
        )

    top_gf_home, offensive_ceiling_home, top_n_home = _top_n_average(home_gf_series, 5)
    top_gf_away, offensive_ceiling_away, top_n_away = _top_n_average(away_gf_series, 5)
    offensive_ceiling_edge = offensive_ceiling_home - offensive_ceiling_away

    if debug_mode:
        _debug_section("OFFENSIVE_CEILING_EDGE")
        _debug_line("HOME GF sorted descending: %s", sorted(home_gf_series, reverse=True))
        _debug_formula(
            "OFFENSIVE_CEILING_HOME",
            "OFFENSIVE_CEILING = sum(top_5_GF) / count(top_5_GF)",
            f"sum({_fmt(top_gf_home)}) / {top_n_home}",
            f"{offensive_ceiling_home:.6f}",
            "Capacidad ofensiva máxima promedio de HOME."
        )
        _debug_line("AWAY GF sorted descending: %s", sorted(away_gf_series, reverse=True))
        _debug_formula(
            "OFFENSIVE_CEILING_AWAY",
            "OFFENSIVE_CEILING = sum(top_5_GF) / count(top_5_GF)",
            f"sum({_fmt(top_gf_away)}) / {top_n_away}",
            f"{offensive_ceiling_away:.6f}",
            "Capacidad ofensiva máxima promedio de AWAY."
        )
        _debug_formula(
            "OFFENSIVE_CEILING_EDGE_RAW",
            "OFFENSIVE_CEILING_EDGE = offensive_ceiling_home - offensive_ceiling_away",
            f"{offensive_ceiling_home:.6f} - {offensive_ceiling_away:.6f}",
            f"{offensive_ceiling_edge:+.12f}",
            "Diferencia de potencial de ataque máximo."
        )

    blanks_home = sum(1 for gf in home_gf_series if gf == 0)
    blanks_away = sum(1 for gf in away_gf_series if gf == 0)
    blank_rate_home = _rate(blanks_home, home_gp)
    blank_rate_away = _rate(blanks_away, away_gp)
    blank_rate_edge = blank_rate_away - blank_rate_home

    if debug_mode:
        _debug_section("BLANK_RATE_EDGE")
        _debug_formula(
            "BLANK_RATE_HOME",
            "BLANK_RATE = blank_games / GP",
            f"{blanks_home} / {home_gp}",
            f"{blank_rate_home:.6f}",
            "Frecuencia con la que HOME se queda sin anotar goles."
        )
        _debug_formula(
            "BLANK_RATE_AWAY",
            "BLANK_RATE = blank_games / GP",
            f"{blanks_away} / {away_gp}",
            f"{blank_rate_away:.6f}",
            "Frecuencia con la que AWAY se queda sin anotar goles."
        )
        _debug_formula(
            "BLANK_RATE_EDGE_RAW",
            "BLANK_RATE_EDGE = blank_rate_away - blank_rate_home",
            f"{blank_rate_away:.6f} - {blank_rate_home:.6f}",
            f"{blank_rate_edge:+.12f}",
            "Diferencial de partidos en blanco (frecuencia de inactividad goleadora)."
        )

    explosion_games_home = sum(1 for gf in home_gf_series if gf >= _EXPLOSION_THRESHOLD)
    explosion_games_away = sum(1 for gf in away_gf_series if gf >= _EXPLOSION_THRESHOLD)
    explosion_rate_home = _rate(explosion_games_home, home_gp)
    explosion_rate_away = _rate(explosion_games_away, away_gp)
    explosion_frequency_edge = explosion_rate_home - explosion_rate_away

    if debug_mode:
        _debug_section("EXPLOSION_FREQUENCY_EDGE")
        _debug_formula(
            "EXPLOSION_RATE_HOME",
            "EXPLOSION_RATE = explosion_games / GP",
            f"{explosion_games_home} / {home_gp}",
            f"{explosion_rate_home:.6f}",
            f"Frecuencia con la que HOME anota >= {_EXPLOSION_THRESHOLD} goles."
        )
        _debug_formula(
            "EXPLOSION_RATE_AWAY",
            "EXPLOSION_RATE = explosion_games / GP",
            f"{explosion_games_away} / {away_gp}",
            f"{explosion_rate_away:.6f}",
            f"Frecuencia con la que AWAY anota >= {_EXPLOSION_THRESHOLD} goles."
        )
        _debug_formula(
            "EXPLOSION_FREQUENCY_EDGE_RAW",
            "EXPLOSION_FREQUENCY_EDGE = explosion_rate_home - explosion_rate_away",
            f"{explosion_rate_home:.6f} - {explosion_rate_away:.6f}",
            f"{explosion_frequency_edge:+.12f}",
            "Diferencial de frecuencia de explosión goleadora."
        )

    consistency_weighted = scoring_consistency_edge * _COMPONENT_WEIGHTS["SCORING_CONSISTENCY_EDGE"]
    ceiling_weighted = offensive_ceiling_edge * _COMPONENT_WEIGHTS["OFFENSIVE_CEILING_EDGE"]
    blank_weighted = blank_rate_edge * _COMPONENT_WEIGHTS["BLANK_RATE_EDGE"]
    explosion_weighted = explosion_frequency_edge * _COMPONENT_WEIGHTS["EXPLOSION_FREQUENCY_EDGE"]
    m2_edge_raw = consistency_weighted + ceiling_weighted + blank_weighted + explosion_weighted
    m2_edge = clamp(m2_edge_raw)

    m2_bias = calculate_bias(m2_edge)
    m2_bias_label = _m2_bias_label(m2_edge)
    m2_strength = classify_strength(m2_edge)

    m2_status = "ACTIVE"
    m2_status_reason = "active"
    if home_gp < 5 or away_gp < 5:
        m2_status = "DEGRADED"
        m2_status_reason = "low_sample_size"

    if debug_mode:
        _debug_section("FORMULA FINAL M2")
        _debug_formula(
            "M2_EDGE_RAW",
            "M2_EDGE_RAW = 0.35(SCORING_CONSISTENCY_EDGE) + 0.30(OFFENSIVE_CEILING_EDGE) + 0.20(BLANK_RATE_EDGE) + 0.15(EXPLOSION_FREQUENCY_EDGE)",
            f"0.35 * ({_fmt(scoring_consistency_edge)}) + 0.30 * ({_fmt(offensive_ceiling_edge)}) + 0.20 * ({_fmt(blank_rate_edge)}) + 0.15 * ({_fmt(explosion_frequency_edge)})",
            _fmt(m2_edge_raw),
            "Suma ponderada de los 4 componentes ofensivos."
        )
        _debug_formula(
            "M2_EDGE_FINAL",
            "M2_EDGE = clamp(M2_EDGE_RAW)",
            f"clamp({_fmt(m2_edge_raw)})",
            f"{m2_edge:+.12f}",
            f"M2_BIAS = {m2_bias} (Label: {m2_bias_label}) | M2_STRENGTH = {m2_strength} | M2_STATUS = {m2_status}"
        )
        
        _debug_section("STRENGTH CLASSIFICATION")
        _debug_line("Nivel de magnitud:")
        _debug_line("  <0.05       -> IGNORE")
        _debug_line("  0.05 - 0.15 -> LOW")
        _debug_line("  0.15 - 0.30 -> MEDIUM")
        _debug_line("  0.30 - 0.60 -> HIGH")
        _debug_line("  >0.60       -> EXTREME")
        _debug_line("Aplicación: ABS_EDGE = %s -> M2_STRENGTH = %s", _fmt(abs(m2_edge)), m2_strength)

        _debug_section("OUTPUT FINAL")
        _debug_line("M2_EDGE = %+f", m2_edge)
        _debug_line("M2_ABS_EDGE = %f", abs(m2_edge))
        _debug_line("M2_BIAS = %s", m2_bias)
        _debug_line("M2_BIAS_LABEL = %s", m2_bias_label)
        _debug_line("M2_STRENGTH = %s", m2_strength)
        _debug_line("M2_STATUS = %s (Reason: %s)", m2_status, m2_status_reason)
        _debug_line("")
        _debug_line("Submódulos:")
        _debug_line("  SCORING_CONSISTENCY_EDGE = %+f (Weighted: %+f)", scoring_consistency_edge, consistency_weighted)
        _debug_line("  OFFENSIVE_CEILING_EDGE = %+f (Weighted: %+f)", offensive_ceiling_edge, ceiling_weighted)
        _debug_line("  BLANK_RATE_EDGE = %+f (Weighted: %+f)", blank_rate_edge, blank_weighted)
        _debug_line("  EXPLOSION_FREQUENCY_EDGE = %+f (Weighted: %+f)", explosion_frequency_edge, explosion_weighted)
        _debug_line("")
        
        _debug_section("LECTURA CORTA PARA NOTES")
        bias_team = home_team if m2_edge > 0 else (away_team if m2_edge < 0 else "Ninguno")
        _debug_line("M2 favorece ofensivamente a %s.", bias_team)
        _debug_line("La ventaja se determina combinando consistencia, techo goleador, menor tasa de partidos sin gol y mayor frecuencia de partidos con múltiples goles.")
        _debug_section("ESTADO FINAL")

    components = [
        _component(
            "SCORING_CONSISTENCY_EDGE",
            scoring_consistency_edge,
            _COMPONENT_WEIGHTS["SCORING_CONSISTENCY_EDGE"],
            {
                "std_gf_home": std_gf_home,
                "std_gf_away": std_gf_away,
                "scoring_consistency_edge": scoring_consistency_edge,
            },
        ),
        _component(
            "OFFENSIVE_CEILING_EDGE",
            offensive_ceiling_edge,
            _COMPONENT_WEIGHTS["OFFENSIVE_CEILING_EDGE"],
            {
                "top_n_home": top_n_home,
                "top_n_away": top_n_away,
                "top_gf_home": top_gf_home,
                "top_gf_away": top_gf_away,
                "offensive_ceiling_home": offensive_ceiling_home,
                "offensive_ceiling_away": offensive_ceiling_away,
                "offensive_ceiling_edge": offensive_ceiling_edge,
            },
        ),
        _component(
            "BLANK_RATE_EDGE",
            blank_rate_edge,
            _COMPONENT_WEIGHTS["BLANK_RATE_EDGE"],
            {
                "blanks_home": blanks_home,
                "blanks_away": blanks_away,
                "blank_rate_home": blank_rate_home,
                "blank_rate_away": blank_rate_away,
                "blank_rate_edge": blank_rate_edge,
            },
        ),
        _component(
            "EXPLOSION_FREQUENCY_EDGE",
            explosion_frequency_edge,
            _COMPONENT_WEIGHTS["EXPLOSION_FREQUENCY_EDGE"],
            {
                "explosion_threshold": _EXPLOSION_THRESHOLD,
                "explosion_games_home": explosion_games_home,
                "explosion_games_away": explosion_games_away,
                "explosion_rate_home": explosion_rate_home,
                "explosion_rate_away": explosion_rate_away,
                "explosion_frequency_edge": explosion_frequency_edge,
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "home_gp": home_gp,
        "away_gp": away_gp,
        "home_game_gf": home_gf_series,
        "away_game_gf": away_gf_series,
        "home_game_gf_sum": home_sum,
        "away_game_gf_sum": away_sum,
        "std_gf_home": std_gf_home,
        "std_gf_away": std_gf_away,
        "scoring_consistency_edge": scoring_consistency_edge,
        "top_n_home": top_n_home,
        "top_n_away": top_n_away,
        "top_gf_home": top_gf_home,
        "top_gf_away": top_gf_away,
        "offensive_ceiling_home": offensive_ceiling_home,
        "offensive_ceiling_away": offensive_ceiling_away,
        "offensive_ceiling_edge": offensive_ceiling_edge,
        "blanks_home": blanks_home,
        "blanks_away": blanks_away,
        "blank_rate_home": blank_rate_home,
        "blank_rate_away": blank_rate_away,
        "blank_rate_edge": blank_rate_edge,
        "explosion_threshold": _EXPLOSION_THRESHOLD,
        "explosion_games_home": explosion_games_home,
        "explosion_games_away": explosion_games_away,
        "explosion_rate_home": explosion_rate_home,
        "explosion_rate_away": explosion_rate_away,
        "explosion_frequency_edge": explosion_frequency_edge,
        "component_weights": dict(_COMPONENT_WEIGHTS),
        "weighted_edges": {
            "SCORING_CONSISTENCY_EDGE": consistency_weighted,
            "OFFENSIVE_CEILING_EDGE": ceiling_weighted,
            "BLANK_RATE_EDGE": blank_weighted,
            "EXPLOSION_FREQUENCY_EDGE": explosion_weighted,
        },
        "m2_edge_raw": m2_edge_raw,
        "m2_edge": m2_edge,
        "m2_abs_edge": abs(m2_edge),
        "m2_bias": m2_bias,
        "m2_bias_label": m2_bias_label,
        "m2_strength": m2_strength,
        "m2_status": m2_status,
        "m2_status_reason": m2_status_reason,
        "engine_version": _ENGINE_VERSION,
        "strength_threshold_profile": _STRENGTH_THRESHOLD_PROFILE,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M2",
        module_name="Offensive Profile Engine",
        event_id=event_id,
        participants=participants,
        value=m2_edge,
        bias=calculate_bias(m2_edge),
        strength=classify_strength(m2_edge),
        components=components,
        raw=raw,
    )
