"""Exact price memory engine for Pillar 5."""

from __future__ import annotations

import datetime
import logging
import math
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.pillar_5.exact_price_memory_engine.historical_samples import (
    ExactPriceMemorySample,
    get_exact_price_memory_sample,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== P5_EXACT_PRICE_MEMORY_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG | %s", name)
    logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG |   Lectura: %s", meaning)


def _fmt(value: Any, decimals: int = 3) -> str:
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
    if isinstance(value, Decimal):
        return f"{float(value):.{decimals}f}"
    if isinstance(value, dict):
        items = list(value.items())
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items)
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence)
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


def _format_timestamp(ts: Any) -> str:
    if ts is None:
        return "N/A"
    try:
        val = float(ts)
        if val > 0:
            dt = datetime.datetime.fromtimestamp(val, tz=datetime.timezone.utc)
            return dt.strftime("%y-%m-%d")
    except Exception:
        pass
    return str(ts)





ENGINE_VERSION = "p5_exact_price_memory_engine_v2_2"
MODULE_ID = "EXACT_PRICE_MEMORY_ENGINE"
MODULE_NAME = "Exact Price Memory Engine"

CURRENT_TARGET_MINUTE = 0
ODDS_ROUND_DECIMALS = 3

ALLOWED_BOOKIES = ["SofaScore"]

MARKET_GROUP_PRIORITY = ["1X2", "Home/Away", "ML"]


def _decimal_to_serializable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _decimal_to_serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_decimal_to_serializable(item) for item in value]
    return value


def _build_sample_payload(sample: ExactPriceMemorySample) -> dict:
    return {
        "sample_size": sample.sample_size,
        "wins_home": sample.wins_home,
        "wins_draw": sample.wins_draw,
        "wins_away": sample.wins_away,
        "rows": sample.rows,
    }


def _build_insufficient_result(
    *,
    event_context: EventContext,
    reason: str,
    sample: ExactPriceMemorySample,
    selected_market_group: Optional[str] = None,
    selected_market_period: Optional[str] = None,
    selected_market_name: Optional[str] = None,
    selected_choice_group: Optional[str] = None,
    selected_bookie_name: Optional[str] = None,
    candidate_line_count: int = 0,
    current_home_odds: Optional[Decimal] = None,
    current_draw_odds: Optional[Decimal] = None,
    current_away_odds: Optional[Decimal] = None,
    has_draw: bool = False,
) -> dict:
    sample_payload = _build_sample_payload(sample)
    return {
        "module_id": MODULE_ID,
        "module_name": MODULE_NAME,
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P5_STATUS": "INSUFFICIENT_DATA",
        "status": "INSUFFICIENT_DATA",
        "P5_VALID": False,
        "P5_DIRECTION": "NONE",
        "P5": 0.0,
        "P5_STRENGTH": "NONE",
        "sample_size": sample.sample_size,
        "wins_home": sample.wins_home,
        "wins_draw": sample.wins_draw,
        "wins_away": sample.wins_away,
        "DOMINANT_RESULT": "NONE",
        "wins_dominant": None,
        "P_hist_DOMINANT": None,
        "BASELINE": None,
        "HIST_EDGE": None,
        "CONSISTENCY": None,
        "SAMPLE_FACTOR": None,
        "MSRI_RAW": None,
        "MSRI_SIGNAL": None,
        "SAMPLE_WEIGHT": None,
        "raw": {
            "sport": event_context.sport,
            "market_group": selected_market_group,
            "market_period": selected_market_period,
            "market_name": selected_market_name,
            "choice_group": selected_choice_group,
            "bookie_name": selected_bookie_name,
            "candidate_line_count": candidate_line_count,
            "target_minute": CURRENT_TARGET_MINUTE,
            "current_home_odds": None if current_home_odds is None else str(current_home_odds),
            "current_draw_odds": None if current_draw_odds is None else str(current_draw_odds),
            "current_away_odds": None if current_away_odds is None else str(current_away_odds),
            "has_draw": has_draw,
            "odds_round_decimals": ODDS_ROUND_DECIMALS,
            "allowed_bookies": ALLOWED_BOOKIES,
            "historical_source": "mv_alert_events",
            "sample": sample_payload,
            "historical_matches": [],
            "reason": reason,
        },
    }


def _discretize_msri_signal(msri_raw: Decimal) -> Decimal:
    if msri_raw >= Decimal("0.60"):
        return Decimal("1.00")
    if msri_raw >= Decimal("0.40"):
        return Decimal("0.75")
    if msri_raw >= Decimal("0.20"):
        return Decimal("0.50")
    if msri_raw >= Decimal("0.10"):
        return Decimal("0.25")
    return Decimal("0.00")


def _sample_weight(sample_size: int) -> Decimal:
    if 3 <= sample_size <= 4:
        return Decimal("0.40")
    if 5 <= sample_size <= 7:
        return Decimal("0.60")
    if 8 <= sample_size <= 12:
        return Decimal("0.80")
    if sample_size >= 13:
        return Decimal("1.00")
    return Decimal("0.00")


def _classify_strength(score: Decimal) -> str:
    if score < Decimal("0.10"):
        return "NONE"
    if score < Decimal("0.25"):
        return "WEAK"
    if score < Decimal("0.50"):
        return "MODERATE"
    return "STRONG"


def _detect_dominant_result(
    *,
    wins_home: int,
    wins_draw: int,
    wins_away: int,
    has_draw: bool,
) -> tuple[str, int, bool]:
    counts = {
        "HOME": wins_home,
        "AWAY": wins_away,
    }
    if has_draw:
        counts["DRAW"] = wins_draw

    max_count = max(counts.values()) if counts else 0
    dominant_results = [result for result, count in counts.items() if count == max_count]
    if len(dominant_results) != 1:
        return "NONE", max_count, True
    return dominant_results[0], max_count, False


def _extract_price_from_choice(choice_container) -> Optional[Decimal]:
    if choice_container is None:
        return None
    return choice_container.odds_values.get(CURRENT_TARGET_MINUTE)


def _extract_current_price_set(
    ft_1x2_odds_trajectory: OddsTrajectoryContext,
    debug_mode: bool = False,
) -> dict:
    trace = {
        "selected_market_group": None,
        "selected_market_period": None,
        "selected_market_name": None,
        "selected_choice_group": None,
        "selected_bookie_name": None,
        "candidate_line_count": 0,
        "current_home_odds": None,
        "current_draw_odds": None,
        "current_away_odds": None,
        "current_target_minute": CURRENT_TARGET_MINUTE,
        "allowed_bookies": ALLOWED_BOOKIES,
        "has_draw": False,
        "reason": None,
    }
    saw_allowed_bookie_line = False
    saw_allowed_bookie_with_pair = False

    if debug_mode:
        _debug_section("Búsqueda y extracción del precio actual (1X2/ML)")
        _debug_line("Trayectoria de cuotas disponible: %s", ft_1x2_odds_trajectory.available)
        _debug_line("Minutos esperados: %s", ft_1x2_odds_trajectory.target_minutes_expected)
        _debug_line("Minutos presentes: %s", ft_1x2_odds_trajectory.target_minutes_present)
        _debug_line("Minutos ausentes: %s", ft_1x2_odds_trajectory.missing_target_minutes)
        if ft_1x2_odds_trajectory.markets:
            for group, periods in ft_1x2_odds_trajectory.markets.items():
                for period, names in periods.items():
                    for name, lines in names.items():
                        _debug_line("  - Mercado en trayectoria: Group=%s, Period=%s, Name=%s (lineas=%s)",
                                    group, period, name, list(lines.keys()))
        else:
            _debug_line("Mercados disponibles en la trayectoria: Ninguno")

    if not ft_1x2_odds_trajectory.available or not ft_1x2_odds_trajectory.markets:
        trace["reason"] = "odds_trajectory_unavailable_or_empty"
        if debug_mode:
            _debug_line("Trayectoria no disponible o vacía (o filtrada). Abortando búsqueda.")
        return trace

    grouped_market_names = list(
        dict.fromkeys(
            [group for group in MARKET_GROUP_PRIORITY if group in ft_1x2_odds_trajectory.markets]
            + sorted(group for group in ft_1x2_odds_trajectory.markets if group not in MARKET_GROUP_PRIORITY)
        )
    )
    if debug_mode:
        _debug_line("Orden de prioridad de búsqueda de mercados: %s", grouped_market_names)

    for market_group in grouped_market_names:
        periods = ft_1x2_odds_trajectory.markets.get(market_group, {})
        for market_period in sorted(periods):
            market_names = periods[market_period]
            for market_name in sorted(market_names):
                choice_groups = market_names[market_name]
                for choice_group_key in sorted(choice_groups):
                    market_line = choice_groups[choice_group_key]
                    selected_choice_group = market_line.choice_group
                    if debug_mode:
                        _debug_line("Evaluando línea de mercado: group=%s, period=%s, name=%s, choice_group=%s",
                                    market_group, market_period, market_name, selected_choice_group)
                    for bookie_key in sorted(market_line.bookies):
                        bookie = market_line.bookies[bookie_key]
                        if debug_mode:
                            _debug_line("  - Analizando bookie: %s", bookie.bookie_name)
                        if bookie.bookie_name not in ALLOWED_BOOKIES:
                            if debug_mode:
                                _debug_line("    - Bookie %s no permitido (no está en %s). Omitiendo.", bookie.bookie_name, ALLOWED_BOOKIES)
                            continue

                        trace["candidate_line_count"] += 1
                        saw_allowed_bookie_line = True
                        home_choice = bookie.choices.get("1")
                        away_choice = bookie.choices.get("2")
                        if home_choice is None or away_choice is None:
                            if debug_mode:
                                _debug_line("    - Falta opción de victoria HOME ('1') o AWAY ('2') para el bookie %s.", bookie.bookie_name)
                            continue

                        saw_allowed_bookie_with_pair = True
                        current_home_odds = _extract_price_from_choice(home_choice)
                        current_away_odds = _extract_price_from_choice(away_choice)
                        if debug_mode:
                            _debug_line("    - Cuotas extraídas para el minuto %s: HOME=%s, AWAY=%s",
                                        CURRENT_TARGET_MINUTE, current_home_odds, current_away_odds)
                        if current_home_odds is None or current_away_odds is None:
                            if debug_mode:
                                _debug_line("    - Cuota HOME o AWAY es nula en el minuto %s. Omitiendo.", CURRENT_TARGET_MINUTE)
                            continue

                        draw_choice = bookie.choices.get("X")
                        current_draw_odds = _extract_price_from_choice(draw_choice)
                        has_draw = current_draw_odds is not None
                        if debug_mode:
                            _debug_line("    - Cuota DRAW ('X') extraída para el minuto %s: %s (has_draw=%s)",
                                        CURRENT_TARGET_MINUTE, current_draw_odds, has_draw)

                        trace.update(
                            {
                                "selected_market_group": market_group,
                                "selected_market_period": market_period,
                                "selected_market_name": market_name,
                                "selected_choice_group": selected_choice_group,
                                "selected_bookie_name": bookie.bookie_name,
                                "current_home_odds": current_home_odds,
                                "current_draw_odds": current_draw_odds,
                                "current_away_odds": current_away_odds,
                                "has_draw": has_draw,
                            }
                        )
                        if debug_mode:
                            _debug_line("  => Línea válida seleccionada exitosamente: %s", _fmt(trace))
                            _debug_line("------------------------------------------------------------")
                        return trace

    if trace["candidate_line_count"] == 0:
        trace["reason"] = "no_allowed_bookie_line"
    elif trace["selected_bookie_name"] is None and saw_allowed_bookie_with_pair:
        trace["reason"] = "missing_current_target_minute"
    elif trace["selected_bookie_name"] is None and saw_allowed_bookie_line:
        trace["reason"] = "missing_current_target_choices"
    else:
        trace["reason"] = "missing_current_target_minute"
    if debug_mode:
        _debug_line("No se pudo extraer un set de cuotas válido. Razón: %s", trace["reason"])
        _debug_line("------------------------------------------------------------")
    return trace



def calculate_p5_exact_price_memory_engine(
    event_context: EventContext,
    ft_1x2_odds_trajectory: OddsTrajectoryContext,
    debug_mode: bool = False,
) -> dict:
    """Calculate the exact price memory engine for Pillar 5."""
    logger.info(
        "P5 exact price memory engine start for event_id=%s participants=%s debug_mode=%s",
        event_context.event_id,
        event_context.participants_label,
        debug_mode,
    )

    if debug_mode:
        _debug_section("Propósito del módulo")
        _debug_line("P5 mide la memoria de precios exactos basándose en cuotas de cierre históricas idénticas.")

    selected_price_set = _extract_current_price_set(ft_1x2_odds_trajectory, debug_mode=debug_mode)
    selected_market_group = selected_price_set.get("selected_market_group")
    selected_market_period = selected_price_set.get("selected_market_period")
    selected_market_name = selected_price_set.get("selected_market_name")
    selected_choice_group = selected_price_set.get("selected_choice_group")
    selected_bookie_name = selected_price_set.get("selected_bookie_name")
    candidate_line_count = int(selected_price_set.get("candidate_line_count") or 0)
    current_home_odds = selected_price_set.get("current_home_odds")
    current_draw_odds = selected_price_set.get("current_draw_odds")
    current_away_odds = selected_price_set.get("current_away_odds")
    has_draw = bool(selected_price_set.get("has_draw"))
    reason = selected_price_set.get("reason")

    if debug_mode:
        _debug_section("Parámetros de Entrada")
        _debug_line("  - event_id: %s", event_context.event_id)
        _debug_line("  - participants: %s", event_context.participants_label)
        _debug_line("  - current_home_odds: %s", current_home_odds)
        _debug_line("  - current_draw_odds: %s", current_draw_odds)
        _debug_line("  - current_away_odds: %s", current_away_odds)
        _debug_line("  - has_draw: %s", has_draw)
        _debug_line("  - selected_market_group: %s", selected_market_group)
        _debug_line("  - selected_bookie_name: %s", selected_bookie_name)

    if current_home_odds is None or current_away_odds is None:
        sample = ExactPriceMemorySample(
            0,
            0,
            0,
            0,
            rows=[],
            historical_matches=[],
        )
        if debug_mode:
            _debug_line("Falta de cuotas obligatorias (HOME/AWAY). Se aborta con INSUFFICIENT_DATA. Razón: %s", reason or "missing_current_target_minute")
            _debug_line("------------------------------------------------------------")
        result = _build_insufficient_result(
            event_context=event_context,
            reason=reason or "missing_current_target_minute",
            sample=sample,
            selected_market_group=selected_market_group,
            selected_market_period=selected_market_period,
            selected_market_name=selected_market_name,
            selected_choice_group=selected_choice_group,
            selected_bookie_name=selected_bookie_name,
            candidate_line_count=candidate_line_count,
            current_home_odds=current_home_odds,
            current_draw_odds=current_draw_odds,
            current_away_odds=current_away_odds,
            has_draw=has_draw,
        )
        serialized_result = _decimal_to_serializable(result)
        logger.info(
            "P5 exact price memory engine done for %s: status=%s direction=%s score=%.3f strength=%s sample_size=%s",
            event_context.participants_label,
            serialized_result.get("P5_STATUS"),
            serialized_result.get("P5_DIRECTION"),
            serialized_result.get("P5", 0.0),
            serialized_result.get("P5_STRENGTH"),
            serialized_result.get("sample_size"),
        )
        return serialized_result

    sample = get_exact_price_memory_sample(
        event_id=event_context.event_id,
        sport=event_context.sport,
        current_home_odds=current_home_odds,
        current_away_odds=current_away_odds,
        current_draw_odds=current_draw_odds if has_draw else None,
        debug_mode=debug_mode,
    )

    sample_size = sample.sample_size
    wins_home = sample.wins_home
    wins_draw = sample.wins_draw if has_draw else 0
    wins_away = sample.wins_away

    if debug_mode:
        _debug_section("Muestras Históricas Recuperadas")
        _debug_line("Datos de la muestra:")
        _debug_line("  - sample_size: %d", sample_size)
        _debug_line("  - wins_home: %d", wins_home)
        _debug_line("  - wins_draw: %d", wins_draw)
        _debug_line("  - wins_away: %d", wins_away)
        _debug_line("Juegos históricos exactos encontrados:")
        for m in sample.historical_matches:
            odds_str = f"{_fmt(m['one_final'], 3)} / {_fmt(m['x_final'], 3)} / {_fmt(m['two_final'], 3)}"
            score_str = f"{m['home_score']}-{m['away_score']}" if m.get('home_score') is not None and m.get('away_score') is not None else "N/A"
            _debug_line(
                "  - event_id=%s | sport=%s | %s vs %s | start_time=%s | odds=%s | score=%s | winner_side=%s",
                m["event_id"],
                m["sport"],
                m["home_team"],
                m["away_team"],
                m["start_time"],
                odds_str,
                score_str,
                m["winner_side"],
            )

    if sample_size < 3:
        if debug_mode:
            _debug_line("El tamaño de la muestra (%d) es inferior al umbral mínimo requerido (3). Se retorna INSUFFICIENT_DATA.", sample_size)
            _debug_line("------------------------------------------------------------")
        result = _build_insufficient_result(
            event_context=event_context,
            reason="sample_size_below_threshold",
            sample=sample,
            selected_market_group=selected_market_group,
            selected_market_period=selected_market_period,
            selected_market_name=selected_market_name,
            selected_choice_group=selected_choice_group,
            selected_bookie_name=selected_bookie_name,
            candidate_line_count=candidate_line_count,
            current_home_odds=current_home_odds,
            current_draw_odds=current_draw_odds if has_draw else None,
            current_away_odds=current_away_odds,
            has_draw=has_draw,
        )
        logger.info(
            "P5 exact price memory engine done for %s: status=%s direction=%s score=%.3f strength=%s sample_size=%s",
            event_context.participants_label,
            result.get("P5_STATUS"),
            result.get("P5_DIRECTION"),
            result.get("P5", 0.0),
            result.get("P5_STRENGTH"),
            result.get("sample_size"),
        )
        return _decimal_to_serializable(result)

    dominant_result, wins_dominant, is_tie = _detect_dominant_result(
        wins_home=wins_home,
        wins_draw=wins_draw,
        wins_away=wins_away,
        has_draw=has_draw,
    )

    if debug_mode:
        _debug_section("Análisis de Resultado Dominante")
        _debug_line("Resultado de _detect_dominant_result:")
        _debug_line("  - dominant_result: %s", dominant_result)
        _debug_line("  - wins_dominant: %d", wins_dominant)
        _debug_line("  - is_tie: %s", is_tie)

    if has_draw:
        baseline = Decimal("0.3333333333333333333333333333")
    else:
        baseline = Decimal("0.50")

    sample_size_decimal = Decimal(sample_size)
    sample_factor = min(sample_size_decimal / Decimal("8"), Decimal("1.0"))
    sample_weight = _sample_weight(sample_size)

    if debug_mode:
        _debug_formula("BASELINE", "baseline = 1/3 if has_draw else 1/2", f"has_draw = {has_draw}", _fmt(baseline))
        _debug_formula("SAMPLE_FACTOR", "sample_factor = min(sample_size / 8, 1.0)", f"min({sample_size} / 8, 1.0)", _fmt(sample_factor))
        _debug_formula("SAMPLE_WEIGHT", "sample_weight (basado en rangos de sample_size)", f"sample_size = {sample_size}", _fmt(sample_weight))

    if is_tie:
        p_hist_dominant = None
        hist_edge = Decimal("0.0")
        consistency = Decimal("0.50")
        msri_raw = Decimal("0.0")
        msri_signal = Decimal("0.0")
        p5 = Decimal("0.0")
        p5_strength = "NONE"
        p5_direction = "NONE"
        if debug_mode:
            _debug_line("Empate en el conteo dominante detectado. Se fuerzan valores predeterminados de empate.")
            _debug_formula("P_HIST_DOMINANT", "p_hist_dominant = None (debido a empate)", "N/A", "None")
            _debug_formula("HIST_EDGE", "hist_edge = 0.0", "N/A", "0.0")
            _debug_formula("CONSISTENCY", "consistency = 0.50", "N/A", "0.50")
            _debug_formula("MSRI_RAW", "msri_raw = 0.0", "N/A", "0.0")
            _debug_formula("MSRI_SIGNAL", "msri_signal = 0.0", "N/A", "0.0")
            _debug_formula("P5", "p5 = 0.0", "N/A", "0.0")
            _debug_formula("P5_STRENGTH", "p5_strength = NONE", "N/A", "NONE")
            _debug_formula("P5_DIRECTION", "p5_direction = NONE", "N/A", "NONE")
    else:
        p_hist_dominant = Decimal(wins_dominant) / sample_size_decimal
        hist_edge = (p_hist_dominant - baseline) / (Decimal("1") - baseline)
        consistency = Decimal("0.5") + (hist_edge * Decimal("0.5"))
        msri_raw = hist_edge * consistency * sample_factor
        msri_signal = _discretize_msri_signal(msri_raw)
        p5 = msri_signal * sample_weight
        p5_strength = _classify_strength(p5)
        p5_direction = dominant_result

        if debug_mode:
            _debug_formula("P_HIST_DOMINANT", "p_hist_dominant = wins_dominant / sample_size", f"{wins_dominant} / {sample_size}", _fmt(p_hist_dominant))
            _debug_formula("HIST_EDGE", "hist_edge = (p_hist_dominant - baseline) / (1 - baseline)", f"({_fmt(p_hist_dominant)} - {_fmt(baseline)}) / (1 - {_fmt(baseline)})", _fmt(hist_edge))
            _debug_formula("CONSISTENCY", "consistency = 0.5 + (hist_edge * 0.5)", f"0.5 + ({_fmt(hist_edge)} * 0.5)", _fmt(consistency))
            _debug_formula("MSRI_RAW", "msri_raw = hist_edge * consistency * sample_factor", f"{_fmt(hist_edge)} * {_fmt(consistency)} * {_fmt(sample_factor)}", _fmt(msri_raw))
            _debug_formula("MSRI_SIGNAL", "msri_signal (discretización de msri_raw)", f"msri_raw = {_fmt(msri_raw)}", _fmt(msri_signal))
            _debug_formula("P5", "p5 = msri_signal * sample_weight", f"{_fmt(msri_signal)} * {_fmt(sample_weight)}", _fmt(p5))
            _debug_formula("P5_STRENGTH", "p5_strength (clasificación de p5)", f"p5 = {_fmt(p5)}", p5_strength)

    result = {
        "module_id": MODULE_ID,
        "module_name": MODULE_NAME,
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P5_STATUS": "ACTIVE",
        "status": "ACTIVE",
        "P5_VALID": True,
        "P5_DIRECTION": p5_direction,
        "P5": p5,
        "P5_STRENGTH": p5_strength,
        "sample_size": sample_size,
        "wins_home": wins_home,
        "wins_draw": wins_draw,
        "wins_away": wins_away,
        "DOMINANT_RESULT": dominant_result,
        "wins_dominant": wins_dominant,
        "P_hist_DOMINANT": p_hist_dominant,
        "BASELINE": baseline,
        "HIST_EDGE": hist_edge,
        "CONSISTENCY": consistency,
        "SAMPLE_FACTOR": sample_factor,
        "MSRI_RAW": msri_raw,
        "MSRI_SIGNAL": msri_signal,
        "SAMPLE_WEIGHT": sample_weight,
        "raw": {
            "sport": event_context.sport,
            "market_group": selected_market_group,
            "market_period": selected_market_period,
            "market_name": selected_market_name,
            "choice_group": selected_choice_group,
            "bookie_name": selected_bookie_name,
            "candidate_line_count": candidate_line_count,
            "target_minute": CURRENT_TARGET_MINUTE,
            "current_home_odds": str(current_home_odds),
            "current_draw_odds": None if current_draw_odds is None else str(current_draw_odds),
            "current_away_odds": str(current_away_odds),
            "has_draw": has_draw,
            "odds_round_decimals": ODDS_ROUND_DECIMALS,
            "allowed_bookies": ALLOWED_BOOKIES,
            "historical_source": "mv_alert_events",
            "sample": _build_sample_payload(sample),
            "historical_matches": sample.historical_matches,
            "reason": None if not is_tie else "dominant_result_tie",
        },
    }

    if debug_mode:
        _debug_section("Resumen de Output")
        _debug_line("Valores finales de Pillar 5:")
        _debug_line("  - P5_STATUS: %s", result.get("P5_STATUS"))
        _debug_line("  - P5_VALID: %s", result.get("P5_VALID"))
        _debug_line("  - P5_DIRECTION: %s", result.get("P5_DIRECTION"))
        _debug_line("  - P5: %s", _fmt(result.get("P5")))
        _debug_line("  - P5_STRENGTH: %s", result.get("P5_STRENGTH"))
        _debug_line("------------------------------------------------------------")

    serialized_result = _decimal_to_serializable(result)
    logger.info(
        "P5 exact price memory engine done for %s: status=%s direction=%s score=%.3f strength=%s sample_size=%s",
        event_context.participants_label,
        serialized_result.get("P5_STATUS"),
        serialized_result.get("P5_DIRECTION"),
        serialized_result.get("P5", 0.0),
        serialized_result.get("P5_STRENGTH"),
        serialized_result.get("sample_size"),
    )
    return serialized_result

