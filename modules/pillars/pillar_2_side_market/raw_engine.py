"""Pure mathematical engine for Pillar 2 SIDE MARKET RAW."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from modules.pillars.pillar_2_side_market.models import (
    AsianHandicapSnapshot,
    P2FirstHalfSnapshot,
    P2FullTimeSnapshot,
    P2MarketSnapshot,
    TwoWayMarketSnapshot,
)
from modules.pillars.pillar_2_side_market.periods import null_optional_metrics


ENGINE_VERSION = "p2-raw-ft-1h-periodized-v2"
HALF = Decimal("0.50")
ONE = Decimal("1")
TWO = Decimal("2")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== P2_RAW_ENGINE DEBUG | %s =========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P2_RAW_ENGINE DEBUG | " + message, *args)


def _fmt(value: Any, decimals: int = 6) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, Decimal):
        return f"{value:.{decimals}f}"
    if isinstance(value, float):
        return f"{value:.{decimals}f}"
    return str(value)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("P2_RAW_ENGINE DEBUG | %s", name)
    logger.info("P2_RAW_ENGINE DEBUG |   Fórmula: %s", formula)
    logger.info("P2_RAW_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("P2_RAW_ENGINE DEBUG |   Resultado: %s", _fmt(result))
    if meaning:
        logger.info("P2_RAW_ENGINE DEBUG |   Lectura: %s", meaning)


def _side_edge(home_price: Decimal, away_price: Decimal) -> Decimal:
    home_raw = ONE / home_price
    away_raw = ONE / away_price
    return (home_raw - away_raw) / (home_raw + away_raw)


def _direction(edge: Decimal) -> str:
    if edge > 0:
        return "HOME"
    if edge < 0:
        return "AWAY"
    return "NEUTRAL"


def _relative_spread(back_price: Decimal, lay_price: Decimal) -> Decimal:
    return (lay_price - back_price) / ((lay_price + back_price) / TWO)


def _as_float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _book_side_edges(
    pinnacle: TwoWayMarketSnapshot,
    bet365: TwoWayMarketSnapshot,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    pin_edge = _side_edge(pinnacle.home.odds_price, pinnacle.away.odds_price)
    b365_edge = _side_edge(bet365.home.odds_price, bet365.away.odds_price)
    return (
        pin_edge,
        b365_edge,
        abs(pin_edge - b365_edge),
        HALF * pin_edge + HALF * b365_edge,
    )


def _ah_edges(
    pinnacle: AsianHandicapSnapshot,
    bet365: AsianHandicapSnapshot,
) -> tuple[Decimal, Decimal | None, Decimal | None, Decimal | None]:
    line_gap = abs(pinnacle.home_line - bet365.home_line)
    if pinnacle.home_line != bet365.home_line:
        return line_gap, None, None, None
    pin_edge = _side_edge(pinnacle.home.odds_price, pinnacle.away.odds_price)
    b365_edge = _side_edge(bet365.home.odds_price, bet365.away.odds_price)
    return line_gap, pin_edge, b365_edge, abs(pin_edge - b365_edge)


def _calculate_full_time(
    snapshot: P2FullTimeSnapshot,
    *,
    debug_mode: bool,
) -> tuple[dict[str, Any], Decimal]:
    pin_side_edge, b365_side_edge, book_gap, book_edge = _book_side_edges(
        snapshot.pinnacle_1x2,
        snapshot.bet365_1x2,
    )
    if debug_mode:
        _debug_formula(
            "PIN_SIDE_EDGE",
            "(1 / PIN_HOME - 1 / PIN_AWAY) / ((1 / PIN_HOME) + (1 / PIN_AWAY))",
            f"(1 / {_fmt(snapshot.pinnacle_1x2.home.odds_price)} - "
            f"1 / {_fmt(snapshot.pinnacle_1x2.away.odds_price)}) / "
            f"((1 / {_fmt(snapshot.pinnacle_1x2.home.odds_price)}) + "
            f"(1 / {_fmt(snapshot.pinnacle_1x2.away.odds_price)}))",
            pin_side_edge,
            "Edge SIDE de Pinnacle en Full Time; positivo favorece HOME y negativo AWAY.",
        )
        _debug_formula(
            "B365_SIDE_EDGE",
            "(1 / B365_HOME - 1 / B365_AWAY) / ((1 / B365_HOME) + (1 / B365_AWAY))",
            f"(1 / {_fmt(snapshot.bet365_1x2.home.odds_price)} - "
            f"1 / {_fmt(snapshot.bet365_1x2.away.odds_price)}) / "
            f"((1 / {_fmt(snapshot.bet365_1x2.home.odds_price)}) + "
            f"(1 / {_fmt(snapshot.bet365_1x2.away.odds_price)}))",
            b365_side_edge,
            "Edge SIDE de bet365 en Full Time.",
        )
        _debug_formula(
            "BOOK_GAP",
            "abs(PIN_SIDE_EDGE - B365_SIDE_EDGE)",
            f"abs({_fmt(pin_side_edge)} - {_fmt(b365_side_edge)})",
            book_gap,
            "Separación entre las señales de Pinnacle y bet365; todavía no es un threshold calibrado.",
        )
        _debug_formula(
            "BOOK_EDGE",
            "0.50 × PIN_SIDE_EDGE + 0.50 × B365_SIDE_EDGE",
            f"0.50 × {_fmt(pin_side_edge)} + 0.50 × {_fmt(b365_side_edge)}",
            book_edge,
            "Edge combinado de Books usando los pesos RAW baseline.",
        )

    ah_line_gap, pin_ah_edge, b365_ah_edge, ah_price_gap = _ah_edges(
        snapshot.pinnacle_ah,
        snapshot.bet365_ah,
    )
    if debug_mode:
        _debug_formula(
            "AH_LINE_GAP",
            "abs(PIN_AH_LINE - B365_AH_LINE)",
            f"abs({_fmt(snapshot.pinnacle_ah.home_line)} - "
            f"{_fmt(snapshot.bet365_ah.home_line)})",
            ah_line_gap,
            "Diferencia entre las líneas AH de Pinnacle y bet365 en Full Time.",
        )
        if pin_ah_edge is None:
            _debug_line(
                "AH Full Time: las líneas no coinciden; PIN_AH_EDGE, B365_AH_EDGE y AH_PRICE_GAP se asignan como NULL."
            )
        else:
            _debug_formula(
                "PIN_AH_EDGE",
                "side_edge(PIN_AH_HOME_PRICE, PIN_AH_AWAY_PRICE)",
                f"side_edge({_fmt(snapshot.pinnacle_ah.home.odds_price)}, "
                f"{_fmt(snapshot.pinnacle_ah.away.odds_price)})",
                pin_ah_edge,
                "Edge AH de Pinnacle; se calcula porque ambas casas utilizan la misma línea.",
            )
            _debug_formula(
                "B365_AH_EDGE",
                "side_edge(B365_AH_HOME_PRICE, B365_AH_AWAY_PRICE)",
                f"side_edge({_fmt(snapshot.bet365_ah.home.odds_price)}, "
                f"{_fmt(snapshot.bet365_ah.away.odds_price)})",
                b365_ah_edge,
                "Edge AH de bet365 con la misma línea que Pinnacle.",
            )
            _debug_formula(
                "AH_PRICE_GAP",
                "abs(PIN_AH_EDGE - B365_AH_EDGE)",
                f"abs({_fmt(pin_ah_edge)} - {_fmt(b365_ah_edge)})",
                ah_price_gap,
                "Separación de precio AH entre Books en Full Time.",
            )

    bf_back = snapshot.betfair_1x2.back
    bf_lay = snapshot.betfair_1x2.lay
    if debug_mode:
        _debug_line("Asignación de bloques Betfair: BACK y LAY para Full Time.")

    back_edge = _side_edge(bf_back.home.odds_price, bf_back.away.odds_price)
    lay_edge = _side_edge(bf_lay.home.odds_price, bf_lay.away.odds_price)
    exchange_internal_gap = abs(back_edge - lay_edge)
    exchange_edge = HALF * back_edge + HALF * lay_edge
    home_spread = _relative_spread(bf_back.home.odds_price, bf_lay.home.odds_price)
    away_spread = _relative_spread(bf_back.away.odds_price, bf_lay.away.odds_price)
    side_spread = (home_spread + away_spread) / TWO
    q_agreement = max(Decimal("0"), min(ONE, ONE - exchange_internal_gap / TWO))
    q_complete = ONE
    exchange_quality_base = (q_agreement * q_complete).sqrt()
    tension_raw = abs(book_edge - exchange_edge)
    dislocation = book_edge * exchange_edge < 0
    dislocation_strength = tension_raw / TWO if dislocation else Decimal("0")
    side_market_edge = HALF * book_edge + HALF * exchange_edge
    book_direction_ft = _direction(book_edge)

    if debug_mode:
        _debug_formula(
            "BACK_EDGE",
            "side_edge(BF_HOME_BACK, BF_AWAY_BACK)",
            f"side_edge({_fmt(bf_back.home.odds_price)}, {_fmt(bf_back.away.odds_price)})",
            back_edge,
            "Edge SIDE de las cuotas BACK de Betfair.",
        )
        _debug_formula(
            "LAY_EDGE",
            "side_edge(BF_HOME_LAY, BF_AWAY_LAY)",
            f"side_edge({_fmt(bf_lay.home.odds_price)}, {_fmt(bf_lay.away.odds_price)})",
            lay_edge,
            "Edge SIDE de las cuotas LAY de Betfair.",
        )
        _debug_formula(
            "EXCHANGE_INTERNAL_GAP",
            "abs(BACK_EDGE - LAY_EDGE)",
            f"abs({_fmt(back_edge)} - {_fmt(lay_edge)})",
            exchange_internal_gap,
            "Diferencia entre las señales BACK y LAY.",
        )
        _debug_formula(
            "EXCHANGE_EDGE",
            "0.50 × BACK_EDGE + 0.50 × LAY_EDGE",
            f"0.50 × {_fmt(back_edge)} + 0.50 × {_fmt(lay_edge)}",
            exchange_edge,
            "Edge combinado del Exchange usando pesos BACK/LAY baseline.",
        )
        _debug_formula(
            "HOME_SPREAD",
            "(HOME_LAY - HOME_BACK) / ((HOME_LAY + HOME_BACK) / 2)",
            f"({_fmt(bf_lay.home.odds_price)} - {_fmt(bf_back.home.odds_price)}) / "
            f"(({_fmt(bf_lay.home.odds_price)} + {_fmt(bf_back.home.odds_price)}) / 2)",
            home_spread,
            "Spread relativo entre BACK y LAY para HOME.",
        )
        _debug_formula(
            "AWAY_SPREAD",
            "(AWAY_LAY - AWAY_BACK) / ((AWAY_LAY + AWAY_BACK) / 2)",
            f"({_fmt(bf_lay.away.odds_price)} - {_fmt(bf_back.away.odds_price)}) / "
            f"(({_fmt(bf_lay.away.odds_price)} + {_fmt(bf_back.away.odds_price)}) / 2)",
            away_spread,
            "Spread relativo entre BACK y LAY para AWAY.",
        )
        _debug_formula(
            "SIDE_SPREAD",
            "(HOME_SPREAD + AWAY_SPREAD) / 2",
            f"({_fmt(home_spread)} + {_fmt(away_spread)}) / 2",
            side_spread,
            "Spread SIDE promedio del Exchange.",
        )
        _debug_formula(
            "Q_AGREEMENT",
            "clamp(0, 1, 1 - EXCHANGE_INTERNAL_GAP / 2)",
            f"clamp(0, 1, 1 - {_fmt(exchange_internal_gap)} / 2)",
            q_agreement,
            "Calidad base por acuerdo interno entre BACK y LAY.",
        )
        _debug_line(
            "Asignación Q_COMPLETE = 1.000000 porque Full Time superó su gate de completitud."
        )
        _debug_formula(
            "EXCHANGE_QUALITY_BASE",
            "sqrt(Q_AGREEMENT × Q_COMPLETE)",
            f"sqrt({_fmt(q_agreement)} × {_fmt(q_complete)})",
            exchange_quality_base,
            "Calidad base RAW del Exchange; no es todavía la calidad calibrada por Minería.",
        )
        _debug_formula(
            "TENSION_RAW",
            "abs(BOOK_EDGE - EXCHANGE_EDGE)",
            f"abs({_fmt(book_edge)} - {_fmt(exchange_edge)})",
            tension_raw,
            "Distancia entre la señal de Books y la señal del Exchange.",
        )
        _debug_line(
            "Asignación DISLOCATION = %s porque BOOK_EDGE=%s y EXCHANGE_EDGE=%s tienen signos opuestos: %s.",
            dislocation,
            _fmt(book_edge),
            _fmt(exchange_edge),
            "sí" if dislocation else "no",
        )
        _debug_formula(
            "DISLOCATION_STRENGTH",
            "TENSION_RAW / 2 si existe dislocación; en otro caso 0",
            f"{_fmt(tension_raw)} / 2 si {dislocation} else 0",
            dislocation_strength,
            "Magnitud RAW de la dislocación; todavía no es una clasificación calibrada.",
        )
        _debug_formula(
            "SIDE_MARKET_EDGE",
            "0.50 × BOOK_EDGE + 0.50 × EXCHANGE_EDGE",
            f"0.50 × {_fmt(book_edge)} + 0.50 × {_fmt(exchange_edge)}",
            side_market_edge,
            "Edge SIDE final RAW combinando Books y Exchange.",
        )
        _debug_line(
            "Asignación BOOK_DIRECTION_FT = %s a partir de BOOK_EDGE=%s.",
            book_direction_ft,
            _fmt(book_edge),
        )

    return {
        "PIN_SIDE_EDGE": _as_float(pin_side_edge),
        "B365_SIDE_EDGE": _as_float(b365_side_edge),
        "BOOK_GAP": _as_float(book_gap),
        "BOOK_EDGE": _as_float(book_edge),
        "AH_LINE_GAP": _as_float(ah_line_gap),
        "PIN_AH_EDGE": _as_float(pin_ah_edge),
        "B365_AH_EDGE": _as_float(b365_ah_edge),
        "AH_PRICE_GAP": _as_float(ah_price_gap),
        "BOOK_DIRECTION_FT": book_direction_ft,
        "BACK_EDGE": _as_float(back_edge),
        "LAY_EDGE": _as_float(lay_edge),
        "EXCHANGE_INTERNAL_GAP": _as_float(exchange_internal_gap),
        "EXCHANGE_EDGE": _as_float(exchange_edge),
        "HOME_SPREAD": _as_float(home_spread),
        "AWAY_SPREAD": _as_float(away_spread),
        "SIDE_SPREAD": _as_float(side_spread),
        "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": _as_float(bf_back.home.exchange_size),
        "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": _as_float(bf_lay.home.exchange_size),
        "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": _as_float(bf_back.draw.exchange_size),
        "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": _as_float(bf_lay.draw.exchange_size),
        "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": _as_float(bf_back.away.exchange_size),
        "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": _as_float(bf_lay.away.exchange_size),
        "Q_AGREEMENT": _as_float(q_agreement),
        "Q_COMPLETE": _as_float(q_complete),
        "EXCHANGE_QUALITY_BASE": _as_float(exchange_quality_base),
        "TENSION_RAW": _as_float(tension_raw),
        "DISLOCATION": dislocation,
        "DISLOCATION_STRENGTH": _as_float(dislocation_strength),
        "SIDE_MARKET_EDGE": _as_float(side_market_edge),
        "P2_DIRECTION_RAW": _direction(side_market_edge),
    }, book_edge


def _calculate_first_half(
    snapshot: P2FirstHalfSnapshot,
    *,
    debug_mode: bool,
) -> tuple[dict[str, Any], Decimal]:
    pin_1h_side_edge, b365_1h_side_edge, book_1h_gap, book_1h_edge = _book_side_edges(
        snapshot.pinnacle_1x2,
        snapshot.bet365_1x2,
    )
    ah_1h_line_gap, pin_ah_1h_edge, b365_ah_1h_edge, ah_1h_price_gap = _ah_edges(
        snapshot.pinnacle_ah,
        snapshot.bet365_ah,
    )
    book_direction_1h = _direction(book_1h_edge)

    if debug_mode:
        _debug_formula(
            "PIN_1H_SIDE_EDGE",
            "side_edge(PIN_1H_HOME, PIN_1H_AWAY)",
            f"side_edge({_fmt(snapshot.pinnacle_1x2.home.odds_price)}, "
            f"{_fmt(snapshot.pinnacle_1x2.away.odds_price)})",
            pin_1h_side_edge,
            "Edge SIDE de Pinnacle para First Half.",
        )
        _debug_formula(
            "B365_1H_SIDE_EDGE",
            "side_edge(B365_1H_HOME, B365_1H_AWAY)",
            f"side_edge({_fmt(snapshot.bet365_1x2.home.odds_price)}, "
            f"{_fmt(snapshot.bet365_1x2.away.odds_price)})",
            b365_1h_side_edge,
            "Edge SIDE de bet365 para First Half.",
        )
        _debug_formula(
            "BOOK_1H_GAP",
            "abs(PIN_1H_SIDE_EDGE - B365_1H_SIDE_EDGE)",
            f"abs({_fmt(pin_1h_side_edge)} - {_fmt(b365_1h_side_edge)})",
            book_1h_gap,
            "Separación entre Books en First Half.",
        )
        _debug_formula(
            "BOOK_1H_EDGE",
            "0.50 × PIN_1H_SIDE_EDGE + 0.50 × B365_1H_SIDE_EDGE",
            f"0.50 × {_fmt(pin_1h_side_edge)} + 0.50 × {_fmt(b365_1h_side_edge)}",
            book_1h_edge,
            "Edge combinado 1H; no modifica el edge FT RAW.",
        )
        _debug_formula(
            "AH_1H_LINE_GAP",
            "abs(PIN_AH_1H_LINE - B365_AH_1H_LINE)",
            f"abs({_fmt(snapshot.pinnacle_ah.home_line)} - "
            f"{_fmt(snapshot.bet365_ah.home_line)})",
            ah_1h_line_gap,
            "Diferencia entre las líneas AH de First Half.",
        )
        if pin_ah_1h_edge is None:
            _debug_line(
                "AH First Half: las líneas no coinciden; PIN_AH_1H_EDGE, B365_AH_1H_EDGE y AH_1H_PRICE_GAP se asignan como NULL."
            )
        else:
            _debug_formula(
                "PIN_AH_1H_EDGE",
                "side_edge(PIN_AH_1H_HOME_PRICE, PIN_AH_1H_AWAY_PRICE)",
                f"side_edge({_fmt(snapshot.pinnacle_ah.home.odds_price)}, "
                f"{_fmt(snapshot.pinnacle_ah.away.odds_price)})",
                pin_ah_1h_edge,
                "Edge AH de Pinnacle para First Half.",
            )
            _debug_formula(
                "B365_AH_1H_EDGE",
                "side_edge(B365_AH_1H_HOME_PRICE, B365_AH_1H_AWAY_PRICE)",
                f"side_edge({_fmt(snapshot.bet365_ah.home.odds_price)}, "
                f"{_fmt(snapshot.bet365_ah.away.odds_price)})",
                b365_ah_1h_edge,
                "Edge AH de bet365 para First Half.",
            )
            _debug_formula(
                "AH_1H_PRICE_GAP",
                "abs(PIN_AH_1H_EDGE - B365_AH_1H_EDGE)",
                f"abs({_fmt(pin_ah_1h_edge)} - {_fmt(b365_ah_1h_edge)})",
                ah_1h_price_gap,
                "Separación de precio AH entre Books en First Half.",
            )
        _debug_line(
            "Asignación BOOK_DIRECTION_1H = %s a partir de BOOK_1H_EDGE=%s.",
            book_direction_1h,
            _fmt(book_1h_edge),
        )

    return {
        "PIN_1H_SIDE_EDGE": _as_float(pin_1h_side_edge),
        "B365_1H_SIDE_EDGE": _as_float(b365_1h_side_edge),
        "BOOK_1H_GAP": _as_float(book_1h_gap),
        "BOOK_1H_EDGE": _as_float(book_1h_edge),
        "PIN_AH_1H_EDGE": _as_float(pin_ah_1h_edge),
        "B365_AH_1H_EDGE": _as_float(b365_ah_1h_edge),
        "AH_1H_LINE_GAP": _as_float(ah_1h_line_gap),
        "AH_1H_PRICE_GAP": _as_float(ah_1h_price_gap),
        "BOOK_DIRECTION_1H": book_direction_1h,
    }, book_1h_edge


def _calculate_cross_period(
    book_edge: Decimal,
    book_1h_edge: Decimal,
    *,
    debug_mode: bool,
) -> dict[str, Any]:
    ft_1h_gap = abs(book_edge - book_1h_edge)
    book_direction_ft = _direction(book_edge)
    book_direction_1h = _direction(book_1h_edge)
    ft_1h_same_direction = book_direction_ft == book_direction_1h
    if debug_mode:
        _debug_formula(
            "FT_1H_GAP",
            "abs(BOOK_EDGE - BOOK_1H_EDGE)",
            f"abs({_fmt(book_edge)} - {_fmt(book_1h_edge)})",
            ft_1h_gap,
            "Diferencia estructural entre FT y First Half; no modifica el edge FT RAW.",
        )
        _debug_line(
            "Asignación FT_1H_SAME_DIRECTION = %s al comparar %s contra %s.",
            ft_1h_same_direction,
            book_direction_ft,
            book_direction_1h,
        )
    return {
        "FT_1H_GAP": _as_float(ft_1h_gap),
        "FT_1H_SAME_DIRECTION": ft_1h_same_direction,
    }


def calculate_p2_raw(
    snapshot: P2MarketSnapshot,
    *,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Calculate RAW outputs; First Half and FT/1H comparisons are optional."""
    if debug_mode:
        _debug_section(f"Inicio del cálculo RAW en target_minute={snapshot.target_minute}")

    metrics, book_edge = _calculate_full_time(snapshot.full_time, debug_mode=debug_mode)

    if snapshot.first_half is None:
        if debug_mode:
            _debug_line(
                "First Half incompleto: métricas 1H y comparaciones FT/1H se asignan como NULL."
            )
        metrics.update(null_optional_metrics())
        return metrics

    first_half_metrics, book_1h_edge = _calculate_first_half(
        snapshot.first_half,
        debug_mode=debug_mode,
    )
    metrics.update(first_half_metrics)
    metrics.update(
        _calculate_cross_period(
            book_edge,
            book_1h_edge,
            debug_mode=debug_mode,
        )
    )
    return metrics
