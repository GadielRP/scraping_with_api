"""Pure RAW calculations for Pillar 3 - Totals Market Context."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from .models import P3MarketSnapshot
from .periods import derived_metric_names


ENGINE_VERSION = "p3-raw-totals-periodized-v2"
W_PIN_TOTALS = Decimal("0.50")
W_B365_TOTALS = Decimal("0.50")
N_REQUIRED = Decimal("6")

logger = logging.getLogger(__name__)


def _debug(message: str, *args: Any) -> None:
    logger.info("P3_TOTALS_MARKET DEBUG | " + message, *args)


def _number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _direction(edge: Decimal | None) -> str | None:
    if edge is None:
        return None
    if edge > 0:
        return "OVER"
    if edge < 0:
        return "UNDER"
    return "NEUTRAL"


def _edge(over_price: Decimal | None, under_price: Decimal | None) -> Decimal | None:
    if over_price is None or under_price is None:
        return None
    over_raw = Decimal(1) / over_price
    under_raw = Decimal(1) / under_price
    return (over_raw - under_raw) / (over_raw + under_raw)


def _debug_edge(
    label: str,
    over_price: Decimal | None,
    under_price: Decimal | None,
    edge: Decimal | None,
) -> None:
    if edge is None:
        _debug(
            "%s no puede calcularse porque OVER=%s o UNDER=%s es NULL; su dirección también será NULL.",
            label,
            over_price,
            under_price,
        )
        return
    over_raw = Decimal(1) / over_price  # type: ignore[operator]
    under_raw = Decimal(1) / under_price  # type: ignore[operator]
    _debug(
        "%s: OVER_RAW=1/%s=%s; UNDER_RAW=1/%s=%s; EDGE=(%s-%s)/(%s+%s)=%s; dirección=%s.",
        label,
        over_price,
        over_raw,
        under_price,
        under_raw,
        over_raw,
        under_raw,
        over_raw,
        under_raw,
        edge,
        _direction(edge),
    )


def calculate_p3_raw(
    snapshot: P3MarketSnapshot,
    *,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Calculate every P3 RAW output with branch-local NULL propagation."""
    period = snapshot.full_time
    names = derived_metric_names(period.period_scope)
    values = period.input_values()
    pin_edge = _edge(values["PIN_OVER_PRICE"], values["PIN_UNDER_PRICE"])
    b365_edge = _edge(values["B365_OVER_PRICE"], values["B365_UNDER_PRICE"])
    pin_direction = _direction(pin_edge)
    b365_direction = _direction(b365_edge)

    if debug_mode:
        _debug_edge(
            names.pin_edge,
            values["PIN_OVER_PRICE"],
            values["PIN_UNDER_PRICE"],
            pin_edge,
        )
        _debug_edge(
            names.b365_edge,
            values["B365_OVER_PRICE"],
            values["B365_UNDER_PRICE"],
            b365_edge,
        )

    pin_line = values["PIN_TOTAL_LINE"]
    b365_line = values["B365_TOTAL_LINE"]
    line_diff = (
        pin_line - b365_line
        if pin_line is not None and b365_line is not None
        else None
    )
    line_gap = abs(line_diff) if line_diff is not None else None
    comparable = (
        pin_line is not None
        and b365_line is not None
        and pin_line == b365_line
        and pin_edge is not None
        and b365_edge is not None
    )

    if debug_mode:
        if line_diff is None:
            _debug(
                "%s y %s son NULL porque falta al menos una línea: Pinnacle=%s, bet365=%s.",
                names.line_diff,
                names.line_gap,
                pin_line,
                b365_line,
            )
        else:
            _debug(
                "%s=%s-%s=%s; %s=abs(%s)=%s.",
                names.line_diff,
                pin_line,
                b365_line,
                line_diff,
                names.line_gap,
                line_diff,
                line_gap,
            )
        _debug(
            "Comparabilidad conjunta=%s: requiere líneas iguales y ambos edges válidos.",
            comparable,
        )

    if comparable:
        total_price_gap = abs(pin_edge - b365_edge)  # type: ignore[operator]
        totals_market_edge = (
            W_PIN_TOTALS * pin_edge  # type: ignore[operator]
            + W_B365_TOTALS * b365_edge  # type: ignore[operator]
        )
    else:
        total_price_gap = None
        totals_market_edge = None

    p3_direction = _direction(totals_market_edge)
    context_direction = {
        "OVER": "OPEN_BIAS",
        "UNDER": "CLOSED_BIAS",
        "NEUTRAL": "NEUTRAL_BIAS",
    }.get(p3_direction)
    available_count = sum(value is not None for value in values.values())
    completeness = Decimal(available_count) / N_REQUIRED

    if debug_mode:
        _debug(
            "Pesos baseline: %s=%s y %s=%s.",
            names.pin_weight,
            W_PIN_TOTALS,
            names.b365_weight,
            W_B365_TOTALS,
        )
        if comparable:
            _debug(
                "%s=abs(%s-%s)=%s.",
                names.price_gap,
                pin_edge,
                b365_edge,
                total_price_gap,
            )
            _debug(
                "%s=(%s×%s)+(%s×%s)=%s; %s=%s; %s=%s.",
                names.market_edge,
                W_PIN_TOTALS,
                pin_edge,
                W_B365_TOTALS,
                b365_edge,
                totals_market_edge,
                names.p3_direction,
                p3_direction,
                names.context_direction,
                context_direction,
            )
        else:
            _debug(
                "%s, %s, %s y %s quedan NULL por falta de comparabilidad.",
                names.price_gap,
                names.market_edge,
                names.p3_direction,
                names.context_direction,
            )
        _debug(
            "%s=%s/%s=%s; mide disponibilidad y no altera el edge.",
            names.completeness,
            available_count,
            int(N_REQUIRED),
            completeness,
        )

    return {
        **{name: _number(value) for name, value in values.items()},
        names.pin_edge: _number(pin_edge),
        names.pin_direction: pin_direction,
        names.b365_edge: _number(b365_edge),
        names.b365_direction: b365_direction,
        names.line_diff: _number(line_diff),
        names.line_gap: _number(line_gap),
        names.price_gap: _number(total_price_gap),
        names.pin_weight: float(W_PIN_TOTALS),
        names.b365_weight: float(W_B365_TOTALS),
        names.market_edge: _number(totals_market_edge),
        names.p3_direction: p3_direction,
        names.context_direction: context_direction,
        names.completeness: float(completeness),
    }
