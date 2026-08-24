"""Pure mathematical engine for Pillar 2 SIDE MARKET RAW."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from modules.pillars.pillar_2_side_market.models import P2MarketSnapshot


ENGINE_VERSION = "p2-raw-ft-1h-v1"
HALF = Decimal("0.50")
ONE = Decimal("1")
TWO = Decimal("2")


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


def calculate_p2_raw(snapshot: P2MarketSnapshot) -> dict[str, Any]:
    """Calculate only the continuous RAW/baseline outputs defined for P2."""
    pin_side_edge = _side_edge(
        snapshot.pinnacle_ft_1x2.home.odds_price,
        snapshot.pinnacle_ft_1x2.away.odds_price,
    )
    b365_side_edge = _side_edge(
        snapshot.bet365_ft_1x2.home.odds_price,
        snapshot.bet365_ft_1x2.away.odds_price,
    )
    book_gap = abs(pin_side_edge - b365_side_edge)
    book_edge = HALF * pin_side_edge + HALF * b365_side_edge

    ah_line_gap = abs(
        snapshot.pinnacle_ft_ah.home_line - snapshot.bet365_ft_ah.home_line
    )
    if snapshot.pinnacle_ft_ah.home_line == snapshot.bet365_ft_ah.home_line:
        pin_ah_edge = _side_edge(
            snapshot.pinnacle_ft_ah.home.odds_price,
            snapshot.pinnacle_ft_ah.away.odds_price,
        )
        b365_ah_edge = _side_edge(
            snapshot.bet365_ft_ah.home.odds_price,
            snapshot.bet365_ft_ah.away.odds_price,
        )
        ah_price_gap = abs(pin_ah_edge - b365_ah_edge)
    else:
        pin_ah_edge = None
        b365_ah_edge = None
        ah_price_gap = None

    pin_1h_side_edge = _side_edge(
        snapshot.pinnacle_1h_1x2.home.odds_price,
        snapshot.pinnacle_1h_1x2.away.odds_price,
    )
    b365_1h_side_edge = _side_edge(
        snapshot.bet365_1h_1x2.home.odds_price,
        snapshot.bet365_1h_1x2.away.odds_price,
    )
    book_1h_gap = abs(pin_1h_side_edge - b365_1h_side_edge)
    book_1h_edge = HALF * pin_1h_side_edge + HALF * b365_1h_side_edge

    ah_1h_line_gap = abs(
        snapshot.pinnacle_1h_ah.home_line - snapshot.bet365_1h_ah.home_line
    )
    if snapshot.pinnacle_1h_ah.home_line == snapshot.bet365_1h_ah.home_line:
        pin_ah_1h_edge = _side_edge(
            snapshot.pinnacle_1h_ah.home.odds_price,
            snapshot.pinnacle_1h_ah.away.odds_price,
        )
        b365_ah_1h_edge = _side_edge(
            snapshot.bet365_1h_ah.home.odds_price,
            snapshot.bet365_1h_ah.away.odds_price,
        )
        ah_1h_price_gap = abs(pin_ah_1h_edge - b365_ah_1h_edge)
    else:
        pin_ah_1h_edge = None
        b365_ah_1h_edge = None
        ah_1h_price_gap = None

    bf_back = snapshot.betfair_ft_1x2.back
    bf_lay = snapshot.betfair_ft_1x2.lay
    back_edge = _side_edge(bf_back.home.odds_price, bf_back.away.odds_price)
    lay_edge = _side_edge(bf_lay.home.odds_price, bf_lay.away.odds_price)
    exchange_internal_gap = abs(back_edge - lay_edge)
    exchange_edge = HALF * back_edge + HALF * lay_edge

    home_spread = _relative_spread(
        bf_back.home.odds_price,
        bf_lay.home.odds_price,
    )
    away_spread = _relative_spread(
        bf_back.away.odds_price,
        bf_lay.away.odds_price,
    )
    side_spread = (home_spread + away_spread) / TWO

    q_agreement = max(Decimal("0"), min(ONE, ONE - exchange_internal_gap / TWO))
    q_complete = ONE
    exchange_quality_base = (q_agreement * q_complete).sqrt()

    tension_raw = abs(book_edge - exchange_edge)
    dislocation = book_edge * exchange_edge < 0
    dislocation_strength = tension_raw / TWO if dislocation else Decimal("0")
    side_market_edge = HALF * book_edge + HALF * exchange_edge

    book_direction_ft = _direction(book_edge)
    book_direction_1h = _direction(book_1h_edge)
    ft_1h_gap = abs(book_edge - book_1h_edge)

    return {
        "PIN_SIDE_EDGE": _as_float(pin_side_edge),
        "B365_SIDE_EDGE": _as_float(b365_side_edge),
        "BOOK_GAP": _as_float(book_gap),
        "BOOK_EDGE": _as_float(book_edge),
        "AH_LINE_GAP": _as_float(ah_line_gap),
        "PIN_AH_EDGE": _as_float(pin_ah_edge),
        "B365_AH_EDGE": _as_float(b365_ah_edge),
        "AH_PRICE_GAP": _as_float(ah_price_gap),
        "PIN_1H_SIDE_EDGE": _as_float(pin_1h_side_edge),
        "B365_1H_SIDE_EDGE": _as_float(b365_1h_side_edge),
        "BOOK_1H_GAP": _as_float(book_1h_gap),
        "BOOK_1H_EDGE": _as_float(book_1h_edge),
        "PIN_AH_1H_EDGE": _as_float(pin_ah_1h_edge),
        "B365_AH_1H_EDGE": _as_float(b365_ah_1h_edge),
        "AH_1H_LINE_GAP": _as_float(ah_1h_line_gap),
        "AH_1H_PRICE_GAP": _as_float(ah_1h_price_gap),
        "BOOK_DIRECTION_FT": book_direction_ft,
        "BOOK_DIRECTION_1H": book_direction_1h,
        "FT_1H_GAP": _as_float(ft_1h_gap),
        "FT_1H_SAME_DIRECTION": book_direction_ft == book_direction_1h,
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
    }
