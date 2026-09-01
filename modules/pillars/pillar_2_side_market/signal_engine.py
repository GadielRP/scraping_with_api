"""Pure domain orchestration from a canonical P2 snapshot to signals."""

from __future__ import annotations

import logging
from decimal import Decimal

from .metrics import absolute_gap, pair_mean, relative_spread, side_edge
from .models import (
    AsianHandicapSnapshot,
    P2FirstHalfSnapshot,
    P2FullTimeSnapshot,
    P2MarketSnapshot,
    PartialAsianHandicapSnapshot,
    PartialTwoWayMarketSnapshot,
    TwoWayMarketSnapshot,
)
from .relations import direction, relation
from .signal_models import (
    AsianHandicapSignal,
    BookExchangeSignal,
    BookMarketSignal,
    CrossMarketSignal,
    ExchangeSignal,
    FirstHalfRelationSignal,
    P2SignalProfile,
    PeriodSignal,
)


ENGINE_VERSION = "p2-signal-profile-v1"

logger = logging.getLogger(__name__)


def _fmt(value: object) -> str:
    if value is None:
        return "None"
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _log_assignment(name: str, value: object, *, debug_mode: bool) -> None:
    if debug_mode:
        logger.info("P2 FORMULA | assignment | %s = %s", name, _fmt(value))


def _log_formula(
    name: str,
    formula: str,
    substitution: str,
    result: object,
    *,
    debug_mode: bool,
) -> None:
    if debug_mode:
        logger.info("P2 FORMULA | %s | formula=%s", name, formula)
        logger.info("P2 FORMULA | %s | substitution=%s", name, substitution)
        logger.info("P2 FORMULA | %s | result=%s", name, _fmt(result))


def _log_direction(
    name: str,
    edge: Decimal | None,
    value: str | None,
    *,
    debug_mode: bool,
) -> None:
    if debug_mode:
        logger.info(
            "P2 FORMULA | %s | direction(edge=%s) -> %s",
            name,
            _fmt(edge),
            value,
        )


def _log_relation(
    name: str,
    left: str | None,
    right: str | None,
    value: str | None,
    *,
    debug_mode: bool,
) -> None:
    if debug_mode:
        logger.info(
            "P2 FORMULA | %s | relation(left=%s, right=%s) -> %s",
            name,
            left,
            right,
            value,
        )


def _market_prices(
    snapshot: TwoWayMarketSnapshot | PartialTwoWayMarketSnapshot | None,
) -> tuple[Decimal | None, Decimal | None]:
    if snapshot is None:
        return None, None
    return (
        None if snapshot.home is None else snapshot.home.odds_price,
        None if snapshot.away is None else snapshot.away.odds_price,
    )


def _edge_if_available(
    home_price: Decimal | None,
    away_price: Decimal | None,
) -> Decimal | None:
    if home_price is None or away_price is None:
        return None
    return side_edge(home_price, away_price)


def _build_book_1x2_signal(
    pinnacle: TwoWayMarketSnapshot | PartialTwoWayMarketSnapshot | None,
    bet365: TwoWayMarketSnapshot | PartialTwoWayMarketSnapshot | None,
    *,
    label: str,
    debug_mode: bool,
) -> BookMarketSignal:
    pin_home, pin_away = _market_prices(pinnacle)
    b365_home, b365_away = _market_prices(bet365)
    if debug_mode:
        logger.info("P2 FORMULA | %s | begin bookmaker 1X2 calculation", label)
        _log_assignment(f"{label}.PIN_HOME_PRICE", pin_home, debug_mode=debug_mode)
        _log_assignment(f"{label}.PIN_AWAY_PRICE", pin_away, debug_mode=debug_mode)
        _log_assignment(f"{label}.B365_HOME_PRICE", b365_home, debug_mode=debug_mode)
        _log_assignment(f"{label}.B365_AWAY_PRICE", b365_away, debug_mode=debug_mode)
    pin_edge = _edge_if_available(pin_home, pin_away)
    b365_edge = _edge_if_available(b365_home, b365_away)
    pin_direction = direction(pin_edge) if pin_edge is not None else None
    b365_direction = direction(b365_edge) if b365_edge is not None else None
    comparable = (
        pin_edge is not None
        and b365_edge is not None
        and pin_direction is not None
        and b365_direction is not None
    )
    if comparable:
        rep_edge = pair_mean(pin_edge, b365_edge)
        book_gap = absolute_gap(pin_edge, b365_edge)
        book_relation = relation(pin_direction, b365_direction)
        representative_direction = direction(rep_edge)
    else:
        rep_edge = None
        book_gap = None
        book_relation = None
        representative_direction = None
    _log_formula(
        f"{label}.PIN_EDGE",
        "(1 / home_price - 1 / away_price) / ((1 / home_price) + (1 / away_price))",
        (
            "unavailable because PIN_HOME_PRICE or PIN_AWAY_PRICE is None"
            if pin_edge is None
            else f"(1 / {_fmt(pin_home)} - 1 / {_fmt(pin_away)}) / ((1 / {_fmt(pin_home)}) + (1 / {_fmt(pin_away)}))"
        ),
        pin_edge,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{label}.B365_EDGE",
        "(1 / home_price - 1 / away_price) / ((1 / home_price) + (1 / away_price))",
        (
            "unavailable because B365_HOME_PRICE or B365_AWAY_PRICE is None"
            if b365_edge is None
            else f"(1 / {_fmt(b365_home)} - 1 / {_fmt(b365_away)}) / ((1 / {_fmt(b365_home)}) + (1 / {_fmt(b365_away)}))"
        ),
        b365_edge,
        debug_mode=debug_mode,
    )
    _log_direction(f"{label}.PIN_DIRECTION", pin_edge, pin_direction, debug_mode=debug_mode)
    _log_direction(f"{label}.B365_DIRECTION", b365_edge, b365_direction, debug_mode=debug_mode)
    _log_formula(
        f"{label}.BOOK_GAP",
        "abs(PIN_EDGE - B365_EDGE)",
        f"abs({_fmt(pin_edge)} - {_fmt(b365_edge)})",
        book_gap,
        debug_mode=debug_mode,
    )
    _log_relation(f"{label}.BOOK_RELATION", pin_direction, b365_direction, book_relation, debug_mode=debug_mode)
    _log_formula(
        f"{label}.REP_EDGE",
        "(PIN_EDGE + B365_EDGE) / 2",
        f"({_fmt(pin_edge)} + {_fmt(b365_edge)}) / 2",
        rep_edge,
        debug_mode=debug_mode,
    )
    _log_direction(f"{label}.DIRECTION", rep_edge, representative_direction, debug_mode=debug_mode)
    return BookMarketSignal(
        pin_edge=pin_edge,
        pin_direction=pin_direction,
        b365_edge=b365_edge,
        b365_direction=b365_direction,
        book_relation=book_relation,
        book_gap=book_gap,
        rep_edge=rep_edge,
        direction=representative_direction,
    )


def _build_ah_signal(
    pinnacle: AsianHandicapSnapshot | PartialAsianHandicapSnapshot | None,
    bet365: AsianHandicapSnapshot | PartialAsianHandicapSnapshot | None,
    *,
    label: str,
    debug_mode: bool,
) -> AsianHandicapSignal:
    pin_line = None if pinnacle is None else pinnacle.home_line
    b365_line = None if bet365 is None else bet365.home_line
    pin_home, pin_away = _market_prices(pinnacle)
    b365_home, b365_away = _market_prices(bet365)
    if debug_mode:
        logger.info("P2 FORMULA | %s | begin Asian Handicap calculation", label)
        _log_assignment(f"{label}.PIN_LINE", pin_line, debug_mode=debug_mode)
        _log_assignment(f"{label}.B365_LINE", b365_line, debug_mode=debug_mode)
        _log_assignment(f"{label}.PIN_HOME_PRICE", pin_home, debug_mode=debug_mode)
        _log_assignment(f"{label}.PIN_AWAY_PRICE", pin_away, debug_mode=debug_mode)
        _log_assignment(f"{label}.B365_HOME_PRICE", b365_home, debug_mode=debug_mode)
        _log_assignment(f"{label}.B365_AWAY_PRICE", b365_away, debug_mode=debug_mode)
    pin_edge = _edge_if_available(pin_home, pin_away)
    b365_edge = _edge_if_available(b365_home, b365_away)
    pin_direction = direction(pin_edge) if pin_edge is not None else None
    b365_direction = direction(b365_edge) if b365_edge is not None else None
    same_line = pin_line is not None and b365_line is not None and pin_line == b365_line
    comparable = (
        same_line
        and pin_edge is not None
        and b365_edge is not None
        and pin_direction is not None
        and b365_direction is not None
    )
    rep_edge = pair_mean(pin_edge, b365_edge) if comparable else None
    line_gap = absolute_gap(pin_line, b365_line) if pin_line is not None and b365_line is not None else None
    price_gap = absolute_gap(pin_edge, b365_edge) if comparable else None
    book_relation = relation(pin_direction, b365_direction) if comparable else None
    representative_direction = direction(rep_edge) if rep_edge is not None else None
    _log_formula(
        f"{label}.PIN_EDGE",
        "side_edge(PIN_HOME_PRICE, PIN_AWAY_PRICE)",
        (
            "unavailable because PIN_HOME_PRICE or PIN_AWAY_PRICE is None"
            if pin_edge is None
            else f"side_edge({_fmt(pin_home)}, {_fmt(pin_away)})"
        ),
        pin_edge,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{label}.B365_EDGE",
        "side_edge(B365_HOME_PRICE, B365_AWAY_PRICE)",
        (
            "unavailable because B365_HOME_PRICE or B365_AWAY_PRICE is None"
            if b365_edge is None
            else f"side_edge({_fmt(b365_home)}, {_fmt(b365_away)})"
        ),
        b365_edge,
        debug_mode=debug_mode,
    )
    _log_direction(f"{label}.PIN_DIRECTION", pin_edge, pin_direction, debug_mode=debug_mode)
    _log_direction(f"{label}.B365_DIRECTION", b365_edge, b365_direction, debug_mode=debug_mode)
    _log_formula(
        f"{label}.LINE_GAP",
        "abs(PIN_LINE - B365_LINE)",
        f"abs({_fmt(pin_line)} - {_fmt(b365_line)})",
        line_gap,
        debug_mode=debug_mode,
    )
    if debug_mode:
        logger.info(
            "P2 FORMULA | %s | same_line=%s | both_edges=%s | comparable_contract=%s",
            label,
            same_line,
            pin_edge is not None and b365_edge is not None,
            comparable,
        )
    if comparable:
        _log_relation(f"{label}.BOOK_RELATION", pin_direction, b365_direction, book_relation, debug_mode=debug_mode)
        _log_formula(
            f"{label}.PRICE_GAP",
            "abs(PIN_EDGE - B365_EDGE)",
            f"abs({_fmt(pin_edge)} - {_fmt(b365_edge)})",
            price_gap,
            debug_mode=debug_mode,
        )
        _log_formula(
            f"{label}.REP_EDGE",
            "(PIN_EDGE + B365_EDGE) / 2",
            f"({_fmt(pin_edge)} + {_fmt(b365_edge)}) / 2",
            rep_edge,
            debug_mode=debug_mode,
        )
        _log_direction(f"{label}.DIRECTION", rep_edge, representative_direction, debug_mode=debug_mode)
    elif debug_mode:
        logger.info(
            "P2 FORMULA | %s | direct price comparison unavailable: equal non-null lines and both edges are required",
            label,
        )
    return AsianHandicapSignal(
        pin_line=pin_line,
        b365_line=b365_line,
        pin_edge=pin_edge,
        pin_direction=pin_direction,
        b365_edge=b365_edge,
        b365_direction=b365_direction,
        book_relation=book_relation,
        line_gap=line_gap,
        price_gap=price_gap,
        rep_edge=rep_edge,
        direction=representative_direction,
    )


def _build_cross_market_signal(
    one_x_two: BookMarketSignal,
    asian_handicap: AsianHandicapSignal,
    *,
    label: str,
    debug_mode: bool,
) -> CrossMarketSignal:
    if (
        one_x_two.rep_edge is None
        or one_x_two.direction is None
        or asian_handicap.rep_edge is None
        or asian_handicap.direction is None
    ):
        if debug_mode:
            logger.info(
                "P2 FORMULA | %s | comparison unavailable because both market representatives are required",
                label,
            )
        return CrossMarketSignal(relation=None, gap=None)
    cross_gap = absolute_gap(one_x_two.rep_edge, asian_handicap.rep_edge)
    cross_relation = relation(one_x_two.direction, asian_handicap.direction)
    _log_formula(
        f"{label}.GAP",
        "abs(1X2_REP_EDGE - AH_REP_EDGE)",
        f"abs({_fmt(one_x_two.rep_edge)} - {_fmt(asian_handicap.rep_edge)})",
        cross_gap,
        debug_mode=debug_mode,
    )
    _log_relation(f"{label}.RELATION", one_x_two.direction, asian_handicap.direction, cross_relation, debug_mode=debug_mode)
    return CrossMarketSignal(
        relation=cross_relation,
        gap=cross_gap,
    )


def _build_period_signal(
    snapshot: P2FullTimeSnapshot | P2FirstHalfSnapshot,
    *,
    label: str,
    debug_mode: bool,
) -> PeriodSignal:
    one_x_two = _build_book_1x2_signal(
        snapshot.pinnacle_1x2,
        snapshot.bet365_1x2,
        label=f"{label}.1X2",
        debug_mode=debug_mode,
    )
    asian_handicap = _build_ah_signal(
        snapshot.pinnacle_ah,
        snapshot.bet365_ah,
        label=f"{label}.AH",
        debug_mode=debug_mode,
    )
    return PeriodSignal(
        one_x_two=one_x_two,
        asian_handicap=asian_handicap,
        cross_market=_build_cross_market_signal(
            one_x_two,
            asian_handicap,
            label=f"{label}.CROSS_MARKET",
            debug_mode=debug_mode,
        ),
    )


def _build_exchange_signal(snapshot: P2FullTimeSnapshot, *, debug_mode: bool) -> ExchangeSignal:
    back = snapshot.betfair_1x2.back
    lay = snapshot.betfair_1x2.lay
    if debug_mode:
        logger.info("P2 FORMULA | FT.EXCHANGE | begin Betfair BACK/LAY calculation")
        for name, value in (
            ("BACK_HOME_PRICE", back.home.odds_price),
            ("BACK_DRAW_PRICE", back.draw.odds_price),
            ("BACK_AWAY_PRICE", back.away.odds_price),
            ("LAY_HOME_PRICE", lay.home.odds_price),
            ("LAY_DRAW_PRICE", lay.draw.odds_price),
            ("LAY_AWAY_PRICE", lay.away.odds_price),
        ):
            _log_assignment(f"FT.EXCHANGE.{name}", value, debug_mode=debug_mode)
    back_edge = side_edge(back.home.odds_price, back.away.odds_price)
    lay_edge = side_edge(lay.home.odds_price, lay.away.odds_price)
    back_direction = direction(back_edge)
    lay_direction = direction(lay_edge)
    rep_edge = pair_mean(back_edge, lay_edge)
    home_spread = relative_spread(back.home.odds_price, lay.home.odds_price)
    away_spread = relative_spread(back.away.odds_price, lay.away.odds_price)
    back_lay_relation = relation(back_direction, lay_direction)
    exchange_internal_gap = absolute_gap(back_edge, lay_edge)
    side_spread = pair_mean(home_spread, away_spread)
    _log_formula(
        "FT.EXCHANGE.BACK_EDGE",
        "side_edge(BACK_HOME_PRICE, BACK_AWAY_PRICE)",
        f"side_edge({_fmt(back.home.odds_price)}, {_fmt(back.away.odds_price)})",
        back_edge,
        debug_mode=debug_mode,
    )
    _log_formula(
        "FT.EXCHANGE.LAY_EDGE",
        "side_edge(LAY_HOME_PRICE, LAY_AWAY_PRICE)",
        f"side_edge({_fmt(lay.home.odds_price)}, {_fmt(lay.away.odds_price)})",
        lay_edge,
        debug_mode=debug_mode,
    )
    _log_direction("FT.EXCHANGE.BACK_DIRECTION", back_edge, back_direction, debug_mode=debug_mode)
    _log_direction("FT.EXCHANGE.LAY_DIRECTION", lay_edge, lay_direction, debug_mode=debug_mode)
    _log_relation("FT.EXCHANGE.BACK_LAY_RELATION", back_direction, lay_direction, back_lay_relation, debug_mode=debug_mode)
    _log_formula(
        "FT.EXCHANGE.INTERNAL_GAP",
        "abs(BACK_EDGE - LAY_EDGE)",
        f"abs({_fmt(back_edge)} - {_fmt(lay_edge)})",
        exchange_internal_gap,
        debug_mode=debug_mode,
    )
    _log_formula(
        "FT.EXCHANGE.REP_EDGE",
        "(BACK_EDGE + LAY_EDGE) / 2",
        f"({_fmt(back_edge)} + {_fmt(lay_edge)}) / 2",
        rep_edge,
        debug_mode=debug_mode,
    )
    _log_direction("FT.EXCHANGE.DIRECTION", rep_edge, direction(rep_edge), debug_mode=debug_mode)
    _log_formula(
        "FT.EXCHANGE.HOME_SPREAD",
        "(HOME_LAY - HOME_BACK) / ((HOME_LAY + HOME_BACK) / 2)",
        f"({_fmt(lay.home.odds_price)} - {_fmt(back.home.odds_price)}) / (({_fmt(lay.home.odds_price)} + {_fmt(back.home.odds_price)}) / 2)",
        home_spread,
        debug_mode=debug_mode,
    )
    _log_formula(
        "FT.EXCHANGE.AWAY_SPREAD",
        "(AWAY_LAY - AWAY_BACK) / ((AWAY_LAY + AWAY_BACK) / 2)",
        f"({_fmt(lay.away.odds_price)} - {_fmt(back.away.odds_price)}) / (({_fmt(lay.away.odds_price)} + {_fmt(back.away.odds_price)}) / 2)",
        away_spread,
        debug_mode=debug_mode,
    )
    _log_formula(
        "FT.EXCHANGE.SIDE_SPREAD",
        "(HOME_SPREAD + AWAY_SPREAD) / 2",
        f"({_fmt(home_spread)} + {_fmt(away_spread)}) / 2",
        side_spread,
        debug_mode=debug_mode,
    )
    return ExchangeSignal(
        back_edge=back_edge,
        back_direction=back_direction,
        lay_edge=lay_edge,
        lay_direction=lay_direction,
        back_lay_relation=back_lay_relation,
        exchange_internal_gap=exchange_internal_gap,
        rep_edge=rep_edge,
        direction=direction(rep_edge),
        home_spread=home_spread,
        away_spread=away_spread,
        side_spread=side_spread,
    )


def _build_book_exchange_signal(
    full_time: PeriodSignal,
    exchange: ExchangeSignal,
    *,
    debug_mode: bool,
) -> BookExchangeSignal:
    assert full_time.one_x_two.rep_edge is not None
    assert full_time.one_x_two.direction is not None
    gap = absolute_gap(full_time.one_x_two.rep_edge, exchange.rep_edge)
    book_exchange_relation = relation(full_time.one_x_two.direction, exchange.direction)
    _log_formula(
        "FT.BOOK_EXCHANGE.GAP",
        "abs(BOOK_REP_EDGE - EXCHANGE_REP_EDGE)",
        f"abs({_fmt(full_time.one_x_two.rep_edge)} - {_fmt(exchange.rep_edge)})",
        gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        "FT.BOOK_EXCHANGE.RELATION",
        full_time.one_x_two.direction,
        exchange.direction,
        book_exchange_relation,
        debug_mode=debug_mode,
    )
    return BookExchangeSignal(
        book_direction=full_time.one_x_two.direction,
        exchange_direction=exchange.direction,
        relation=book_exchange_relation,
        gap=gap,
    )


def _build_ft_1h_signal(
    full_time: PeriodSignal,
    first_half: PeriodSignal,
    *,
    debug_mode: bool,
) -> FirstHalfRelationSignal | None:
    if (
        full_time.one_x_two.rep_edge is None
        or full_time.one_x_two.direction is None
        or first_half.one_x_two.rep_edge is None
        or first_half.one_x_two.direction is None
    ):
        if debug_mode:
            logger.info(
                "P2 FORMULA | FT_1H | unavailable because both 1X2 representatives are required"
            )
        return None
    gap = absolute_gap(full_time.one_x_two.rep_edge, first_half.one_x_two.rep_edge)
    ft_1h_relation = relation(full_time.one_x_two.direction, first_half.one_x_two.direction)
    _log_formula(
        "FT_1H.1X2_GAP",
        "abs(FT_1X2_REP_EDGE - 1H_1X2_REP_EDGE)",
        f"abs({_fmt(full_time.one_x_two.rep_edge)} - {_fmt(first_half.one_x_two.rep_edge)})",
        gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        "FT_1H.1X2_RELATION",
        full_time.one_x_two.direction,
        first_half.one_x_two.direction,
        ft_1h_relation,
        debug_mode=debug_mode,
    )
    return FirstHalfRelationSignal(
        relation=ft_1h_relation,
        gap=gap,
        ft_cross_market=full_time.cross_market.relation,
        first_half_cross_market=first_half.cross_market.relation,
    )


def build_p2_signal_profile(
    snapshot: P2MarketSnapshot,
    *,
    debug_mode: bool = False,
) -> P2SignalProfile:
    """Build structural signals without status, extraction, or persistence concerns."""
    if debug_mode:
        logger.info("P2 FORMULA | engine | begin profile build target_minute=%s", snapshot.target_minute)
    full_time = _build_period_signal(
        snapshot.full_time,
        label="FT",
        debug_mode=debug_mode,
    )
    first_half = (
        None
        if snapshot.first_half is None
        else _build_period_signal(
            snapshot.first_half,
            label="1H",
            debug_mode=debug_mode,
        )
    )
    exchange = _build_exchange_signal(snapshot.full_time, debug_mode=debug_mode)
    ft_1h = None if first_half is None else _build_ft_1h_signal(
        full_time,
        first_half,
        debug_mode=debug_mode,
    )
    book_exchange = _build_book_exchange_signal(
        full_time,
        exchange,
        debug_mode=debug_mode,
    )
    profile = P2SignalProfile(
        full_time=full_time,
        first_half=first_half,
        ft_1h=ft_1h,
        exchange=exchange,
        book_exchange=book_exchange,
    )
    if debug_mode:
        logger.info(
            "P2 FORMULA | engine | profile DTO assigned | full_time=%s | first_half=%s | ft_1h=%s | exchange=%s | book_exchange=%s",
            profile.full_time is not None,
            profile.first_half is not None,
            profile.ft_1h is not None,
            profile.exchange is not None,
            profile.book_exchange is not None,
        )
    return profile


__all__ = ["ENGINE_VERSION", "build_p2_signal_profile"]
