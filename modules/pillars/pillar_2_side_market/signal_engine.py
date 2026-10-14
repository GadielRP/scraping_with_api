"""Pure domain orchestration from a canonical P2 snapshot to signals."""

from __future__ import annotations

import logging
from decimal import Decimal

from modules.pillars.market_math import (
    absolute_gap,
    pair_mean,
    relative_spread,
    side_edge,
)
from .models import (
    P2FirstHalfSnapshot,
    P2FullTimeSnapshot,
    P2MarketSnapshot,
    PartialAsianHandicapExchangeSnapshot,
    PartialAsianHandicapSnapshot,
    PartialTwoWayMarketSnapshot,
    TwoWayMarketSnapshot,
)
from .relations import direction, relation
from modules.pillars.evaluation_contracts import (
    INTERPRETATION_VERSION,
    EXECUTIVE_SUMMARY_VERSION,
    interpretation_reading,
    interpretation_failure_reason,
    project_executive_sources,
    executive_source_sentences,
)
from .signal_models import (
    AsianHandicapSignal,
    AsianHandicapBookExchangeSignal,
    AsianHandicapExchangeReading,
    AsianHandicapExchangeSignal,
    BookExchangeSignal,
    BookMarketSignal,
    CrossMarketSignal,
    ExchangeSignal,
    FirstHalfRelationSignal,
    P2SignalProfile,
    PeriodSignal,
)

ENGINE_VERSION = "p2-signal-profile-v4.2-executive-summary"

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
        pinnacle is not None
        and bet365 is not None
        and pinnacle.home is not None
        and bet365.home is not None
        and pinnacle.home.trace.market_period == bet365.home.trace.market_period
        and pinnacle.home.trace.market_group == bet365.home.trace.market_group
        and pin_edge is not None
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
    _log_direction(
        f"{label}.PIN_DIRECTION", pin_edge, pin_direction, debug_mode=debug_mode
    )
    _log_direction(
        f"{label}.B365_DIRECTION", b365_edge, b365_direction, debug_mode=debug_mode
    )
    _log_formula(
        f"{label}.BOOK_GAP",
        "abs(PIN_EDGE - B365_EDGE)",
        f"abs({_fmt(pin_edge)} - {_fmt(b365_edge)})",
        book_gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        f"{label}.BOOK_RELATION",
        pin_direction,
        b365_direction,
        book_relation,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{label}.REP_EDGE",
        "(PIN_EDGE + B365_EDGE) / 2",
        f"({_fmt(pin_edge)} + {_fmt(b365_edge)}) / 2",
        rep_edge,
        debug_mode=debug_mode,
    )
    _log_direction(
        f"{label}.DIRECTION", rep_edge, representative_direction, debug_mode=debug_mode
    )
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
    pinnacle: PartialAsianHandicapSnapshot | None,
    bet365: PartialAsianHandicapSnapshot | None,
    *,
    label: str,
    debug_mode: bool,
) -> AsianHandicapSignal:
    pin_line = None if pinnacle is None else pinnacle.home_line
    b365_line = None if bet365 is None else bet365.home_line
    pin_home, pin_away = _market_prices(pinnacle)
    b365_home, b365_away = _market_prices(bet365)
    if debug_mode:
        logger.info("P2 FORMULA | %s | begin spread calculation", label)
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
        and pinnacle is not None
        and bet365 is not None
        and pinnacle.home is not None
        and bet365.home is not None
        and pinnacle.home.trace.market_period == bet365.home.trace.market_period
        and pinnacle.home.trace.market_group == bet365.home.trace.market_group
        and pin_edge is not None
        and b365_edge is not None
        and pin_direction is not None
        and b365_direction is not None
    )
    rep_edge = pair_mean(pin_edge, b365_edge) if comparable else None
    line_gap = (
        absolute_gap(pin_line, b365_line)
        if pin_line is not None and b365_line is not None
        else None
    )
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
    _log_direction(
        f"{label}.PIN_DIRECTION", pin_edge, pin_direction, debug_mode=debug_mode
    )
    _log_direction(
        f"{label}.B365_DIRECTION", b365_edge, b365_direction, debug_mode=debug_mode
    )
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
        _log_relation(
            f"{label}.BOOK_RELATION",
            pin_direction,
            b365_direction,
            book_relation,
            debug_mode=debug_mode,
        )
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
        _log_direction(
            f"{label}.DIRECTION",
            rep_edge,
            representative_direction,
            debug_mode=debug_mode,
        )
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
    spread: AsianHandicapSignal | None,
    *,
    label: str,
    spread_label: str,
    debug_mode: bool,
) -> CrossMarketSignal:
    if (
        one_x_two.rep_edge is None
        or one_x_two.direction is None
        or spread is None
        or spread.rep_edge is None
        or spread.direction is None
    ):
        if debug_mode:
            logger.info(
                "P2 FORMULA | %s | comparison unavailable because both market representatives are required",
                label,
            )
        return CrossMarketSignal(relation=None, gap=None)
    cross_gap = absolute_gap(one_x_two.rep_edge, spread.rep_edge)
    cross_relation = relation(one_x_two.direction, spread.direction)
    _log_formula(
        f"{label}.GAP",
        f"abs(1X2_REP_EDGE - {spread_label}_REP_EDGE)",
        f"abs({_fmt(one_x_two.rep_edge)} - {_fmt(spread.rep_edge)})",
        cross_gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        f"{label}.RELATION",
        one_x_two.direction,
        spread.direction,
        cross_relation,
        debug_mode=debug_mode,
    )
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
    asian_handicap = None
    handicap = None
    if snapshot.pinnacle_ah is not None or snapshot.bet365_ah is not None:
        asian_handicap = _build_ah_signal(
            snapshot.pinnacle_ah,
            snapshot.bet365_ah,
            label=f"{label}.AH",
            debug_mode=debug_mode,
        )
    if snapshot.pinnacle_handicap is not None or snapshot.bet365_handicap is not None:
        handicap = _build_ah_signal(
            snapshot.pinnacle_handicap,
            snapshot.bet365_handicap,
            label=f"{label}.HANDICAP",
            debug_mode=debug_mode,
        )
    return PeriodSignal(
        one_x_two=one_x_two,
        asian_handicap=asian_handicap,
        handicap=handicap,
        cross_market=_build_cross_market_signal(
            one_x_two,
            asian_handicap,
            label=f"{label}.CROSS_MARKET",
            spread_label="AH",
            debug_mode=debug_mode,
        ),
        cross_market_handicap=_build_cross_market_signal(
            one_x_two,
            handicap,
            label=f"{label}.CROSS_MARKET_HANDICAP",
            spread_label="HANDICAP",
            debug_mode=debug_mode,
        ),
    )


def _build_exchange_signal(
    snapshot: P2FullTimeSnapshot, *, debug_mode: bool
) -> ExchangeSignal | None:
    if snapshot.betfair_1x2 is None:
        return None
    back, lay = snapshot.betfair_1x2.back, snapshot.betfair_1x2.lay
    back_edge = (
        None if back is None else side_edge(back.home.odds_price, back.away.odds_price)
    )
    lay_edge = (
        None if lay is None else side_edge(lay.home.odds_price, lay.away.odds_price)
    )
    back_direction = None if back_edge is None else direction(back_edge)
    lay_direction = None if lay_edge is None else direction(lay_edge)
    comparable = (
        back is not None
        and lay is not None
        and back.home.trace.market_period == lay.home.trace.market_period
        and back.home.trace.market_group == lay.home.trace.market_group
    )
    rep = pair_mean(back_edge, lay_edge) if comparable else None
    home_spread = (
        relative_spread(back.home.odds_price, lay.home.odds_price)
        if comparable
        else None
    )
    away_spread = (
        relative_spread(back.away.odds_price, lay.away.odds_price)
        if comparable
        else None
    )
    result = ExchangeSignal(
        back_edge,
        back_direction,
        lay_edge,
        lay_direction,
        relation(back_direction, lay_direction) if comparable else None,
        absolute_gap(back_edge, lay_edge) if comparable else None,
        rep,
        direction(rep) if rep is not None else None,
        home_spread,
        away_spread,
        pair_mean(home_spread, away_spread) if comparable else None,
    )
    if debug_mode:
        logger.info(
            "P2 FORMULA | FT.EXCHANGE | side_edge; pair_mean; relative_spread | result=%s",
            result.to_dict(),
        )
    return result


def _build_exchange_ah_reading(
    branch: PartialAsianHandicapSnapshot | None,
    *,
    label: str,
    debug_mode: bool,
) -> AsianHandicapExchangeReading | None:
    if branch is None:
        if debug_mode:
            logger.info("P2 FORMULA | %s | unavailable: branch not present", label)
        return None
    home = None if branch.home is None else branch.home.odds_price
    away = None if branch.away is None else branch.away.odds_price
    home_size = None if branch.home is None else branch.home.exchange_size
    away_size = None if branch.away is None else branch.away.exchange_size
    _log_assignment(f"{label}.HOME_ODDS", home, debug_mode=debug_mode)
    _log_assignment(f"{label}.AWAY_ODDS", away, debug_mode=debug_mode)
    _log_assignment(f"{label}.HOME_SIZE", home_size, debug_mode=debug_mode)
    _log_assignment(f"{label}.AWAY_SIZE", away_size, debug_mode=debug_mode)
    edge = _edge_if_available(home, away)
    edge_direction = direction(edge) if edge is not None else None
    _log_formula(
        f"{label}.EDGE",
        "(1 / HOME_ODDS - 1 / AWAY_ODDS) / ((1 / HOME_ODDS) + (1 / AWAY_ODDS))",
        (
            "unavailable because both HOME_ODDS and AWAY_ODDS are required"
            if edge is None
            else f"(1 / {_fmt(home)} - 1 / {_fmt(away)}) / ((1 / {_fmt(home)}) + (1 / {_fmt(away)}))"
        ),
        edge,
        debug_mode=debug_mode,
    )
    _log_direction(f"{label}.DIRECTION", edge, edge_direction, debug_mode=debug_mode)
    return AsianHandicapExchangeReading(
        home_odds=home,
        away_odds=away,
        home_size=home_size,
        away_size=away_size,
        edge=edge,
        direction=edge_direction,
    )


def _build_exchange_ah_signal(
    snapshot: PartialAsianHandicapExchangeSnapshot | None,
    *,
    period_label: str = "FT",
    spread_label: str = "AH",
    debug_mode: bool,
) -> AsianHandicapExchangeSignal | None:
    prefix = f"{period_label}.BETFAIR_{spread_label}"
    if snapshot is None:
        if debug_mode:
            logger.info("P2 FORMULA | %s | unavailable: market not present", prefix)
        return None
    line = snapshot.line
    back = _build_exchange_ah_reading(
        snapshot.back, label=f"{prefix}.BACK", debug_mode=debug_mode
    )
    lay = _build_exchange_ah_reading(
        snapshot.lay, label=f"{prefix}.LAY", debug_mode=debug_mode
    )
    back_edge = None if back is None else back.edge
    lay_edge = None if lay is None else lay.edge
    back_direction = None if back is None else back.direction
    lay_direction = None if lay is None else lay.direction
    comparable = (
        snapshot.lines_match
        and back_edge is not None
        and lay_edge is not None
        and back_direction is not None
        and lay_direction is not None
    )
    internal_gap = absolute_gap(back_edge, lay_edge) if comparable else None
    back_lay_relation = relation(back_direction, lay_direction) if comparable else None
    rep_edge = pair_mean(back_edge, lay_edge) if comparable else None
    representative_direction = direction(rep_edge) if rep_edge is not None else None
    _log_assignment(f"{prefix}.LINE", line, debug_mode=debug_mode)
    if debug_mode:
        logger.info(
            "P2 FORMULA | %s | lines_match=%s | comparable=%s",
            prefix,
            snapshot.lines_match,
            comparable,
        )
    _log_formula(
        f"{prefix}.INTERNAL_GAP",
        "abs(BACK_EDGE - LAY_EDGE)",
        f"abs({_fmt(back_edge)} - {_fmt(lay_edge)})",
        internal_gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        f"{prefix}.BACK_LAY_RELATION",
        back_direction,
        lay_direction,
        back_lay_relation,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{prefix}.REP_EDGE",
        "(BACK_EDGE + LAY_EDGE) / 2",
        f"({_fmt(back_edge)} + {_fmt(lay_edge)}) / 2",
        rep_edge,
        debug_mode=debug_mode,
    )
    _log_direction(
        f"{prefix}.DIRECTION", rep_edge, representative_direction, debug_mode=debug_mode
    )
    return AsianHandicapExchangeSignal(
        line=line,
        back=back,
        lay=lay,
        back_lay_relation=back_lay_relation,
        internal_gap=internal_gap,
        rep_edge=rep_edge,
        direction=representative_direction,
    )


def _build_book_exchange_ah_signal(
    full_time: PeriodSignal | None,
    exchange_ah: AsianHandicapExchangeSignal | None,
    *,
    period_label: str = "FT",
    spread_label: str = "AH",
    expected_spread_type: str = "asian_handicap",
    debug_mode: bool,
) -> AsianHandicapBookExchangeSignal | None:
    prefix = f"{period_label}.BOOK_EXCHANGE_{spread_label}"
    if exchange_ah is None:
        if debug_mode:
            logger.info("P2 FORMULA | %s | unavailable: exchange spread absent", prefix)
        return None
    spread = (
        None
        if full_time is None
        else (
            full_time.handicap
            if expected_spread_type == "handicap"
            else full_time.asian_handicap
        )
    )
    if spread is None:
        return AsianHandicapBookExchangeSignal(
            line_diff_raw=None,
            line_gap=None,
            relation=None,
            gap=None,
        )

    pin_line = spread.pin_line
    b365_line = spread.b365_line

    books_line = pin_line if pin_line is not None and pin_line == b365_line else None
    line_diff_raw = (
        None
        if books_line is None or exchange_ah.line is None
        else books_line - exchange_ah.line
    )
    line_gap = (
        absolute_gap(line_diff_raw, Decimal(0)) if line_diff_raw is not None else None
    )
    comparable = (
        line_diff_raw == Decimal(0)
        and spread.rep_edge is not None
        and spread.direction is not None
        and exchange_ah.rep_edge is not None
        and exchange_ah.direction is not None
    )
    gap = absolute_gap(spread.rep_edge, exchange_ah.rep_edge) if comparable else None
    comparison = (
        relation(spread.direction, exchange_ah.direction) if comparable else None
    )
    _log_assignment(f"{prefix}.BOOK_LINE", books_line, debug_mode=debug_mode)
    _log_assignment(f"{prefix}.BETFAIR_LINE", exchange_ah.line, debug_mode=debug_mode)
    _log_formula(
        f"{prefix}.LINE_DIFF_RAW",
        f"BOOK_{spread_label}_LINE - BETFAIR_{spread_label}_LINE",
        (
            "unavailable because both representative lines are required"
            if line_diff_raw is None
            else f"{_fmt(books_line)} - {_fmt(exchange_ah.line)}"
        ),
        line_diff_raw,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{prefix}.LINE_GAP",
        "abs(LINE_DIFF_RAW)",
        (
            "unavailable because LINE_DIFF_RAW is unavailable"
            if line_gap is None
            else f"abs({_fmt(line_diff_raw)})"
        ),
        line_gap,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{prefix}.GAP",
        f"abs(BOOK_{spread_label}_REP_EDGE - BETFAIR_{spread_label}_REP_EDGE)",
        (
            "unavailable because same-line representatives are required"
            if gap is None
            else f"abs({_fmt(spread.rep_edge)} - {_fmt(exchange_ah.rep_edge)})"
        ),
        gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        f"{prefix}.RELATION",
        spread.direction,
        exchange_ah.direction,
        comparison,
        debug_mode=debug_mode,
    )
    return AsianHandicapBookExchangeSignal(
        line_diff_raw=line_diff_raw,
        line_gap=line_gap,
        relation=comparison,
        gap=gap,
    )


def _build_book_exchange_signal(
    full_time: PeriodSignal,
    exchange: ExchangeSignal | None,
    *,
    debug_mode: bool,
) -> BookExchangeSignal | None:
    if (
        exchange is None
        or exchange.rep_edge is None
        or full_time.one_x_two.rep_edge is None
        or full_time.one_x_two.direction is None
    ):
        return None
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
    ft_1h_relation = relation(
        full_time.one_x_two.direction, first_half.one_x_two.direction
    )
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
        logger.info(
            "P2 FORMULA | engine | begin profile build target_minute=%s",
            snapshot.target_minute,
        )
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
    ft_1h = (
        None
        if first_half is None
        else _build_ft_1h_signal(
            full_time,
            first_half,
            debug_mode=debug_mode,
        )
    )
    book_exchange = _build_book_exchange_signal(
        full_time,
        exchange,
        debug_mode=debug_mode,
    )
    exchange_ah = _build_exchange_ah_signal(
        snapshot.full_time.betfair_ah,
        debug_mode=debug_mode,
    )
    book_exchange_ah = _build_book_exchange_ah_signal(
        full_time,
        exchange_ah,
        debug_mode=debug_mode,
    )
    exchange_handicap = _build_exchange_ah_signal(
        snapshot.full_time.betfair_handicap,
        spread_label="HANDICAP",
        debug_mode=debug_mode,
    )
    book_exchange_handicap = _build_book_exchange_ah_signal(
        full_time,
        exchange_handicap,
        spread_label="HANDICAP",
        expected_spread_type="handicap",
        debug_mode=debug_mode,
    )
    exchange_ah_1h = _build_exchange_ah_signal(
        None if snapshot.first_half is None else snapshot.first_half.betfair_ah,
        period_label="1H",
        debug_mode=debug_mode,
    )
    book_exchange_ah_1h = _build_book_exchange_ah_signal(
        first_half,
        exchange_ah_1h,
        period_label="1H",
        debug_mode=debug_mode,
    )
    exchange_handicap_1h = _build_exchange_ah_signal(
        None if snapshot.first_half is None else snapshot.first_half.betfair_handicap,
        period_label="1H",
        spread_label="HANDICAP",
        debug_mode=debug_mode,
    )
    book_exchange_handicap_1h = _build_book_exchange_ah_signal(
        first_half,
        exchange_handicap_1h,
        period_label="1H",
        spread_label="HANDICAP",
        expected_spread_type="handicap",
        debug_mode=debug_mode,
    )

    profile = P2SignalProfile(
        full_time=full_time,
        first_half=first_half,
        ft_1h=ft_1h,
        exchange=exchange,
        book_exchange=book_exchange,
        exchange_ah=exchange_ah,
        book_exchange_ah=book_exchange_ah,
        exchange_ah_1h=exchange_ah_1h,
        book_exchange_ah_1h=book_exchange_ah_1h,
        exchange_handicap=exchange_handicap,
        book_exchange_handicap=book_exchange_handicap,
        exchange_handicap_1h=exchange_handicap_1h,
        book_exchange_handicap_1h=book_exchange_handicap_1h,
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


def _interpret_side_pair(
    left, right, representative, comparable, *, kind="books", evidence=None
):
    available = [value for value in (left, right) if value is not None]
    if not available:
        return interpretation_reading(
            "MISSING_INPUT",
            status="BLOCKED",
            reason="MISSING_INPUT",
            comparability="UNAVAILABLE",
            evidence=evidence,
        )
    if len(available) == 1:
        state = "SINGLE_BOOK_READING" if kind == "books" else "SINGLE_EXCHANGE_READING"
        return interpretation_reading(
            state, direction=available[0], comparability="PARTIAL", evidence=evidence
        )
    if not comparable:
        return interpretation_reading(
            "INCOMPARABLE_CONTRACT",
            status="BLOCKED",
            reason="INCOMPATIBLE_CONTRACT",
            comparability="INCOMPARABLE_CONTRACT",
            evidence=evidence,
        )
    if left == right == "NEUTRAL":
        state = "NEUTRAL_STRUCTURE"
    elif "NEUTRAL" in (left, right):
        state = "PARTIAL_DIRECTIONAL_AGREEMENT"
    elif left == right:
        state = {
            "books": "BOOK_CONSENSUS",
            "exchange": "BACK_LAY_CONSENSUS",
            "sources": "CROSS_SOURCE_CONSENSUS",
        }[kind]
    else:
        state = {
            "books": "BOOK_DISAGREEMENT",
            "exchange": "BACK_LAY_DIVERGENCE",
            "sources": "CROSS_SOURCE_CONFLICT",
        }[kind]
    return interpretation_reading(state, direction=representative, evidence=evidence)


def _interpret_side_period(branches):
    supporting = [
        key
        for key, reading in branches.items()
        if reading["direction"] in ("HOME", "AWAY")
    ]
    complete = bool(branches) and all(
        r["status"] == "COMPUTED"
        and r["comparability"] == "COMPARABLE"
        and r["state"] in ("BOOK_CONSENSUS", "NEUTRAL_STRUCTURE")
        for r in branches.values()
    )
    directions = {
        r["direction"] for r in branches.values() if r["direction"] is not None
    }
    comparable_directions = {
        r["direction"]
        for r in branches.values()
        if r["comparability"] == "COMPARABLE" and r["direction"] in ("HOME", "AWAY")
    }
    if comparable_directions == {"HOME", "AWAY"}:
        state, value = "CROSS_MARKET_CONFLICT", "MIXED"
    elif (
        complete
        and len(branches) >= 2
        and len(directions) == 1
        and "NEUTRAL" not in directions
    ):
        state, value = "MULTI_MARKET_CONSENSUS", next(iter(directions))
    elif complete and directions == {"NEUTRAL"}:
        state, value = "NEUTRAL_STRUCTURE", "NEUTRAL"
    else:
        available_directions = {
            r["direction"]
            for r in branches.values()
            if r["status"] == "COMPUTED"
            and r["comparability"] in ("COMPARABLE", "PARTIAL")
            and r["direction"] is not None
        }
        state = "PARTIAL_STRUCTURE"
        value = (
            next(iter(available_directions))
            if len(available_directions) == 1
            and not any(r["state"] == "BOOK_DISAGREEMENT" for r in branches.values())
            else None
        )
    return interpretation_reading(
        state,
        direction=value,
        comparability="COMPARABLE" if complete else "PARTIAL",
        supporting_branches=supporting,
        evidence={"branch_refs": list(branches)},
    )


def interpret_p2_result(result):
    """Describe existing side readings without creating representatives or scores."""
    scopes, structures = {}, {}
    for scope, profile in result["analysis"].items():
        periods = {}
        for period in ("FT", "1H", "FIRST_FIVE_INNINGS"):
            values = profile.get(period)
            if not values:
                continue
            branches = {}
            for family in ("1X2", "AH", "HANDICAP"):
                raw = values.get(family)
                if raw is None:
                    continue
                reading = _interpret_side_pair(
                    raw.get("PIN_DIRECTION"),
                    raw.get("B365_DIRECTION"),
                    raw.get("DIRECTION"),
                    raw.get("REP_EDGE") is not None,
                    evidence={"analysis_ref": f"analysis.{scope}.{period}.{family}"},
                )
                reading.update(
                    pinnacle_direction=raw.get("PIN_DIRECTION"),
                    bet365_direction=raw.get("B365_DIRECTION"),
                )
                reading["relation"] = raw.get("BOOK_RELATION")
                if reading["state"] == "MISSING_INPUT":
                    reason = interpretation_failure_reason(
                        result, scope, f"{period}.{family}"
                    )
                    state = (
                        "INCOMPARABLE_CONTRACT"
                        if reason == "INCOMPATIBLE_CONTRACT"
                        else reason
                    )
                    reading = {
                        **reading,
                        **interpretation_reading(
                            state,
                            reason=reason,
                            status=(
                                "ERROR" if reason == "CALCULATION_ERROR" else "BLOCKED"
                            ),
                            comparability="UNAVAILABLE",
                            evidence=reading["evidence"],
                        ),
                    }
                if family != "1X2":
                    reading.update(
                        pinnacle_line=raw.get("PIN_LINE"),
                        bet365_line=raw.get("B365_LINE"),
                        lines_match=(
                            None
                            if raw.get("PIN_LINE") is None
                            or raw.get("B365_LINE") is None
                            else raw["PIN_LINE"] == raw["B365_LINE"]
                        ),
                    )
                    for bookmaker, line_key, direction_key in (
                        ("PINNACLE", "PIN_LINE", "PIN_DIRECTION"),
                        ("BET365", "B365_LINE", "B365_DIRECTION"),
                    ):
                        if raw.get(direction_key) is not None:
                            structures.setdefault(
                                (scope.split(":", 1)[0], period, family), {}
                            ).setdefault(
                                (bookmaker, raw.get(line_key)),
                                {
                                    "bookmaker": bookmaker,
                                    "line": raw.get(line_key),
                                    "direction": raw[direction_key],
                                    "analysis_ref": f"analysis.{scope}.{period}.{family}",
                                },
                            )
                branches[family] = reading
            exchanges, comparisons = {}, {}
            exchange_keys = {"1X2": "EXCHANGE"} if period == "FT" else {}
            suffix = period
            exchange_keys.update(
                {
                    family: f"BETFAIR_{suffix}_{family}"
                    for family in ("AH", "HANDICAP") if family in branches
                }
            )
            for family, key in exchange_keys.items():
                raw = profile.get(key) or {}
                exchanges[family] = _interpret_side_pair(
                    raw.get("BACK_DIRECTION"),
                    raw.get("LAY_DIRECTION"),
                    raw.get("DIRECTION"),
                    raw.get("REP_EDGE") is not None,
                    kind="exchange",
                    evidence={"analysis_ref": f"analysis.{scope}.{key}"},
                )
                exchanges[family].update(
                    back_direction=raw.get("BACK_DIRECTION"),
                    lay_direction=raw.get("LAY_DIRECTION"),
                    line=raw.get("LINE"),
                    relation=raw.get("BACK_LAY_RELATION"),
                )
                book = values.get(family) or {}
                comparison_key = (
                    "BOOK_EXCHANGE"
                    if family == "1X2"
                    else f"BOOK_EXCHANGE_{'' if period == 'FT' else suffix + '_'}{family}"
                )
                comparison = profile.get(comparison_key) or {}
                book_direction, exchange_direction = book.get("DIRECTION"), raw.get(
                    "DIRECTION"
                )
                comparable = comparison.get("RELATION") is not None
                if book_direction is None or exchange_direction is None:
                    comparisons[family] = interpretation_reading(
                        "PARTIAL_STRUCTURE",
                        status="BLOCKED",
                        reason="MISSING_INPUT",
                        comparability="PARTIAL",
                    )
                else:
                    representative = (
                        book_direction if book_direction == exchange_direction else None
                    )
                    comparisons[family] = _interpret_side_pair(
                        book_direction,
                        exchange_direction,
                        representative,
                        comparable,
                        kind="sources",
                    )
                comparisons[family].update(
                    book_direction=book_direction,
                    exchange_direction=exchange_direction,
                    relation=comparison.get("RELATION"),
                    evidence={"analysis_ref": f"analysis.{scope}.{comparison_key}"},
                )
                if family != "1X2":
                    comparisons[family].update(
                        books_line=(
                            book.get("PIN_LINE")
                            if book.get("REP_EDGE") is not None
                            else None
                        ),
                        exchange_line=raw.get("LINE"),
                        line_gap=comparison.get("LINE_GAP"),
                    )
            periods[period] = {
                "markets": branches,
                "exchange": exchanges,
                "book_exchange": comparisons,
                "reading": _interpret_side_period(branches),
            }
        secondary = "FIRST_FIVE_INNINGS" if "FIRST_FIVE_INNINGS" in profile else "1H"
        cross_key = "FT_FIRST_FIVE_INNINGS" if secondary != "1H" else "FT_1H"
        cross = profile.get(cross_key) or {}
        ft_direction = ((profile.get("FT") or {}).get("1X2") or {}).get("DIRECTION")
        secondary_direction = ((profile.get(secondary) or {}).get("1X2") or {}).get(
            "DIRECTION"
        )
        cross_state = (
            "PARTIAL_STRUCTURE"
            if not cross
            else (
                "CROSS_PERIOD_CONSENSUS"
                if ft_direction == secondary_direction
                else "CROSS_PERIOD_DIFFERENCE"
            )
        )
        scopes[scope] = {
            "periods": periods,
            "cross_period": interpretation_reading(
                cross_state,
                status="COMPUTED" if cross else "BLOCKED",
                reason=None if cross else "MISSING_INPUT",
                comparability="PARTIAL",
                full_time_direction=ft_direction,
                secondary_direction=secondary_direction,
                relation=cross.get(f"{cross_key}_1X2_RELATION"),
                evidence={"analysis_ref": f"analysis.{scope}.{cross_key}"},
            ),
        }
    line_structures = {}
    for (family, period, market), readings in structures.items():
        if len({r["line"] for r in readings.values()}) > 1:
            line_structures[f"{family}:{period}:{market}"] = interpretation_reading(
                "INCOMPARABLE_CONTRACT",
                status="BLOCKED",
                reason="INCOMPATIBLE_CONTRACT",
                comparability="INCOMPARABLE_LINE",
                readings=list(readings.values()),
            )
    return {
        "version": INTERPRETATION_VERSION,
        "status": result["status"],
        "scopes": scopes,
        "line_structures": line_structures,
        "reasons": result["diagnostics"]["signals"]["reasons"],
        "summary": "Lectura SIDE por contrato y periodo, sin score agregado.",
    }


def build_p2_executive_summary(result, interpretation=None):
    """Select P2's existing period readings for a compact operator summary."""
    interpretation = interpretation or result.get("INTERPRETATION") or interpret_p2_result(result)
    scopes = interpretation.get("scopes") or {}

    def scope_priority(item):
        name, scope = item
        periods = scope.get("periods") or {}
        available = sum(
            1
            for value in periods.values()
            if value.get("reading", {}).get("status") == "COMPUTED"
            and value.get("reading", {}).get("direction") is not None
        )
        family = name.split(":", 1)[0]
        lane = name.rsplit(":", 1)[-1]
        return available, family == "1X2", lane == "base", name

    selected_scope = (
        max(scopes.items(), key=scope_priority)[1] if scopes else {}
    )
    periods = selected_scope.get("periods") or {}
    period_names = {
        "FT": "full_time",
        "1H": "first_half",
        "FIRST_FIVE_INNINGS": "first_five_innings",
    }
    period_readings = {}
    for source_name, summary_name in period_names.items():
        available_periods = [
            scope["periods"][source_name]
            for scope in scopes.values()
            if source_name in scope.get("periods", {})
        ]
        if not available_periods:
            continue
        period = periods.get(source_name) or {}
        reading = period.get("reading") or {}
        supported = reading.get("supporting_branches") or []
        period_readings[summary_name] = {
            "status": reading.get("status"),
            "direction": reading.get("direction"),
            "state": reading.get("state"),
            "supporting_markets": [
                {"AH": "ASIAN_HANDICAP", "HANDICAP": "HANDICAP"}.get(
                    market, market
                )
                for market in supported
            ],
            "source_readings": project_executive_sources(
                (role, market, source)
                for available in available_periods
                for role, key in (("books", "markets"), ("exchange", "exchange"), ("book_exchange", "book_exchange"))
                for market, source in available.get(key, {}).items()
            ),
        }

    cross = selected_scope.get("cross_period") or {}
    cross_period = {
        "status": cross.get("status"),
        "state": cross.get("state"),
        "full_time_direction": cross.get("full_time_direction"),
        "secondary_direction": cross.get("secondary_direction"),
    }

    sentences = []
    labels = {
        "full_time": "FT",
        "first_half": "1H",
        "first_five_innings": "First Five Innings",
    }
    state_phrases = {
        "MULTI_MARKET_CONSENSUS": "presenta consenso multimercado",
        "PARTIAL_STRUCTURE": "presenta una estructura parcial con dirección",
        "CROSS_MARKET_CONFLICT": "presenta direcciones distintas entre mercados",
        "NEUTRAL_STRUCTURE": "presenta una estructura neutral",
        "MISSING_INPUT": "no tiene inputs suficientes para una lectura",
    }
    for name, reading in period_readings.items():
        direction = reading["direction"]
        phrase = state_phrases.get(reading["state"])
        if phrase is None:
            continue
        if direction is not None:
            sentences.append(f"{labels[name]} {phrase} {direction}.")
        elif reading["state"] in {"CROSS_MARKET_CONFLICT", "MISSING_INPUT"}:
            sentences.append(f"{labels[name]} {phrase}.")
        sentences.extend(executive_source_sentences(labels[name], reading["source_readings"]))
    if cross_period["state"] == "CROSS_PERIOD_CONSENSUS":
        sentences.append("FT y el periodo secundario están alineados.")
    elif cross_period["state"] == "CROSS_PERIOD_DIFFERENCE":
        sentences.append("FT y el periodo secundario muestran direcciones diferentes.")

    return {
        "version": EXECUTIVE_SUMMARY_VERSION,
        "status": result.get("status"),
        **period_readings,
        "cross_period": cross_period,
        "summary": " ".join(sentences)
        or "P2 no dispone de una lectura ejecutiva calculada para este evento.",
    }


__all__ = [
    "ENGINE_VERSION",
    "build_p2_executive_summary",
    "build_p2_signal_profile",
    "interpret_p2_result",
]
