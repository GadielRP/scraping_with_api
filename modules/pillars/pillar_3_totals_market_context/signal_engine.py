"""Pure domain orchestration from a canonical P3 snapshot to signals."""

from __future__ import annotations

import logging
from decimal import Decimal

from .metrics import absolute_gap, ou_edge, pair_mean
from .models import (
    P3MarketSnapshot,
    P3PeriodSnapshot,
    TotalsBookSnapshot,
    TotalsExchangeSnapshot,
)
from .relations import context_direction, direction, relation
from .signal_models import (
    BookOUReading,
    BookRelationSignal,
    BookExchangeOUSignal,
    ExchangeOUReading,
    ExchangeOUSignal,
    FT1HSignal,
    LineStructureSignal,
    P3SignalProfile,
    PeriodOUSignal,
    RepresentativeSignal,
)


ENGINE_VERSION = "p3-signal-profile-v1"

logger = logging.getLogger(__name__)


def _fmt(value: object) -> str:
    return "None" if value is None else str(value)


def _log_assignment(name: str, value: object, *, debug_mode: bool) -> None:
    if debug_mode:
        logger.info("P3 FORMULA | assignment | %s = %s", name, _fmt(value))


def _log_formula(
    name: str,
    formula: str,
    substitution: str,
    result: object,
    *,
    debug_mode: bool,
) -> None:
    if debug_mode:
        logger.info("P3 FORMULA | %s | formula=%s", name, formula)
        logger.info("P3 FORMULA | %s | substitution=%s", name, substitution)
        logger.info("P3 FORMULA | %s | result=%s", name, _fmt(result))


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
            "P3 FORMULA | %s | relation(left=%s, right=%s) -> %s",
            name,
            left,
            right,
            value,
        )


def _edge_if_available(
    over_odds: Decimal | None,
    under_odds: Decimal | None,
) -> Decimal | None:
    if over_odds is None or under_odds is None:
        return None
    return ou_edge(over_odds, under_odds)


def _build_book_reading(
    snapshot: TotalsBookSnapshot | None,
    *,
    label: str,
    debug_mode: bool,
) -> BookOUReading:
    line = None if snapshot is None else snapshot.line
    over_odds = (
        None if snapshot is None or snapshot.over is None else snapshot.over.odds_price
    )
    under_odds = (
        None
        if snapshot is None or snapshot.under is None
        else snapshot.under.odds_price
    )
    _log_assignment(f"{label}.LINE", line, debug_mode=debug_mode)
    _log_assignment(f"{label}.OVER_ODDS", over_odds, debug_mode=debug_mode)
    _log_assignment(f"{label}.UNDER_ODDS", under_odds, debug_mode=debug_mode)

    edge = _edge_if_available(over_odds, under_odds)
    reading_direction = direction(edge) if edge is not None else None
    substitution = (
        "unavailable because OVER_ODDS or UNDER_ODDS is None"
        if edge is None
        else (
            f"((1 / {_fmt(over_odds)}) - (1 / {_fmt(under_odds)})) / "
            f"((1 / {_fmt(over_odds)}) + (1 / {_fmt(under_odds)}))"
        )
    )
    _log_formula(
        f"{label}.EDGE",
        "((1 / over_odds) - (1 / under_odds)) / ((1 / over_odds) + (1 / under_odds))",
        substitution,
        edge,
        debug_mode=debug_mode,
    )
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.DIRECTION | direction(edge=%s) -> %s",
            label,
            _fmt(edge),
            reading_direction,
        )
    return BookOUReading(
        line=line,
        over_odds=over_odds,
        under_odds=under_odds,
        edge=edge,
        direction=reading_direction,
    )


def _build_line_structure(
    pinnacle: BookOUReading,
    bet365: BookOUReading,
    *,
    label: str,
    debug_mode: bool,
) -> LineStructureSignal:
    if pinnacle.line is None or bet365.line is None:
        line_diff_raw = None
        line_gap = None
    else:
        line_diff_raw = pinnacle.line - bet365.line
        line_gap = abs(line_diff_raw)
    _log_formula(
        f"{label}.LINE_STRUCTURE.LINE_DIFF_RAW",
        "PINNACLE.LINE - BET365.LINE",
        f"{_fmt(pinnacle.line)} - {_fmt(bet365.line)}",
        line_diff_raw,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{label}.LINE_STRUCTURE.LINE_GAP",
        "abs(LINE_DIFF_RAW)",
        f"abs({_fmt(line_diff_raw)})",
        line_gap,
        debug_mode=debug_mode,
    )
    return LineStructureSignal(line_diff_raw=line_diff_raw, line_gap=line_gap)


def _build_period_signal(
    snapshot: P3PeriodSnapshot,
    *,
    label: str,
    debug_mode: bool,
) -> PeriodOUSignal:
    pinnacle = _build_book_reading(
        snapshot.pinnacle,
        label=f"{label}.PINNACLE",
        debug_mode=debug_mode,
    )
    bet365 = _build_book_reading(
        snapshot.bet365,
        label=f"{label}.BET365",
        debug_mode=debug_mode,
    )
    line_structure = _build_line_structure(
        pinnacle,
        bet365,
        label=label,
        debug_mode=debug_mode,
    )
    comparable = (
        pinnacle.line is not None
        and bet365.line is not None
        and pinnacle.line == bet365.line
        and pinnacle.edge is not None
        and bet365.edge is not None
        and pinnacle.direction is not None
        and bet365.direction is not None
    )
    if comparable:
        assert pinnacle.edge is not None and bet365.edge is not None
        assert pinnacle.direction is not None and bet365.direction is not None
        book_gap = absolute_gap(pinnacle.edge, bet365.edge)
        book_relation = relation(pinnacle.direction, bet365.direction)
        representative_edge = pair_mean(pinnacle.edge, bet365.edge)
        representative_direction = direction(representative_edge)
    else:
        book_gap = None
        book_relation = None
        representative_edge = None
        representative_direction = None

    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.COMPARABLE | same_line=%s both_edges=%s -> %s",
            label,
            pinnacle.line is not None
            and bet365.line is not None
            and pinnacle.line == bet365.line,
            pinnacle.edge is not None and bet365.edge is not None,
            comparable,
        )
        logger.info(
            "P3 FORMULA | %s.BOOK_RELATION.RELATION | relation(left=%s, right=%s) -> %s",
            label,
            pinnacle.direction,
            bet365.direction,
            book_relation,
        )
    _log_formula(
        f"{label}.BOOK_RELATION.GAP",
        "abs(PINNACLE.EDGE - BET365.EDGE) when lines are equal",
        f"abs({_fmt(pinnacle.edge)} - {_fmt(bet365.edge)})",
        book_gap,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{label}.REPRESENTATIVE.EDGE",
        "(PINNACLE.EDGE + BET365.EDGE) / 2 when lines are equal",
        f"({_fmt(pinnacle.edge)} + {_fmt(bet365.edge)}) / 2",
        representative_edge,
        debug_mode=debug_mode,
    )
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.REPRESENTATIVE.DIRECTION | direction(edge=%s) -> %s",
            label,
            _fmt(representative_edge),
            representative_direction,
        )
    context = context_direction(representative_direction)
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.CONTEXT_DIRECTION_RAW | context_direction(%s) -> %s",
            label,
            representative_direction,
            context,
        )
    return PeriodOUSignal(
        pinnacle=pinnacle,
        bet365=bet365,
        line_structure=line_structure,
        book_relation=BookRelationSignal(relation=book_relation, gap=book_gap),
        representative=RepresentativeSignal(
            edge=representative_edge,
            direction=representative_direction,
        ),
        context_direction_raw=context,
    )


def _build_exchange_ou_reading(
    snapshot: TotalsBookSnapshot | None,
    *,
    label: str,
    debug_mode: bool,
) -> ExchangeOUReading | None:
    if snapshot is None:
        if debug_mode:
            logger.info("P3 FORMULA | %s | unavailable: branch not present", label)
        return None
    over_odds = None if snapshot.over is None else snapshot.over.odds_price
    under_odds = None if snapshot.under is None else snapshot.under.odds_price
    over_size = None if snapshot.over is None else snapshot.over.exchange_size
    under_size = None if snapshot.under is None else snapshot.under.exchange_size
    _log_assignment(f"{label}.OVER_ODDS", over_odds, debug_mode=debug_mode)
    _log_assignment(f"{label}.UNDER_ODDS", under_odds, debug_mode=debug_mode)
    _log_assignment(f"{label}.OVER_SIZE", over_size, debug_mode=debug_mode)
    _log_assignment(f"{label}.UNDER_SIZE", under_size, debug_mode=debug_mode)
    edge = _edge_if_available(over_odds, under_odds)
    reading_direction = direction(edge) if edge is not None else None
    _log_formula(
        f"{label}.EDGE",
        "((1 / OVER_ODDS) - (1 / UNDER_ODDS)) / ((1 / OVER_ODDS) + (1 / UNDER_ODDS))",
        "unavailable because OVER_ODDS or UNDER_ODDS is None"
        if edge is None
        else f"((1 / {_fmt(over_odds)}) - (1 / {_fmt(under_odds)})) / ((1 / {_fmt(over_odds)}) + (1 / {_fmt(under_odds)}))",
        edge,
        debug_mode=debug_mode,
    )
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.DIRECTION | direction(edge=%s) -> %s",
            label,
            _fmt(edge),
            reading_direction,
        )
    return ExchangeOUReading(
        over_odds=over_odds,
        under_odds=under_odds,
        over_size=over_size,
        under_size=under_size,
        edge=edge,
        direction=reading_direction,
    )


def _build_exchange_ou_signal(
    snapshot: TotalsExchangeSnapshot | None,
    *,
    period_label: str = "FT",
    debug_mode: bool,
) -> ExchangeOUSignal | None:
    prefix = f"{period_label}.BETFAIR_OU"
    if snapshot is None:
        if debug_mode:
            logger.info("P3 FORMULA | %s | unavailable: market not present", prefix)
        return None
    back = _build_exchange_ou_reading(
        snapshot.back,
        label=f"{prefix}.BACK",
        debug_mode=debug_mode,
    )
    lay = _build_exchange_ou_reading(
        snapshot.lay,
        label=f"{prefix}.LAY",
        debug_mode=debug_mode,
    )
    comparable = (
        snapshot.lines_match
        and back is not None
        and lay is not None
        and back.edge is not None
        and lay.edge is not None
        and back.direction is not None
        and lay.direction is not None
    )
    internal_gap = absolute_gap(back.edge, lay.edge) if comparable else None
    back_lay_relation = relation(back.direction, lay.direction) if comparable else None
    representative_edge = pair_mean(back.edge, lay.edge) if comparable else None
    representative_direction = direction(representative_edge) if representative_edge is not None else None
    _log_assignment(f"{prefix}.LINE", snapshot.line, debug_mode=debug_mode)
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.COMPARABLE | same_line=%s both_edges=%s -> %s",
            prefix,
            snapshot.lines_match,
            back is not None and back.edge is not None and lay is not None and lay.edge is not None,
            comparable,
        )
    _log_formula(
        f"{prefix}.EXCHANGE_INTERNAL_GAP",
        "abs(BACK_EDGE - LAY_EDGE)",
        "unavailable because matching complete BACK/LAY contracts are required"
        if internal_gap is None
        else f"abs({_fmt(back.edge)} - {_fmt(lay.edge)})",
        internal_gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        f"{prefix}.BACK_LAY_RELATION",
        None if back is None else back.direction,
        None if lay is None else lay.direction,
        back_lay_relation,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{prefix}.REPRESENTATIVE.EDGE",
        "(BACK_EDGE + LAY_EDGE) / 2",
        "unavailable because matching complete BACK/LAY contracts are required"
        if representative_edge is None
        else f"({_fmt(back.edge)} + {_fmt(lay.edge)}) / 2",
        representative_edge,
        debug_mode=debug_mode,
    )
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.REPRESENTATIVE.DIRECTION | direction(edge=%s) -> %s",
            prefix,
            _fmt(representative_edge),
            representative_direction,
        )
    return ExchangeOUSignal(
        line=snapshot.line,
        back=back,
        lay=lay,
        back_lay_relation=back_lay_relation,
        exchange_internal_gap=internal_gap,
        representative=RepresentativeSignal(
            edge=representative_edge,
            direction=representative_direction,
        ),
    )


def _build_book_exchange_ou_signal(
    full_time: PeriodOUSignal,
    exchange_ou: ExchangeOUSignal | None,
    *,
    period_label: str = "FT",
    debug_mode: bool,
) -> BookExchangeOUSignal | None:
    prefix = f"{period_label}.BOOK_EXCHANGE_OU"
    if exchange_ou is None:
        if debug_mode:
            logger.info("P3 FORMULA | %s | unavailable: Betfair O/U absent", prefix)
        return None
    books_line = (
        full_time.pinnacle.line
        if full_time.pinnacle.line is not None
        and full_time.pinnacle.line == full_time.bet365.line
        else None
    )
    line_diff_raw = (
        None if books_line is None or exchange_ou.line is None else books_line - exchange_ou.line
    )
    line_gap = abs(line_diff_raw) if line_diff_raw is not None else None
    comparable = (
        line_diff_raw == Decimal(0)
        and full_time.representative.edge is not None
        and full_time.representative.direction is not None
        and exchange_ou.representative.edge is not None
        and exchange_ou.representative.direction is not None
    )
    gap = (
        absolute_gap(full_time.representative.edge, exchange_ou.representative.edge)
        if comparable
        else None
    )
    comparison = (
        relation(full_time.representative.direction, exchange_ou.representative.direction)
        if comparable
        else None
    )
    _log_formula(
        f"{prefix}.LINE_DIFF_RAW",
        "BOOK_OU_LINE - BETFAIR_OU_LINE",
        "unavailable because one unique bookmaker line and the Betfair line are required"
        if line_diff_raw is None
        else f"{_fmt(books_line)} - {_fmt(exchange_ou.line)}",
        line_diff_raw,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{prefix}.LINE_GAP",
        "abs(LINE_DIFF_RAW)",
        "unavailable because LINE_DIFF_RAW is unavailable"
        if line_gap is None
        else f"abs({_fmt(line_diff_raw)})",
        line_gap,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{prefix}.GAP",
        "abs(BOOK_OU_REP_EDGE - BETFAIR_OU_REP_EDGE)",
        "unavailable because same-line representatives are required"
        if gap is None
        else f"abs({_fmt(full_time.representative.edge)} - {_fmt(exchange_ou.representative.edge)})",
        gap,
        debug_mode=debug_mode,
    )
    _log_relation(
        f"{prefix}.RELATION",
        full_time.representative.direction,
        exchange_ou.representative.direction,
        comparison,
        debug_mode=debug_mode,
    )
    return BookExchangeOUSignal(
        line_diff_raw=line_diff_raw,
        line_gap=line_gap,
        relation=comparison,
        gap=gap,
    )


def _build_ft_1h_signal(
    full_time: PeriodOUSignal,
    first_half: PeriodOUSignal | None,
    *,
    debug_mode: bool,
) -> FT1HSignal | None:
    if (
        first_half is None
        or full_time.representative.edge is None
        or full_time.representative.direction is None
        or full_time.book_relation.relation is None
        or first_half.representative.edge is None
        or first_half.representative.direction is None
        or first_half.book_relation.relation is None
    ):
        if debug_mode:
            logger.info(
                "P3 FORMULA | FT_1H | unavailable because both period representatives are required"
            )
        return None

    ft_1h_relation = relation(
        full_time.representative.direction,
        first_half.representative.direction,
    )
    ft_1h_gap = absolute_gap(
        full_time.representative.edge,
        first_half.representative.edge,
    )
    if debug_mode:
        logger.info(
            "P3 FORMULA | FT_1H.RELATION | relation(left=%s, right=%s) -> %s",
            full_time.representative.direction,
            first_half.representative.direction,
            ft_1h_relation,
        )
    _log_formula(
        "FT_1H.GAP",
        "abs(FT.REPRESENTATIVE.EDGE - 1H.REPRESENTATIVE.EDGE)",
        f"abs({_fmt(full_time.representative.edge)} - {_fmt(first_half.representative.edge)})",
        ft_1h_gap,
        debug_mode=debug_mode,
    )
    return FT1HSignal(
        relation=ft_1h_relation,
        gap=ft_1h_gap,
        ft_book_relation=full_time.book_relation.relation,
        ft_rep_direction=full_time.representative.direction,
        first_half_book_relation=first_half.book_relation.relation,
        first_half_rep_direction=first_half.representative.direction,
    )


def build_p3_signal_profile(
    snapshot: P3MarketSnapshot,
    *,
    debug_mode: bool = False,
) -> P3SignalProfile:
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
    exchange_ou = _build_exchange_ou_signal(
        snapshot.exchange_ou,
        debug_mode=debug_mode,
    )
    exchange_ou_1h = _build_exchange_ou_signal(
        snapshot.exchange_ou_1h,
        period_label="1H",
        debug_mode=debug_mode,
    )
    return P3SignalProfile(
        full_time=full_time,
        first_half=first_half,
        ft_1h=_build_ft_1h_signal(
            full_time,
            first_half,
            debug_mode=debug_mode,
        ),
        exchange_ou=exchange_ou,
        book_exchange_ou=_build_book_exchange_ou_signal(
            full_time,
            exchange_ou,
            debug_mode=debug_mode,
        ),
        exchange_ou_1h=exchange_ou_1h,
        book_exchange_ou_1h=(
            None
            if first_half is None
            else _build_book_exchange_ou_signal(
                first_half,
                exchange_ou_1h,
                period_label="1H",
                debug_mode=debug_mode,
            )
        ),
    )


__all__ = ["ENGINE_VERSION", "build_p3_signal_profile"]
