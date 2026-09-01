"""Pure domain orchestration from a canonical P3 snapshot to signals."""

from __future__ import annotations

import logging
from decimal import Decimal

from .metrics import absolute_gap, ou_edge, pair_mean
from .models import P3MarketSnapshot, P3PeriodSnapshot, TotalsBookSnapshot
from .relations import context_direction, direction, relation
from .signal_models import (
    BookOUReading,
    BookRelationSignal,
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
    return P3SignalProfile(
        full_time=full_time,
        first_half=first_half,
        ft_1h=_build_ft_1h_signal(
            full_time,
            first_half,
            debug_mode=debug_mode,
        ),
    )


__all__ = ["ENGINE_VERSION", "build_p3_signal_profile"]
