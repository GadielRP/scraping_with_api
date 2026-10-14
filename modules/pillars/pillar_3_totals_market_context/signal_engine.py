"""Pure domain orchestration from a canonical P3 snapshot to signals."""

from __future__ import annotations

import logging
from decimal import Decimal

from modules.pillars.market_math import absolute_gap, ou_edge, pair_mean
from .models import (
    P3MarketSnapshot,
    P3PeriodSnapshot,
    TotalsBookSnapshot,
    TotalsExchangeSnapshot,
)
from .relations import context_direction, direction, relation
from modules.pillars.evaluation_contracts import (
    INTERPRETATION_VERSION,
    EXECUTIVE_SUMMARY_VERSION,
    interpretation_reading,
    interpretation_failure_reason,
    project_executive_sources,
    executive_source_sentences,
)
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

ENGINE_VERSION = "p3-signal-profile-v4.2-executive-summary"

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
        snapshot.pinnacle is not None
        and snapshot.bet365 is not None
        and snapshot.pinnacle.market_period == snapshot.bet365.market_period
        and pinnacle.line is not None
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
        (
            "unavailable because OVER_ODDS or UNDER_ODDS is None"
            if edge is None
            else f"((1 / {_fmt(over_odds)}) - (1 / {_fmt(under_odds)})) / ((1 / {_fmt(over_odds)}) + (1 / {_fmt(under_odds)}))"
        ),
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
    representative_direction = (
        direction(representative_edge) if representative_edge is not None else None
    )
    _log_assignment(f"{prefix}.LINE", snapshot.line, debug_mode=debug_mode)
    if debug_mode:
        logger.info(
            "P3 FORMULA | %s.COMPARABLE | same_line=%s both_edges=%s -> %s",
            prefix,
            snapshot.lines_match,
            back is not None
            and back.edge is not None
            and lay is not None
            and lay.edge is not None,
            comparable,
        )
    _log_formula(
        f"{prefix}.EXCHANGE_INTERNAL_GAP",
        "abs(BACK_EDGE - LAY_EDGE)",
        (
            "unavailable because matching complete BACK/LAY contracts are required"
            if internal_gap is None
            else f"abs({_fmt(back.edge)} - {_fmt(lay.edge)})"
        ),
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
        (
            "unavailable because matching complete BACK/LAY contracts are required"
            if representative_edge is None
            else f"({_fmt(back.edge)} + {_fmt(lay.edge)}) / 2"
        ),
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
        None
        if books_line is None or exchange_ou.line is None
        else books_line - exchange_ou.line
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
        relation(
            full_time.representative.direction, exchange_ou.representative.direction
        )
        if comparable
        else None
    )
    _log_formula(
        f"{prefix}.LINE_DIFF_RAW",
        "BOOK_OU_LINE - BETFAIR_OU_LINE",
        (
            "unavailable because one unique bookmaker line and the Betfair line are required"
            if line_diff_raw is None
            else f"{_fmt(books_line)} - {_fmt(exchange_ou.line)}"
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
        "abs(BOOK_OU_REP_EDGE - BETFAIR_OU_REP_EDGE)",
        (
            "unavailable because same-line representatives are required"
            if gap is None
            else f"abs({_fmt(full_time.representative.edge)} - {_fmt(exchange_ou.representative.edge)})"
        ),
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


def _interpret_ou_pair(left, right, representative, *, kind="books", evidence=None):
    left, right = left or {}, right or {}
    available = [
        reading for reading in (left, right) if reading.get("DIRECTION") is not None
    ]
    fields = {
        "left_line": left.get("LINE"),
        "right_line": right.get("LINE"),
        "left_direction": left.get("DIRECTION"),
        "right_direction": right.get("DIRECTION"),
    }
    if not available:
        return interpretation_reading(
            "MISSING_INPUT",
            status="BLOCKED",
            reason="MISSING_INPUT",
            comparability="UNAVAILABLE",
            evidence=evidence,
            **fields
        )
    if len(available) == 1:
        return interpretation_reading(
            "SINGLE_BOOK_READING" if kind == "books" else "SINGLE_EXCHANGE_READING",
            direction=available[0]["DIRECTION"],
            comparability="PARTIAL",
            evidence=evidence,
            **fields
        )
    if left.get("LINE") != right.get("LINE"):
        gap = (
            None
            if left.get("LINE") is None or right.get("LINE") is None
            else abs(left["LINE"] - right["LINE"])
        )
        return interpretation_reading(
            "DIFFERENT_LINE_STRUCTURE",
            status="BLOCKED",
            reason="INCOMPATIBLE_CONTRACT",
            comparability="INCOMPARABLE_LINE",
            line_gap=gap,
            evidence=evidence,
            **fields
        )
    if representative.get("EDGE") is None:
        return interpretation_reading(
            "INCOMPARABLE_CONTRACT",
            status="BLOCKED",
            reason="INCOMPATIBLE_CONTRACT",
            comparability="INCOMPARABLE_CONTRACT",
            evidence=evidence,
            **fields
        )
    a, b = left["DIRECTION"], right["DIRECTION"]
    if a == b == "NEUTRAL":
        state = "NEUTRAL_STRUCTURE"
    elif "NEUTRAL" in (a, b):
        state = "PARTIAL_DIRECTIONAL_AGREEMENT"
    elif a == b:
        state = "BOOK_CONSENSUS" if kind == "books" else "BACK_LAY_CONSENSUS"
    else:
        state = "BOOK_DISAGREEMENT" if kind == "books" else "BACK_LAY_DIVERGENCE"
    return interpretation_reading(
        state,
        direction=representative.get("DIRECTION"),
        line=left.get("LINE"),
        evidence=evidence,
        **fields
    )


def _canonical_ou_periods(scopes, analyses):
    """Index existing book readings once per period, retaining distinct contracts."""
    periods = {}
    for scope, profile in scopes.items():
        for period, values in profile["periods"].items():
            entries = periods.setdefault(period, {})
            books = values["bookmakers"]
            pin, bet = books["PINNACLE"], books["BET365"]
            if not any(r["status"] == "COMPUTED" for r in (pin, bet)):
                continue
            representative = values["representative"]
            identity = (
                pin["line"],
                pin["direction"],
                bet["line"],
                bet["direction"],
                representative["direction"],
                values["books"]["state"],
            )
            if identity in entries:
                entries[identity]["evidence"]["scope_refs"].append(scope)
                continue
            entries[identity] = {
                **values["books"],
                "pinnacle_line": pin["line"],
                "pinnacle_direction": pin["direction"],
                "bet365_line": bet["line"],
                "bet365_direction": bet["direction"],
                "books_direction": values["books"]["direction"],
                "representative_direction": representative["direction"],
                "evidence": {"scope_refs": [scope]},
            }
    readings = {}
    for period, indexed in periods.items():
        contracts = list(indexed.values())
        choices = {
            book: {
                (r[f"{book}_line"], r[f"{book}_direction"])
                for r in contracts
                if r[f"{book}_direction"] is not None
            }
            for book in ("pinnacle", "bet365")
        }
        refs = [ref for r in contracts for ref in r["evidence"]["scope_refs"]]
        if any(len(values) > 1 for values in choices.values()):
            reading = interpretation_reading(
                "AMBIGUOUS_CANDIDATE",
                status="BLOCKED",
                reason="AMBIGUOUS_CANDIDATE",
                comparability="PARTIAL",
                books_direction=None,
                representative_direction=None,
            )
        elif len(contracts) == 1:
            reading = dict(contracts[0])
        else:
            pin_line, pin_direction = next(iter(choices["pinnacle"]), (None, None))
            bet_line, bet_direction = next(iter(choices["bet365"]), (None, None))
            # A representative must already exist on the same contract in analysis.
            representative = next(
                (
                    analyses[r["evidence"]["scope_refs"][0]][period]["REPRESENTATIVE"]
                    for r in contracts
                    if r["pinnacle_line"] == pin_line
                    and r["bet365_line"] == bet_line
                    and r["representative_direction"] is not None
                ),
                {},
            )
            reading = _interpret_ou_pair(
                {"LINE": pin_line, "DIRECTION": pin_direction},
                {"LINE": bet_line, "DIRECTION": bet_direction},
                representative,
            )
            reading.update(
                pinnacle_line=pin_line,
                pinnacle_direction=pin_direction,
                bet365_line=bet_line,
                bet365_direction=bet_direction,
                books_direction=reading["direction"],
                representative_direction=representative.get("DIRECTION"),
            )
        reading["evidence"] = {"scope_refs": refs}
        if any(len(values) > 1 for values in choices.values()):
            reading["contract_readings"] = contracts
        readings[period] = reading
    return readings


def interpret_p3_result(result):
    """Retain the line structure, including lines split into analytical lanes."""
    scopes, structures = {}, {}
    for scope, profile in result["analysis"].items():
        periods = {}
        for period in ("FT", "1H"):
            values = profile.get(period)
            if not values:
                continue
            pin, b365 = values["PINNACLE"], values["BET365"]
            representative = values["REPRESENTATIVE"]
            books = _interpret_ou_pair(
                pin,
                b365,
                representative,
                evidence={"analysis_ref": f"analysis.{scope}.{period}"},
            )
            individual = {}
            for name, reading in (("PINNACLE", pin), ("BET365", b365)):
                valid = reading.get("DIRECTION") is not None
                individual[name] = interpretation_reading(
                    "BOOK_READING" if valid else "MISSING_INPUT",
                    direction=reading.get("DIRECTION"),
                    status="COMPUTED" if valid else "BLOCKED",
                    reason=None if valid else "MISSING_INPUT",
                    comparability="PARTIAL" if valid else "UNAVAILABLE",
                    line=reading.get("LINE"),
                    evidence={"analysis_ref": f"analysis.{scope}.{period}.{name}"},
                )
                if not valid:
                    reason = interpretation_failure_reason(
                        result, scope, f"{period}.{name}"
                    )
                    state = (
                        "INCOMPARABLE_CONTRACT"
                        if reason == "INCOMPATIBLE_CONTRACT"
                        else reason
                    )
                    individual[name] = interpretation_reading(
                        state,
                        reason=reason,
                        status="ERROR" if reason == "CALCULATION_ERROR" else "BLOCKED",
                        comparability="UNAVAILABLE",
                        line=reading.get("LINE"),
                        evidence=individual[name]["evidence"],
                    )
                if valid:
                    structure = structures.setdefault(period, {})
                    structure.setdefault(
                        (name, reading.get("LINE")),
                        {
                            "bookmaker": name,
                            "line": reading.get("LINE"),
                            "direction": reading["DIRECTION"],
                            "analysis_ref": f"analysis.{scope}.{period}.{name}",
                        },
                    )
            exchange_key = "BETFAIR_FT_OU" if period == "FT" else "BETFAIR_1H_OU"
            exchange_raw = profile.get(exchange_key) or {}
            back, lay = exchange_raw.get("BACK") or {}, exchange_raw.get("LAY") or {}
            line = exchange_raw.get("LINE")
            exchange = _interpret_ou_pair(
                {**back, "LINE": line},
                {**lay, "LINE": line},
                exchange_raw.get("REPRESENTATIVE") or {},
                kind="exchange",
                evidence={"analysis_ref": f"analysis.{scope}.{exchange_key}"},
            )
            if exchange["direction"] is not None:
                structures.setdefault(period, {}).setdefault(
                    ("BETFAIR", line),
                    {
                        "bookmaker": "BETFAIR",
                        "line": line,
                        "direction": exchange["direction"],
                        "analysis_ref": f"analysis.{scope}.{exchange_key}",
                    },
                )
            comparison_key = (
                "BOOK_EXCHANGE_OU" if period == "FT" else "BOOK_EXCHANGE_1H_OU"
            )
            comparison_raw = profile.get(comparison_key) or {}
            exchange_rep = exchange_raw.get("REPRESENTATIVE") or {}
            book_line = (
                pin.get("LINE") if representative.get("EDGE") is not None else None
            )
            exchange_direction = exchange_rep.get("DIRECTION")
            book_direction = representative.get("DIRECTION")
            fields = {
                "books_line": book_line,
                "books_direction": book_direction,
                "exchange_line": line,
                "exchange_direction": exchange_direction,
                "relation": comparison_raw.get("RELATION"),
                "line_gap": comparison_raw.get("LINE_GAP"),
            }
            if book_line is not None and line is not None and book_line != line:
                comparison = interpretation_reading(
                    "DIFFERENT_LINE_STRUCTURE",
                    status="BLOCKED",
                    reason="INCOMPATIBLE_CONTRACT",
                    comparability="INCOMPARABLE_LINE",
                    **fields,
                )
            elif book_direction is None or exchange_direction is None:
                comparison = interpretation_reading(
                    "PARTIAL_STRUCTURE",
                    status="BLOCKED",
                    reason="MISSING_INPUT",
                    comparability="PARTIAL",
                    **fields,
                )
            elif comparison_raw.get("RELATION") is None:
                comparison = interpretation_reading(
                    "INCOMPARABLE_CONTRACT",
                    status="BLOCKED",
                    reason="INCOMPATIBLE_CONTRACT",
                    comparability="INCOMPARABLE_CONTRACT",
                    **fields,
                )
            else:
                a, b = book_direction, exchange_direction
                state = (
                    "NEUTRAL_STRUCTURE"
                    if a == b == "NEUTRAL"
                    else (
                        "PARTIAL_DIRECTIONAL_AGREEMENT"
                        if "NEUTRAL" in (a, b)
                        else (
                            "CROSS_SOURCE_CONSENSUS"
                            if a == b
                            else "CROSS_SOURCE_CONFLICT"
                        )
                    )
                )
                comparison = interpretation_reading(
                    state, direction=a if a == b else None, **fields
                )
            comparison["evidence"] = {
                "analysis_ref": f"analysis.{scope}.{comparison_key}"
            }
            periods[period] = {
                "bookmakers": individual,
                "books": books,
                "exchange": exchange,
                "representative": interpretation_reading(
                    (
                        "REPRESENTATIVE_AVAILABLE"
                        if representative.get("EDGE") is not None
                        else "MISSING_INPUT"
                    ),
                    direction=book_direction,
                    status=(
                        "COMPUTED"
                        if representative.get("EDGE") is not None
                        else "BLOCKED"
                    ),
                    reason=(
                        None
                        if representative.get("EDGE") is not None
                        else "MISSING_INPUT"
                    ),
                    line=book_line,
                    evidence={
                        "analysis_ref": f"analysis.{scope}.{period}.REPRESENTATIVE"
                    },
                ),
                "book_exchange": comparison,
            }
        cross = profile.get("FT_1H") or {}
        ft_direction = ((profile.get("FT") or {}).get("REPRESENTATIVE") or {}).get(
            "DIRECTION"
        )
        first_half_direction = (
            (profile.get("1H") or {}).get("REPRESENTATIVE") or {}
        ).get("DIRECTION")
        cross_state = (
            "PARTIAL_STRUCTURE"
            if not cross
            else (
                "CROSS_PERIOD_CONSENSUS"
                if ft_direction == first_half_direction
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
                first_half_direction=first_half_direction,
                relation=cross.get("FT_1H_OU_RELATION"),
                evidence={"analysis_ref": f"analysis.{scope}.FT_1H"},
            ),
        }
    line_structures = {}
    for period, readings in structures.items():
        values = list(readings.values())
        lines = {reading["line"] for reading in values}
        if len(lines) <= 1:
            continue
        line_structures[period] = interpretation_reading(
            "DIFFERENT_LINE_STRUCTURE",
            comparability="INCOMPARABLE_LINE",
            readings=values,
            representative_direction=None,
        )
    return {
        "version": INTERPRETATION_VERSION,
        "status": result["status"],
        "period_readings": _canonical_ou_periods(scopes, result["analysis"]),
        "scopes": scopes,
        "line_structures": line_structures,
        "reasons": result["diagnostics"]["signals"]["reasons"],
        "summary": "Lectura O/U por periodo y línea; no hay una dirección global agregada.",
    }


def build_p3_executive_summary(result, interpretation=None):
    """Expose each canonical O/U period reading without merging unlike lines."""
    interpretation = interpretation or result.get("INTERPRETATION") or interpret_p3_result(result)
    canonical_periods = interpretation.get("period_readings") or {}
    period_names = {"FT": "full_time", "1H": "first_half"}
    readings = {}
    sentences = []
    for period, name in period_names.items():
        source = canonical_periods.get(period)
        available_periods = [
            scope["periods"][period]
            for scope in interpretation.get("scopes", {}).values()
            if period in scope.get("periods", {})
        ]
        if source is None and not available_periods:
            continue
        source = source or {}
        direction = source.get("representative_direction")
        if direction is None and source.get("status") == "COMPUTED":
            direction = source.get("direction")
        line = source.get("line")
        if line is None and source.get("status") == "COMPUTED":
            available_lines = {source.get(key) for key in ("pinnacle_line", "bet365_line")} - {None}
            if len(available_lines) == 1:
                line = available_lines.pop()
        reading = {
            "line": line,
            "direction": direction,
            "state": source.get("state"),
            "status": source.get("status"),
            "pinnacle_line": source.get("pinnacle_line"),
            "pinnacle_direction": source.get("pinnacle_direction"),
            "bet365_line": source.get("bet365_line"),
            "bet365_direction": source.get("bet365_direction"),
            "source_readings": project_executive_sources(
                (role, "Over/Under", available[role])
                for available in available_periods
                for role in ("books", "exchange", "book_exchange")
            ),
        }
        readings[name] = reading
        line = reading["line"]
        if line is None and reading["pinnacle_line"] == reading["bet365_line"]:
            line = reading["pinnacle_line"]
        label = f"{period} {line}" if line is not None else period
        if reading["state"] == "DIFFERENT_LINE_STRUCTURE":
            sentences.append(
                f"En {period}, las líneas {reading['pinnacle_line']} y "
                f"{reading['bet365_line']} son distintas y no admiten una dirección conjunta."
            )
        elif direction is not None:
            sentences.append(f"{label} presenta orientación {direction}.")
        elif reading["state"] == "AMBIGUOUS_CANDIDATE":
            sentences.append(f"{period} conserva candidatos O/U ambiguos.")
        sentences.extend(executive_source_sentences(period, reading["source_readings"]))

    return {
        "version": EXECUTIVE_SUMMARY_VERSION,
        "status": result.get("status"),
        **readings,
        "summary": " ".join(sentences)
        or "P3 no dispone de una lectura ejecutiva calculada para estos periodos.",
    }


__all__ = [
    "ENGINE_VERSION",
    "build_p3_executive_summary",
    "build_p3_signal_profile",
    "interpret_p3_result",
]
