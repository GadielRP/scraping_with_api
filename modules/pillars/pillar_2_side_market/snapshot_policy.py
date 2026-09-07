"""P2-specific assembly and per-period completeness policy over the shared extractor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from modules.pillars.market_snapshot_extractor import (
    ChoiceRequest,
    MarketCandidate,
    MarketSnapshotExtraction,
    MarketSnapshotRequest,
    TargetMinuteSelection,
    extract_market_snapshot,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.market_candidate_selection import select_market_candidate

from .models import (
    AsianHandicapSnapshot,
    side_bookie_complete,
    ExchangeSnapshot,
    HandicapSnapshot,
    P2ExtractionResult,
    P2FirstHalfSnapshot,
    P2FullTimeSnapshot,
    PartialAsianHandicapSnapshot,
    PartialAsianHandicapExchangeSnapshot,
    PartialHandicapSnapshot,
    PartialTwoWayMarketSnapshot,
    PeriodDiagnostics,
    ThreeWayMarketSnapshot,
    TwoWayMarketSnapshot,
)
from .periods import (
    EXCHANGE_AH_1H_LINE_INPUT_NAME,
    EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    EXCHANGE_HANDICAP_1H_LINE_INPUT_NAME,
    EXCHANGE_HANDICAP_1H_ODDS_INPUT_NAMES,
    EXCHANGE_HANDICAP_LINE_INPUT_NAME,
    EXCHANGE_HANDICAP_ODDS_INPUT_NAMES,
    FIRST_HALF_SIDE_SCOPE,
    FULL_TIME_SIDE_SCOPE,
    EXCHANGE_AH_LINE_INPUT_NAME,
    EXCHANGE_AH_ODDS_INPUT_NAMES,
    TwoWayMarketSpec,
    SidePeriodScope,
)


PINNACLE_BOOKIE_ID = 302
BET365_BOOKIE_ID = 3
BETFAIR_EXCHANGE_BOOKIE_ID = 4


@dataclass
class _PeriodGate:
    missing: set[str] = field(default_factory=set)
    invalid: set[str] = field(default_factory=set)
    ambiguous: set[str] = field(default_factory=set)

    def diagnostics(self, *, complete: bool) -> PeriodDiagnostics:
        return PeriodDiagnostics.from_gate(
            complete=complete,
            missing_inputs=self.missing,
            invalid_inputs=self.invalid,
            ambiguous_inputs=self.ambiguous,
        )


@dataclass(frozen=True, slots=True)
class _PartialSelection:
    snapshot: PartialTwoWayMarketSnapshot | PartialAsianHandicapSnapshot | None
    missing: frozenset[str]
    invalid: frozenset[str]
    ambiguous: frozenset[str]


def _two_way_request(
    spec: TwoWayMarketSpec,
    *,
    bookie_id: int,
) -> MarketSnapshotRequest:
    if bookie_id == PINNACLE_BOOKIE_ID:
        home_input, away_input, line_input = (
            spec.pinnacle_home,
            spec.pinnacle_away,
            spec.pinnacle_line,
        )
    else:
        home_input, away_input, line_input = (
            spec.bet365_home,
            spec.bet365_away,
            spec.bet365_line,
        )
    return MarketSnapshotRequest(
        identities=spec.identities,
        bookie_id=bookie_id,
        line_input_name=line_input,
        choices=(
            ChoiceRequest("home", "1", home_input),
            ChoiceRequest("away", "2", away_input),
        ),
    )


def _exchange_request(
    exchange_side: str, *, is_2way: bool = False
) -> MarketSnapshotRequest:
    prefix = {
        "1": "BF_HOME",
        "x": "BF_DRAW",
        "2": "BF_AWAY",
    }
    choices = ("1", "2") if is_2way else ("1", "x", "2")
    return MarketSnapshotRequest(
        identities=tuple(
            identity
            for identity in FULL_TIME_SIDE_SCOPE.one_x_two.identities
            if not is_2way or identity.market_group == "Home/Away"
        ),
        bookie_id=BETFAIR_EXCHANGE_BOOKIE_ID,
        exchange_side=exchange_side,
        exchange_level=0,
        choices=tuple(
            ChoiceRequest(
                key=choice_name,
                choice_name=choice_name,
                input_name=f"{prefix[choice_name]}_{exchange_side.upper()}_1X2_FULL_TIME_ODDS_PRICE",
            )
            for choice_name in choices
        ),
    )


def _exchange_ah_request(
    exchange_side: str,
    *,
    scope=FULL_TIME_SIDE_SCOPE,
    line_name: str = EXCHANGE_AH_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_AH_ODDS_INPUT_NAMES,
    spec: TwoWayMarketSpec | None = None,
) -> MarketSnapshotRequest:
    names = {
        "1": odds_names[0 if exchange_side == "back" else 2],
        "2": odds_names[1 if exchange_side == "back" else 3],
    }
    identities = (
        spec.identities if spec is not None else scope.asian_handicap.identities
    )
    return MarketSnapshotRequest(
        identities=identities,
        bookie_id=BETFAIR_EXCHANGE_BOOKIE_ID,
        line_input_name=line_name,
        exchange_side=exchange_side,
        exchange_level=0,
        choices=tuple(
            ChoiceRequest(
                key=choice_name, choice_name=choice_name, input_name=input_name
            )
            for choice_name, input_name in names.items()
        ),
    )


def _partial_snapshot(
    candidate: MarketCandidate,
    request: MarketSnapshotRequest,
) -> PartialTwoWayMarketSnapshot | PartialAsianHandicapSnapshot:
    # Book requests use semantic ``home``/``away`` keys, while exchange
    # requests use the source choice labels ``1``/``2``.  Both map to the
    # canonical home/away DTO fields.
    home = candidate.choices.get("home") or candidate.choices.get("1")
    away = candidate.choices.get("away") or candidate.choices.get("2")
    if request.line_input_name is not None:
        group_norm = candidate.market_line.market_group.lower()
        if "handicap" in group_norm and "asian" not in group_norm:
            return PartialHandicapSnapshot(
                home=home,
                away=away,
                home_line=candidate.line,
            )
        return PartialAsianHandicapSnapshot(
            home=home,
            away=away,
            home_line=candidate.line,
        )
    return PartialTwoWayMarketSnapshot(home=home, away=away)


def _select_partial_candidate(
    extraction: MarketSnapshotExtraction, request: MarketSnapshotRequest
) -> _PartialSelection:
    selection = select_market_candidate(extraction, request)
    return _PartialSelection(
        snapshot=None
        if selection.candidate is None
        else _partial_snapshot(selection.candidate, request),
        missing=selection.missing,
        invalid=selection.invalid,
        ambiguous=selection.ambiguous,
    )


def _extract_partial_two_way(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    request: MarketSnapshotRequest,
    gate: _PeriodGate,
) -> PartialTwoWayMarketSnapshot | PartialAsianHandicapSnapshot | None:
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    selection = _select_partial_candidate(extraction, request)
    gate.missing.update(selection.missing)
    gate.invalid.update(selection.invalid)
    gate.ambiguous.update(selection.ambiguous)
    return selection.snapshot


def _extract_required_two_way(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    request: MarketSnapshotRequest,
    gate: _PeriodGate,
) -> TwoWayMarketSnapshot | AsianHandicapSnapshot | None:
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    selection = select_market_candidate(extraction, request, allow_partial=False)
    gate.missing.update(selection.missing)
    gate.invalid.update(selection.invalid)
    gate.ambiguous.update(selection.ambiguous)
    candidate = selection.candidate
    if candidate is None:
        return None
    home = candidate.choices["home"]
    away = candidate.choices["away"]
    assert home is not None and away is not None
    if request.line_input_name:
        assert candidate.line is not None
        group_norm = candidate.market_line.market_group.lower()
        if "handicap" in group_norm and "asian" not in group_norm:
            return HandicapSnapshot(
                home=home,
                away=away,
                home_line=candidate.line,
            )
        return AsianHandicapSnapshot(
            home=home,
            away=away,
            home_line=candidate.line,
        )
    return TwoWayMarketSnapshot(home=home, away=away)


def _extract_required_exchange(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    request: MarketSnapshotRequest,
    gate: _PeriodGate,
) -> ThreeWayMarketSnapshot | TwoWayMarketSnapshot | None:
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    selection = select_market_candidate(extraction, request, allow_partial=False)
    gate.missing.update(selection.missing)
    gate.invalid.update(selection.invalid)
    gate.ambiguous.update(selection.ambiguous)
    candidate = selection.candidate
    if candidate is None:
        return None
    home = candidate.choices["1"]
    away = candidate.choices["2"]
    assert home is not None and away is not None
    if "x" in candidate.choices and candidate.choices["x"] is not None:
        draw = candidate.choices["x"]
        return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)
    return TwoWayMarketSnapshot(home=home, away=away)


def _extract_side_books(
    context: OddsTrajectoryContext,
    target_minute: int,
    scope: SidePeriodScope,
    *,
    partial: bool = False,
) -> tuple[dict[str, Any], dict[str, PeriodDiagnostics]]:
    """Apply 1X2 AND (AH OR Handicap) independently for each bookmaker."""
    branches = {}
    bookies = {}
    extract = _extract_partial_two_way if partial else _extract_required_two_way
    for name, bookie_id in (
        ("pinnacle", PINNACLE_BOOKIE_ID),
        ("bet365", BET365_BOOKIE_ID),
    ):
        gates = {}
        for family, spec in (
            ("1x2", scope.one_x_two),
            ("ah", scope.asian_handicap),
            ("handicap", scope.handicap),
        ):
            gate = _PeriodGate()
            branch = (
                None
                if spec is None
                else extract(
                    context,
                    target_minute=target_minute,
                    request=_two_way_request(spec, bookie_id=bookie_id),
                    gate=gate,
                )
            )
            branches[f"{name}_{family}"] = branch
            gates[family] = gate
        side = branches[f"{name}_1x2"]
        spreads = [branches[f"{name}_{family}"] for family in ("ah", "handicap")]
        gate = gates["1x2"]
        spread_complete = any(
            branch is not None and branch.is_complete() for branch in spreads
        )
        if not spread_complete:
            for family in ("ah", "handicap"):
                gate.missing.update(gates[family].missing)
                gate.invalid.update(gates[family].invalid)
                gate.ambiguous.update(gates[family].ambiguous)
        bookies[name] = gate.diagnostics(complete=side_bookie_complete(side, *spreads))
    return branches, bookies


def _spread_market_type(branches: dict[str, Any]) -> str:
    """Prefer a complete common family for comparisons, independently of coverage."""
    for family, market_type in (("ah", "asian_handicap"), ("handicap", "handicap")):
        if all(
            branches[f"{book}_{family}"] is not None
            and branches[f"{book}_{family}"].is_complete()
            for book in ("pinnacle", "bet365")
        ):
            return market_type
    return (
        "asian_handicap"
        if any(branches[f"{book}_ah"] is not None for book in ("pinnacle", "bet365"))
        else "handicap"
    )


def _extract_full_time(
    context: OddsTrajectoryContext,
    target_minute: int,
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None,
    betfair_handicap: PartialAsianHandicapExchangeSnapshot | None = None,
) -> tuple[P2FullTimeSnapshot | None, PeriodDiagnostics]:
    branches, bookies = _extract_side_books(
        context, target_minute, FULL_TIME_SIDE_SCOPE
    )
    gate = _PeriodGate()
    exchange_branches = {}
    for side in ("back", "lay"):
        side_gate = _PeriodGate()
        branch = _extract_required_exchange(
            context,
            target_minute=target_minute,
            request=_exchange_request(side),
            gate=side_gate,
        )
        if branch is None and not side_gate.ambiguous:
            # Only actual two-way contracts may omit Draw. Missing 1X2 Draw
            # must not be silently reinterpreted as a complete moneyline.
            alt_gate = _PeriodGate()
            alternative = _extract_required_exchange(
                context,
                target_minute=target_minute,
                request=_exchange_request(side, is_2way=True),
                gate=alt_gate,
            )
            if alternative is not None:
                branch, side_gate = alternative, alt_gate
        exchange_branches[side] = branch
        gate.missing.update(side_gate.missing)
        gate.invalid.update(side_gate.invalid)
        gate.ambiguous.update(side_gate.ambiguous)
    back, lay = exchange_branches["back"], exchange_branches["lay"]
    exchange_complete = back is not None and lay is not None
    if exchange_complete and (
        back.home.trace.market_period != lay.home.trace.market_period
        or back.home.trace.market_group != lay.home.trace.market_group
    ):
        exchange_complete = False
        gate.ambiguous.update(
            choice.input_name
            for side in ("back", "lay")
            for choice in _exchange_request(side).choices
        )
    bookies["betfair"] = gate.diagnostics(complete=exchange_complete)
    diagnostics = PeriodDiagnostics.from_bookies(bookies)
    snapshot = P2FullTimeSnapshot(
        **branches,
        betfair_1x2=ExchangeSnapshot(back=back, lay=lay) if exchange_complete else None,
        betfair_ah=betfair_ah,
        betfair_handicap=betfair_handicap,
        spread_market_type=_spread_market_type(branches),
    )
    return snapshot, diagnostics


def _extract_first_half(
    context: OddsTrajectoryContext,
    target_minute: int,
) -> tuple[P2FirstHalfSnapshot | None, PeriodDiagnostics]:
    branches, bookies = _extract_side_books(
        context, target_minute, FIRST_HALF_SIDE_SCOPE, partial=True
    )
    snapshot = P2FirstHalfSnapshot(
        **branches,
        spread_market_type=_spread_market_type(branches),
    )
    return (
        snapshot if snapshot.has_any_input() else None
    ), PeriodDiagnostics.from_bookies(bookies)


def _extract_optional_exchange_spread(
    context: OddsTrajectoryContext,
    target_minute: int,
    *,
    scope=FULL_TIME_SIDE_SCOPE,
    spec: TwoWayMarketSpec,
    line_name: str = EXCHANGE_AH_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_AH_ODDS_INPUT_NAMES,
) -> tuple[PartialAsianHandicapExchangeSnapshot | None, PeriodDiagnostics]:
    """Extract one optional Betfair spread family without relabelling it."""
    back_gate = _PeriodGate()
    lay_gate = _PeriodGate()
    back = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_exchange_ah_request(
            "back", scope=scope, line_name=line_name, odds_names=odds_names, spec=spec
        ),
        gate=back_gate,
    )
    lay = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_exchange_ah_request(
            "lay", scope=scope, line_name=line_name, odds_names=odds_names, spec=spec
        ),
        gate=lay_gate,
    )
    gate = _PeriodGate(
        missing=back_gate.missing | lay_gate.missing,
        invalid=back_gate.invalid | lay_gate.invalid,
        ambiguous=back_gate.ambiguous | lay_gate.ambiguous,
    )
    if gate.ambiguous:
        return None, gate.diagnostics(complete=False)
    assert back is None or isinstance(back, PartialAsianHandicapSnapshot)
    assert lay is None or isinstance(lay, PartialAsianHandicapSnapshot)
    snapshot = PartialAsianHandicapExchangeSnapshot(back=back, lay=lay)
    if not snapshot.has_any_input():
        return None, gate.diagnostics(complete=False)
    complete = (
        back is not None
        and lay is not None
        and back.is_complete()
        and lay.is_complete()
        and snapshot.lines_match
        and not gate.missing
        and not gate.invalid
    )
    return snapshot, gate.diagnostics(complete=complete)


def _empty_period_diagnostics() -> PeriodDiagnostics:
    return PeriodDiagnostics.empty()


def extract_p2_market_snapshot(
    event_id: int,
    context: OddsTrajectoryContext | None,
    target_selection: TargetMinuteSelection,
) -> P2ExtractionResult:
    """Extract every registered P2 period independently over the shared extractor."""
    if target_selection.target_minute is None or context is None:
        return P2ExtractionResult(
            target_minute=None,
            full_time=_empty_period_diagnostics(),
            first_half=_empty_period_diagnostics(),
            exchange_ah=_empty_period_diagnostics(),
            exchange_ah_1h=_empty_period_diagnostics(),
            abort_reason=target_selection.reason,
            extraction_diagnostics={"target_selection": target_selection.diagnostics},
        )

    target_minute = target_selection.target_minute
    exchange_ah, exchange_ah_diagnostics = _extract_optional_exchange_spread(
        context,
        target_minute,
        spec=FULL_TIME_SIDE_SCOPE.asian_handicap,
    )
    exchange_handicap, exchange_handicap_diagnostics = (
        _extract_optional_exchange_spread(
            context,
            target_minute,
            spec=FULL_TIME_SIDE_SCOPE.handicap,
            line_name=EXCHANGE_HANDICAP_LINE_INPUT_NAME,
            odds_names=EXCHANGE_HANDICAP_ODDS_INPUT_NAMES,
        )
    )
    exchange_ah_1h, exchange_ah_1h_diagnostics = _extract_optional_exchange_spread(
        context,
        target_minute,
        spec=FIRST_HALF_SIDE_SCOPE.asian_handicap,
        scope=FIRST_HALF_SIDE_SCOPE,
        line_name=EXCHANGE_AH_1H_LINE_INPUT_NAME,
        odds_names=EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    )
    exchange_handicap_1h, exchange_handicap_1h_diagnostics = (
        _extract_optional_exchange_spread(
            context,
            target_minute,
            spec=FIRST_HALF_SIDE_SCOPE.handicap,
            scope=FIRST_HALF_SIDE_SCOPE,
            line_name=EXCHANGE_HANDICAP_1H_LINE_INPUT_NAME,
            odds_names=EXCHANGE_HANDICAP_1H_ODDS_INPUT_NAMES,
        )
    )
    full_time, full_time_diagnostics = _extract_full_time(
        context,
        target_minute,
        betfair_ah=exchange_ah,
        betfair_handicap=exchange_handicap,
    )
    first_half, first_half_diagnostics = _extract_first_half(context, target_minute)
    if exchange_ah_1h is not None or exchange_handicap_1h is not None:
        if first_half is None:
            first_half = P2FirstHalfSnapshot(
                None, None, None, None, None, None, exchange_ah_1h, exchange_handicap_1h
            )
        else:
            first_half = P2FirstHalfSnapshot(
                first_half.pinnacle_1x2,
                first_half.bet365_1x2,
                first_half.pinnacle_ah,
                first_half.bet365_ah,
                first_half.pinnacle_handicap,
                first_half.bet365_handicap,
                exchange_ah_1h,
                exchange_handicap_1h,
                first_half.spread_market_type,
            )

    if full_time is None or not full_time_diagnostics.usable:
        return P2ExtractionResult(
            target_minute=target_minute,
            full_time=full_time_diagnostics,
            first_half=first_half_diagnostics,
            exchange_ah=exchange_ah_diagnostics,
            exchange_ah_1h=exchange_ah_1h_diagnostics,
            exchange_handicap=exchange_handicap_diagnostics,
            exchange_handicap_1h=exchange_handicap_1h_diagnostics,
            full_time_snapshot=full_time,
            first_half_snapshot=first_half,
            exchange_ah_snapshot=exchange_ah,
            exchange_handicap_snapshot=exchange_handicap,
            abort_reason="full_time_completeness_gate_failed",
            extraction_diagnostics={"target_selection": target_selection.diagnostics},
        )

    return P2ExtractionResult(
        target_minute=target_minute,
        full_time=full_time_diagnostics,
        first_half=first_half_diagnostics,
        exchange_ah=exchange_ah_diagnostics,
        exchange_ah_1h=exchange_ah_1h_diagnostics,
        exchange_handicap=exchange_handicap_diagnostics,
        exchange_handicap_1h=exchange_handicap_1h_diagnostics,
        full_time_snapshot=full_time,
        first_half_snapshot=first_half,
        exchange_ah_snapshot=exchange_ah,
        exchange_handicap_snapshot=exchange_handicap,
        extraction_diagnostics={"target_selection": target_selection.diagnostics},
    )


__all__ = ["extract_p2_market_snapshot"]
