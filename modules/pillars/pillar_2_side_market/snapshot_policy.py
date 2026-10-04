"""P2-specific assembly and per-period completeness policy over the shared extractor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from modules.pillars.market_snapshot_extractor import (
    ChoiceRequest,
    MarketCandidate,
    MarketSnapshotExtraction,
    MarketSnapshotRequest,
    extract_market_snapshot,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.market_candidate_selection import select_market_candidate
from modules.pillars.trajectory_selection import TargetMinuteSelection

from .models import (
    ExchangeSnapshot,
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


from modules.pillars.market_evaluation import PINNACLE, BET365, BETFAIR

PINNACLE_BOOKIE_ID = PINNACLE.id
BET365_BOOKIE_ID = BET365.id
BETFAIR_EXCHANGE_BOOKIE_ID = BETFAIR.id


@dataclass
class _InputDiagnostics:
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
    target_minute: int,
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
                line_trace=candidate.contract_trace(target_minute),
            )
        return PartialAsianHandicapSnapshot(
            home=home,
            away=away,
            home_line=candidate.line,
            line_trace=candidate.contract_trace(target_minute),
        )
    return PartialTwoWayMarketSnapshot(home=home, away=away)


def _select_partial_candidate(
    extraction: MarketSnapshotExtraction, request: MarketSnapshotRequest
) -> _PartialSelection:
    selection = select_market_candidate(extraction, request)
    return _PartialSelection(
        snapshot=(
            None
            if selection.candidate is None
            else _partial_snapshot(
                selection.candidate, request, extraction.target_minute
            )
        ),
        missing=selection.missing,
        invalid=selection.invalid,
        ambiguous=selection.ambiguous,
    )


def _extract_partial_two_way(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    request: MarketSnapshotRequest,
    gate: _InputDiagnostics,
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


def _extract_exchange_price_pair(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    request: MarketSnapshotRequest,
    gate: _InputDiagnostics,
) -> ThreeWayMarketSnapshot | TwoWayMarketSnapshot | None:
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    selection = select_market_candidate(extraction, request, allow_partial=True)
    gate.missing.update(selection.missing)
    gate.invalid.update(selection.invalid)
    gate.ambiguous.update(selection.ambiguous)
    candidate = selection.candidate
    if candidate is None:
        return None
    home = candidate.choices["1"]
    away = candidate.choices["2"]
    if home is None or away is None:
        return None
    if "x" in candidate.choices and candidate.choices["x"] is not None:
        draw = candidate.choices["x"]
        return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)
    return TwoWayMarketSnapshot(home=home, away=away)


def _extract_side_books(
    context: OddsTrajectoryContext,
    target_minute: int,
    scope: SidePeriodScope,
) -> tuple[dict[str, Any], dict[str, PeriodDiagnostics]]:
    """Assemble independent families; diagnostics never gate another family."""
    branches = {}
    bookies = {}
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
            gate = _InputDiagnostics()
            branch = (
                None
                if spec is None
                else _extract_partial_two_way(
                    context,
                    target_minute=target_minute,
                    request=_two_way_request(spec, bookie_id=bookie_id),
                    gate=gate,
                )
            )
            branches[f"{name}_{family}"] = branch
            gates[family] = gate
        gate = _InputDiagnostics()
        for family_gate in gates.values():
            gate.missing.update(family_gate.missing)
            gate.invalid.update(family_gate.invalid)
            gate.ambiguous.update(family_gate.ambiguous)
        bookies[name] = gate.diagnostics(
            complete=any(
                branches[f"{name}_{family}"] is not None
                and branches[f"{name}_{family}"].is_complete()
                for family in gates
            )
        )
    return branches, bookies


def _extract_full_time(
    context: OddsTrajectoryContext,
    target_minute: int,
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None,
    betfair_handicap: PartialAsianHandicapExchangeSnapshot | None = None,
) -> tuple[P2FullTimeSnapshot | None, PeriodDiagnostics]:
    branches, bookies = _extract_side_books(
        context, target_minute, FULL_TIME_SIDE_SCOPE
    )
    gate = _InputDiagnostics()
    exchange_branches = {}
    for side in ("back", "lay"):
        side_gate = _InputDiagnostics()
        branch = _extract_exchange_price_pair(
            context,
            target_minute=target_minute,
            request=_exchange_request(side),
            gate=side_gate,
        )
        if branch is None and not side_gate.ambiguous:
            # Request Home/Away independently; each reading retains its canonical
            # family in the quote traces and in dimensional coverage.
            alt_gate = _InputDiagnostics()
            alternative = _extract_exchange_price_pair(
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
        betfair_1x2=(
            ExchangeSnapshot(back=back, lay=lay)
            if back is not None or lay is not None
            else None
        ),
        betfair_ah=betfair_ah,
        betfair_handicap=betfair_handicap,
    )
    return snapshot, diagnostics


def _extract_first_half(
    context: OddsTrajectoryContext,
    target_minute: int,
) -> tuple[P2FirstHalfSnapshot | None, PeriodDiagnostics]:
    branches, bookies = _extract_side_books(
        context, target_minute, FIRST_HALF_SIDE_SCOPE
    )
    snapshot = P2FirstHalfSnapshot(
        **branches,
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
    back_gate = _InputDiagnostics()
    lay_gate = _InputDiagnostics()
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
    gate = _InputDiagnostics(
        missing=back_gate.missing | lay_gate.missing,
        invalid=back_gate.invalid | lay_gate.invalid,
        ambiguous=back_gate.ambiguous | lay_gate.ambiguous,
    )
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
