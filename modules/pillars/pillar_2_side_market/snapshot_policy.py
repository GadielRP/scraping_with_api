"""P2-specific assembly and per-period completeness policy over the shared extractor."""

from __future__ import annotations

from dataclasses import dataclass, field

from modules.pillars.market_snapshot_extractor import (
    ChoiceRequest,
    MarketCandidate,
    MarketSnapshotExtraction,
    MarketSnapshotRequest,
    TargetMinuteSelection,
    extract_market_snapshot,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .models import (
    AsianHandicapSnapshot,
    ExchangeSnapshot,
    P2ExtractionResult,
    P2FirstHalfSnapshot,
    P2FullTimeSnapshot,
    PartialAsianHandicapSnapshot,
    PartialAsianHandicapExchangeSnapshot,
    PartialTwoWayMarketSnapshot,
    PeriodDiagnostics,
    ThreeWayMarketSnapshot,
    TwoWayMarketSnapshot,
)
from .periods import (
    EXCHANGE_AH_1H_LINE_INPUT_NAME,
    EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    FIRST_HALF_SIDE_SCOPE,
    FULL_TIME_SIDE_SCOPE,
    EXCHANGE_AH_LINE_INPUT_NAME,
    EXCHANGE_AH_ODDS_INPUT_NAMES,
    TwoWayMarketSpec,
)


PINNACLE_BOOKIE_ID = 302
BET365_BOOKIE_ID = 3
BETFAIR_EXCHANGE_BOOKIE_ID = 4


@dataclass
class _PeriodGate:
    missing: set[str] = field(default_factory=set)
    invalid: set[str] = field(default_factory=set)
    ambiguous: set[str] = field(default_factory=set)

    def merge(self, extraction: MarketSnapshotExtraction) -> None:
        self.missing.update(extraction.missing_inputs)
        self.invalid.update(extraction.invalid_inputs)
        self.ambiguous.update(extraction.ambiguous_inputs)

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


def _exchange_request(exchange_side: str) -> MarketSnapshotRequest:
    prefix = {
        "1": "BF_HOME",
        "x": "BF_DRAW",
        "2": "BF_AWAY",
    }
    return MarketSnapshotRequest(
        identities=FULL_TIME_SIDE_SCOPE.one_x_two.identities,
        bookie_id=BETFAIR_EXCHANGE_BOOKIE_ID,
        exchange_side=exchange_side,
        exchange_level=0,
        choices=tuple(
            ChoiceRequest(
                key=choice_name,
                choice_name=choice_name,
                input_name=f"{prefix[choice_name]}_{exchange_side.upper()}_1X2_FULL_TIME_ODDS_PRICE",
            )
            for choice_name in ("1", "x", "2")
        ),
    )


def _exchange_ah_request(
    exchange_side: str,
    *,
    scope=FULL_TIME_SIDE_SCOPE,
    line_name: str = EXCHANGE_AH_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_AH_ODDS_INPUT_NAMES,
) -> MarketSnapshotRequest:
    names = {
        "1": odds_names[0 if exchange_side == "back" else 2],
        "2": odds_names[1 if exchange_side == "back" else 3],
    }
    return MarketSnapshotRequest(
        identities=scope.asian_handicap.identities,
        bookie_id=BETFAIR_EXCHANGE_BOOKIE_ID,
        line_input_name=line_name,
        exchange_side=exchange_side,
        exchange_level=0,
        choices=tuple(
            ChoiceRequest(key=choice_name, choice_name=choice_name, input_name=input_name)
            for choice_name, input_name in names.items()
        ),
    )


def _unique_required_candidate(
    extraction: MarketSnapshotExtraction,
    request: MarketSnapshotRequest,
    gate: _PeriodGate,
) -> MarketCandidate | None:
    complete = [
        candidate
        for candidate in extraction.candidates
        if candidate.is_complete(request)
    ]
    if len(complete) > 1:
        if request.line_input_name:
            gate.ambiguous.add(request.line_input_name)
        for choice in request.choices:
            gate.ambiguous.add(choice.input_name)
            if choice.exchange_size_input_name:
                gate.ambiguous.add(choice.exchange_size_input_name)
        return None
    return complete[0] if complete else None


def _all_input_names(request: MarketSnapshotRequest) -> set[str]:
    names = {choice.input_name for choice in request.choices}
    if request.line_input_name is not None:
        names.add(request.line_input_name)
    return names


def _has_any_candidate_input(candidate: MarketCandidate) -> bool:
    return candidate.line is not None or any(
        point is not None for point in candidate.choices.values()
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
        return PartialAsianHandicapSnapshot(
            home=home,
            away=away,
            home_line=candidate.line,
        )
    return PartialTwoWayMarketSnapshot(home=home, away=away)


def _selected_input_diagnostics(
    candidate: MarketCandidate,
    request: MarketSnapshotRequest,
    extraction: MarketSnapshotExtraction,
) -> tuple[set[str], set[str]]:
    invalid = set(extraction.invalid_inputs) & _all_input_names(request)
    missing: set[str] = set()
    if request.line_input_name is not None and candidate.line is None:
        if request.line_input_name not in invalid:
            missing.add(request.line_input_name)
    for choice in request.choices:
        if candidate.choices.get(choice.key) is None and choice.input_name not in invalid:
            missing.add(choice.input_name)
    return missing, invalid


def _select_partial_candidate(
    extraction: MarketSnapshotExtraction,
    request: MarketSnapshotRequest,
) -> _PartialSelection:
    names = _all_input_names(request)
    extractor_ambiguities = set(extraction.ambiguous_inputs) & names
    if extraction.container_ambiguities or extractor_ambiguities:
        return _PartialSelection(
            snapshot=None,
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(names | extractor_ambiguities),
        )

    complete = [
        candidate for candidate in extraction.candidates if candidate.is_complete(request)
    ]
    if len(complete) > 1:
        return _PartialSelection(
            snapshot=None,
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(names),
        )
    if len(complete) == 1:
        return _PartialSelection(
            snapshot=_partial_snapshot(complete[0], request),
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(),
        )

    partial = [
        candidate
        for candidate in extraction.candidates
        if _has_any_candidate_input(candidate)
    ]
    if len(partial) > 1:
        return _PartialSelection(
            snapshot=None,
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(names),
        )
    if len(partial) == 1:
        missing, invalid = _selected_input_diagnostics(
            partial[0],
            request,
            extraction,
        )
        return _PartialSelection(
            snapshot=_partial_snapshot(partial[0], request),
            missing=frozenset(missing),
            invalid=frozenset(invalid),
            ambiguous=frozenset(),
        )

    return _PartialSelection(
        snapshot=None,
        missing=frozenset(names),
        invalid=frozenset(),
        ambiguous=frozenset(),
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
    gate.merge(extraction)
    candidate = _unique_required_candidate(extraction, request, gate)
    if candidate is None:
        return None
    home = candidate.choices["home"]
    away = candidate.choices["away"]
    assert home is not None and away is not None
    if request.line_input_name:
        assert candidate.line is not None
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
) -> ThreeWayMarketSnapshot | None:
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    gate.merge(extraction)
    candidate = _unique_required_candidate(extraction, request, gate)
    if candidate is None:
        return None
    home = candidate.choices["1"]
    draw = candidate.choices["x"]
    away = candidate.choices["2"]
    assert home is not None and draw is not None and away is not None
    return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)


def _required_full_time_is_complete(
    components: tuple[object, ...],
    gate: _PeriodGate,
) -> bool:
    return (
        all(item is not None for item in components)
        and not gate.missing
        and not gate.invalid
        and not gate.ambiguous
    )


def _extract_required_book_pair(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    spec: TwoWayMarketSpec,
    gate: _PeriodGate,
) -> tuple[
    TwoWayMarketSnapshot | AsianHandicapSnapshot | None,
    TwoWayMarketSnapshot | AsianHandicapSnapshot | None,
]:
    pinnacle = _extract_required_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(spec, bookie_id=PINNACLE_BOOKIE_ID),
        gate=gate,
    )
    bet365 = _extract_required_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(spec, bookie_id=BET365_BOOKIE_ID),
        gate=gate,
    )
    return pinnacle, bet365


def _extract_full_time(
    context: OddsTrajectoryContext,
    target_minute: int,
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None,
) -> tuple[P2FullTimeSnapshot | None, PeriodDiagnostics]:
    gate = _PeriodGate()
    pin_1x2, b365_1x2 = _extract_required_book_pair(
        context,
        target_minute=target_minute,
        spec=FULL_TIME_SIDE_SCOPE.one_x_two,
        gate=gate,
    )
    pin_ah, b365_ah = _extract_required_book_pair(
        context,
        target_minute=target_minute,
        spec=FULL_TIME_SIDE_SCOPE.asian_handicap,
        gate=gate,
    )
    bf_back = _extract_required_exchange(
        context,
        target_minute=target_minute,
        request=_exchange_request("back"),
        gate=gate,
    )
    bf_lay = _extract_required_exchange(
        context,
        target_minute=target_minute,
        request=_exchange_request("lay"),
        gate=gate,
    )
    components = (pin_1x2, b365_1x2, pin_ah, b365_ah, bf_back, bf_lay)
    if not _required_full_time_is_complete(components, gate):
        return None, gate.diagnostics(complete=False)

    assert isinstance(pin_1x2, TwoWayMarketSnapshot)
    assert isinstance(b365_1x2, TwoWayMarketSnapshot)
    assert isinstance(pin_ah, AsianHandicapSnapshot)
    assert isinstance(b365_ah, AsianHandicapSnapshot)
    assert bf_back is not None and bf_lay is not None
    snapshot = P2FullTimeSnapshot(
        pinnacle_1x2=pin_1x2,
        bet365_1x2=b365_1x2,
        pinnacle_ah=pin_ah,
        bet365_ah=b365_ah,
        betfair_1x2=ExchangeSnapshot(back=bf_back, lay=bf_lay),
        betfair_ah=betfair_ah,
    )
    return snapshot, gate.diagnostics(complete=True)


def _extract_first_half(
    context: OddsTrajectoryContext,
    target_minute: int,
) -> tuple[P2FirstHalfSnapshot | None, PeriodDiagnostics]:
    gate = _PeriodGate()
    pin_1x2 = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(
            FIRST_HALF_SIDE_SCOPE.one_x_two,
            bookie_id=PINNACLE_BOOKIE_ID,
        ),
        gate=gate,
    )
    b365_1x2 = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(
            FIRST_HALF_SIDE_SCOPE.one_x_two,
            bookie_id=BET365_BOOKIE_ID,
        ),
        gate=gate,
    )
    pin_ah = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(
            FIRST_HALF_SIDE_SCOPE.asian_handicap,
            bookie_id=PINNACLE_BOOKIE_ID,
        ),
        gate=gate,
    )
    b365_ah = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(
            FIRST_HALF_SIDE_SCOPE.asian_handicap,
            bookie_id=BET365_BOOKIE_ID,
        ),
        gate=gate,
    )
    if gate.ambiguous:
        return None, gate.diagnostics(complete=False)

    assert pin_1x2 is None or isinstance(pin_1x2, PartialTwoWayMarketSnapshot)
    assert b365_1x2 is None or isinstance(b365_1x2, PartialTwoWayMarketSnapshot)
    assert pin_ah is None or isinstance(pin_ah, PartialAsianHandicapSnapshot)
    assert b365_ah is None or isinstance(b365_ah, PartialAsianHandicapSnapshot)
    snapshot = P2FirstHalfSnapshot(
        pinnacle_1x2=pin_1x2,
        bet365_1x2=b365_1x2,
        pinnacle_ah=pin_ah,
        bet365_ah=b365_ah,
    )
    if not snapshot.has_any_input():
        return None, gate.diagnostics(complete=False)
    return snapshot, gate.diagnostics(complete=snapshot.is_complete())


def _extract_optional_exchange_ah(
    context: OddsTrajectoryContext,
    target_minute: int,
    *,
    scope=FULL_TIME_SIDE_SCOPE,
    line_name: str = EXCHANGE_AH_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_AH_ODDS_INPUT_NAMES,
) -> tuple[PartialAsianHandicapExchangeSnapshot | None, PeriodDiagnostics]:
    """Extract optional Betfair AH for the requested period."""
    back_gate = _PeriodGate()
    lay_gate = _PeriodGate()
    back = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_exchange_ah_request(
            "back", scope=scope, line_name=line_name, odds_names=odds_names
        ),
        gate=back_gate,
    )
    lay = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_exchange_ah_request(
            "lay", scope=scope, line_name=line_name, odds_names=odds_names
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
        )

    target_minute = target_selection.target_minute
    exchange_ah, exchange_ah_diagnostics = _extract_optional_exchange_ah(context, target_minute)
    exchange_ah_1h, exchange_ah_1h_diagnostics = _extract_optional_exchange_ah(
        context,
        target_minute,
        scope=FIRST_HALF_SIDE_SCOPE,
        line_name=EXCHANGE_AH_1H_LINE_INPUT_NAME,
        odds_names=EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    )
    full_time, full_time_diagnostics = _extract_full_time(
        context,
        target_minute,
        betfair_ah=exchange_ah,
    )
    first_half, first_half_diagnostics = _extract_first_half(context, target_minute)
    if exchange_ah_1h is not None:
        if first_half is None:
            first_half = P2FirstHalfSnapshot(None, None, None, None, exchange_ah_1h)
        else:
            first_half = P2FirstHalfSnapshot(
                first_half.pinnacle_1x2,
                first_half.bet365_1x2,
                first_half.pinnacle_ah,
                first_half.bet365_ah,
                exchange_ah_1h,
            )

    if full_time is None:
        return P2ExtractionResult(
            target_minute=target_minute,
            full_time=full_time_diagnostics,
            first_half=first_half_diagnostics,
            exchange_ah=exchange_ah_diagnostics,
            exchange_ah_1h=exchange_ah_1h_diagnostics,
            first_half_snapshot=first_half,  # type: ignore[arg-type]
            exchange_ah_snapshot=exchange_ah,
            abort_reason="full_time_completeness_gate_failed",
        )

    return P2ExtractionResult(
        target_minute=target_minute,
        full_time=full_time_diagnostics,
            first_half=first_half_diagnostics,
            exchange_ah=exchange_ah_diagnostics,
            exchange_ah_1h=exchange_ah_1h_diagnostics,
        full_time_snapshot=full_time,  # type: ignore[arg-type]
        first_half_snapshot=first_half,  # type: ignore[arg-type]
        exchange_ah_snapshot=exchange_ah,
    )


__all__ = ["extract_p2_market_snapshot"]
