"""P2-specific assembly and per-period completeness policy over the shared extractor."""

from __future__ import annotations

from dataclasses import dataclass, field

from infrastructure.settings import Config
from modules.pillars.market_snapshot_extractor import (
    ChoiceRequest,
    MarketCandidate,
    MarketSnapshotExtraction,
    MarketSnapshotRequest,
    extract_market_snapshot,
    select_target_minute,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .models import (
    AsianHandicapSnapshot,
    ExchangeSnapshot,
    P2ExtractionResult,
    P2FirstHalfSnapshot,
    P2FullTimeSnapshot,
    PeriodDiagnostics,
    ThreeWayMarketSnapshot,
    TwoWayMarketSnapshot,
)
from .periods import (
    FIRST_HALF_SIDE_SCOPE,
    FULL_TIME_SIDE_SCOPE,
    P2_SIDE_PERIOD_SCOPES,
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

    def diagnostics(self, snapshot: object | None) -> PeriodDiagnostics:
        return PeriodDiagnostics.from_gate(
            snapshot=snapshot,
            missing_inputs=self.missing,
            invalid_inputs=self.invalid,
            ambiguous_inputs=self.ambiguous,
        )


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
                input_name=f"{prefix[choice_name]}_{exchange_side.upper()}_FULL_TIME_ODDS_PRICE",
                exchange_size_input_name=(
                    f"{prefix[choice_name]}_{exchange_side.upper()}_FULL_TIME_EXCHANGE_SIZE"
                ),
            )
            for choice_name in ("1", "x", "2")
        ),
    )


def _unique_complete_candidate(
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


def _extract_two_way(
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
    candidate = _unique_complete_candidate(extraction, request, gate)
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


def _extract_exchange(
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
    candidate = _unique_complete_candidate(extraction, request, gate)
    if candidate is None:
        return None
    home = candidate.choices["1"]
    draw = candidate.choices["x"]
    away = candidate.choices["2"]
    assert home is not None and draw is not None and away is not None
    return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)


def _is_complete(components: tuple[object, ...], gate: _PeriodGate) -> bool:
    return (
        all(item is not None for item in components)
        and not gate.missing
        and not gate.invalid
        and not gate.ambiguous
    )


def _extract_book_pair(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    spec: TwoWayMarketSpec,
    gate: _PeriodGate,
) -> tuple[
    TwoWayMarketSnapshot | AsianHandicapSnapshot | None,
    TwoWayMarketSnapshot | AsianHandicapSnapshot | None,
]:
    pinnacle = _extract_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(spec, bookie_id=PINNACLE_BOOKIE_ID),
        gate=gate,
    )
    bet365 = _extract_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(spec, bookie_id=BET365_BOOKIE_ID),
        gate=gate,
    )
    return pinnacle, bet365


def _extract_full_time(
    context: OddsTrajectoryContext,
    target_minute: int,
) -> tuple[P2FullTimeSnapshot | None, PeriodDiagnostics]:
    gate = _PeriodGate()
    pin_1x2, b365_1x2 = _extract_book_pair(
        context,
        target_minute=target_minute,
        spec=FULL_TIME_SIDE_SCOPE.one_x_two,
        gate=gate,
    )
    pin_ah, b365_ah = _extract_book_pair(
        context,
        target_minute=target_minute,
        spec=FULL_TIME_SIDE_SCOPE.asian_handicap,
        gate=gate,
    )
    bf_back = _extract_exchange(
        context,
        target_minute=target_minute,
        request=_exchange_request("back"),
        gate=gate,
    )
    bf_lay = _extract_exchange(
        context,
        target_minute=target_minute,
        request=_exchange_request("lay"),
        gate=gate,
    )
    components = (pin_1x2, b365_1x2, pin_ah, b365_ah, bf_back, bf_lay)
    if not _is_complete(components, gate):
        return None, gate.diagnostics(None)

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
    )
    return snapshot, gate.diagnostics(snapshot)


def _extract_first_half(
    context: OddsTrajectoryContext,
    target_minute: int,
) -> tuple[P2FirstHalfSnapshot | None, PeriodDiagnostics]:
    gate = _PeriodGate()
    pin_1x2, b365_1x2 = _extract_book_pair(
        context,
        target_minute=target_minute,
        spec=FIRST_HALF_SIDE_SCOPE.one_x_two,
        gate=gate,
    )
    pin_ah, b365_ah = _extract_book_pair(
        context,
        target_minute=target_minute,
        spec=FIRST_HALF_SIDE_SCOPE.asian_handicap,
        gate=gate,
    )
    components = (pin_1x2, b365_1x2, pin_ah, b365_ah)
    if not _is_complete(components, gate):
        return None, gate.diagnostics(None)

    assert isinstance(pin_1x2, TwoWayMarketSnapshot)
    assert isinstance(b365_1x2, TwoWayMarketSnapshot)
    assert isinstance(pin_ah, AsianHandicapSnapshot)
    assert isinstance(b365_ah, AsianHandicapSnapshot)
    snapshot = P2FirstHalfSnapshot(
        pinnacle_1x2=pin_1x2,
        bet365_1x2=b365_1x2,
        pinnacle_ah=pin_ah,
        bet365_ah=b365_ah,
    )
    return snapshot, gate.diagnostics(snapshot)


def _empty_period_diagnostics() -> PeriodDiagnostics:
    return PeriodDiagnostics.empty()


_PERIOD_EXTRACTORS = {
    FULL_TIME_SIDE_SCOPE.key: _extract_full_time,
    FIRST_HALF_SIDE_SCOPE.key: _extract_first_half,
}


def extract_p2_market_snapshot(
    event_id: int,
    context: OddsTrajectoryContext | None,
) -> P2ExtractionResult:
    """Extract every registered P2 period independently over the shared extractor."""
    selection = select_target_minute(
        context,
        flow_id="pillar_2",
        expected_event_id=event_id,
        allowed_target_minutes=Config.PRE_START_ODDS_MOMENTS,
    )
    if selection.target_minute is None or context is None:
        return P2ExtractionResult(
            target_minute=None,
            full_time=_empty_period_diagnostics(),
            first_half=_empty_period_diagnostics(),
            abort_reason=selection.reason,
        )

    target_minute = selection.target_minute
    extracted: dict[str, tuple[object | None, PeriodDiagnostics]] = {}
    for scope in P2_SIDE_PERIOD_SCOPES:
        extracted[scope.key] = _PERIOD_EXTRACTORS[scope.key](context, target_minute)

    full_time, full_time_diagnostics = extracted[FULL_TIME_SIDE_SCOPE.key]
    first_half, first_half_diagnostics = extracted[FIRST_HALF_SIDE_SCOPE.key]

    if full_time is None:
        return P2ExtractionResult(
            target_minute=target_minute,
            full_time=full_time_diagnostics,
            first_half=first_half_diagnostics,
            first_half_snapshot=first_half,  # type: ignore[arg-type]
            abort_reason="full_time_completeness_gate_failed",
        )

    return P2ExtractionResult(
        target_minute=target_minute,
        full_time=full_time_diagnostics,
        first_half=first_half_diagnostics,
        full_time_snapshot=full_time,  # type: ignore[arg-type]
        first_half_snapshot=first_half,  # type: ignore[arg-type]
    )


__all__ = ["extract_p2_market_snapshot"]
