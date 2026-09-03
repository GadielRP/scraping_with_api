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


def _exchange_request(exchange_side: str, *, is_2way: bool = False) -> MarketSnapshotRequest:
    prefix = {
        "1": "BF_HOME",
        "x": "BF_DRAW",
        "2": "BF_AWAY",
    }
    choices = ("1", "2") if is_2way else ("1", "x", "2")
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
    identities = spec.identities if spec is not None else scope.asian_handicap.identities
    return MarketSnapshotRequest(
        identities=identities,
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
    gate.merge(extraction)
    candidate = _unique_required_candidate(extraction, request, gate)
    if candidate is None:
        return None
    home = candidate.choices["1"]
    away = candidate.choices["2"]
    assert home is not None and away is not None
    if "x" in candidate.choices and candidate.choices["x"] is not None:
        draw = candidate.choices["x"]
        return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)
    return TwoWayMarketSnapshot(home=home, away=away)



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

    # Spread layer: try Asian Handicap first; fallback to Handicap if not found
    ah_gate = _PeriodGate()
    pin_ah, b365_ah = _extract_required_book_pair(
        context,
        target_minute=target_minute,
        spec=FULL_TIME_SIDE_SCOPE.asian_handicap,
        gate=ah_gate,
    )
    spread_market_type = "asian_handicap"
    if (
        pin_ah is not None
        and b365_ah is not None
        and not ah_gate.missing
        and not ah_gate.invalid
        and not ah_gate.ambiguous
    ):
        gate.missing.update(ah_gate.missing)
        gate.invalid.update(ah_gate.invalid)
        gate.ambiguous.update(ah_gate.ambiguous)
    elif FULL_TIME_SIDE_SCOPE.handicap is not None:
        hc_gate = _PeriodGate()
        pin_hc, b365_hc = _extract_required_book_pair(
            context,
            target_minute=target_minute,
            spec=FULL_TIME_SIDE_SCOPE.handicap,
            gate=hc_gate,
        )
        if (
            pin_hc is not None
            and b365_hc is not None
            and not hc_gate.missing
            and not hc_gate.invalid
            and not hc_gate.ambiguous
        ):
            pin_ah, b365_ah = pin_hc, b365_hc
            spread_market_type = "handicap"
            gate.missing.update(hc_gate.missing)
            gate.invalid.update(hc_gate.invalid)
            gate.ambiguous.update(hc_gate.ambiguous)
        else:
            gate.missing.update(ah_gate.missing)
            gate.invalid.update(ah_gate.invalid)
            gate.ambiguous.update(ah_gate.ambiguous)
    else:
        gate.missing.update(ah_gate.missing)
        gate.invalid.update(ah_gate.invalid)
        gate.ambiguous.update(ah_gate.ambiguous)

    # Exchange layer: detect whether side market is 2-way
    is_2way = (
        pin_1x2 is not None
        and hasattr(pin_1x2.home, "trace")
        and pin_1x2.home.trace.market_group.lower() in {"home/away", "homeaway", "winner", "moneyline"}
    )
    bf_back_gate = _PeriodGate()
    bf_lay_gate = _PeriodGate()
    bf_back = _extract_required_exchange(
        context,
        target_minute=target_minute,
        request=_exchange_request("back", is_2way=is_2way),
        gate=bf_back_gate,
    )
    bf_lay = _extract_required_exchange(
        context,
        target_minute=target_minute,
        request=_exchange_request("lay", is_2way=is_2way),
        gate=bf_lay_gate,
    )
    # If 3-way exchange failed because Draw is missing, fallback to 2-way if possible
    if (bf_back is None or bf_lay is None) and not is_2way:
        alt_back_gate = _PeriodGate()
        alt_lay_gate = _PeriodGate()
        alt_back = _extract_required_exchange(
            context,
            target_minute=target_minute,
            request=_exchange_request("back", is_2way=True),
            gate=alt_back_gate,
        )
        alt_lay = _extract_required_exchange(
            context,
            target_minute=target_minute,
            request=_exchange_request("lay", is_2way=True),
            gate=alt_lay_gate,
        )
        if (
            alt_back is not None
            and alt_lay is not None
            and not alt_back_gate.missing
            and not alt_lay_gate.missing
        ):
            bf_back, bf_lay = alt_back, alt_lay
            bf_back_gate, bf_lay_gate = alt_back_gate, alt_lay_gate

    gate.missing.update(bf_back_gate.missing | bf_lay_gate.missing)
    gate.invalid.update(bf_back_gate.invalid | bf_lay_gate.invalid)
    gate.ambiguous.update(bf_back_gate.ambiguous | bf_lay_gate.ambiguous)

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
        spread_market_type=spread_market_type,
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

    ah_gate = _PeriodGate()
    pin_ah = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(
            FIRST_HALF_SIDE_SCOPE.asian_handicap,
            bookie_id=PINNACLE_BOOKIE_ID,
        ),
        gate=ah_gate,
    )
    b365_ah = _extract_partial_two_way(
        context,
        target_minute=target_minute,
        request=_two_way_request(
            FIRST_HALF_SIDE_SCOPE.asian_handicap,
            bookie_id=BET365_BOOKIE_ID,
        ),
        gate=ah_gate,
    )
    spread_market_type = "asian_handicap"
    if (pin_ah is not None and pin_ah.has_any_input()) or (b365_ah is not None and b365_ah.has_any_input()):
        gate.missing.update(ah_gate.missing)
        gate.invalid.update(ah_gate.invalid)
        gate.ambiguous.update(ah_gate.ambiguous)
    elif FIRST_HALF_SIDE_SCOPE.handicap is not None:
        hc_gate = _PeriodGate()
        pin_hc = _extract_partial_two_way(
            context,
            target_minute=target_minute,
            request=_two_way_request(
                FIRST_HALF_SIDE_SCOPE.handicap,
                bookie_id=PINNACLE_BOOKIE_ID,
            ),
            gate=hc_gate,
        )
        b365_hc = _extract_partial_two_way(
            context,
            target_minute=target_minute,
            request=_two_way_request(
                FIRST_HALF_SIDE_SCOPE.handicap,
                bookie_id=BET365_BOOKIE_ID,
            ),
            gate=hc_gate,
        )
        if (pin_hc is not None and pin_hc.has_any_input()) or (b365_hc is not None and b365_hc.has_any_input()):
            pin_ah, b365_ah = pin_hc, b365_hc
            spread_market_type = "handicap"
            gate.missing.update(hc_gate.missing)
            gate.invalid.update(hc_gate.invalid)
            gate.ambiguous.update(hc_gate.ambiguous)
        else:
            gate.missing.update(ah_gate.missing)
            gate.invalid.update(ah_gate.invalid)
            gate.ambiguous.update(ah_gate.ambiguous)
    else:
        gate.missing.update(ah_gate.missing)
        gate.invalid.update(ah_gate.invalid)
        gate.ambiguous.update(ah_gate.ambiguous)

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
        spread_market_type=spread_market_type,
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
    def try_extract(spec_to_use: TwoWayMarketSpec):
        back_gate = _PeriodGate()
        lay_gate = _PeriodGate()
        back = _extract_partial_two_way(
            context,
            target_minute=target_minute,
            request=_exchange_ah_request(
                "back",
                scope=scope,
                line_name=line_name,
                odds_names=odds_names,
                spec=spec_to_use,
            ),
            gate=back_gate,
        )
        lay = _extract_partial_two_way(
            context,
            target_minute=target_minute,
            request=_exchange_ah_request(
                "lay",
                scope=scope,
                line_name=line_name,
                odds_names=odds_names,
                spec=spec_to_use,
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

    ah_snap, ah_diag = try_extract(scope.asian_handicap)
    if ah_snap is not None and ah_snap.has_any_input():
        return ah_snap, ah_diag

    if getattr(scope, "handicap", None) is not None:
        hc_snap, hc_diag = try_extract(scope.handicap)
        if hc_snap is not None and hc_snap.has_any_input():
            return hc_snap, hc_diag

    return None, ah_diag



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
