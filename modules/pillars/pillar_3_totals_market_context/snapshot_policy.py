"""P3-specific candidate selection and partial-period extraction policy."""

from __future__ import annotations

from dataclasses import dataclass

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
    P3ExtractionResult,
    P3PeriodSnapshot,
    PeriodDiagnostics,
    TotalsBookSnapshot,
    TotalsExchangeSnapshot,
)
from .periods import (
    EXCHANGE_OU_1H_LINE_INPUT_NAME,
    EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
    EXCHANGE_OU_LINE_INPUT_NAME,
    EXCHANGE_OU_ODDS_INPUT_NAMES,
    EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
    FIRST_HALF_TOTALS_SCOPE,
    FULL_TIME_TOTALS_SCOPE,
    P3_TOTALS_PERIOD_SCOPES,
    TotalsBookInputSpec,
    TotalsPeriodScope,
)


PINNACLE_BOOKIE_ID = 302
BET365_BOOKIE_ID = 3
BETFAIR_EXCHANGE_BOOKIE_ID = 4


@dataclass(frozen=True, slots=True)
class _BookSelection:
    snapshot: TotalsBookSnapshot | None
    missing: frozenset[str]
    invalid: frozenset[str]
    ambiguous: frozenset[str]
    diagnostics: dict[str, object]


def _request(
    *,
    period_scope: TotalsPeriodScope,
    bookie_id: int,
    inputs: TotalsBookInputSpec,
) -> MarketSnapshotRequest:
    return MarketSnapshotRequest(
        identities=period_scope.identities,
        bookie_id=bookie_id,
        line_input_name=inputs.line,
        choices=(
            ChoiceRequest("over", "over", inputs.over),
            ChoiceRequest("under", "under", inputs.under),
        ),
    )


def _exchange_request(
    exchange_side: str,
    *,
    period_scope: TotalsPeriodScope = FULL_TIME_TOTALS_SCOPE,
    line_name: str = EXCHANGE_OU_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_OU_ODDS_INPUT_NAMES,
) -> MarketSnapshotRequest:
    inputs = TotalsBookInputSpec(
        line=line_name,
        over=odds_names[0 if exchange_side == "back" else 2],
        under=odds_names[1 if exchange_side == "back" else 3],
    )
    return MarketSnapshotRequest(
        identities=period_scope.identities,
        bookie_id=BETFAIR_EXCHANGE_BOOKIE_ID,
        line_input_name=inputs.line,
        exchange_side=exchange_side,
        exchange_level=0,
        choices=(
            ChoiceRequest("over", "over", inputs.over),
            ChoiceRequest("under", "under", inputs.under),
        ),
    )


def _candidate_diagnostics(
    extraction: MarketSnapshotExtraction,
) -> dict[str, object]:
    return {
        "candidate_count": len(extraction.candidates),
        "candidates": [
            {
                "market_period": candidate.market_line.market_period,
                "market_name": candidate.market_line.market_name,
                "choice_group": candidate.market_line.choice_group,
                "source": candidate.bookie.source,
                "bookie_name": candidate.bookie.bookie_name,
                "complete": candidate.line is not None
                and candidate.choices.get("over") is not None
                and candidate.choices.get("under") is not None,
            }
            for candidate in extraction.candidates
        ],
        "container_ambiguities": list(extraction.container_ambiguities),
    }


def _book_snapshot(candidate: MarketCandidate) -> TotalsBookSnapshot:
    return TotalsBookSnapshot(
        market_period=candidate.market_period,
        line=candidate.line,
        over=candidate.choices.get("over"),
        under=candidate.choices.get("under"),
    )


def _has_any_candidate_input(candidate: MarketCandidate) -> bool:
    return candidate.line is not None or any(
        point is not None for point in candidate.choices.values()
    )


def _all_input_names(request: MarketSnapshotRequest) -> set[str]:
    names = {choice.input_name for choice in request.choices}
    if request.line_input_name is not None:
        names.add(request.line_input_name)
    return names


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


def _select_book_candidate(
    extraction: MarketSnapshotExtraction,
    request: MarketSnapshotRequest,
) -> _BookSelection:
    names = _all_input_names(request)
    diagnostics = _candidate_diagnostics(extraction)
    extractor_ambiguities = set(extraction.ambiguous_inputs) & names
    if extraction.container_ambiguities or extractor_ambiguities:
        return _BookSelection(
            snapshot=None,
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(names | extractor_ambiguities),
            diagnostics=diagnostics,
        )

    complete = [
        candidate
        for candidate in extraction.candidates
        if candidate.is_complete(request)
    ]
    if len(complete) > 1:
        return _BookSelection(
            snapshot=None,
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(names),
            diagnostics=diagnostics,
        )
    if len(complete) == 1:
        return _BookSelection(
            snapshot=_book_snapshot(complete[0]),
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(),
            diagnostics=diagnostics,
        )

    partial = [
        candidate
        for candidate in extraction.candidates
        if _has_any_candidate_input(candidate)
    ]
    if len(partial) > 1:
        return _BookSelection(
            snapshot=None,
            missing=frozenset(),
            invalid=frozenset(),
            ambiguous=frozenset(names),
            diagnostics=diagnostics,
        )
    if len(partial) == 1:
        missing, invalid = _selected_input_diagnostics(
            partial[0],
            request,
            extraction,
        )
        return _BookSelection(
            snapshot=_book_snapshot(partial[0]),
            missing=frozenset(missing),
            invalid=frozenset(invalid),
            ambiguous=frozenset(),
            diagnostics=diagnostics,
        )

    return _BookSelection(
        snapshot=None,
        missing=frozenset(names),
        invalid=frozenset(),
        ambiguous=frozenset(),
        diagnostics=diagnostics,
    )


def _extract_book(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    period_scope: TotalsPeriodScope,
    bookie_id: int,
    inputs: TotalsBookInputSpec,
) -> _BookSelection:
    request = _request(
        period_scope=period_scope,
        bookie_id=bookie_id,
        inputs=inputs,
    )
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    return _select_book_candidate(extraction, request)


def _extract_exchange_side(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    exchange_side: str,
    period_scope: TotalsPeriodScope = FULL_TIME_TOTALS_SCOPE,
    line_name: str = EXCHANGE_OU_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_OU_ODDS_INPUT_NAMES,
) -> _BookSelection:
    request = _exchange_request(
        exchange_side,
        period_scope=period_scope,
        line_name=line_name,
        odds_names=odds_names,
    )
    extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=request,
    )
    return _select_book_candidate(extraction, request)


def _extract_optional_exchange_ou(
    context: OddsTrajectoryContext,
    target_minute: int,
    *,
    period_scope: TotalsPeriodScope = FULL_TIME_TOTALS_SCOPE,
    line_name: str = EXCHANGE_OU_LINE_INPUT_NAME,
    odds_names: tuple[str, ...] = EXCHANGE_OU_ODDS_INPUT_NAMES,
) -> tuple[TotalsExchangeSnapshot | None, PeriodDiagnostics]:
    back = _extract_exchange_side(
        context,
        target_minute=target_minute,
        exchange_side="back",
        period_scope=period_scope,
        line_name=line_name,
        odds_names=odds_names,
    )
    lay = _extract_exchange_side(
        context,
        target_minute=target_minute,
        exchange_side="lay",
        period_scope=period_scope,
        line_name=line_name,
        odds_names=odds_names,
    )
    missing = set(back.missing | lay.missing)
    invalid = set(back.invalid | lay.invalid)
    ambiguous = set(back.ambiguous | lay.ambiguous)
    if ambiguous:
        return None, PeriodDiagnostics.from_gate(
            complete=False,
            missing_inputs=missing,
            invalid_inputs=invalid,
            ambiguous_inputs=ambiguous,
        )
    snapshot = TotalsExchangeSnapshot(
        back=back.snapshot,
        lay=lay.snapshot,
    )
    if not snapshot.has_any_input():
        return None, PeriodDiagnostics.from_gate(
            complete=False,
            missing_inputs=missing,
            invalid_inputs=invalid,
        )
    complete = (
        snapshot.back is not None
        and snapshot.lay is not None
        and snapshot.back.is_complete()
        and snapshot.lay.is_complete()
        and snapshot.lines_match
        and not missing
        and not invalid
    )
    return snapshot, PeriodDiagnostics.from_gate(
        complete=complete,
        missing_inputs=missing,
        invalid_inputs=invalid,
    )


def _extract_period(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    period_scope: TotalsPeriodScope,
) -> tuple[P3PeriodSnapshot | None, PeriodDiagnostics, str | None, dict[str, object]]:
    pinnacle = _extract_book(
        context,
        target_minute=target_minute,
        period_scope=period_scope,
        bookie_id=PINNACLE_BOOKIE_ID,
        inputs=period_scope.pinnacle,
    )
    bet365 = _extract_book(
        context,
        target_minute=target_minute,
        period_scope=period_scope,
        bookie_id=BET365_BOOKIE_ID,
        inputs=period_scope.bet365,
    )
    missing = set(pinnacle.missing | bet365.missing)
    invalid = set(pinnacle.invalid | bet365.invalid)
    ambiguous = set(pinnacle.ambiguous | bet365.ambiguous)
    diagnostics = {
        "period_scope": {
            "key": period_scope.key,
            "display_name": period_scope.display_name,
            "metric_token": period_scope.metric_token,
        },
        "pinnacle": pinnacle.diagnostics,
        "bet365": bet365.diagnostics,
    }

    if ambiguous:
        return (
            None,
            PeriodDiagnostics.from_gate(
                complete=False,
                missing_inputs=missing,
                invalid_inputs=invalid,
                ambiguous_inputs=ambiguous,
            ),
            "ambiguous_market_structure",
            diagnostics,
        )

    periods = {
        snapshot.market_period
        for snapshot in (pinnacle.snapshot, bet365.snapshot)
        if snapshot is not None
    }
    if len(periods) > 1:
        all_names = set(period_scope.input_names())
        diagnostics["selected_periods"] = sorted(periods)
        return (
            None,
            PeriodDiagnostics.from_gate(
                complete=False,
                missing_inputs=missing,
                invalid_inputs=invalid,
                ambiguous_inputs=all_names,
            ),
            "bookmaker_period_mismatch",
            diagnostics,
        )

    snapshot = P3PeriodSnapshot(
        period=next(iter(periods)) if periods else None,
        period_scope=period_scope,
        pinnacle=pinnacle.snapshot,
        bet365=bet365.snapshot,
    )
    if not snapshot.has_any_input():
        snapshot = None
    complete = snapshot is not None and snapshot.is_complete()
    period_diagnostics = PeriodDiagnostics.from_gate(
        complete=complete,
        missing_inputs=missing,
        invalid_inputs=invalid,
        ambiguous_inputs=ambiguous,
    )
    reason = None if complete else "period_completeness_gate_failed"
    return snapshot, period_diagnostics, reason, diagnostics


def extract_p3_market_snapshot(
    event_id: int,
    context: OddsTrajectoryContext | None,
    target_selection: TargetMinuteSelection,
) -> P3ExtractionResult:
    """Extract FT and 1H at the target selected once by the pillar pipeline."""
    if target_selection.target_minute is None or context is None:
        return P3ExtractionResult(
            target_minute=None,
            full_time=PeriodDiagnostics.empty(),
            first_half=PeriodDiagnostics.empty(),
            exchange_ou=PeriodDiagnostics.empty(),
            exchange_ou_1h=PeriodDiagnostics.empty(),
            abort_reason=target_selection.reason,
            extraction_diagnostics={
                "target_selection": target_selection.diagnostics,
            },
        )

    target_minute = target_selection.target_minute
    exchange_ou_snapshot, exchange_ou_diagnostics = _extract_optional_exchange_ou(
        context,
        target_minute,
    )
    exchange_ou_1h_snapshot, exchange_ou_1h_diagnostics = _extract_optional_exchange_ou(
        context,
        target_minute,
        period_scope=FIRST_HALF_TOTALS_SCOPE,
        line_name=EXCHANGE_OU_1H_LINE_INPUT_NAME,
        odds_names=EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
    )
    extracted = {
        scope.key: _extract_period(
            context,
            target_minute=target_minute,
            period_scope=scope,
        )
        for scope in P3_TOTALS_PERIOD_SCOPES
    }
    full_time_snapshot, full_time_diagnostics, full_time_reason, full_time_extra = (
        extracted[FULL_TIME_TOTALS_SCOPE.key]
    )
    first_half_snapshot, first_half_diagnostics, _, first_half_extra = extracted[
        FIRST_HALF_TOTALS_SCOPE.key
    ]
    full_time_complete = (
        full_time_snapshot is not None and full_time_snapshot.is_complete()
    )

    return P3ExtractionResult(
        target_minute=target_minute,
        full_time=full_time_diagnostics,
        first_half=first_half_diagnostics,
        full_time_snapshot=full_time_snapshot,
        first_half_snapshot=first_half_snapshot,
        exchange_ou_snapshot=exchange_ou_snapshot,
        exchange_ou=exchange_ou_diagnostics,
        exchange_ou_1h_snapshot=exchange_ou_1h_snapshot,
        exchange_ou_1h=exchange_ou_1h_diagnostics,
        abort_reason=(
            None
            if full_time_complete
            else full_time_reason or "full_time_completeness_gate_failed"
        ),
        extraction_diagnostics={
            "target_selection": target_selection.diagnostics,
            FULL_TIME_TOTALS_SCOPE.key: full_time_extra,
            FIRST_HALF_TOTALS_SCOPE.key: first_half_extra,
        },
    )


__all__ = ["extract_p3_market_snapshot"]
