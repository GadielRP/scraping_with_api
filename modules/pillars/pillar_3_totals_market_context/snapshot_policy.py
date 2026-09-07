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
from modules.pillars.market_candidate_selection import select_market_candidate

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


def _select_book_candidate(
    extraction: MarketSnapshotExtraction, request: MarketSnapshotRequest
) -> _BookSelection:
    selection = select_market_candidate(extraction, request)
    return _BookSelection(
        snapshot=None
        if selection.candidate is None
        else _book_snapshot(selection.candidate),
        missing=selection.missing,
        invalid=selection.invalid,
        ambiguous=selection.ambiguous,
        diagnostics=_candidate_diagnostics(extraction),
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
    diagnostics = {
        "period_scope": {
            "key": period_scope.key,
            "display_name": period_scope.display_name,
            "metric_token": period_scope.metric_token,
        },
        "pinnacle": pinnacle.diagnostics,
        "bet365": bet365.diagnostics,
    }

    # Period aliases identify independent contracts; a mismatch prevents
    # comparison, not use of each complete bookmaker reading.
    periods = {
        book.market_period
        for book in (pinnacle.snapshot, bet365.snapshot)
        if book is not None
    }
    diagnostics["selected_periods"] = sorted(periods)
    snapshot = P3PeriodSnapshot(
        period=next(iter(periods)) if len(periods) == 1 else None,
        period_scope=period_scope,
        pinnacle=pinnacle.snapshot,
        bet365=bet365.snapshot,
    )
    if not snapshot.has_any_input():
        snapshot = None
    period_diagnostics = PeriodDiagnostics.from_bookies(
        {
            name: PeriodDiagnostics.from_gate(
                complete=selection.snapshot is not None
                and selection.snapshot.is_complete(),
                missing_inputs=selection.missing,
                invalid_inputs=selection.invalid,
                ambiguous_inputs=selection.ambiguous,
            )
            for name, selection in (("pinnacle", pinnacle), ("bet365", bet365))
        }
    )
    reason = None if period_diagnostics.usable else "period_completeness_gate_failed"
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

    def with_exchange(
        diagnostics: PeriodDiagnostics, exchange: PeriodDiagnostics
    ) -> PeriodDiagnostics:
        return PeriodDiagnostics.from_bookies(
            {**diagnostics.bookies, "betfair": exchange},
            required=("pinnacle", "bet365"),
        )

    full_time_diagnostics = with_exchange(
        full_time_diagnostics, exchange_ou_diagnostics
    )
    first_half_diagnostics = with_exchange(
        first_half_diagnostics, exchange_ou_1h_diagnostics
    )
    if full_time_snapshot is None and full_time_diagnostics.usable:
        full_time_snapshot = P3PeriodSnapshot(None, FULL_TIME_TOTALS_SCOPE, None, None)

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
            if full_time_diagnostics.usable
            else full_time_reason or "full_time_completeness_gate_failed"
        ),
        extraction_diagnostics={
            "target_selection": target_selection.diagnostics,
            FULL_TIME_TOTALS_SCOPE.key: full_time_extra,
            FIRST_HALF_TOTALS_SCOPE.key: first_half_extra,
        },
    )


__all__ = ["extract_p3_market_snapshot"]
