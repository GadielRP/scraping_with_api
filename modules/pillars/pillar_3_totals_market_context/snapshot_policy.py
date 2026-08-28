"""P3-specific candidate and partial-data policy over the shared extractor."""

from __future__ import annotations

from modules.pillars.market_snapshot_extractor import (
    ChoiceRequest,
    MarketCandidate,
    MarketSnapshotExtraction,
    MarketSnapshotRequest,
    extract_market_snapshot,
    select_target_minute,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .models import P3ExtractionResult, P3PeriodSnapshot, PeriodDiagnostics, TotalsBookSnapshot
from .periods import P3_TOTALS_PERIOD_SCOPES, TotalsPeriodScope


PINNACLE_BOOKIE_ID = 302
BET365_BOOKIE_ID = 3


def _request(
    *,
    period_scope: TotalsPeriodScope,
    bookie_id: int,
    prefix: str,
) -> MarketSnapshotRequest:
    return MarketSnapshotRequest(
        identities=period_scope.identities,
        bookie_id=bookie_id,
        line_input_name=f"{prefix}_TOTAL_LINE",
        choices=(
            ChoiceRequest("over", "over", f"{prefix}_OVER_PRICE"),
            ChoiceRequest("under", "under", f"{prefix}_UNDER_PRICE"),
        ),
    )


def _bookie_diagnostics(extraction: MarketSnapshotExtraction) -> dict[str, object]:
    return {
        "candidate_count": len(extraction.candidates),
        "candidates": [
            {
                "market_period": candidate.market_line.market_period,
                "market_name": candidate.market_line.market_name,
                "choice_group": candidate.market_line.choice_group,
                "source": candidate.bookie.source,
                "bookie_name": candidate.bookie.bookie_name,
            }
            for candidate in extraction.candidates
        ],
        "container_ambiguities": list(extraction.container_ambiguities),
    }


def _book_snapshot(candidate: MarketCandidate | None) -> TotalsBookSnapshot | None:
    if candidate is None:
        return None
    return TotalsBookSnapshot(
        market_period=candidate.market_period,
        line=candidate.line,
        over=candidate.choices.get("over"),
        under=candidate.choices.get("under"),
    )


def _extract_period(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    period_scope: TotalsPeriodScope,
) -> tuple[P3PeriodSnapshot | None, PeriodDiagnostics, str | None, dict[str, object]]:
    pin_request = _request(
        period_scope=period_scope,
        bookie_id=PINNACLE_BOOKIE_ID,
        prefix="PIN",
    )
    b365_request = _request(
        period_scope=period_scope,
        bookie_id=BET365_BOOKIE_ID,
        prefix="B365",
    )
    pin_extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=pin_request,
    )
    b365_extraction = extract_market_snapshot(
        context,
        target_minute=target_minute,
        request=b365_request,
    )

    missing = set(pin_extraction.missing_inputs) | set(b365_extraction.missing_inputs)
    invalid = set(pin_extraction.invalid_inputs) | set(b365_extraction.invalid_inputs)
    ambiguous = set(pin_extraction.ambiguous_inputs) | set(
        b365_extraction.ambiguous_inputs
    )
    extra = {
        "period_scope": {
            "key": period_scope.key,
            "display_name": period_scope.display_name,
            "metric_token": period_scope.metric_token,
        },
        "pinnacle": _bookie_diagnostics(pin_extraction),
        "bet365": _bookie_diagnostics(b365_extraction),
    }

    abort_reason: str | None = None
    if pin_extraction.container_ambiguities or b365_extraction.container_ambiguities:
        abort_reason = "ambiguous_bookie_containers"
    elif len(pin_extraction.candidates) > 1 or len(b365_extraction.candidates) > 1:
        abort_reason = "multiple_candidate_lines"
        if len(pin_extraction.candidates) > 1:
            ambiguous.update({"PIN_TOTAL_LINE", "PIN_OVER_PRICE", "PIN_UNDER_PRICE"})
        if len(b365_extraction.candidates) > 1:
            ambiguous.update(
                {"B365_TOTAL_LINE", "B365_OVER_PRICE", "B365_UNDER_PRICE"}
            )
    else:
        pin_candidate = (
            pin_extraction.candidates[0] if pin_extraction.candidates else None
        )
        b365_candidate = (
            b365_extraction.candidates[0] if b365_extraction.candidates else None
        )
        periods = {
            candidate.market_period
            for candidate in (pin_candidate, b365_candidate)
            if candidate is not None
        }
        if len(periods) > 1:
            abort_reason = "bookmaker_period_mismatch"
            extra = {**extra, "periods": sorted(periods)}
        elif pin_candidate is not None or b365_candidate is not None:
            snapshot = P3PeriodSnapshot(
                period=next(iter(periods)) if periods else None,
                period_scope=period_scope,
                pinnacle=_book_snapshot(pin_candidate),
                bet365=_book_snapshot(b365_candidate),
            )
            return (
                snapshot,
                PeriodDiagnostics.from_gate(
                    snapshot=snapshot,
                    missing_inputs=missing,
                    invalid_inputs=invalid,
                    ambiguous_inputs=ambiguous,
                ),
                None,
                extra,
            )

    return (
        None,
        PeriodDiagnostics.from_gate(
            snapshot=None,
            missing_inputs=missing,
            invalid_inputs=invalid,
            ambiguous_inputs=ambiguous,
        ),
        abort_reason,
        extra,
    )


def extract_p3_market_snapshot(
    event_id: int,
    context: OddsTrajectoryContext | None,
) -> P3ExtractionResult:
    """Extract every registered P3 period independently over the shared extractor."""
    selection = select_target_minute(
        context,
        flow_id="pillar_3",
        expected_event_id=event_id,
    )
    if selection.target_minute is None or context is None:
        return P3ExtractionResult(
            target_minute=None,
            full_time=PeriodDiagnostics.empty(),
            abort_reason=selection.reason,
            extraction_diagnostics={
                "target_selection": selection.diagnostics,
            },
        )

    target_minute = selection.target_minute
    required_abort: str | None = None
    full_time_snapshot: P3PeriodSnapshot | None = None
    full_time_diagnostics = PeriodDiagnostics.empty()
    extraction_diagnostics: dict[str, object] = {
        "target_selection": selection.diagnostics,
    }

    for scope in P3_TOTALS_PERIOD_SCOPES:
        snapshot, diagnostics, abort_reason, extra = _extract_period(
            context,
            target_minute=target_minute,
            period_scope=scope,
        )
        extraction_diagnostics[scope.key] = extra
        if scope.key == "full_time":
            full_time_snapshot = snapshot
            full_time_diagnostics = diagnostics
            if scope.required and snapshot is None:
                required_abort = abort_reason or "full_time_completeness_gate_failed"

    return P3ExtractionResult(
        target_minute=target_minute,
        full_time=full_time_diagnostics,
        full_time_snapshot=full_time_snapshot,
        abort_reason=required_abort,
        extraction_diagnostics=extraction_diagnostics,
    )


__all__ = ["extract_p3_market_snapshot"]
