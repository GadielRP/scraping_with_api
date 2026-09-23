"""P5-specific assembly and completeness policy over the shared extractor."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from modules.pillars.market_candidate_selection import select_market_candidate
from modules.pillars.market_coverage import PeriodDiagnostics
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
    ExchangeSnapshot,
    P5ExtractionResult,
    P5FullTimeSnapshot,
    ThreeWayMarketSnapshot,
    TwoWayMarketSnapshot,
)
from .periods import (
    Book1X2InputSpec,
    FULL_TIME_PRICE_MEMORY_SCOPE,
    PriceMemoryPeriodScope,
)
from .market_selection import select_p5_moneyline_target

PINNACLE_BOOKIE_ID = 302
BET365_BOOKIE_ID = 3
BETFAIR_EXCHANGE_BOOKIE_ID = 4
SOFASCORE_BOOKIE_ID = 1
logger = logging.getLogger(__name__)


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


def _log_book_gate(
    name: str,
    snapshot: ThreeWayMarketSnapshot | None,
    gate: _PeriodGate,
) -> None:
    complete = snapshot is not None and snapshot.is_complete()
    logger.info(
        "P5 EXTRACTION | bookmaker=%s complete=%s home=%s draw=%s away=%s missing=%s invalid=%s ambiguous=%s",
        name,
        complete,
        snapshot is not None and snapshot.home is not None,
        snapshot is not None and snapshot.draw is not None,
        snapshot is not None and snapshot.away is not None,
        sorted(gate.missing),
        sorted(gate.invalid),
        sorted(gate.ambiguous),
    )


def _book_request(
    spec: Book1X2InputSpec,
    *,
    identities: tuple,
    bookie_id: int,
    market_group: str,
) -> MarketSnapshotRequest:
    choices = [
        ChoiceRequest("1", "1", spec.home),
        ChoiceRequest("2", "2", spec.away),
    ]
    if market_group == "1X2" and spec.draw is not None:
        choices.append(ChoiceRequest("x", "x", spec.draw))

    return MarketSnapshotRequest(
        identities=identities,
        bookie_id=bookie_id,
        choices=tuple(choices),
    )


def _exchange_request(
    exchange_side: str,
    *,
    identities: tuple,
    is_2way: bool = False,
) -> MarketSnapshotRequest:
    prefix = {
        "1": "BF_HOME",
        "x": "BF_DRAW",
        "2": "BF_AWAY",
    }
    side_upper = exchange_side.upper()
    choice_keys = ("1", "2") if is_2way else ("1", "x", "2")
    choices = [
        ChoiceRequest(
            key=key,
            choice_name=key,
            input_name=f"{prefix[key]}_{side_upper}_1X2_FULL_TIME_ODDS_PRICE",
            exchange_size_input_name=f"{prefix[key]}_{side_upper}_1X2_FULL_TIME_EXCHANGE_SIZE",
        )
        for key in choice_keys
    ]
    return MarketSnapshotRequest(
        identities=identities,
        bookie_id=BETFAIR_EXCHANGE_BOOKIE_ID,
        exchange_side=exchange_side,
        exchange_level=0,
        choices=tuple(choices),
    )


def _extract_book(
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
    selection = select_market_candidate(extraction, request)
    gate.missing.update(selection.missing)
    gate.invalid.update(selection.invalid)
    gate.ambiguous.update(selection.ambiguous)

    if selection.candidate is None:
        return None

    cand = selection.candidate
    home = cand.choices.get("1")
    away = cand.choices.get("2")
    draw = cand.choices.get("x")

    if home is None or away is None:
        return None

    return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)


def _extract_exchange_side(
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
    selection = select_market_candidate(extraction, request)
    gate.missing.update(selection.missing)
    gate.invalid.update(selection.invalid)
    gate.ambiguous.update(selection.ambiguous)

    if selection.candidate is None:
        return None

    cand = selection.candidate
    home = cand.choices.get("1")
    away = cand.choices.get("2")
    draw = cand.choices.get("x")

    if home is None or away is None:
        return None

    return ThreeWayMarketSnapshot(home=home, draw=draw, away=away)


def extract_p5_market_snapshot(
    event_id: int,
    context: OddsTrajectoryContext | None,
    target_selection: TargetMinuteSelection,
    *,
    scope: PriceMemoryPeriodScope = FULL_TIME_PRICE_MEMORY_SCOPE,
) -> P5ExtractionResult:
    """Extract one exact canonical moneyline snapshot for Pillar 5."""
    logger.info(
        "P5 EXTRACTION | begin event_id=%s target_minute=%s selection_reason=%s context_available=%s scope=%s",
        event_id,
        target_selection.target_minute if target_selection else None,
        target_selection.reason if target_selection else "missing_target_selection",
        context is not None and context.available,
        scope.key,
    )
    if target_selection.target_minute is None or context is None:
        logger.info(
            "P5 EXTRACTION | aborted event_id=%s reason=%s",
            event_id,
            target_selection.reason if target_selection else "missing_context",
        )
        return P5ExtractionResult(
            target_minute=None,
            full_time_snapshot=None,
            full_time=PeriodDiagnostics.empty(),
            abort_reason=target_selection.reason if target_selection else "missing_context",
            extraction_diagnostics={
                "event_id": event_id,
                "target_selection": target_selection.diagnostics if target_selection else {},
            },
        )

    moneyline_selection = select_p5_moneyline_target(
        context,
        supported_identities=scope.identities,
    )
    moneyline_target = moneyline_selection.target
    if moneyline_target is None:
        logger.info(
            "P5 EXTRACTION | aborted event_id=%s stage=moneyline_selection reason=%s candidates=%s",
            event_id,
            moneyline_selection.reason,
            [candidate.to_dict() for candidate in moneyline_selection.candidates],
        )
        marker = ("P5_MONEYLINE_TARGET",)
        period_diagnostics = PeriodDiagnostics.from_gate(
            complete=False,
            missing_inputs=marker if not moneyline_selection.is_ambiguous else (),
            ambiguous_inputs=marker if moneyline_selection.is_ambiguous else (),
        )
        return P5ExtractionResult(
            target_minute=target_selection.target_minute,
            full_time_snapshot=None,
            full_time=period_diagnostics,
            abort_reason=moneyline_selection.reason,
            missing_inputs=marker if not moneyline_selection.is_ambiguous else (),
            ambiguous_inputs=marker if moneyline_selection.is_ambiguous else (),
            extraction_diagnostics={
                "event_id": event_id,
                "target_minute": target_selection.target_minute,
                "moneyline_selection": moneyline_selection.to_dict(),
            },
        )

    identities = (moneyline_target.identity,)

    target_minute = target_selection.target_minute
    all_missing: set[str] = set()
    all_invalid: set[str] = set()
    all_ambiguous: set[str] = set()

    bookie_diagnostics: dict[str, PeriodDiagnostics] = {}

    # 1. Pinnacle
    pin_gate = _PeriodGate()
    pin_snap = _extract_book(
        context,
        target_minute=target_minute,
        request=_book_request(
            scope.pinnacle,
            identities=identities,
            bookie_id=PINNACLE_BOOKIE_ID,
            market_group=moneyline_target.market_group,
        ),
        gate=pin_gate,
    )
    pin_complete = pin_snap is not None and pin_snap.is_complete()
    _log_book_gate("pinnacle", pin_snap, pin_gate)
    bookie_diagnostics["pinnacle"] = pin_gate.diagnostics(complete=pin_complete)
    all_missing.update(pin_gate.missing)
    all_invalid.update(pin_gate.invalid)
    all_ambiguous.update(pin_gate.ambiguous)

    # 2. Bet365
    b365_gate = _PeriodGate()
    b365_snap = _extract_book(
        context,
        target_minute=target_minute,
        request=_book_request(
            scope.bet365,
            identities=identities,
            bookie_id=BET365_BOOKIE_ID,
            market_group=moneyline_target.market_group,
        ),
        gate=b365_gate,
    )
    b365_complete = b365_snap is not None and b365_snap.is_complete()
    _log_book_gate("bet365", b365_snap, b365_gate)
    bookie_diagnostics["bet365"] = b365_gate.diagnostics(complete=b365_complete)
    all_missing.update(b365_gate.missing)
    all_invalid.update(b365_gate.invalid)
    all_ambiguous.update(b365_gate.ambiguous)

    # 3. SofaScore
    sofa_gate = _PeriodGate()
    sofa_snap = _extract_book(
        context,
        target_minute=target_minute,
        request=_book_request(
            scope.sofascore,
            identities=identities,
            bookie_id=SOFASCORE_BOOKIE_ID,
            market_group=moneyline_target.market_group,
        ),
        gate=sofa_gate,
    )
    sofa_complete = sofa_snap is not None and sofa_snap.is_complete()
    _log_book_gate("sofascore", sofa_snap, sofa_gate)
    bookie_diagnostics["sofascore"] = sofa_gate.diagnostics(complete=sofa_complete)
    all_missing.update(sofa_gate.missing)
    all_invalid.update(sofa_gate.invalid)
    all_ambiguous.update(sofa_gate.ambiguous)

    # 4. Betfair Exchange
    exchange_snap: ExchangeSnapshot | None = None
    if scope.includes_exchange:
        ex_gate = _PeriodGate()
        back_gate = _PeriodGate()
        back_snap = _extract_exchange_side(
            context,
            target_minute=target_minute,
            request=_exchange_request(
                "back",
                identities=identities,
                is_2way=moneyline_target.is_two_way,
            ),
            gate=back_gate,
        )

        lay_gate = _PeriodGate()
        lay_snap = _extract_exchange_side(
            context,
            target_minute=target_minute,
            request=_exchange_request(
                "lay",
                identities=identities,
                is_2way=moneyline_target.is_two_way,
            ),
            gate=lay_gate,
        )

        ex_gate.missing.update(back_gate.missing | lay_gate.missing)
        ex_gate.invalid.update(back_gate.invalid | lay_gate.invalid)
        ex_gate.ambiguous.update(back_gate.ambiguous | lay_gate.ambiguous)

        ex_complete = (
            back_snap is not None
            and back_snap.is_complete()
            and lay_snap is not None
            and lay_snap.is_complete()
        )
        logger.info(
            "P5 EXTRACTION | bookmaker=betfair complete=%s back_complete=%s lay_complete=%s missing=%s invalid=%s ambiguous=%s",
            ex_complete,
            back_snap is not None and back_snap.is_complete(),
            lay_snap is not None and lay_snap.is_complete(),
            sorted(ex_gate.missing),
            sorted(ex_gate.invalid),
            sorted(ex_gate.ambiguous),
        )
        bookie_diagnostics["betfair"] = ex_gate.diagnostics(complete=ex_complete)
        all_missing.update(ex_gate.missing)
        all_invalid.update(ex_gate.invalid)
        all_ambiguous.update(ex_gate.ambiguous)

        if back_snap is not None or lay_snap is not None:
            exchange_snap = ExchangeSnapshot(back=back_snap, lay=lay_snap)

    period_diagnostics = PeriodDiagnostics.from_bookies(bookie_diagnostics)

    # The selector already established the settlement contract.  Keep this
    # label even when a bookmaker is missing; never infer Full Time from a
    # mixed or incomplete set of traces.
    period_name = moneyline_target.market_period

    snapshot = P5FullTimeSnapshot(
        period=period_name,
        period_scope=scope,
        pinnacle=pin_snap,
        bet365=b365_snap,
        sofascore=sofa_snap,
        betfair=exchange_snap,
    )

    reason = None if period_diagnostics.usable else "period_completeness_gate_failed"
    logger.info(
        "P5 EXTRACTION | done event_id=%s target_minute=%s market=%s/%s status=%s usable=%s abort_reason=%s missing_count=%s invalid_count=%s ambiguous_count=%s",
        event_id,
        target_minute,
        moneyline_target.market_group,
        period_name,
        period_diagnostics.status,
        period_diagnostics.usable,
        reason,
        len(all_missing),
        len(all_invalid),
        len(all_ambiguous),
    )

    return P5ExtractionResult(
        target_minute=target_minute,
        full_time_snapshot=snapshot if snapshot.has_any_input() else None,
        full_time=period_diagnostics,
        abort_reason=reason,
        missing_inputs=tuple(sorted(all_missing)),
        invalid_inputs=tuple(sorted(all_invalid)),
        ambiguous_inputs=tuple(sorted(all_ambiguous)),
        extraction_diagnostics={
            "event_id": event_id,
            "target_minute": target_minute,
            "moneyline_selection": moneyline_selection.to_dict(),
            "selected_market_identity": moneyline_target.to_dict(),
            "bookie_diagnostics": {k: v.to_dict() for k, v in bookie_diagnostics.items()},
        },
    )


__all__ = [
    "BET365_BOOKIE_ID",
    "BETFAIR_EXCHANGE_BOOKIE_ID",
    "PINNACLE_BOOKIE_ID",
    "SOFASCORE_BOOKIE_ID",
    "extract_p5_market_snapshot",
]
