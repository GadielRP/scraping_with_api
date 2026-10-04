"""Preserve current exchange quotes for audit without creating memory signals."""

from modules.pillars.market_evaluation import BETFAIR, context_view, contract_key
from modules.pillars.market_candidate_selection import select_market_candidate
from modules.pillars.market_snapshot_extractor import (
    MarketIdentity, MarketSnapshotRequest, ChoiceRequest, extract_market_snapshot,
)


def collect_exchange_inputs(evaluation, inputs: dict) -> tuple[dict, ...]:
    target = evaluation.target_selection.target_minute
    if target is None or evaluation.selected_full_time_period is None:
        return ()
    diagnostics = []
    for family, outcomes in (("1X2", ("1", "x", "2")), ("Home/Away", ("1", "2"))):
        lines = [line for line in evaluation.lines
                 if line.market_group == family
                 and line.market_period == evaluation.selected_full_time_period]
        if not lines:
            continue
        view = context_view(evaluation.context, lines)
        for side in ("back", "lay"):
            request = MarketSnapshotRequest(
                tuple(MarketIdentity(line.market_group, line.market_period, line.market_name)
                      for line in lines),
                BETFAIR.id, tuple(ChoiceRequest(name, name, name) for name in outcomes),
                exchange_side=side,
            )
            extraction = extract_market_snapshot(view, target_minute=target, request=request)
            selection = select_market_candidate(extraction, request)
            refs, contracts = [], []
            if selection.candidate is not None:
                contracts.append(contract_key(selection.candidate.market_line))
                for outcome, point in selection.candidate.choices.items():
                    if point is None:
                        continue
                    ref = f"diagnostic:{BETFAIR.id}:{family}:{evaluation.selected_full_time_period}:{side}:{outcome}"
                    inputs[ref] = {
                        "value": float(point.odds_price),
                        "exchange_size": float(point.exchange_size) if point.exchange_size is not None else None,
                        "trace": point.trace.to_dict(),
                        "role": "DIAGNOSTIC",
                    }
                    refs.append(ref)
            diagnostics.append({
                "kind": "exchange_exposure",
                "bookie_id": BETFAIR.id,
                "family": family,
                "period": evaluation.selected_full_time_period,
                "exchange_side": side,
                "participates_in_score": False,
                "status": (
                    "AMBIGUOUS" if selection.ambiguous else
                    "INVALID" if selection.invalid else
                    "COMPLETE" if selection.candidate is not None and selection.candidate.is_complete(request) else
                    "INCOMPLETE" if refs else "MISSING"
                ),
                "input_refs": refs,
                "contract_refs": contracts,
                "missing": sorted(selection.missing),
                "invalid": list(extraction.invalid_inputs),
                "ambiguous": sorted(selection.ambiguous),
            })
    return tuple(diagnostics)
