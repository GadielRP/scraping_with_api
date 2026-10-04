"""Build auditable signal outcomes from typed analytical DTOs."""

from __future__ import annotations
from typing import Any
from dataclasses import replace
from hashlib import sha256
from .market_evaluation import context_view
from .evaluation_contracts import EvaluationResult, SignalResult
from .market_evaluation import (
    EventMarketEvaluation,
    FIRST_HALF,
    FIRST_FIVE,
    contract_key,
)
from .market_audit import json_inputs
from .signal_dependencies import resolve_dependencies


def profile_signals(
    profile: dict,
    *,
    namespace: str,
    inputs: dict,
    contracts: dict,
    family: str | None,
    secondary: str,
    fields: frozenset[str],
    coverage: tuple,
) -> tuple[SignalResult, ...]:
    signals = []

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                visit(child, (*path, key))
        elif path and path[-1] in fields:
            refs, contract_refs = _dependencies(
                path, inputs, contracts, family, secondary
            )
            identity = (
                (path, refs, contract_refs) if value is not None else (namespace, path)
            )
            key = "reading_" + sha256(repr(identity).encode()).hexdigest()[:24]
            signals.append(
                SignalResult(
                    key,
                    "BLOCKED" if value is None else "COMPUTED",
                    value,
                    (
                        _blocked_reason(path, family, secondary, coverage)
                        if value is None
                        else None
                    ),
                    refs,
                    contract_refs,
                    {
                        "metric": ".".join(path),
                        "scope": namespace,
                        "reading_kind": (
                            "HOME_AWAY_PRICES_FROM_1X2"
                            if family == "1X2"
                            else family or "Over/Under"
                        ),
                    },
                )
            )
        elif value is None and path and len(path) == 1:
            signals.append(
                SignalResult(
                    f"{namespace}:{path[0]}", "BLOCKED", reason="MISSING_INPUT"
                )
            )

    visit(profile, ())
    return tuple(signals)


def _blocked_reason(path, family, secondary, coverage):
    dependencies = resolve_dependencies(path, moneyline=family, secondary=secondary)
    states = [
        cell.observed_status or cell.status
        for cell in coverage
        if cell.reason != "UNSUPPORTED_CONTRACT"
        and cell.bookie_id in dependencies.book_ids
        and cell.family in dependencies.families
        and cell.period in dependencies.periods
        and (not dependencies.sides or cell.exchange_side in dependencies.sides)
    ]
    if "AMBIGUOUS" in states:
        return "AMBIGUOUS_CANDIDATE"
    if "INVALID" in states:
        return "INVALID_VALUE"
    if states and all(state in ("COMPLETE", "NOT_APPLICABLE") for state in states):
        return "INCOMPATIBLE_CONTRACT"
    return "MISSING_INPUT"


def _dependencies(path, inputs, contracts, family, secondary):
    dependencies = resolve_dependencies(path, moneyline=family, secondary=secondary)
    refs, traces = [], []
    for key, point in inputs.items():
        trace = point.get("trace") or {}
        if not dependencies.accepts(trace):
            continue
        if point.get("kind") == "SIZE" or (
            family == "1X2" and trace.get("choice_name", "").lower() == "x"
        ):
            continue
        if path[-1] in ("LINE_GAP", "LINE_DIFF_RAW") and point.get("kind") != "LINE":
            continue
        if (
            path[-1] in ("PIN_EDGE", "B365_EDGE", "BACK_EDGE", "LAY_EDGE", "EDGE")
            and point.get("kind") == "LINE"
        ):
            continue
        refs.append(key)
        traces.append(trace)
    contract_refs = tuple(
        key
        for key, contract in contracts.items()
        if any(
            contract["market_group"] == trace.get("market_group")
            and contract["market_period"] == trace.get("market_period")
            and contract["line_value"] == trace.get("line_value")
            and contract["canonical_market_key"] == trace.get("canonical_market_key")
            and contract["is_live"] == trace.get("is_live", False)
            for trace in traces
        )
    )
    return tuple(sorted(refs)), tuple(sorted(contract_refs))


def _contract_views(view):
    lines = [
        line
        for group in view.markets.values()
        for period in group.values()
        for names in period.values()
        for line in names.values()
    ]
    split = {}
    for line in lines:
        key = (line.market_group, line.market_period)
        for book in line.bookies.values():
            split.setdefault(key, {}).setdefault(book.bookie_id, set()).add(
                line.line_value
            )
    split = {
        key
        for key, books in split.items()
        if any(len(values) > 1 for values in books.values())
    }
    if not split:
        return (("base", view),)
    base = [
        line for line in lines if (line.market_group, line.market_period) not in split
    ]
    views = [("base", context_view(view, base))] if base else []
    for group, period in sorted(split):
        selected = [
            line
            for line in lines
            if (line.market_group, line.market_period) == (group, period)
        ]
        for value in sorted({line.line_value for line in selected}, key=str):
            views.append(
                (
                    f"{group}:{period}:{value}",
                    context_view(
                        view,
                        [
                            *base,
                            *(line for line in selected if line.line_value == value),
                        ],
                    ),
                )
            )
    return tuple(views)


def _display_period(value, secondary):
    if secondary != FIRST_FIVE or not isinstance(value, dict):
        return value
    return {
        key.replace("1H", "FIRST_FIVE_INNINGS"): _display_period(child, secondary)
        for key, child in value.items()
    }


def evaluate_snapshot_profile(
    event_context,
    evaluation: EventMarketEvaluation,
    *,
    pillar: int,
    engine_version: str,
    extract,
    build,
    metric_fields: frozenset[str],
    debug_mode: bool = False,
) -> dict:
    analyses, inputs, signals, diagnostics = {}, {}, [], list(evaluation.diagnostics)
    coverage = evaluation.coverage(pillar)
    families = (
        sorted(
            {
                line.market_group
                for line in evaluation.lines
                if line.market_group in ("1X2", "Home/Away")
            }
        )
        if pillar == 2
        else [None]
    )
    families = families or ["1X2"]
    secondaries = [FIRST_HALF]
    if pillar == 2 and any(
        line.market_period == FIRST_FIVE for line in evaluation.lines
    ):
        secondaries.append(FIRST_FIVE)
    for family in families:
        for secondary in secondaries:
            namespace = f"{family or 'Over/Under'}:{secondary}"
            initial_view = evaluation.view(
                pillar, moneyline_family=family, secondary_period=secondary
            )
            for lane, view in _contract_views(initial_view):
                scope = f"{namespace}:{lane}"
                refs = tuple(
                    contract_key(line)
                    for group in view.markets.values()
                    for period in group.values()
                    for name in period.values()
                    for line in name.values()
                )
                try:
                    extraction = extract(
                        event_context.event_id, view, evaluation.target_selection
                    )
                    snapshot = extraction.snapshot
                    if snapshot is None:
                        signals.append(
                            SignalResult(
                                scope,
                                "BLOCKED",
                                reason="MISSING_INPUT",
                                contract_refs=refs,
                            )
                        )
                        continue
                    values, traces = (
                        json_inputs(snapshot.input_values()),
                        snapshot.input_trace(),
                    )
                    local_refs = []
                    for name, value in values.items():
                        if value is None:
                            continue
                        key = (
                            "input_"
                            + sha256(
                                repr((name, traces.get(name))).encode()
                            ).hexdigest()[:24]
                        )
                        inputs[key] = {
                            "value": value,
                            "trace": traces.get(name),
                            "kind": (
                                "LINE"
                                if name.endswith("_LINE")
                                else "SIZE" if "SIZE" in name else "PRICE"
                            ),
                        }
                        local_refs.append(key)
                    profile = build(snapshot, debug_mode=debug_mode).to_dict()
                    analyses[scope] = _display_period(profile, secondary)
                    readings = profile_signals(
                        profile,
                        namespace=scope,
                        inputs={key: inputs[key] for key in local_refs},
                        contracts=evaluation.contracts(pillar),
                        family=family,
                        secondary=secondary,
                        fields=metric_fields,
                        coverage=coverage,
                    )
                    if secondary == FIRST_FIVE:
                        readings = tuple(
                            replace(
                                signal,
                                evidence={
                                    **signal.evidence,
                                    "metric": signal.evidence.get("metric", "").replace(
                                        "1H", "FIRST_FIVE_INNINGS"
                                    ),
                                },
                            )
                            for signal in readings
                        )
                    signals.extend(readings)
                    diagnostics.append(
                        {
                            "scope": namespace,
                            "missing": list(extraction.missing_inputs),
                            "invalid": list(extraction.invalid_inputs),
                            "ambiguous": list(extraction.ambiguous_inputs),
                        }
                    )
                except Exception as exc:
                    signals.append(
                        SignalResult(
                            scope,
                            "ERROR",
                            reason="CALCULATION_ERROR",
                            contract_refs=refs,
                            evidence={"error_class": type(exc).__name__},
                        )
                    )
    return EvaluationResult(
        event_context.event_id,
        "pillar_2_side_market" if pillar == 2 else "pillar_3_totals_market_context",
        engine_version,
        evaluation.target_selection.target_minute,
        evaluation.selection,
        tuple({signal.key: signal for signal in signals}.values()),
        coverage,
        inputs,
        evaluation.contracts(pillar),
        analyses,
        tuple(diagnostics),
    ).to_dict()
