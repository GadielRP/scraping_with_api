"""P4 independent temporal metrics over the shared event selection."""

import logging
from hashlib import sha256

from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.market_evaluation import prepare_event_markets, contract_key
from .periods import P4_PILLAR_ID
from .signal_engine import ENGINE_VERSION, build_p4_series
from .trajectory_policy import extract_p4_trajectory_inputs
from .diagnostics import log_trajectory_diagnostics
from .provenance import resolve_series_contracts

logger = logging.getLogger(__name__)


def _point_references(value, refs):
    if isinstance(value, list):
        for index, item in enumerate(value):
            value[index] = _point_references(item, refs)
        return value
    if not isinstance(value, dict):
        return value
    keys = {
        "FROM_POINT_ID",
        "TO_POINT_ID",
        "START_POINT_ID",
        "END_POINT_ID",
        "NET_START_OBSERVATION",
        "NET_END_OBSERVATION",
    }
    for key, item in value.items():
        value[key] = (
            refs.get(item, item) if key in keys else _point_references(item, refs)
        )
    return value


def _metrics(values, prefix=""):
    for key, value in values.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            yield from _metrics(value, name)
        elif key.endswith("_RAW") and not isinstance(value, list):
            yield name, value


def _blocked_reason(series, name):
    if not series.traceability["OPERATIVE_ENDPOINT_PRESENT"]:
        return "MISSING_ENDPOINT"
    if len(series.points) < 2:
        return "INSUFFICIENT_OBSERVATIONS"
    if series.market["VALUE_TYPE"] == "EXCHANGE_SIZE" and "VELOCITY" in name:
        return "NOT_APPLICABLE"
    if series.traceability["MISSING_TARGET_MINUTES"]:
        return "NON_CONTIGUOUS_GAP"
    if (
        name == "PATH_EFFICIENCY_RAW"
        and series.raw_temporal_features["PATH_LENGTH_RAW"] == 0
    ):
        return "ZERO_DENOMINATOR"
    return "UNDEFINED_METRIC"


def calculate_pillar_4(
    event_context,
    odds_trajectory_context,
    target_selection,
    *,
    debug_mode=False,
    market_evaluation=None,
):
    evaluation = market_evaluation or prepare_event_markets(
        odds_trajectory_context, target_selection, event_context
    )
    extraction = extract_p4_trajectory_inputs(
        event_context, evaluation.view(4), target_selection
    )
    signals, inputs, analysis = [], {}, {}
    contracts = evaluation.contracts(4)
    if extraction.usable:
        series_results = build_p4_series(extraction)
        accepted_refs = {contract_key(line) for line in evaluation.lines}
        series_contracts = resolve_series_contracts(
            series_results,
            {ref: contract for ref, contract in contracts.items()
             if ref in accepted_refs},
        )
        for series in series_results:
            if series.status == "ERROR":
                signals.append(
                    SignalResult(
                        series.series_id,
                        "ERROR",
                        reason="CALCULATION_ERROR",
                        evidence={
                            "market": series.market,
                            "error_class": series.traceability["ERROR_CLASS"],
                        },
                    )
                )
                continue
            refs, point_refs = [], {}
            for point in series.points:
                original = getattr(point, "original", point)
                base = series.traceability["BASE_SERIES_ID"]
                ref = (
                    "point_"
                    + sha256(f"{base}:{point.point_id}".encode()).hexdigest()[:24]
                )
                point_refs[point.point_id] = ref
                provenance_ref = (
                    f"snapshot:{original.snapshot_id}"
                    if original.snapshot_id is not None
                    else "observation_"
                    + sha256(
                        f"{base}:{original.quote_id}:{original.effective_at}:{original.availability_at}".encode()
                    ).hexdigest()[:24]
                )
                if provenance_ref not in inputs:
                    inputs[provenance_ref] = {
                        key: value
                        for key, value in original.to_dict().items()
                        if key
                        not in {
                            "VALUE",
                            "POINT_ID",
                            "TARGET_MINUTE",
                            "OBSERVATION_KIND",
                            "DISTANCE_FROM_TARGET_MINUTES",
                        }
                    }
                inputs.setdefault(
                    ref,
                    {
                        "value": float(point.value),
                        "observation_ref": provenance_ref,
                        "target_minute": point.target_minute,
                        "observation_kind": point.observation_kind,
                        "distance_from_target_minutes": (
                            float(point.distance_from_target_minutes)
                            if point.distance_from_target_minutes is not None
                            else None
                        ),
                    },
                )
                refs.append(ref)
            sufficient = (
                len(refs) >= 2 and series.traceability["OPERATIVE_ENDPOINT_PRESENT"]
            )
            key = series.series_id
            contract_refs = series_contracts[key]
            series_ref = f"series:{key}"
            inputs[series_ref] = {
                "point_refs": refs,
                "trace": {
                    "bookie_id": series.market["BOOKIE_ID"],
                    "market_group": series.market["MARKET_GROUP"],
                    "market_period": series.market["MARKET_PERIOD"],
                    "line_value": series.market["CHOICE_GROUP"],
                    "source": series.market["SOURCE"],
                    "exchange_side": series.market["EXCHANGE_SIDE"],
                    "exchange_level": series.market["EXCHANGE_LEVEL"],
                },
            }
            metrics = (
                *_metrics(series.raw_temporal_features),
                *_metrics(series.structural_signals, "STRUCTURAL"),
            )
            for name, value in metrics:
                signals.append(
                    SignalResult(
                        f"{key}:{name}",
                        (
                            "COMPUTED"
                            if sufficient and value is not None
                            else "BLOCKED"
                        ),
                        value if sufficient else None,
                        (
                            None
                            if sufficient and value is not None
                            else _blocked_reason(series, name)
                        ),
                        (series_ref,),
                        contract_refs,
                        evidence={
                            "market": series.market,
                            "observations": len(refs),
                            "missing_checkpoints": series.traceability[
                                "MISSING_TARGET_MINUTES"
                            ],
                        },
                    )
                )
            analysis[key] = {
                "market": series.market,
                "structural_signals": (
                    _point_references(series.structural_signals, point_refs)
                    if sufficient
                    else {}
                ),
                "raw_temporal_features": _point_references(
                    series.raw_temporal_features, point_refs
                ),
                "legs": _point_references(list(series.legs), point_refs),
                "status": series.status,
                "input_refs": [series_ref],
                "traceability": series.traceability,
            }
    if not signals:
        signals.append(
            SignalResult("temporal_metrics", "BLOCKED", reason="MISSING_INPUT")
        )
    result = EvaluationResult(
        event_context.event_id,
        P4_PILLAR_ID,
        ENGINE_VERSION,
        target_selection.target_minute,
        evaluation.selection,
        tuple(signals),
        evaluation.coverage(4),
        inputs,
        contracts,
        analysis,
        (
            *evaluation.diagnostics,
            {
                "source_series_seen": extraction.source_series_seen,
                "endpoint_series_present": extraction.endpoint_series_present,
                "missing": list(extraction.missing_inputs),
                "invalid": list(extraction.invalid_inputs),
                "ambiguous": list(extraction.ambiguous_inputs),
                "excluded_future_points": extraction.excluded_future_points,
                "missing_endpoints": list(extraction.missing_endpoint_details),
            },
        ),
    ).to_dict()
    result["checkpoint"] = {
        **result["checkpoint"],
        "nominal_target_as_of": (
            extraction.nominal_target_as_of.isoformat()
            if extraction.nominal_target_as_of else None
        ),
        "operative_as_of": (
            extraction.operative_as_of.isoformat() if extraction.operative_as_of else None
        ),
    }
    if debug_mode:
        log_trajectory_diagnostics(logger, result)
    return result
