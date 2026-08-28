"""Translate the P3 RAW output into the hierarchical mining contract."""

from __future__ import annotations

from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.pillar_3_totals_market_context.raw_engine import ENGINE_VERSION
from modules.pillars.pillar_3_totals_market_context.periods import (
    DEFAULT_P3_TOTALS_PERIOD_SCOPE,
    TotalsPeriodScope,
    derived_metric_names,
    period_scope_from_token,
)

from ..contracts import PillarMiningRun, PillarMiningUnit
from ..execution_slot import build_execution_slot
from ..serialization import optional_decimal, scalar_metric, to_json_value
from ..status_policy import normalize_status


P3_INPUT_METRIC_GROUPS = {
    "PIN_TOTAL_LINE": "pinnacle_inputs",
    "PIN_OVER_PRICE": "pinnacle_inputs",
    "PIN_UNDER_PRICE": "pinnacle_inputs",
    "B365_TOTAL_LINE": "bet365_inputs",
    "B365_OVER_PRICE": "bet365_inputs",
    "B365_UNDER_PRICE": "bet365_inputs",
}


def _metric_groups(period_scope: TotalsPeriodScope) -> dict[str, str]:
    names = derived_metric_names(period_scope)
    return {
        **P3_INPUT_METRIC_GROUPS,
        names.pin_edge: "pinnacle",
        names.pin_direction: "pinnacle",
        names.b365_edge: "bet365",
        names.b365_direction: "bet365",
        names.line_diff: "line_comparison",
        names.line_gap: "line_comparison",
        names.price_gap: "price_comparison",
        names.pin_weight: "baseline_weights",
        names.b365_weight: "baseline_weights",
        names.market_edge: "result",
        names.p3_direction: "result",
        names.context_direction: "result",
        names.completeness: "quality",
    }


def p3_raw_metric_names(
    period_scope: TotalsPeriodScope = DEFAULT_P3_TOTALS_PERIOD_SCOPE,
) -> tuple[str, ...]:
    """Return the mineable P3 metric contract for one configured period."""
    return tuple(_metric_groups(period_scope))


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dictionary(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


class P3MiningAdapter:
    pillar_id = "pillar_3_totals_market_context"
    result_scope_prefix = "totals_market_context"

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        engine_raw = _dictionary(raw.get("p3_raw_engine"))
        mining_context = _dictionary(
            engine_raw.get("mining_context") or raw.get("mining_context")
        )
        target_minute = _optional_int(result.get("TARGET_MINUTE"))
        evaluation_minute = _optional_int(
            getattr(event_context, "minutes_until_start", None)
        )
        producer_status, canonical_status = normalize_status(
            result.get("P3_STATUS") or result.get("status") or "ERROR"
        )
        period_scope = (
            period_scope_from_token(result.get("PERIOD_SCOPE"))
            or DEFAULT_P3_TOTALS_PERIOD_SCOPE
        )
        names = derived_metric_names(period_scope)
        metric_groups = _metric_groups(period_scope)
        result_scope = f"{self.result_scope_prefix}_{period_scope.key}"

        metrics = tuple(
            metric
            for name in metric_groups
            if name in result
            for metric in (
                scalar_metric(name, result[name], group=metric_groups[name]),
            )
            if metric is not None
        )
        outer_diagnostics = {
            key: value
            for key, value in raw.items()
            if key not in {"p3_raw_engine", "mining_context"}
        }
        engine_diagnostics = {
            key: value
            for key, value in engine_raw.items()
            if key not in {"mining_context", "inputs", "input_trace"}
        }
        diagnostics = to_json_value(
            {
                "reason": raw.get("reason"),
                "error": result.get("error"),
                "missing_inputs": result.get("MISSING_INPUTS", []),
                "invalid_inputs": result.get("INVALID_INPUTS", []),
                "ambiguous_inputs": result.get("AMBIGUOUS_INPUTS", []),
                "periods": result.get("PERIODS") or raw.get("periods") or {},
                "module_ids": raw.get("module_ids", []),
                "input_trace": engine_raw.get("input_trace", {}),
                "outer_raw": outer_diagnostics,
                "engine": engine_diagnostics,
            }
        )

        competition = getattr(event_context, "competition", None)
        competition_id = _optional_int(getattr(competition, "competition_id", None))
        period = result.get("PERIOD")
        context = to_json_value(
            {
                **mining_context,
                "event_id": event_context.event_id,
                "participants": event_context.participants_label,
                "sport": event_context.sport,
                "competition_id": competition_id,
                "competition": getattr(competition, "display_name", None),
                "season_id": getattr(event_context, "season_id", None),
                "season_name": getattr(event_context, "season_name", None),
                "season_year": getattr(event_context, "season_year", None),
                "minutes_to_start": evaluation_minute,
                "TARGET_MINUTE": target_minute,
                "PERIOD": period,
                "PERIOD_SCOPE": period_scope.metric_token,
                "event_start_time_utc": getattr(event_context, "start_time_utc", None),
                "context_status": getattr(event_context, "context_status", None),
            }
        )
        score = optional_decimal(result.get(names.market_edge))
        direction = (
            str(result[names.p3_direction])
            if result.get(names.p3_direction) is not None
            else None
        )
        dimensions = {
            "market_group": "Over/Under",
            "market_period": period,
            "period_scope": period_scope.metric_token,
            "bookie_ids": [302, 3],
        }
        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            signal_axis="TOTALS",
            is_valid=None,
            score_name=names.market_edge,
            score=score,
            direction=direction,
            target_minute=target_minute,
            market_group="Over/Under",
            market_period=str(period) if period is not None else None,
            dimensions=dimensions,
            diagnostics=diagnostics,
        )
        units = [summary]
        modules = result.get("modules") or []
        if modules and isinstance(modules[0], dict):
            module = modules[0]
            units.append(
                PillarMiningUnit(
                    unit_type="module",
                    unit_key=str(module.get("module_id") or "p3_raw_engine"),
                    parent_unit_key="summary",
                    ordinal=1,
                    module_id=str(module.get("module_id") or "p3_raw_engine"),
                    producer_status=producer_status,
                    canonical_status=canonical_status,
                    signal_axis="TOTALS",
                    is_valid=None,
                    score_name=names.market_edge,
                    score=score,
                    direction=direction,
                    target_minute=target_minute,
                    market_group="Over/Under",
                    market_period=str(period) if period is not None else None,
                    dimensions=dimensions,
                    payload=to_json_value(
                        {"baseline_weights": engine_raw.get("baseline_weights", {})}
                    ),
                    diagnostics=to_json_value(
                        {
                            "input_trace": engine_raw.get("input_trace", {}),
                            "extraction": engine_raw.get(
                                "extraction_diagnostics",
                                {},
                            ),
                            "periods": engine_raw.get("periods")
                            or result.get("PERIODS")
                            or {},
                        }
                    ),
                    metrics=metrics,
                )
            )

        return PillarMiningRun(
            event_id=int(event_context.event_id),
            pillar_id=self.pillar_id,
            result_scope=result_scope,
            execution_slot=build_execution_slot(evaluation_minute, target_minute),
            engine_version=str(result.get("engine_version") or ENGINE_VERSION),
            payload_schema_version=1,
            producer_status=producer_status,
            canonical_status=canonical_status,
            sport=str(event_context.sport),
            evaluation_minute=evaluation_minute,
            target_minute=target_minute,
            competition_id=competition_id,
            context=context,
            inputs=to_json_value(_dictionary(engine_raw.get("inputs"))),
            diagnostics=diagnostics,
            output_payload=to_json_value(result),
            units=tuple(units),
        )
