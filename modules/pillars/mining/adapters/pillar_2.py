"""Translate the P2 RAW output into the hierarchical mining contract."""

from __future__ import annotations

from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.pillar_2_side_market.raw_engine import ENGINE_VERSION

from ..contracts import PillarMiningRun, PillarMiningUnit
from ..execution_slot import build_execution_slot
from ..serialization import optional_decimal, scalar_metric, to_json_value
from ..status_policy import normalize_status


P2_RAW_METRIC_GROUPS = {
    "PIN_SIDE_EDGE": "full_time",
    "B365_SIDE_EDGE": "full_time",
    "BOOK_GAP": "full_time",
    "BOOK_EDGE": "full_time",
    "AH_LINE_GAP": "asian_handicap_full_time",
    "PIN_AH_EDGE": "asian_handicap_full_time",
    "B365_AH_EDGE": "asian_handicap_full_time",
    "AH_PRICE_GAP": "asian_handicap_full_time",
    "PIN_1H_SIDE_EDGE": "first_half",
    "B365_1H_SIDE_EDGE": "first_half",
    "BOOK_1H_GAP": "first_half",
    "BOOK_1H_EDGE": "first_half",
    "PIN_AH_1H_EDGE": "asian_handicap_first_half",
    "B365_AH_1H_EDGE": "asian_handicap_first_half",
    "AH_1H_LINE_GAP": "asian_handicap_first_half",
    "AH_1H_PRICE_GAP": "asian_handicap_first_half",
    "BOOK_DIRECTION_FT": "cross_period",
    "BOOK_DIRECTION_1H": "cross_period",
    "FT_1H_GAP": "cross_period",
    "FT_1H_SAME_DIRECTION": "cross_period",
    "BACK_EDGE": "exchange",
    "LAY_EDGE": "exchange",
    "EXCHANGE_INTERNAL_GAP": "exchange",
    "EXCHANGE_EDGE": "exchange",
    "HOME_SPREAD": "exchange_quality",
    "AWAY_SPREAD": "exchange_quality",
    "SIDE_SPREAD": "exchange_quality",
    "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": "liquidity",
    "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": "liquidity",
    "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": "liquidity",
    "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": "liquidity",
    "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": "liquidity",
    "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": "liquidity",
    "Q_AGREEMENT": "quality",
    "Q_COMPLETE": "quality",
    "EXCHANGE_QUALITY_BASE": "quality",
    "TENSION_RAW": "tension",
    "DISLOCATION": "dislocation",
    "DISLOCATION_STRENGTH": "dislocation",
    "SIDE_MARKET_EDGE": "result",
    "P2_DIRECTION_RAW": "result",
}
P2_RAW_METRIC_NAMES = tuple(P2_RAW_METRIC_GROUPS)


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dictionary(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


class P2MiningAdapter:
    pillar_id = "pillar_2_side_market"
    result_scope = "side_market"

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        engine_raw = _dictionary(raw.get("p2_raw_engine"))
        mining_context = _dictionary(
            engine_raw.get("mining_context") or raw.get("mining_context")
        )
        target_minute = _optional_int(result.get("P2_TARGET_MINUTE"))
        evaluation_minute = _optional_int(
            getattr(event_context, "minutes_until_start", None)
        )
        producer_status, canonical_status = normalize_status(
            result.get("P2_STATUS") or result.get("status") or "ERROR"
        )

        metrics = tuple(
            metric
            for name in P2_RAW_METRIC_NAMES
            if name in result
            for metric in (
                scalar_metric(name, result[name], group=P2_RAW_METRIC_GROUPS[name]),
            )
            if metric is not None
        )
        outer_raw_diagnostics = {
            key: value
            for key, value in raw.items()
            if key not in {"p2_raw_engine", "mining_context"}
        }
        engine_diagnostics = {
            key: value
            for key, value in engine_raw.items()
            if key not in {"mining_context", "inputs"}
        }
        diagnostics = to_json_value(
            {
                "reason": raw.get("reason"),
                "error": result.get("error"),
                "missing_inputs": result.get("MISSING_INPUTS", []),
                "invalid_inputs": result.get("INVALID_INPUTS", []),
                "ambiguous_inputs": result.get("AMBIGUOUS_INPUTS", []),
                "module_ids": raw.get("module_ids", []),
                "outer_raw": outer_raw_diagnostics,
                "engine": engine_diagnostics,
            }
        )

        competition = getattr(event_context, "competition", None)
        competition_id = _optional_int(getattr(competition, "competition_id", None))
        context = to_json_value(
            {
                **mining_context,
                "event_id": event_context.event_id,
                "participants": event_context.participants_label,
                "sport": event_context.sport,
                "competition_id": competition_id,
                "competition": getattr(competition, "display_name", None),
                "minutes_to_start": evaluation_minute,
                "P2_TARGET_MINUTE": target_minute,
                "event_start_time_utc": getattr(event_context, "start_time_utc", None),
                "context_status": getattr(event_context, "context_status", None),
            }
        )
        score = optional_decimal(result.get("SIDE_MARKET_EDGE"))
        direction = (
            str(result["P2_DIRECTION_RAW"])
            if result.get("P2_DIRECTION_RAW") is not None
            else None
        )

        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            signal_axis="SIDE",
            is_valid=None,
            score_name="SIDE_MARKET_EDGE",
            score=score,
            direction=direction,
            target_minute=target_minute,
            dimensions={
                "market_types": ["1X2", "ASIAN_HANDICAP"],
                "market_periods": ["FULL_TIME", "FIRST_HALF"],
            },
            diagnostics=diagnostics,
        )
        units = [summary]
        modules = result.get("modules") or []
        if modules and isinstance(modules[0], dict):
            module = modules[0]
            units.append(
                PillarMiningUnit(
                    unit_type="module",
                    unit_key=str(module.get("module_id") or "p2_raw_engine"),
                    parent_unit_key="summary",
                    ordinal=1,
                    module_id=str(module.get("module_id") or "p2_raw_engine"),
                    producer_status=producer_status,
                    canonical_status=canonical_status,
                    signal_axis="SIDE",
                    is_valid=None,
                    score_name="SIDE_MARKET_EDGE",
                    score=score,
                    direction=direction,
                    target_minute=target_minute,
                    dimensions=summary.dimensions,
                    payload=to_json_value(
                        {"baseline_weights": engine_raw.get("baseline_weights", {})}
                    ),
                    diagnostics=to_json_value(
                        {"input_trace": engine_raw.get("input_trace", {})}
                    ),
                    metrics=metrics,
                )
            )

        return PillarMiningRun(
            event_id=int(event_context.event_id),
            pillar_id=self.pillar_id,
            result_scope=self.result_scope,
            execution_slot=build_execution_slot(evaluation_minute, target_minute),
            engine_version=str(result.get("engine_version") or ENGINE_VERSION),
            payload_schema_version=2,
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
