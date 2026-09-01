"""Translate the structural P3 signal profile into the mining contract."""

from __future__ import annotations

from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.pillar_3_totals_market_context.signal_engine import ENGINE_VERSION

from ..contracts import PillarMiningRun, PillarMiningUnit
from ..execution_slot import build_execution_slot
from ..serialization import to_json_value
from ..status_policy import normalize_status


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
    """Persist P3's structural profile without inventing a scalar result."""

    pillar_id = "pillar_3_totals_market_context"
    result_scope = "totals_market_context"

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        inputs = _dictionary(raw.get("inputs"))
        input_trace = _dictionary(raw.get("input_trace"))
        profile = result.get("P3_SIGNAL_PROFILE")
        if not isinstance(profile, dict):
            profile = None

        target_minute = _optional_int(result.get("P3_TARGET_MINUTE"))
        evaluation_minute = _optional_int(
            getattr(event_context, "minutes_until_start", None)
        )
        producer_status, canonical_status = normalize_status(
            result.get("P3_STATUS") or result.get("status") or "ERROR"
        )
        periods = result.get("PERIODS") or raw.get("periods") or {}
        period_statuses = {
            period: _dictionary(details).get("status")
            for period, details in _dictionary(periods).items()
        }
        market_periods = [
            token
            for period, token in (
                ("full_time", "FULL_TIME"),
                ("first_half", "FIRST_HALF"),
            )
            if period_statuses.get(period) == "COMPLETE"
        ]

        diagnostics = to_json_value(
            {
                "reason": raw.get("reason"),
                "error": result.get("error"),
                "missing_inputs": result.get("MISSING_INPUTS", []),
                "invalid_inputs": result.get("INVALID_INPUTS", []),
                "ambiguous_inputs": result.get("AMBIGUOUS_INPUTS", []),
                "periods": periods,
                "input_trace": input_trace,
                "raw": {
                    key: value
                    for key, value in raw.items()
                    if key not in {"inputs", "input_trace", "periods"}
                },
            }
        )

        competition = getattr(event_context, "competition", None)
        competition_id = _optional_int(getattr(competition, "competition_id", None))
        context = to_json_value(
            {
                "event_id": event_context.event_id,
                "participants": event_context.participants_label,
                "sport": event_context.sport,
                "competition_id": competition_id,
                "competition": getattr(competition, "display_name", None),
                "minutes_to_start": evaluation_minute,
                "P3_TARGET_MINUTE": target_minute,
                "event_start_time_utc": getattr(event_context, "start_time_utc", None),
                "context_status": getattr(event_context, "context_status", None),
            }
        )
        dimensions = {
            "market_types": ["OVER_UNDER"],
            "market_periods": market_periods,
        }
        profile_payload = {"P3_SIGNAL_PROFILE": to_json_value(profile)}

        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            signal_axis=None,
            is_valid=None,
            score_name=None,
            score=None,
            direction=None,
            target_minute=target_minute,
            market_group="Over/Under",
            dimensions=dimensions,
            payload=profile_payload,
            diagnostics=diagnostics,
        )
        units = [summary]

        modules = result.get("modules") or []
        if modules and isinstance(modules[0], dict):
            module = modules[0]
            units.append(
                PillarMiningUnit(
                    unit_type="module",
                    unit_key=str(module.get("module_id") or "p3_signal_engine"),
                    parent_unit_key="summary",
                    ordinal=1,
                    module_id=str(module.get("module_id") or "p3_signal_engine"),
                    producer_status=producer_status,
                    canonical_status=canonical_status,
                    signal_axis=None,
                    is_valid=None,
                    score_name=None,
                    score=None,
                    direction=None,
                    target_minute=target_minute,
                    market_group="Over/Under",
                    dimensions=dimensions,
                    payload=profile_payload,
                    diagnostics=to_json_value(
                        {
                            "input_trace": input_trace,
                            "periods": periods,
                        }
                    ),
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
            inputs=to_json_value(inputs),
            diagnostics=diagnostics,
            output_payload=to_json_value(result),
            units=tuple(units),
        )
