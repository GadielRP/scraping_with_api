"""Translate P4's temporal signal profile into the common mining contract."""

from __future__ import annotations

from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.pillar_4.signal_engine import ENGINE_VERSION

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


class P4MiningAdapter:
    """Persist a structural temporal profile without manufacturing a score."""

    pillar_id = "pillar_4_temporal_market_drift"
    result_scope = "temporal_market_drift"

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        profile = result.get("P4_SIGNAL_PROFILE")
        if not isinstance(profile, dict):
            profile = None
        inputs = _dictionary(raw.get("inputs"))
        input_trace = _dictionary(raw.get("input_trace"))
        periods = _dictionary(result.get("PERIODS") or raw.get("periods"))
        target_minute = _optional_int(result.get("P4_TARGET_MINUTE"))
        evaluation_minute = _optional_int(
            getattr(event_context, "minutes_until_start", None)
        )
        producer_status, canonical_status = normalize_status(
            result.get("P4_STATUS") or result.get("status") or "ERROR"
        )
        diagnostics = to_json_value(
            {
                "reason": raw.get("reason"),
                "error": result.get("error"),
                "missing_inputs": result.get("MISSING_INPUTS", []),
                "invalid_inputs": result.get("INVALID_INPUTS", []),
                "ambiguous_inputs": result.get("AMBIGUOUS_INPUTS", []),
                "periods": periods,
                "input_trace": input_trace,
                "extraction_diagnostics": raw.get("extraction_diagnostics", {}),
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
                "P4_TARGET_MINUTE": target_minute,
                "event_start_time_utc": getattr(event_context, "start_time_utc", None),
                "context_status": getattr(event_context, "context_status", None),
            }
        )

        market_types: set[str] = set()
        market_periods: set[str] = set()
        views: set[str] = set()
        bookie_ids: set[int] = set()
        sources: set[str] = set()
        exchange_sides: set[str] = set()
        value_types: set[str] = set()
        series_ids: set[str] = set()
        if profile is not None:
            for view_name in ("ADAPTIVE_VIEW", "CHECKPOINT_VIEW"):
                view = _dictionary(profile.get(view_name))
                if view:
                    views.add(view_name.removesuffix("_VIEW"))
                for series in view.get("SERIES", []):
                    series_payload = _dictionary(series)
                    market = _dictionary(series_payload.get("MARKET"))
                    if series_payload.get("SERIES_ID"):
                        series_ids.add(str(series_payload["SERIES_ID"]))
                    if market.get("MARKET_GROUP"):
                        market_types.add(str(market["MARKET_GROUP"]))
                    if market.get("MARKET_PERIOD"):
                        market_periods.add(str(market["MARKET_PERIOD"]))
                    if (bookie_id := _optional_int(market.get("BOOKIE_ID"))) is not None:
                        bookie_ids.add(bookie_id)
                    if market.get("SOURCE"):
                        sources.add(str(market["SOURCE"]))
                    if market.get("EXCHANGE_SIDE"):
                        exchange_sides.add(str(market["EXCHANGE_SIDE"]))
                    if market.get("VALUE_TYPE"):
                        value_types.add(str(market["VALUE_TYPE"]))
        dimensions = {
            "market_types": sorted(market_types),
            "market_periods": sorted(market_periods),
            "views": sorted(views),
            "bookie_ids": sorted(bookie_ids),
            "sources": sorted(sources),
            "exchange_sides": sorted(exchange_sides),
            "value_types": sorted(value_types),
            "series_ids": sorted(series_ids),
        }
        profile_payload = {"P4_SIGNAL_PROFILE": to_json_value(profile)}
        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            target_minute=target_minute,
            market_group="SIDE/TOTALS",
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
                    unit_key=str(module.get("module_id") or "p4_signal_engine"),
                    parent_unit_key="summary",
                    ordinal=1,
                    module_id=str(module.get("module_id") or "p4_signal_engine"),
                    producer_status=producer_status,
                    canonical_status=canonical_status,
                    target_minute=target_minute,
                    market_group="SIDE/TOTALS",
                    dimensions=dimensions,
                    payload=profile_payload,
                    diagnostics=to_json_value(
                        {"periods": periods, "input_trace": input_trace}
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


__all__ = ["P4MiningAdapter"]
