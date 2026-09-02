"""Translate Pillar 1 side and totals outputs into the mining contract."""

from __future__ import annotations

from typing import Any, Iterable

from modules.pillars.context import EventContext

from ..contracts import PillarMiningRun, PillarMiningUnit
from ..execution_slot import build_execution_slot
from ..serialization import optional_decimal, scalar_metric, to_json_value
from ..status_policy import normalize_status


P1_PILLAR_ID = "pillar_1_team_structure"
P1_SIDE_REGISTRATION_KEY = "pillar_1_team_structure_side"
P1_TOTALS_REGISTRATION_KEY = "pillar_1_team_structure_totals"


def _dictionary(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _event_fields(event_context: EventContext) -> tuple[int | None, int | None, int | None]:
    evaluation_minute = _optional_int(
        getattr(event_context, "minutes_until_start", None)
    )
    competition = getattr(event_context, "competition", None)
    competition_id = _optional_int(getattr(competition, "competition_id", None))
    return evaluation_minute, competition_id, _optional_int(
        getattr(event_context, "event_id", None)
    )


def _context(
    event_context: EventContext,
    *,
    evaluation_minute: int | None,
    target_minute: int | None,
    target_field: str,
) -> dict[str, Any]:
    competition = getattr(event_context, "competition", None)
    return to_json_value(
        {
            "event_id": getattr(event_context, "event_id", None),
            "participants": getattr(event_context, "participants_label", None),
            "sport": getattr(event_context, "sport", None),
            "competition_id": getattr(competition, "competition_id", None),
            "competition": getattr(competition, "display_name", None),
            "minutes_to_start": evaluation_minute,
            target_field: target_minute,
            "event_start_time_utc": getattr(event_context, "start_time_utc", None),
            "context_status": getattr(event_context, "context_status", None),
        }
    )


def _module_status(module: dict[str, Any]) -> str:
    module_id = str(module.get("module_id") or "").strip()
    raw = _dictionary(module.get("raw"))
    return str(raw.get(f"{module_id.lower()}_status") or "ACTIVE").strip().upper()


def _normalize_p1_status(status: object) -> tuple[str, str]:
    """Normalize P1's module vocabulary without changing its producer status."""

    producer_status = str(status or "ERROR").strip().upper()
    if producer_status in {"DEGRADED", "PARTIAL"}:
        return producer_status, "PARTIAL"
    if producer_status == "INACTIVE":
        return producer_status, "INSUFFICIENT"
    if producer_status in {"INSUFFICIENT_DATA", "IGNORE", "SKIPPED"}:
        return producer_status, normalize_status(producer_status)[1]
    return normalize_status(producer_status)


def _aggregate_side_status(modules: list[dict[str, Any]]) -> tuple[str, str]:
    statuses = [_module_status(module) for module in modules]
    if not statuses:
        return "ERROR", "ERROR"
    if all(status == "ACTIVE" for status in statuses):
        return "ACTIVE", "SUCCESS"
    if any(status in {"ACTIVE", "DEGRADED"} for status in statuses):
        return "PARTIAL", "PARTIAL"
    return "INSUFFICIENT_DATA", "INSUFFICIENT"


def _metrics(items: Iterable[tuple[str, Any]]) -> tuple:
    values = []
    for name, value in items:
        metric = scalar_metric(name, value)
        if metric is not None:
            values.append(metric)
    return tuple(values)


def _module_payload(module: dict[str, Any]) -> dict[str, Any]:
    return to_json_value({"raw": module.get("raw", {})})


class P1SideMiningAdapter:
    """Persist P1 Side's summary, M1-M7 modules, and their components."""

    registration_key = P1_SIDE_REGISTRATION_KEY
    pillar_id = P1_PILLAR_ID
    result_scope = "side"

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        final = _dictionary(raw.get("final"))
        modules = [module for module in result.get("modules", []) if isinstance(module, dict)]
        producer_status, canonical_status = _aggregate_side_status(modules)
        evaluation_minute, competition_id, event_id = _event_fields(event_context)
        engine_version = str(
            raw.get("engine_version") or result.get("engine_version") or "unknown"
        )
        target_minute = _optional_int(result.get("target_minute"))

        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            signal_axis="SIDE",
            score_name="value",
            score=optional_decimal(result.get("value")),
            direction=final.get("p1_final_bias"),
            target_minute=target_minute,
            payload=to_json_value(
                {
                    "pillar_name": result.get("pillar_name"),
                    "final": final,
                    "layer_a": raw.get("layer_a"),
                    "layer_b": raw.get("layer_b"),
                    "module_statuses": raw.get("module_statuses"),
                    "active_modules": raw.get("active_modules"),
                    "skipped_modules": raw.get("skipped_modules"),
                }
            ),
            diagnostics=to_json_value(
                {
                    "anomalies": raw.get("anomalies", []),
                    "p1_final_context_balance_is_decision": raw.get(
                        "p1_final_context_balance_is_decision"
                    ),
                    "value_is_evidence_only": raw.get("value_is_evidence_only"),
                }
            ),
        )
        units = [summary]

        for module_ordinal, module in enumerate(modules, start=1):
            module_id = str(module.get("module_id") or f"M{module_ordinal}")
            module_producer_status, module_canonical_status = _normalize_p1_status(
                _module_status(module)
            )
            module_key = f"module:{module_id}"
            module_unit = PillarMiningUnit(
                unit_type="module",
                unit_key=module_key,
                parent_unit_key="summary",
                ordinal=module_ordinal,
                module_id=module_id,
                producer_status=module_producer_status,
                canonical_status=module_canonical_status,
                signal_axis="SIDE",
                is_valid=module_canonical_status in {"SUCCESS", "PARTIAL"},
                score_name="value",
                score=optional_decimal(module.get("value")),
                direction=module.get("bias"),
                strength=module.get("strength"),
                payload=_module_payload(module),
                diagnostics=to_json_value(
                    {
                        "status_reason": _dictionary(module.get("raw")).get(
                            f"{module_id.lower()}_status_reason"
                        )
                    }
                ),
            )
            units.append(module_unit)

            for component_ordinal, component in enumerate(
                module.get("components", []), start=1
            ):
                if not isinstance(component, dict):
                    continue
                component_name = str(component.get("name") or f"component_{component_ordinal}")
                component_key = f"component:{module_id}:{component_name}"
                units.append(
                    PillarMiningUnit(
                        unit_type="component",
                        unit_key=component_key,
                        parent_unit_key=module_key,
                        ordinal=component_ordinal,
                        module_id=module_id,
                        producer_status=module_producer_status,
                        canonical_status=module_canonical_status,
                        signal_axis="SIDE",
                        is_valid=module_canonical_status in {"SUCCESS", "PARTIAL"},
                        score_name="edge",
                        score=optional_decimal(component.get("edge")),
                        direction=component.get("bias"),
                        strength=component.get("strength"),
                        payload=to_json_value({"raw": component.get("raw", {})}),
                        metrics=_metrics(
                            (
                                ("weight", component.get("weight")),
                                ("weighted_edge", component.get("weighted_edge")),
                            )
                        ),
                    )
                )

        if event_id is None:
            raise ValueError("P1 mining requires a numeric event_id")
        return PillarMiningRun(
            event_id=event_id,
            pillar_id=self.pillar_id,
            result_scope=self.result_scope,
            execution_slot=build_execution_slot(evaluation_minute, target_minute),
            engine_version=engine_version,
            payload_schema_version=2,
            producer_status=producer_status,
            canonical_status=canonical_status,
            sport=str(getattr(event_context, "sport", "unknown")),
            evaluation_minute=evaluation_minute,
            target_minute=target_minute,
            competition_id=competition_id,
            context=_context(
                event_context,
                evaluation_minute=evaluation_minute,
                target_minute=target_minute,
                target_field="P1_TARGET_MINUTE",
            ),
            diagnostics=to_json_value(
                {"raw": raw, "module_count": len(modules)}
            ),
            output_payload=to_json_value(result),
            units=tuple(units),
        )


class P1TotalsMiningAdapter:
    """Persist P1 Totals' scalar profile and structural layer outputs."""

    registration_key = P1_TOTALS_REGISTRATION_KEY
    pillar_id = P1_PILLAR_ID
    result_scope = "totals"

    _SUMMARY_EXCLUDED_FIELDS = frozenset(
        {
            "pillar_id",
            "module_id",
            "module_name",
            "engine_version",
            "event_id",
            "participants",
            "status",
            "status_reason",
            "active_layers",
            "ignored_layers",
            "P1_TOTALS_INTERNAL_STATE",
            "WINDOWS_USED",
            "WINDOW_COMPLETENESS_BY_WINDOW",
            "raw",
        }
    )

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        producer_status, canonical_status = normalize_status(result.get("status", "ERROR"))
        evaluation_minute, competition_id, event_id = _event_fields(event_context)
        engine_version = str(result.get("engine_version") or raw.get("engine_version") or "unknown")

        summary_metrics = _metrics(
            (name, value)
            for name, value in result.items()
            if name not in self._SUMMARY_EXCLUDED_FIELDS
            and not isinstance(value, (dict, list, tuple, set, frozenset))
        )
        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            signal_axis="TOTALS",
            is_valid=canonical_status == "SUCCESS",
            score_name="P1_TOTALS_DIRECTIONAL_SCORE",
            score=optional_decimal(result.get("P1_TOTALS_DIRECTIONAL_SCORE")),
            direction=result.get("P1_TOTALS_DIRECTION"),
            strength=result.get("P1_TOTALS_STRENGTH"),
            payload=to_json_value(
                {
                    "P1_TOTALS_INTERNAL_STATE": result.get("P1_TOTALS_INTERNAL_STATE"),
                    "WINDOWS_USED": result.get("WINDOWS_USED"),
                    "WINDOW_COMPLETENESS_BY_WINDOW": result.get(
                        "WINDOW_COMPLETENESS_BY_WINDOW"
                    ),
                    "raw": raw,
                }
            ),
            diagnostics=to_json_value({"status_reason": result.get("status_reason")}),
            metrics=summary_metrics,
        )
        units = [summary]

        layers = [
            layer
            for layer in [
                *(result.get("active_layers") or []),
                *(result.get("ignored_layers") or []),
            ]
            if isinstance(layer, dict)
        ]
        for ordinal, layer in enumerate(layers, start=1):
            layer_producer_status, layer_canonical_status = normalize_status(
                layer.get("status", "ERROR")
            )
            layer_name = str(layer.get("layer") or f"layer_{ordinal}")
            units.append(
                PillarMiningUnit(
                    unit_type="layer",
                    unit_key=f"layer:{layer_name}",
                    parent_unit_key="summary",
                    ordinal=ordinal,
                    producer_status=layer_producer_status,
                    canonical_status=layer_canonical_status,
                    signal_axis="TOTALS",
                    is_valid=layer_canonical_status == "SUCCESS",
                    score_name="final_signal",
                    score=optional_decimal(layer.get("final_signal")),
                    payload=to_json_value({"raw": layer.get("raw", {})}),
                    diagnostics=to_json_value(
                        {"ignored_reason": layer.get("ignored_reason")}
                    ),
                    metrics=_metrics(
                        (
                            ("raw_signal", layer.get("raw_signal")),
                            ("weight", layer.get("weight")),
                            ("weighted_signal", layer.get("weighted_signal")),
                        )
                    ),
                )
            )

        if event_id is None:
            raise ValueError("P1 mining requires a numeric event_id")
        return PillarMiningRun(
            event_id=event_id,
            pillar_id=self.pillar_id,
            result_scope=self.result_scope,
            execution_slot=build_execution_slot(evaluation_minute, None),
            engine_version=engine_version,
            payload_schema_version=2,
            producer_status=producer_status,
            canonical_status=canonical_status,
            sport=str(getattr(event_context, "sport", "unknown")),
            evaluation_minute=evaluation_minute,
            target_minute=None,
            competition_id=competition_id,
            context=_context(
                event_context,
                evaluation_minute=evaluation_minute,
                target_minute=None,
                target_field="P1_TARGET_MINUTE",
            ),
            diagnostics=to_json_value(
                {
                    "status_reason": result.get("status_reason"),
                    "raw": raw,
                }
            ),
            output_payload=to_json_value(result),
            units=tuple(units),
        )
