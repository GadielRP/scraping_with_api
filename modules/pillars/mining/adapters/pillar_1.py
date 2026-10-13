"""P1-specific projections, keeping the native structural output once."""

from modules.pillars.evaluation_contracts import PAYLOAD_SCHEMA_VERSION
from modules.pillars.instrumentation import signal_diagnostics, PILLAR_STATUSES
from ..contracts import PillarMiningRun, PillarMiningUnit
from ..serialization import numeric_metrics, optional_decimal, to_json_value

P1_PILLAR_ID = "pillar_1_team_structure"
P1_SIDE_REGISTRATION_KEY = "pillar_1_team_structure_side"
P1_TOTALS_REGISTRATION_KEY = "pillar_1_team_structure_totals"


def _module_status(module):
    raw = module.get("raw") or {}
    name = str(module.get("module_id", "")).lower()
    native = str(raw.get(f"{name}_status", "ACTIVE")).upper()
    if native == "ERROR":
        return "ERROR"
    if native in {"INACTIVE", "INSUFFICIENT_DATA", "SKIPPED"}:
        return "BLOCKED"
    return "COMPUTED"


def _build(adapter, context, result, units):
    status = result.get("status")
    if status not in PILLAR_STATUSES:
        status = (
            "ACTIVE"
            if any(u.status == "COMPUTED" for u in units)
            else (
                "ERROR"
                if any(u.status == "ERROR" for u in units)
                else "INSUFFICIENT_DATA"
            )
        )
    signals = [
        {"key": u.unit_key, "status": u.status, "reason": u.diagnostics.get("reason")}
        for u in units
    ]
    diagnostics = {
        "signals": signal_diagnostics(signals),
        "details": result.get("diagnostics", {}),
        "reason": result.get("reason") or result.get("status_reason"),
    }
    summary = PillarMiningUnit(
        "summary",
        "summary",
        status,
        signal_axis=adapter.axis,
        score=optional_decimal(result.get(adapter.score_name)),
        score_name=adapter.score_name,
        payload={"result_ref": "run.output_payload"},
        metrics=tuple(
            numeric_metrics(
                {
                    k: v
                    for k, v in result.items()
                    if k not in {"modules", "active_layers", "ignored_layers"}
                },
                group="derived",
            )
        ),
    )
    return PillarMiningRun(
        event_id=context.event_id,
        pillar_id=adapter.pillar_id,
        result_scope=adapter.result_scope,
        engine_version=result.get("engine_version")
        or result.get("raw", {}).get("engine_version")
        or "p1-input-contract-v1",
        payload_schema_version=PAYLOAD_SCHEMA_VERSION,
        status=status,
        sport=context.sport,
        evaluation_minute=context.minutes_until_start,
        target_minute=result.get("target_minute", result.get("P1_TARGET_MINUTE")),
        competition_id=getattr(context, "competition_id", None),
        output_payload=to_json_value(
            {
                **{k: v for k, v in result.items() if k != "diagnostics"},
                "status": status,
                "calculation_status": result.get("status"),
            }
        ),
        diagnostics=to_json_value(diagnostics),
        units=(summary, *units),
    )


class P1SideMiningAdapter:
    registration_key = P1_SIDE_REGISTRATION_KEY
    pillar_id = P1_PILLAR_ID
    result_scope = "side"
    axis = "SIDE"
    score_name = "value"

    def build(self, event_context, result):
        units = []
        for index, module in enumerate(result.get("modules", [])):
            key = f"module:{module['module_id']}"
            status = _module_status(module)
            raw = module.get("raw") or {}
            reason = (
                "CALCULATION_ERROR"
                if status == "ERROR"
                else "MISSING_INPUT" if status == "BLOCKED" else None
            )
            if (
                status == "BLOCKED"
                and raw.get(f"{module['module_id'].lower()}_status_reason")
                == "OUTSIDE_LATE_SEASON_WINDOW"
            ):
                reason = "NOT_APPLICABLE"
            units.append(
                PillarMiningUnit(
                    "module",
                    key,
                    status,
                    parent_unit_key="summary",
                    ordinal=index,
                    module_id=module["module_id"],
                    signal_axis="SIDE",
                    is_valid=status == "COMPUTED",
                    score_name="value",
                    score=(
                        optional_decimal(module.get("value"))
                        if status == "COMPUTED"
                        else None
                    ),
                    direction=module.get("bias"),
                    strength=module.get("strength"),
                    payload={"native_output_path": ["modules", index]},
                    diagnostics={
                        "reason": reason,
                        "evidence": {
                            "native_status": raw.get(
                                f"{module['module_id'].lower()}_status"
                            ),
                            "status_reason": raw.get(
                                f"{module['module_id'].lower()}_status_reason"
                            )
                            or raw.get("status_reason"),
                        },
                    },
                    metrics=tuple(
                        numeric_metrics(
                            {k: v for k, v in module.items() if k != "components"}
                        )
                    ),
                )
            )
            for ordinal, component in enumerate(module.get("components", [])):
                units.append(
                    PillarMiningUnit(
                        "component",
                        f"{key}:component:{ordinal}",
                        status,
                        parent_unit_key=key,
                        ordinal=ordinal,
                        signal_axis="SIDE",
                        score_name="edge",
                        score=(
                            optional_decimal(component.get("edge"))
                            if status == "COMPUTED"
                            else None
                        ),
                        direction=component.get("bias"),
                        strength=component.get("strength"),
                        payload={
                            "native_output_path": [
                                "modules",
                                index,
                                "components",
                                ordinal,
                            ]
                        },
                        diagnostics={"reason": reason},
                        metrics=tuple(numeric_metrics(component)),
                    )
                )
        if result.get("status") == "ERROR" or (
            not units and result.get("status") != "SKIPPED"
        ):
            units.append(_unavailable(result))
        return _build(self, event_context, result, units)


def _unavailable(result):
    status = "ERROR" if result.get("status") == "ERROR" else "BLOCKED"
    return PillarMiningUnit(
        "signal",
        "unavailable",
        status,
        parent_unit_key="summary",
        diagnostics={
            "reason": result.get("reason")
            or ("CALCULATION_ERROR" if status == "ERROR" else "MISSING_INPUT"),
            "evidence": result.get("evidence", {}),
        },
        payload={"signal_key": "unavailable"},
    )


class P1TotalsMiningAdapter:
    registration_key = P1_TOTALS_REGISTRATION_KEY
    pillar_id = P1_PILLAR_ID
    result_scope = "totals"
    axis = "TOTALS"
    score_name = "P1_TOTALS_COMPOSITE"

    def build(self, event_context, result):
        units = []
        for collection in ("active_layers", "ignored_layers"):
            for index, layer in enumerate(result.get(collection, [])):
                # IGNORE is a successfully calculated policy classification.
                units.append(
                    PillarMiningUnit(
                        "layer",
                        f"layer:{layer['layer']}",
                        "COMPUTED",
                        parent_unit_key="summary",
                        signal_axis="TOTALS",
                        is_valid=True,
                        score_name="final_signal",
                        score=optional_decimal(layer.get("final_signal")),
                        payload={"native_output_path": [collection, index]},
                        metrics=tuple(numeric_metrics(layer)),
                    )
                )
        if result.get("status") == "ERROR" or (
            not units and result.get("status") != "SKIPPED"
        ):
            units.append(_unavailable(result))
        return _build(self, event_context, result, units)
