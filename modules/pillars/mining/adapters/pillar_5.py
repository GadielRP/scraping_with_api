"""Project P5 bookmaker memories into the common mining contract."""

from __future__ import annotations

from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.pillar_5.run_pillar_5 import ENGINE_VERSION

from ..contracts import PillarMiningRun, PillarMiningUnit
from ..execution_slot import build_execution_slot
from ..serialization import optional_decimal, scalar_metric, to_json_value
from ..status_policy import normalize_status


def _dictionary(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class P5MiningAdapter:
    """Persist independent P5 profiles without aggregating their scores."""

    pillar_id = "pillar_5"
    result_scope = "exact_price_memory"

    def build(
        self,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> PillarMiningRun:
        raw = _dictionary(result.get("raw"))
        profiles = _dictionary(result.get("P5_MEMORY_PROFILES"))
        target_minute = _optional_int(result.get("P5_TARGET_MINUTE"))
        evaluation_minute = _optional_int(
            getattr(event_context, "minutes_until_start", None)
        )
        producer_status, canonical_status = normalize_status(
            result.get("P5_STATUS") or result.get("status") or "ERROR"
        )
        competition = getattr(event_context, "competition", None)
        competition_id = _optional_int(
            getattr(event_context, "competition_id", None)
            or getattr(competition, "competition_id", None)
        )
        competition_name = (
            getattr(event_context, "competition_name", None)
            or getattr(competition, "display_name", None)
        )
        diagnostics = to_json_value(
            {
                "abort_reason": raw.get("abort_reason"),
                "error": result.get("error"),
                "missing_inputs": result.get("MISSING_INPUTS", []),
                "invalid_inputs": result.get("INVALID_INPUTS", []),
                "ambiguous_inputs": result.get("AMBIGUOUS_INPUTS", []),
                "periods": result.get("PERIODS") or raw.get("periods") or {},
                "extraction_status": result.get("P5_EXTRACTION_STATUS"),
                "extraction_diagnostics": raw.get("extraction_diagnostics", {}),
                "memory_diagnostics": raw.get("memory_diagnostics", {}),
            }
        )
        context = to_json_value(
            {
                "event_id": event_context.event_id,
                "participants": event_context.participants_label,
                "sport": event_context.sport,
                "competition_id": competition_id,
                "competition": competition_name,
                "minutes_to_start": evaluation_minute,
                "P5_TARGET_MINUTE": target_minute,
                "event_starts_at": getattr(event_context, "starts_at", None),
                "context_status": getattr(event_context, "context_status", None),
            }
        )

        summary = PillarMiningUnit(
            unit_type="summary",
            unit_key="summary",
            producer_status=producer_status,
            canonical_status=canonical_status,
            ordinal=0,
            target_minute=target_minute,
            market_group="1X2/Home/Away",
            payload={"P5_MEMORY_PROFILES": to_json_value(profiles)},
            diagnostics=diagnostics,
        )
        module = PillarMiningUnit(
            unit_type="module",
            unit_key="p5_memory_engine",
            parent_unit_key="summary",
            ordinal=1,
            module_id="p5_memory_engine",
            producer_status=producer_status,
            canonical_status=canonical_status,
            target_minute=target_minute,
            market_group="1X2/Home/Away",
            payload={"P5_MEMORY_PROFILES": to_json_value(profiles)},
            diagnostics=diagnostics,
        )
        units: list[PillarMiningUnit] = [summary, module]

        for ordinal, (bookmaker, profile_value) in enumerate(profiles.items(), start=2):
            profile = _dictionary(profile_value)
            profile_producer, profile_canonical = normalize_status(
                profile.get("P5_STATUS") or "INSUFFICIENT_DATA"
            )
            metrics = tuple(
                metric
                for metric in (
                    scalar_metric("P5", profile.get("P5"), group="score"),
                    scalar_metric("sample_size", profile.get("sample_size"), group="sample"),
                    scalar_metric("wins_home", profile.get("wins_home"), group="sample"),
                    scalar_metric("wins_draw", profile.get("wins_draw"), group="sample"),
                    scalar_metric("wins_away", profile.get("wins_away"), group="sample"),
                    scalar_metric("HIST_EDGE", profile.get("HIST_EDGE"), group="calculation"),
                    scalar_metric("MSRI_RAW", profile.get("MSRI_RAW"), group="calculation"),
                    scalar_metric("MSRI_SIGNAL", profile.get("MSRI_SIGNAL"), group="calculation"),
                    scalar_metric("SAMPLE_WEIGHT", profile.get("SAMPLE_WEIGHT"), group="calculation"),
                )
                if metric is not None
            )
            units.append(
                PillarMiningUnit(
                    unit_type="bookmaker",
                    unit_key=f"bookmaker:{profile.get('bookie_id')}",
                    parent_unit_key="p5_memory_engine",
                    ordinal=ordinal,
                    module_id="p5_memory_engine",
                    producer_status=profile_producer,
                    canonical_status=profile_canonical,
                    is_valid=bool(profile.get("P5_VALID", False)),
                    score_name="P5",
                    score=optional_decimal(profile.get("P5")),
                    direction=str(profile.get("P5_DIRECTION") or "NONE"),
                    strength=str(profile.get("P5_STRENGTH") or "NONE"),
                    target_minute=_optional_int(profile.get("target_minute")),
                    market_group=profile.get("market_group"),
                    market_period=profile.get("market_period"),
                    bookie_id=_optional_int(profile.get("bookie_id")),
                    dimensions={
                        "bookmaker": bookmaker,
                        "market_shape": profile.get("market_shape"),
                    },
                    payload=to_json_value(profile),
                    diagnostics=to_json_value(
                        {
                            "reason": profile.get("reason"),
                            "memory_status": profile.get("memory_status"),
                            "diagnostics": profile.get("diagnostics", {}),
                        }
                    ),
                    metrics=metrics,
                )
            )

        betfair = _dictionary(raw.get("betfair_exposure"))
        betfair_inputs = _dictionary(betfair.get("inputs"))
        betfair_active = any(value is not None for value in betfair_inputs.values())
        betfair_producer, betfair_canonical = normalize_status(
            "ACTIVE" if betfair_active else "INSUFFICIENT_DATA"
        )
        units.append(
            PillarMiningUnit(
                unit_type="bookmaker",
                unit_key="bookmaker:4",
                parent_unit_key="p5_memory_engine",
                ordinal=len(units),
                module_id="p5_memory_engine",
                producer_status=betfair_producer,
                canonical_status=betfair_canonical,
                is_valid=False,
                score_name=None,
                score=None,
                direction=None,
                strength=None,
                target_minute=target_minute,
                market_group="1X2/Home/Away",
                bookie_id=4,
                dimensions={"bookmaker": "betfair", "diagnostic_only": True},
                payload=to_json_value(betfair),
                diagnostics={"participates_in_score": False},
            )
        )

        return PillarMiningRun(
            event_id=int(event_context.event_id),
            pillar_id=self.pillar_id,
            result_scope=self.result_scope,
            execution_slot=build_execution_slot(evaluation_minute, target_minute),
            engine_version=str(result.get("engine_version") or ENGINE_VERSION),
            payload_schema_version=3,
            producer_status=producer_status,
            canonical_status=canonical_status,
            sport=str(event_context.sport),
            evaluation_minute=evaluation_minute,
            target_minute=target_minute,
            competition_id=competition_id,
            context=context,
            inputs=to_json_value(_dictionary(raw.get("inputs"))),
            diagnostics=diagnostics,
            output_payload=to_json_value(result),
            units=tuple(units),
        )


__all__ = ["P5MiningAdapter"]
