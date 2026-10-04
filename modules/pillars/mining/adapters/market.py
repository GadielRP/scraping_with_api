"""One writer contract for versioned market results, without profile copies."""

from decimal import Decimal
from hashlib import sha256
from modules.pillars.evaluation_contracts import PAYLOAD_SCHEMA_VERSION
from ..contracts import PillarMiningRun, PillarMiningUnit, PillarMiningMetric
from ..execution_slot import build_execution_slot
from ..serialization import to_json_value
from ..status_policy import normalize_status


class MarketMiningAdapter:
    pillar_id: str
    result_scope: str

    def build(self, event_context, result):
        if result.get("payload_schema_version") != PAYLOAD_SCHEMA_VERSION:
            raise ValueError(
                "market writers require schema v4; historical payloads must not be rewritten"
            )
        producer, canonical = normalize_status(result["status"])
        target = result["target_minute"]
        evaluation = getattr(event_context, "minutes_until_start", None)
        units = [
            PillarMiningUnit(
                "summary",
                "summary",
                producer,
                canonical,
                payload={"result_ref": "run.output_payload"},
                target_minute=target,
            )
        ]
        output = {
            key: value
            for key, value in result.items()
            if key not in {"inputs", "signals"}
        }
        output["signal_unit_refs"] = []
        for ordinal, signal in enumerate(result["signals"], 1):
            key = "signal_" + sha256(signal["key"].encode()).hexdigest()
            output["signal_unit_refs"].append(key)
            status = {
                "COMPUTED": "ACTIVE",
                "BLOCKED": "INSUFFICIENT_DATA",
                "ERROR": "ERROR",
            }[signal["status"]]
            source_status, unit_status = normalize_status(status)
            value = signal["value"]
            metric = ()
            if isinstance(value, bool):
                metric = (PillarMiningMetric("value", "boolean", value),)
            elif isinstance(value, (int, float, Decimal)):
                metric = (PillarMiningMetric("value", "number", Decimal(str(value))),)
            elif isinstance(value, str):
                metric = (PillarMiningMetric("value", "text", value),)
            refs = signal["contract_refs"]
            related = [result["contracts"].get(ref, {}) for ref in refs]

            def common(field):
                values = {contract.get(field) for contract in related}
                return next(iter(values)) if len(values) == 1 else None

            contract = {
                field: common(field)
                for field in (
                    "market_group",
                    "market_period",
                    "line_value",
                    "market_type_id",
                )
            }
            traces = [
                result["inputs"].get(ref, {}).get("trace") or {}
                for ref in signal["input_refs"]
            ]
            books = {
                trace["bookie_id"]
                for trace in traces
                if trace.get("bookie_id") is not None
            }
            market = signal.get("evidence", {}).get("market", {})

            def trace_common(field):
                values = {trace.get(field) for trace in traces}
                return next(iter(values)) if len(values) == 1 else None

            units.append(
                PillarMiningUnit(
                    "signal",
                    key,
                    source_status,
                    unit_status,
                    parent_unit_key="summary",
                    ordinal=ordinal,
                    is_valid=signal["status"] == "COMPUTED",
                    target_minute=target,
                    bookie_id=(
                        next(iter(books))
                        if len(books) == 1
                        else market.get("BOOKIE_ID")
                    ),
                    market_group=contract.get("market_group")
                    or market.get("MARKET_GROUP"),
                    market_period=contract.get("market_period")
                    or market.get("MARKET_PERIOD"),
                    line_value=contract.get("line_value") or market.get("CHOICE_GROUP"),
                    dimensions={
                        "contract_refs": refs,
                        "market_type_id": contract.get("market_type_id"),
                    },
                    source=trace_common("source"),
                    exchange_side=trace_common("exchange_side"),
                    exchange_level=trace_common("exchange_level"),
                    quote_id=trace_common("quote_id"),
                    payload={
                        "signal_key": signal["key"],
                        "input_refs": signal["input_refs"],
                        "contract_refs": refs,
                    },
                    diagnostics={
                        "reason": signal["reason"],
                        "evidence": signal["evidence"],
                    },
                    metrics=metric,
                )
            )
        competition = getattr(event_context, "competition", None)
        return PillarMiningRun(
            event_id=event_context.event_id,
            pillar_id=self.pillar_id,
            result_scope=self.result_scope,
            execution_slot=build_execution_slot(evaluation, target),
            engine_version=result["engine_version"],
            payload_schema_version=PAYLOAD_SCHEMA_VERSION,
            producer_status=producer,
            canonical_status=canonical,
            sport=event_context.sport,
            evaluation_minute=evaluation,
            target_minute=target,
            competition_id=getattr(event_context, "competition_id", None)
            or getattr(competition, "competition_id", None),
            context=to_json_value(
                {
                    "event_id": event_context.event_id,
                    "starts_at": getattr(event_context, "starts_at", None),
                }
            ),
            inputs=to_json_value(result["inputs"]),
            output_payload=to_json_value(output),
            units=tuple(units),
        )
