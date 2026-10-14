"""Human-readable summaries of committed pillar outputs, without input payloads."""

from collections import Counter
from collections.abc import Mapping
from logging import INFO


_SCOPES = {
    "pillar_1": "P1/SIDE",
    "pillar_1_totals": "P1/TOTALS",
    "pillar_2": "P2",
    "pillar_3": "P3",
    "pillar_4": "P4",
    "pillar_5": "P5",
}


def _scalars(values):
    return {
        key: value
        for key, value in values.items()
        if not isinstance(value, (Mapping, list, tuple, set))
    }


def _rows(values, indent=2):
    rows = []
    for key, value in values.items():
        prefix = " " * indent + str(key)
        if isinstance(value, Mapping):
            rows.append(prefix + ":")
            rows.extend(_rows(value, indent + 2))
        else:
            rows.append(f"{prefix}: {value if value is not None else 'no disponible'}")
    return rows


def log_pillar_results(
    logger, *, evaluation_id, event_id, evaluation_as_of, participants, results,
    enabled_pillars,
):
    """Summarize enabled scopes after the caller commits persistence."""
    if not logger.isEnabledFor(INFO):
        return
    for key, label in _SCOPES.items():
        pillar_key = "pillar_1" if key == "pillar_1_totals" else key
        if not enabled_pillars.get(pillar_key, True):
            continue
        result = results[key]
        diagnostics = result.get("diagnostics") or {}
        summary = {
            "evaluation_id": evaluation_id,
            "event_id": event_id,
            "participants": participants,
            "evaluation_as_of": evaluation_as_of.isoformat(),
            "status": result["status"],
            "reason": result.get("reason")
            or result.get("status_reason")
            or diagnostics.get("reason"),
            "signals": diagnostics.get("signals", {}),
        }
        details = diagnostics.get("details")
        if isinstance(details, Mapping):
            details = [details]
        if isinstance(details, list):
            reasons = [
                item["reason"] for item in details
                if isinstance(item, Mapping) and item.get("reason")
            ]
            if reasons:
                summary["diagnostic_reasons"] = dict(Counter(reasons))
        if key in {"pillar_1", "pillar_1_totals"}:
            summary["final"] = {
                field: value
                for field, value in _scalars(result).items()
                if field not in {"status", "reason", "status_reason"}
            }
            raw = result.get("raw") or {}
            summary["aggregation"] = {
                **_scalars(raw), "final": raw.get("final", {})
            }
        else:
            summary["target_minute"] = result.get("target_minute")
            summary["execution_status"] = result.get("execution_status")
            summary["selection"] = _scalars(result.get("selection") or {})
            summary["coverage"] = dict(
                Counter(item["status"] for item in result.get("coverage", []))
            )
            markets = {}
            for scope, profile in result.get("analysis", {}).items():
                markets[scope] = {
                    **_scalars(profile),
                    "market": _scalars(profile.get("market") or {}),
                }
                if "current_price_vector" in profile:
                    markets[scope]["current_price_vector"] = profile[
                        "current_price_vector"
                    ]
            for signal in result.get("signals", []):
                evidence = signal.get("evidence") or {}
                scope = evidence.get("scope", "signals")
                name = evidence.get("metric", signal["key"])
                markets.setdefault(scope, {})[name] = {
                    "status": signal["status"],
                    "value": signal.get("value"),
                    "reason": signal.get("reason"),
                }
            summary["results_by_market"] = markets
        logger.info(
            "🏁 %s RESULT — RESUMEN FINAL\n%s\n%s",
            label, "\n".join(_rows(summary)), "-" * 72,
        )
