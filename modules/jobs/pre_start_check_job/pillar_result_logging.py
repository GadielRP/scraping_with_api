"""Human-readable summaries of committed pillar outputs, without input payloads."""

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
        if key in {
            "evidence",
            "interpretation_code",
            "series_refs",
            "representative_refs",
        }:
            continue
        prefix = " " * indent + str(key)
        if isinstance(value, Mapping):
            if "interpretation_code" in value:
                fields = "; ".join(
                    f"{name}={item}"
                    for name, item in _scalars(value).items()
                    if name not in {"interpretation_code", "summary"}
                    and item is not None
                )
                rows.append(f"{prefix}: {value['summary']} [{fields}]")
                if value.get("source_readings"):
                    rows.extend(_rows(value["source_readings"], indent + 2))
                if value.get("contract_readings"):
                    rows.extend(
                        _rows(
                            {
                                f"contract_{i + 1}": reading
                                for i, reading in enumerate(value["contract_readings"])
                            },
                            indent + 2,
                        )
                    )
            else:
                rows.append(prefix + ":")
                rows.extend(_rows(value, indent + 2))
        elif isinstance(value, (list, tuple)) and value and all(
            isinstance(item, Mapping) for item in value
        ):
            rows.append(" " * indent + str(key) + ":")
            for index, item in enumerate(value, 1):
                rows.append(" " * (indent + 2) + f"[{index}]")
                rows.extend(_rows(item, indent + 4))
        else:
            rows.append(f"{prefix}: {value if value is not None else 'no disponible'}")
    return rows


def log_pillar_results(
    logger,
    *,
    evaluation_id,
    event_id,
    evaluation_as_of,
    participants,
    results,
    enabled_pillars,
    debug_mode=False,
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
            "status": result.get("status"),
            "signals": diagnostics.get("signals", {}),
            "target_minute": result.get(
                "target_minute", result.get("P1_TARGET_MINUTE")
            ),
            "EXECUTIVE_SUMMARY": result.get("EXECUTIVE_SUMMARY"),
        }
        if debug_mode and result.get("INTERPRETATION"):
            summary["INTERPRETATION_DETAIL"] = result["INTERPRETATION"]
        header = (
            f"🏁 {label} RESULT — RESUMEN FINAL"
            f" | evaluation_id={evaluation_id}"
            f" | event_id={event_id}"
            f" | participants={participants}"
            f" | evaluation_as_of={evaluation_as_of.isoformat()}"
        )
        logger.info(
            "%s\n%s\n%s",
            header,
            "\n".join(_rows(summary)),
            "-" * 72,
        )
