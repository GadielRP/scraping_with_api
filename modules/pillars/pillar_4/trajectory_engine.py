"""Build per-series temporal features and deterministic structural readings."""

from __future__ import annotations

import logging
from typing import Iterable

from .metrics import build_temporal_features
from .models import P4SeriesInput
from .relations import build_book_exchange_relation_changes
from .signal_models import P4SeriesResult
from modules.pillars.evaluation_contracts import (
    EXECUTIVE_SUMMARY_VERSION,
    interpretation_reading,
)

logger = logging.getLogger(__name__)
INTERPRETATION_VERSION = 2
_EXECUTIVE_VALUE_TYPES = {
    "SIDE_EDGE",
    "OU_EDGE",
    "BOOK_REP_EDGE",
    "EXCHANGE_REP_EDGE",
}


def _semantic_movement(value_type, domain, value):
    if value is None:
        return None
    positive, negative, zero = {
        "ODDS_PRICE": ("CHOICE_WEAKENING", "CHOICE_STRENGTHENING", "NO_MOVEMENT"),
        "LINE": ("LINE_UP", "LINE_DOWN", "LINE_UNCHANGED"),
        "EXCHANGE_SIZE": ("SIZE_INCREASE", "SIZE_DECREASE", "SIZE_UNCHANGED"),
    }.get(value_type, (None, None, None))
    if "EDGE" in value_type:
        if domain not in ("SIDE", "TOTALS"):
            return None
        positive, negative, zero = (
            ("HOME", "AWAY", "NO_MOVEMENT")
            if domain == "SIDE"
            else ("OVER", "UNDER", "NO_MOVEMENT")
        )
    elif "GAP" in value_type or "SPREAD" in value_type:
        positive, negative, zero = "WIDENING", "NARROWING", "UNCHANGED"
    if value == "POSITIVE":
        return positive
    if value == "NEGATIVE":
        return negative
    return zero


def _contract_interpretation(group, series):
    """Summarize existing representatives, keeping separate sources explicit."""
    edges = [
        key
        for key in group["series_refs"]
        if series[key]["value_type"]
        in ("SIDE_EDGE", "OU_EDGE", "BOOK_REP_EDGE", "EXCHANGE_REP_EDGE")
    ]
    has_books_rep = any(series[key]["value_type"] == "BOOK_REP_EDGE" for key in edges)
    has_exchange_rep = any(
        series[key]["value_type"] == "EXCHANGE_REP_EDGE" for key in edges
    )
    selected = [
        key
        for key in edges
        if series[key]["value_type"] in ("BOOK_REP_EDGE", "EXCHANGE_REP_EDGE")
        or not (has_exchange_rep if series[key]["exchange_side"] else has_books_rep)
    ]
    if not edges:
        selected = [
            key
            for key in group["series_refs"]
            if series[key]["value_type"] in ("ODDS_PRICE", "LINE")
        ] or group["series_refs"]
    fields = (
        "status",
        "state",
        "reason",
        "comparability",
        "interpretation_code",
        "summary",
        "endpoint_direction",
        "net_move_direction",
        "final_run_direction",
        "path_pattern",
        "bookmaker",
        "choice_name",
        "main_line",
        "exchange_side",
        "value_type",
    )
    sources = {
        key: {
            **{name: series[key][name] for name in fields},
            "evidence": {"series_ref": key},
        }
        for key in selected
    }
    if len(sources) == 1:
        reading = dict(next(iter(sources.values())))
    else:
        computed = any(r["status"] == "COMPUTED" for r in sources.values())
        error = any(r["status"] == "ERROR" for r in sources.values())
        reading = interpretation_reading(
            "PARTIAL_STRUCTURE" if computed else "INSUFFICIENT_TRAJECTORY",
            status="COMPUTED" if computed else "ERROR" if error else "BLOCKED",
            reason=(
                None
                if computed
                else "CALCULATION_ERROR" if error else "INSUFFICIENT_OBSERVATIONS"
            ),
            comparability="PARTIAL",
            endpoint_direction=None,
            net_move_direction=None,
            final_run_direction=None,
            path_pattern=None,
        )
        reading.pop("direction")
        reading["summary"] = (
            "El contrato conserva lecturas separadas por fuente o selección."
            if computed
            else reading["summary"]
        )
        reading["source_readings"] = sources
    return {**group, **reading}


def interpret_p4_result(result):
    """Separate endpoint position from net movement and final-run movement."""
    readings, groups = {}, {}
    for key, series in result["analysis"].items():
        market = series["market"]
        raw, structural = series["raw_temporal_features"], series["structural_signals"]
        value_type, domain = market["VALUE_TYPE"], market["DOMAIN"]
        pattern = structural.get("PATH_PATTERN_RAW")
        net, final = structural.get("NET_DIRECTION_RAW"), structural.get(
            "FINAL_RUN_DIRECTION_RAW"
        )
        point_refs = result["inputs"][series["input_refs"][0]]["point_refs"]
        endpoint = result["inputs"][point_refs[-1]]["value"] if point_refs else None
        endpoint_direction = None
        if (
            "EDGE" in value_type
            and endpoint is not None
            and series["traceability"]["OPERATIVE_ENDPOINT_PRESENT"]
        ):
            endpoint_direction = (
                "NEUTRAL"
                if endpoint == 0
                else _semantic_movement(
                    value_type,
                    domain,
                    (
                        "POSITIVE"
                        if endpoint > 0
                        else "NEGATIVE" if endpoint < 0 else "ZERO"
                    ),
                )
            )
        status, reason = "COMPUTED", None
        if series["status"] != "ACTIVE":
            state, status, reason = (
                "INSUFFICIENT_TRAJECTORY",
                "BLOCKED",
                "INSUFFICIENT_OBSERVATIONS",
            )
            if not series["traceability"]["OPERATIVE_ENDPOINT_PRESENT"]:
                reason = "MISSING_ENDPOINT"
        elif pattern is None or raw.get("GAP_PRESENT_RAW"):
            state, status, reason = (
                "PARTIAL_TRAJECTORY",
                "BLOCKED",
                "NON_CONTIGUOUS_GAP",
            )
        elif pattern == "NO_MOVEMENT":
            state = "STABLE_MARKET"
        elif net in (None, "ZERO", "NEUTRAL"):
            state = "FINAL_RUN_WITHOUT_NET_MOVE"
        else:
            state = "FINAL_RUN_ALIGNED" if net == final else "FINAL_RUN_COUNTERMOVE"
        reading = interpretation_reading(
            state,
            status=status,
            reason=reason,
            comparability="COMPARABLE" if status == "COMPUTED" else "PARTIAL",
            net_move_direction=(
                _semantic_movement(value_type, domain, net)
                if status == "COMPUTED"
                else None
            ),
            domain=domain,
            value_type=value_type,
            endpoint_direction=endpoint_direction,
            final_run_direction=(
                _semantic_movement(value_type, domain, final)
                if status == "COMPUTED" and final in ("POSITIVE", "NEGATIVE")
                else None
            ),
            path_pattern=pattern,
            choice_name=market["CHOICE_NAME"],
            bookmaker=market.get("BOOKIE_NAME"),
            main_line=market.get("MAIN_LINE"),
            source_role=market.get("SOURCE_ROLE"),
            exchange_side=market.get("EXCHANGE_SIDE"),
            market_group=market["MARKET_GROUP"],
            market_period=market["MARKET_PERIOD"],
            line=market["CHOICE_GROUP"],
            view=market["VIEW"],
            evidence={
                "analysis_ref": f"analysis.{key}",
                "input_refs": series["input_refs"],
            },
        )
        reading.pop("direction")
        reading["summary"] += (
            f" Endpoint: {endpoint_direction or 'no disponible'}."
            f" Movimiento neto: {reading['net_move_direction'] or 'no disponible'}."
            f" Último tramo: {reading['final_run_direction'] or 'no disponible'}."
        )
        readings[key] = reading
        contract = (
            domain,
            market["MARKET_GROUP"],
            market["MARKET_PERIOD"],
            market.get("MARKET_NAME"),
            market["CHOICE_GROUP_KEY"],
            market["VIEW"],
        )
        group = groups.setdefault(
            contract,
            {
                "domain": domain,
                "market_group": market["MARKET_GROUP"],
                "market_period": market["MARKET_PERIOD"],
                "market_name": market.get("MARKET_NAME"),
                "line": market["CHOICE_GROUP"],
                "main_line": market.get("MAIN_LINE"),
                "choice_group_key": market["CHOICE_GROUP_KEY"],
                "view": market["VIEW"],
                "series_refs": [],
                "representative_refs": [],
            },
        )
        group["series_refs"].append(key)
        if market.get("MAIN_LINE") is True:
            group["main_line"] = True
        if value_type in ("BOOK_REP_EDGE", "EXCHANGE_REP_EDGE"):
            group["representative_refs"].append(key)
    for signal in result.get("signals", []):
        if signal["status"] == "ERROR":
            error = interpretation_reading(
                "CALCULATION_ERROR",
                status="ERROR",
                reason=signal["reason"],
                comparability="UNAVAILABLE",
                evidence={"signal_ref": signal["key"]},
                endpoint_direction=None,
                net_move_direction=None,
                final_run_direction=None,
                path_pattern=None,
            )
            error.pop("direction")
            readings[signal["key"]] = error
    return {
        "version": INTERPRETATION_VERSION,
        "status": result["status"],
        "series": readings,
        "contracts": {
            "|".join(str(value) for value in contract): _contract_interpretation(
                group, readings
            )
            for contract, group in groups.items()
        },
        "reasons": result["diagnostics"]["signals"]["reasons"],
        "summary": "Recorrido descriptivo por contrato y vista; no atribuye causalidad ni probabilidad deportiva.",
    }


def _p4_executive_source(contract):
    sources = contract.get("source_readings")
    candidates = list(sources.values()) if isinstance(sources, dict) else [contract]
    candidates = [reading for reading in candidates if reading.get("value_type") in _EXECUTIVE_VALUE_TYPES]
    if not candidates:
        return None
    rank = {
        "BOOK_REP_EDGE": 0,
        "SIDE_EDGE": 1,
        "OU_EDGE": 1,
        "EXCHANGE_REP_EDGE": 2,
    }
    candidates.sort(
        key=lambda reading: (
            reading.get("status") != "COMPUTED",
            rank[reading["value_type"]],
            reading.get("bookmaker") or "",
            reading.get("summary") or "",
        )
    )
    return candidates[0]


def _p4_executive_reading(contract, source):
    return {
        "line": contract.get("line"),
        "status": source.get("status"),
        "state": source.get("state"),
        "endpoint_direction": source.get("endpoint_direction"),
        "net_move_direction": source.get("net_move_direction"),
        "final_run_direction": source.get("final_run_direction"),
        "path_pattern": source.get("path_pattern"),
    }


def _p4_executive_contracts(interpretation, domain, market_group, period):
    candidates = []
    for contract in (interpretation.get("contracts") or {}).values():
        if (
            contract.get("view") != "CHECKPOINT_VIEW"
            or contract.get("domain") != domain
            or contract.get("market_group") != market_group
            or contract.get("market_period") != period
        ):
            continue
        source = _p4_executive_source(contract)
        if source is not None:
            candidates.append((contract, source))
    if any(contract.get("main_line") is True for contract, _ in candidates):
        candidates = [
            pair for pair in candidates if pair[0].get("main_line") is True
        ]
    elif any(contract.get("main_line") is None for contract, _ in candidates):
        candidates = [
            pair for pair in candidates if pair[0].get("main_line") is None
        ]
    else:
        candidates = []
    readings = {}
    for contract, source in candidates:
        reading = _p4_executive_reading(contract, source)
        identity = (
            reading["line"],
            reading["endpoint_direction"],
            reading["net_move_direction"],
            reading["final_run_direction"],
            reading["path_pattern"],
            reading["state"],
        )
        readings.setdefault(identity, reading)
    return list(readings.values())


def build_p4_executive_summary(result, interpretation=None):
    """Select main CHECKPOINT_VIEW contract readings from P4's detailed output."""
    interpretation = interpretation or result.get("INTERPRETATION") or interpret_p4_result(result)
    pillar_status = result.get("status")

    def unavailable_reading():
        is_error = pillar_status == "ERROR"
        return {
            "status": "ERROR" if is_error else "BLOCKED",
            "state": (
                "CALCULATION_ERROR"
                if is_error
                else "SKIPPED" if pillar_status == "SKIPPED" else "MISSING_INPUT"
            ),
            "reason": result.get("reason"),
            "line": None,
            "endpoint_direction": None,
            "net_move_direction": None,
            "final_run_direction": None,
            "path_pattern": None,
        }

    def slot(domain, market_group, period):
        readings = _p4_executive_contracts(
            interpretation, domain, market_group, period
        )
        if not readings:
            return unavailable_reading()
        return readings[0] if len(readings) == 1 else {"readings": readings}

    side = {
        "full_time_1x2": slot("SIDE", "1X2", "Full Time"),
        "full_time_asian_handicap": slot("SIDE", "Asian Handicap", "Full Time"),
        "first_half_1x2": slot("SIDE", "1X2", "1st Half"),
        "first_half_asian_handicap": slot("SIDE", "Asian Handicap", "1st Half"),
    }
    totals_full_time = _p4_executive_contracts(
        interpretation, "TOTALS", "Over/Under", "Full Time"
    )
    totals_first_half = _p4_executive_contracts(
        interpretation, "TOTALS", "Over/Under", "1st Half"
    )
    totals = {
        "full_time": (
            totals_full_time[0]
            if len(totals_full_time) == 1
            else (
                {"readings": totals_full_time}
                if totals_full_time
                else {**unavailable_reading(), "readings": []}
            )
        ),
        "first_half": (
            {"readings": totals_first_half}
            if totals_first_half
            else {**unavailable_reading(), "readings": []}
        ),
    }

    side_readings = []
    for value in side.values():
        side_readings.extend(value.get("readings", [value]))
    side_readings = [
        reading
        for reading in side_readings
        if reading.get("endpoint_direction") or reading.get("net_move_direction")
    ]
    summary_parts = []
    side_endpoints = {reading["endpoint_direction"] for reading in side_readings if reading.get("endpoint_direction")}
    side_moves = {reading["net_move_direction"] for reading in side_readings if reading.get("net_move_direction")}
    if len(side_endpoints) == len(side_moves) == 1:
        endpoint = next(iter(side_endpoints))
        move = next(iter(side_moves))
        summary_parts.append(
            f"SIDE termina y se desplaza hacia {endpoint} en los contratos principales."
            if endpoint == move
            else f"SIDE termina hacia {endpoint} y su movimiento neto va hacia {move}."
        )

    def flatten(value):
        return value.get("readings", [value]) if isinstance(value, dict) else []

    full_time_totals = flatten(totals["full_time"])
    if len(full_time_totals) == 1:
        reading = full_time_totals[0]
        line = f" {reading['line']}" if reading.get("line") is not None else ""
        endpoint = reading.get("endpoint_direction")
        move = reading.get("net_move_direction")
        final = reading.get("final_run_direction")
        if endpoint and (move or final):
            movement = move or final
            suffix = (
                f" y el último tramo también es {final}"
                if final is not None and final != movement
                else ""
            )
            summary_parts.append(
                f"FT O/U{line} termina {endpoint}; el movimiento neto es {movement}{suffix}."
            )
    first_half_totals = totals_first_half
    first_half_moves = {
        reading.get("net_move_direction")
        for reading in first_half_totals
        if reading.get("net_move_direction")
    }
    first_half_endpoints = [
        reading.get("endpoint_direction")
        for reading in first_half_totals
        if reading.get("endpoint_direction")
    ]
    if len(first_half_totals) > 1 and len(first_half_moves) == 1:
        movement = next(iter(first_half_moves))
        endpoints = set(first_half_endpoints)
        if len(endpoints) > 1:
            summary_parts.append(
                f"En 1H O/U ambas líneas se desplazan hacia {movement}, aunque sus endpoints son distintos."
            )
        else:
            summary_parts.append(f"En 1H O/U las líneas se desplazan hacia {movement}.")

    return {
        "version": EXECUTIVE_SUMMARY_VERSION,
        "status": result.get("status"),
        "side": side,
        "totals": totals,
        "summary": " ".join(summary_parts)
        or "P4 no dispone de una lectura ejecutiva calculada en los contratos principales.",
    }


def _series_result(series: P4SeriesInput) -> P4SeriesResult:
    gap_present = bool(series.missing_target_minutes)
    legs, features, signals = build_temporal_features(
        series.points,
        gap_present=gap_present,
        expected_target_minutes=series.expected_target_minutes,
    )
    if series.value_type == "EXCHANGE_SIZE":
        legs = [{**leg, "VELOCITY_RAW": None} for leg in legs]
        features = {
            **features,
            "VELOCITY_RAW": None,
            "VELOCITY_CHANGES_RAW": [],
        }
        signals = {
            **signals,
            "ACCELERATION_RAW": None,
            "DECELERATION_RAW": None,
        }
    if (
        not series.operative_endpoint_present
        and "LINE_CONTRACT_ENDED" in series.diagnostics
    ):
        status = "CONTRACT_ENDED"
    elif not series.operative_endpoint_present or len(series.points) < 2:
        status = "INSUFFICIENT_DATA"
    else:
        status = "ACTIVE"
    if status != "ACTIVE":
        # Keep counts and observation identity, but expose no artificial movement.
        features = {
            key: (
                ([] if isinstance(value, list) else None)
                if key.endswith("_RAW")
                else value
            )
            for key, value in features.items()
        }
        signals = {key: None for key in signals}
    return P4SeriesResult(
        series_id=series.series_id,
        status=status,
        market=series.market_dict(),
        points=series.points,
        legs=tuple(legs),
        raw_temporal_features=features,
        structural_signals=signals,
        traceability={
            "BASE_SERIES_ID": series.base_series_id,
            "TRAJECTORY_SOURCE_MODE": (
                "PERSISTED_SNAPSHOTS_WITH_LOGICAL_ENDPOINT"
                if series.view == "ADAPTIVE_VIEW"
                else "SOURCE_STATES_OR_NEARBY_OBSERVATIONS"
            ),
            "EXPECTED_TARGET_MINUTES": list(series.expected_target_minutes),
            "MISSING_TARGET_MINUTES": list(series.missing_target_minutes),
            "DIAGNOSTICS": list(series.diagnostics),
            "CONSTITUENT_SERIES_IDS": list(series.constituent_series_ids),
            "OPERATIVE_ENDPOINT_PRESENT": series.operative_endpoint_present,
            "OBSERVATION_COUNT": len(series.points),
            "LEG_COUNT": len(legs),
        },
    )


def build_trajectory_features(
    series_inputs: Iterable[P4SeriesInput],
    *,
    apply_relations: bool,
    debug_mode: bool = False,
) -> tuple[P4SeriesResult, ...]:
    results = []
    for series in series_inputs:
        try:
            results.append(_series_result(series))
        except Exception as exc:
            if debug_mode:
                logger.exception(
                    "P4 FORMULA | %s | calculation failed error_class=%s",
                    series.series_id,
                    type(exc).__name__,
                )
            results.append(
                P4SeriesResult(
                    series_id=series.series_id,
                    market=series.market_dict(),
                    points=series.points,
                    legs=(),
                    raw_temporal_features={},
                    structural_signals={},
                    status="ERROR",
                    traceability={
                        "OPERATIVE_ENDPOINT_PRESENT": series.operative_endpoint_present,
                        "OBSERVATION_COUNT": len(series.points),
                        "MISSING_TARGET_MINUTES": list(series.missing_target_minutes),
                        "ERROR_CLASS": type(exc).__name__,
                    },
                )
            )
    if apply_relations:
        # Relations need checkpoint values only; never serialize provenance here.
        payloads = [
            {
                "SERIES_ID": result.series_id,
                "MARKET": result.market,
                "POINTS": [
                    {"TARGET_MINUTE": point.target_minute, "VALUE": point.value}
                    for point in result.points
                ],
            }
            for result in results
            if result.status != "ERROR"
        ]
        relation_by_series = build_book_exchange_relation_changes(payloads)
        updated: list[P4SeriesResult] = []
        for result in results:
            signals = dict(result.structural_signals)
            if result.status == "ACTIVE":
                signals["BOOK_EXCHANGE_RELATION_CHANGE_RAW"] = relation_by_series.get(
                    result.series_id
                )
            updated.append(
                P4SeriesResult(
                    series_id=result.series_id,
                    market=result.market,
                    points=result.points,
                    legs=result.legs,
                    raw_temporal_features=result.raw_temporal_features,
                    structural_signals=signals,
                    traceability=result.traceability,
                    status=result.status,
                )
            )
        results = updated
    return tuple(results)


__all__ = ["build_trajectory_features"]
