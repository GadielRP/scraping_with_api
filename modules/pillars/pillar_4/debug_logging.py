"""Human-readable debug logging for the P4 extraction and signal profile."""

from __future__ import annotations

import logging
from typing import Any

from .models import P4ExtractionResult, P4Point, P4SeriesInput


_TEMPORAL_FORMULAS = {
    "OBSERVATION_COUNT": "count(POINTS)",
    "LEG_COUNT": "count(LEGS)",
    "SIGN_SEQUENCE_RAW": "[sign(leg.DELTA_RAW) for leg in LEGS]",
    "NET_MOVE_RAW": "final.VALUE - initial.VALUE",
    "PATH_LENGTH_RAW": "sum(abs(leg.DELTA_RAW) for contiguous leg in LEGS)",
    "PATH_EFFICIENCY_RAW": "abs(NET_MOVE_RAW) / PATH_LENGTH_RAW",
    "NO_MOVEMENT_RAW": "complete_path and PATH_LENGTH_RAW == 0",
    "ELAPSED_MINUTES_ACTUAL": "final.EFFECTIVE_AT - initial.EFFECTIVE_AT",
    "VELOCITY_RAW": "NET_MOVE_RAW / ELAPSED_MINUTES_ACTUAL",
    "DIRECTIONAL_RUNS": "maximal same-sign runs over contiguous legs",
    "VELOCITY_CHANGES_RAW": "current.VELOCITY_RAW - previous.VELOCITY_RAW",
    "GAP_PRESENT_RAW": "missing checkpoint or non-contiguous leg exists",
    "WINDOWS": "group contiguous legs by end-observation time window",
    "DOMINANT_SEGMENTS_RAW": "argmax_all(abs(leg.DELTA_RAW))",
    "DOMINANT_WINDOWS_RAW": "argmax_all(window.PATH_LENGTH_RAW)",
}

_SEMANTIC_FORMULAS = {
    "SIDE_EDGE": "((1 / left) - (1 / right)) / ((1 / left) + (1 / right))",
    "OU_EDGE": "((1 / left) - (1 / right)) / ((1 / left) + (1 / right))",
    "BOOK_REP_EDGE": "(left + right) / 2",
    "EXCHANGE_REP_EDGE": "(left + right) / 2",
    "BOOK_INTERNAL_GAP": "abs(left - right)",
    "EXCHANGE_INTERNAL_GAP": "abs(left - right)",
    "BOOK_EXCHANGE_GAP": "abs(left - right)",
    "BACK_LAY_RELATIVE_SPREAD": "(right - left) / ((right + left) / 2)",
}


def _log_formula(
    logger: logging.Logger,
    name: str,
    formula: str,
    substitution: Any,
    result: Any,
) -> None:
    logger.info("P4 FORMULA | %s | formula=%s", name, formula)
    logger.info("P4 FORMULA | %s | substitution=%s", name, substitution)
    logger.info("P4 FORMULA | %s | result=%s", name, result)


def _log_extraction_point(
    logger: logging.Logger,
    series: P4SeriesInput,
    point: P4Point,
) -> None:
    logger.info(
        "P4 DEBUG | input assignment | view=%s | series_id=%s | "
        "point_id=%s | value_type=%s | value=%s",
        series.view,
        series.series_id,
        point.point_id,
        series.value_type,
        point.value,
    )
    logger.info(
        "P4 DEBUG | input lineage | series_id=%s | point_id=%s | "
        "target=%s | snapshot=%s | quote=%s | observation_kind=%s",
        series.series_id,
        point.point_id,
        point.target_minute,
        point.snapshot_id,
        point.quote_id,
        point.observation_kind,
    )
    logger.info(
        "P4 DEBUG | input lineage | series_id=%s | point_id=%s | "
        "effective_at=%s | availability_at=%s | collected_at=%s | "
        "source_collected_at=%s | minutes_before_start=%s",
        series.series_id,
        point.point_id,
        point.effective_at.isoformat(),
        point.availability_at.isoformat(),
        None if point.collected_at is None else point.collected_at.isoformat(),
        (
            None
            if point.source_collected_at is None
            else point.source_collected_at.isoformat()
        ),
        point.minutes_before_start,
    )


def log_p4_extraction(
    logger: logging.Logger,
    extraction: P4ExtractionResult,
) -> None:
    """Log causal selection, diagnostics and every normalized input point."""
    logger.info(
        "P4 DEBUG | extraction | event_id=%s | target_minute=%s | "
        "operative_as_of=%s | usable=%s | reason=%s",
        extraction.event_id,
        extraction.target_minute,
        extraction.operative_as_of.isoformat(),
        extraction.usable,
        extraction.reason,
    )
    logger.info(
        "P4 DEBUG | extraction counts | source_series_seen=%s | "
        "endpoint_series_present=%s | excluded_future_points=%s | "
        "adaptive_series=%s | checkpoint_series=%s",
        extraction.source_series_seen,
        extraction.endpoint_series_present,
        extraction.excluded_future_points,
        len(extraction.adaptive_series),
        len(extraction.checkpoint_series),
    )
    for category, values in (
        ("missing", extraction.missing_inputs),
        ("invalid", extraction.invalid_inputs),
        ("ambiguous", extraction.ambiguous_inputs),
    ):
        logger.info("P4 DEBUG | extraction diagnostics | %s=%s", category, list(values))
    for period, diagnostics in extraction.periods.items():
        logger.info(
            "P4 EXTRACTION | event_id=%s | target_minute=%s | period=%s | "
            "status=%s | diagnostics=%s",
            extraction.event_id,
            extraction.target_minute,
            period,
            diagnostics.get("status"),
            diagnostics,
        )
    for series in (*extraction.adaptive_series, *extraction.checkpoint_series):
        logger.info(
            "P4 DEBUG | input series | view=%s | series_id=%s | "
            "value_type=%s | points=%s | endpoint_present=%s | "
            "missing_targets=%s | diagnostics=%s",
            series.view,
            series.series_id,
            series.value_type,
            len(series.points),
            series.operative_endpoint_present,
            list(series.missing_target_minutes),
            list(series.diagnostics),
        )
        logger.info(
            "P4 DEBUG | input series lineage | series_id=%s | domain=%s | "
            "market_group=%s | period=%s | market_name=%s | line_value=%s | "
            "choice=%s | bookie_id=%s | bookie=%s | source=%s | "
            "exchange_side=%s | level=%s",
            series.series_id,
            series.domain,
            series.market_group,
            series.market_period,
            series.market_name,
            series.line_value,
            series.choice_name,
            series.bookie_id,
            series.bookie_name,
            series.source,
            series.exchange_side,
            series.exchange_level,
        )
        for point in series.points:
            _log_extraction_point(logger, series, point)


def _point_by_id(series: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(point["POINT_ID"]): point for point in series.get("POINTS", ())}


def _point_by_target(series: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {
        int(point["TARGET_MINUTE"]): point
        for point in series.get("POINTS", ())
        if point.get("TARGET_MINUTE") is not None
    }


def _log_semantic_formulas(
    logger: logging.Logger,
    series: dict[str, Any],
    series_by_id: dict[str, dict[str, Any]],
) -> None:
    market = series.get("MARKET") or {}
    value_type = str(market.get("VALUE_TYPE"))
    formula = _SEMANTIC_FORMULAS.get(value_type)
    constituent_ids = (series.get("TRACEABILITY") or {}).get(
        "CONSTITUENT_SERIES_IDS"
    ) or []
    if formula is None or len(constituent_ids) != 2:
        return
    left = series_by_id.get(str(constituent_ids[0]))
    right = series_by_id.get(str(constituent_ids[1]))
    if left is None or right is None:
        return
    left_points = _point_by_target(left)
    right_points = _point_by_target(right)
    for point in series.get("POINTS", ()):
        target = point.get("TARGET_MINUTE")
        if target is None or int(target) not in left_points or int(target) not in right_points:
            continue
        left_value = left_points[int(target)].get("VALUE")
        right_value = right_points[int(target)].get("VALUE")
        name = f"{series['SERIES_ID']}.TARGET_{target}.{value_type}"
        _log_formula(
            logger,
            name,
            formula,
            f"left={left_value}, right={right_value}",
            point.get("VALUE"),
        )


def _log_series_formulas(logger: logging.Logger, series: dict[str, Any]) -> None:
    series_id = str(series["SERIES_ID"])
    points = _point_by_id(series)
    for leg in series.get("LEGS", ()):
        prefix = f"{series_id}.LEG.{leg['LEG_ID']}"
        start = points.get(str(leg.get("FROM_POINT_ID")), {})
        end = points.get(str(leg.get("TO_POINT_ID")), {})
        _log_formula(
            logger,
            f"{prefix}.DELTA_RAW",
            "end.VALUE - start.VALUE",
            f"{end.get('VALUE')} - {start.get('VALUE')}",
            leg.get("DELTA_RAW"),
        )
        _log_formula(
            logger,
            f"{prefix}.VELOCITY_RAW",
            "DELTA_RAW / ELAPSED_MINUTES_ACTUAL when leg is contiguous",
            (
                f"delta={leg.get('DELTA_RAW')}, "
                f"elapsed={leg.get('ELAPSED_MINUTES_ACTUAL')}, "
                f"contiguous={leg.get('CONTIGUOUS_RAW')}"
            ),
            leg.get("VELOCITY_RAW"),
        )
    features = series.get("RAW_TEMPORAL_FEATURES") or {}
    feature_context = {
        "point_values": [point.get("VALUE") for point in series.get("POINTS", ())],
        "leg_deltas": [leg.get("DELTA_RAW") for leg in series.get("LEGS", ())],
        "leg_velocities": [
            leg.get("VELOCITY_RAW") for leg in series.get("LEGS", ())
        ],
        "contiguous": [
            leg.get("CONTIGUOUS_RAW") for leg in series.get("LEGS", ())
        ],
    }
    for field, formula in _TEMPORAL_FORMULAS.items():
        if field not in features:
            continue
        _log_formula(
            logger,
            f"{series_id}.{field}",
            formula,
            feature_context,
            features[field],
        )


def log_p4_signal_profile(logger: logging.Logger, profile: dict[str, Any]) -> None:
    """Log formulas and final structural readings for the serialized profile."""
    views = {
        name: profile.get(name)
        for name in ("ADAPTIVE_VIEW", "CHECKPOINT_VIEW")
    }
    all_series = [
        series
        for view in views.values()
        if isinstance(view, dict)
        for series in view.get("SERIES", ())
    ]
    series_by_id = {str(series["SERIES_ID"]): series for series in all_series}
    for view_name, view in views.items():
        if not isinstance(view, dict):
            logger.info("P4 SIGNAL | %s | value=None", view_name)
            continue
        logger.info(
            "P4 SIGNAL | %s | source_mode=%s | status=%s | series_count=%s",
            view_name,
            view.get("SOURCE_MODE"),
            view.get("STATUS"),
            len(view.get("SERIES", ())),
        )
        for series in view.get("SERIES", ()):
            series_id = str(series["SERIES_ID"])
            market = series.get("MARKET") or {}
            logger.info(
                "P4 SIGNAL | %s | series=%s | status=%s | domain=%s | "
                "value_type=%s | points=%s | legs=%s",
                view_name,
                series_id,
                series.get("STATUS"),
                market.get("DOMAIN"),
                market.get("VALUE_TYPE"),
                len(series.get("POINTS", ())),
                len(series.get("LEGS", ())),
            )
            _log_semantic_formulas(logger, series, series_by_id)
            _log_series_formulas(logger, series)
            for field, value in (series.get("STRUCTURAL_SIGNALS") or {}).items():
                logger.info(
                    "P4 SIGNAL | %s.%s | field=%s | value=%s",
                    view_name,
                    series_id,
                    field,
                    value,
                )
            for field, value in (series.get("TRACEABILITY") or {}).items():
                logger.info(
                    "P4 DEBUG | series traceability | view=%s | series_id=%s | "
                    "field=%s | value=%s",
                    view_name,
                    series_id,
                    field,
                    value,
                )
    for domain, summary in profile.get("STRUCTURAL_DOMAIN_SUMMARY", {}).items():
        logger.info(
            "P4 SIGNAL | STRUCTURAL_DOMAIN_SUMMARY.%s | series_ids=%s | "
            "views=%s | structural_signals=%s",
            domain,
            summary.get("SERIES_IDS"),
            summary.get("VIEWS"),
            summary.get("STRUCTURAL_SIGNALS"),
        )
    logger.info("P4 SIGNAL | SUMMARY | value=%s", profile.get("SUMMARY"))
    logger.info("P4 DEBUG | profile traceability | value=%s", profile.get("TRACEABILITY"))


__all__ = ["log_p4_extraction", "log_p4_signal_profile"]
