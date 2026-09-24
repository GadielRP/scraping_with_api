"""Public orchestrator for Pillar 5 Exact Price Memory."""

from __future__ import annotations

import logging
from typing import Any

from infrastructure.settings import Config
from modules.pillars.context import EventContext, EventIdentity
from modules.pillars.extraction_logging import log_extraction_diagnostics
from modules.pillars.market_snapshot_extractor import (
    TargetMinuteSelection,
    select_target_minute,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from shared.temporal import as_utc

from .calculation_models import BookmakerMemoryProfile, PopulationFilters
from .memory_sample import build_memory_query_key, build_memory_sample
from .memory_score import (
    calculate_memory_profile,
    error_profile,
    not_eligible_profile,
)
from .models import P5ExtractionResult
from .periods import P5_PRICE_MEMORY_PERIOD_SCOPES
from .snapshot_policy import (
    BET365_BOOKIE_ID,
    PINNACLE_BOOKIE_ID,
    SOFASCORE_BOOKIE_ID,
    extract_p5_market_snapshot,
)

logger = logging.getLogger(__name__)

ENGINE_VERSION = "p5_price_memory_v3_0"

_STANDARD_BOOKMAKERS = (
    ("sofascore", SOFASCORE_BOOKIE_ID, "sofascore"),
    ("pinnacle", PINNACLE_BOOKIE_ID, "pinnacle"),
    ("bet365", BET365_BOOKIE_ID, "bet365"),
)


def _empty_inputs() -> dict[str, float | None]:
    return {
        name: None
        for scope in P5_PRICE_MEMORY_PERIOD_SCOPES
        for name in scope.input_names()
    }


def _memory_repository():
    """Composition boundary kept private so calculation tests stay DB-neutral."""
    from infrastructure.persistence.database import db_manager
    from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
        Pillar5PriceMemoryRepository,
    )

    return Pillar5PriceMemoryRepository(db_manager.SessionLocal)


def _population_filters(
    event_context: EventIdentity | EventContext,
) -> tuple[PopulationFilters, dict[str, bool], tuple[str, ...]]:
    enabled = {
        "competition_id": bool(
            getattr(Config, "P5_PRICE_MEMORY_FILTER_BY_COMPETITION", False)
        ),
        "season_id": bool(getattr(Config, "P5_PRICE_MEMORY_FILTER_BY_SEASON", False)),
        "country": bool(getattr(Config, "P5_PRICE_MEMORY_FILTER_BY_COUNTRY", False)),
    }
    competition = getattr(event_context, "competition", None)
    competition_id = (
        getattr(event_context, "competition_id", None)
        or getattr(competition, "competition_id", None)
    )
    values = {
        "competition_id": competition_id,
        "season_id": getattr(event_context, "season_id", None),
        "country": getattr(event_context, "country", None),
    }
    missing = tuple(
        name for name, is_enabled in enabled.items() if is_enabled and values[name] is None
    )
    return (
        PopulationFilters(
            competition_id=values["competition_id"] if enabled["competition_id"] else None,
            season_id=values["season_id"] if enabled["season_id"] else None,
            country=values["country"] if enabled["country"] else None,
        ),
        enabled,
        missing,
    )


def _global_status(
    profiles: dict[str, BookmakerMemoryProfile],
    *,
    extraction_status: str,
) -> str:
    values = list(profiles.values())
    valid_count = sum(profile.p5_valid for profile in values)
    if valid_count:
        if valid_count == len(values) and extraction_status == "ACTIVE":
            return "ACTIVE"
        return "PARTIAL"
    if any(profile.p5_status == "ERROR" for profile in values):
        return "ERROR"
    return "INSUFFICIENT_DATA"


def _calculate_profiles(
    *,
    event_context: EventContext,
    extraction: P5ExtractionResult,
    population_filters: PopulationFilters,
    missing_population_filters: tuple[str, ...],
    debug_mode: bool = False,
) -> dict[str, BookmakerMemoryProfile]:
    snapshot = extraction.full_time_snapshot
    profiles: dict[str, BookmakerMemoryProfile] = {}
    repository = None

    for bookmaker, bookie_id, snapshot_attribute in _STANDARD_BOOKMAKERS:
        logger.info(
            "P5 PROFILE | bookmaker=%s begin bookie_id=%s target_minute=%s",
            bookmaker,
            bookie_id,
            extraction.target_minute,
        )
        book_snapshot = getattr(snapshot, snapshot_attribute, None) if snapshot else None
        if book_snapshot is None:
            logger.info(
                "P5 PROFILE | bookmaker=%s skipped reason=current_price_vector_unavailable",
                bookmaker,
            )
            profiles[bookmaker] = not_eligible_profile(
                bookmaker=bookmaker,
                bookie_id=bookie_id,
                target_minute=extraction.target_minute,
                reason="current_price_vector_unavailable",
            )
            continue

        key = None
        try:
            key = build_memory_query_key(
                event_context,
                book_snapshot,
                expected_bookie_id=bookie_id,
            )
            if debug_mode:
                logger.info(
                    "P5 DEBUG | current vector bookmaker=%s query_key=%s",
                    bookmaker,
                    key.to_dict(),
                )
        except ValueError as exc:
            logger.info(
                "P5 PROFILE | bookmaker=%s skipped stage=current_vector_validation reason=%s",
                bookmaker,
                exc,
            )
            profiles[bookmaker] = not_eligible_profile(
                bookmaker=bookmaker,
                bookie_id=bookie_id,
                target_minute=extraction.target_minute,
                reason=str(exc),
                diagnostics={"stage": "current_vector_validation"},
            )
            continue

        if missing_population_filters:
            logger.info(
                "P5 PROFILE | bookmaker=%s skipped stage=population_filter_validation missing_filters=%s",
                bookmaker,
                missing_population_filters,
            )
            profiles[bookmaker] = not_eligible_profile(
                bookmaker=bookmaker,
                bookie_id=bookie_id,
                target_minute=extraction.target_minute,
                key=key,
                reason="active_population_filter_value_missing",
                diagnostics={
                    "stage": "population_filter_validation",
                    "missing_filters": list(missing_population_filters),
                },
            )
            continue

        try:
            if repository is None:
                repository = _memory_repository()
            sample = build_memory_sample(
                repository,
                key=key,
                current_event_id=event_context.event_id,
                current_starts_at=event_context.starts_at,
                population_filters=population_filters,
                debug_mode=debug_mode,
            )
            profiles[bookmaker] = calculate_memory_profile(
                bookmaker=bookmaker,
                target_minute=extraction.target_minute,
                sample=sample,
                debug_mode=debug_mode,
            )
        except Exception as exc:
            logger.exception(
                "P5 memory evaluation failed event_id=%s bookmaker=%s stage=historical_memory",
                event_context.event_id,
                bookmaker,
            )
            profiles[bookmaker] = error_profile(
                bookmaker=bookmaker,
                bookie_id=bookie_id,
                target_minute=extraction.target_minute,
                key=key,
                reason="historical_memory_evaluation_failed",
                diagnostics={
                    "stage": "historical_memory",
                    "error_class": type(exc).__name__,
                },
            )

    return profiles


def calculate_pillar_5(
    event_context: EventIdentity | EventContext,
    odds_trajectory_context: OddsTrajectoryContext,
    *,
    target_selection: TargetMinuteSelection | None = None,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Extract current prices, evaluate exact memories, and serialize P5 v3.0."""
    if odds_trajectory_context is None:
        raise ValueError("odds_trajectory_context is required for Pillar 5")
    odds_context = odds_trajectory_context

    if target_selection is None:
        target_selection = select_target_minute(
            odds_context,
            flow_id="pillar_5_price_memory",
            expected_event_id=event_context.event_id,
            allowed_target_minutes=getattr(Config, "PRE_START_ODDS_MOMENTS", None),
            evaluation_minute=getattr(event_context, "minutes_until_start", None),
        )

    logger.info(
        "P5 orchestrator start event_id=%s participants=%s debug_mode=%s target_minute=%s",
        event_context.event_id,
        event_context.participants_label,
        debug_mode,
        target_selection.target_minute,
    )
    extraction = extract_p5_market_snapshot(
        event_context.event_id,
        odds_context,
        target_selection,
    )
    periods = extraction.period_diagnostics()
    extraction_status = extraction.status
    log_extraction_diagnostics(
        logger,
        pillar="P5",
        event_id=event_context.event_id,
        target_minute=extraction.target_minute,
        periods=periods,
        full_time_requirement="any complete bookie: 1X2 or Home/Away",
        debug_mode=debug_mode,
    )

    snapshot = extraction.full_time_snapshot
    inputs = _empty_inputs()
    input_trace: dict[str, dict[str, Any]] = {}
    if snapshot is not None:
        inputs.update(snapshot.input_values())
        input_trace.update(snapshot.traces())

    population_filters, enabled_filters, missing_population_filters = (
        _population_filters(event_context)
    )
    logger.info(
        "P5 population filters event_id=%s enabled=%s values=%s missing_enabled=%s",
        event_context.event_id,
        enabled_filters,
        population_filters.to_dict(),
        missing_population_filters,
    )
    profiles = _calculate_profiles(
        event_context=event_context,
        extraction=extraction,
        population_filters=population_filters,
        missing_population_filters=missing_population_filters,
        debug_mode=debug_mode,
    )
    serialized_profiles = {
        bookmaker: profile.to_dict() for bookmaker, profile in profiles.items()
    }
    pillar_status = _global_status(
        profiles,
        extraction_status=extraction_status,
    )

    if debug_mode:
        for bookmaker, profile in serialized_profiles.items():
            for field, value in profile.items():
                if field == "historical_matches":
                    logger.info(
                        "P5 PROFILE | %s | historical_match_count=%s",
                        bookmaker,
                        len(value),
                    )
                    for index, match in enumerate(value, start=1):
                        logger.info(
                            "P5 PROFILE | %s | historical_match[%s]=%s",
                            bookmaker,
                            index,
                            match,
                        )
                elif field == "diagnostics":
                    logger.info(
                        "P5 PROFILE | %s | field=%s | value=%s",
                        bookmaker,
                        field,
                        value,
                    )
                else:
                    logger.info(
                        "P5 PROFILE | %s | field=%s | value=%s",
                        bookmaker,
                        field,
                        value,
                    )

    betfair_inputs = {
        key: value for key, value in inputs.items() if key.startswith("BF_")
    }
    betfair_trace = {
        key: value for key, value in input_trace.items() if key.startswith("BF_")
    }
    raw = {
        "inputs": inputs,
        "input_trace": input_trace,
        "traces": input_trace,
        "periods": periods,
        "extraction_diagnostics": extraction.extraction_diagnostics,
        "abort_reason": extraction.abort_reason,
        "historical_source": "mv_p5_price_memory",
        "historical_cutoff_starts_at": as_utc(event_context.starts_at).isoformat(),
        "price_quantization": {
            "quantum": "0.001",
            "rounding": "ROUND_HALF_UP",
            "comparison": "exact_after_quantization",
        },
        "population_filters": {
            "enabled": enabled_filters,
            "values": population_filters.to_dict(),
            "missing_enabled_values": list(missing_population_filters),
        },
        "checkpoint": {
            "evaluation_minute": getattr(event_context, "minutes_until_start", None),
            "target_minute": extraction.target_minute,
            "selection_reason": target_selection.reason,
            "selection_diagnostics": target_selection.diagnostics,
        },
        "memory_diagnostics": {
            name: profile.diagnostics for name, profile in profiles.items()
        },
        "betfair_exposure": {
            "bookie_id": 4,
            "inputs": betfair_inputs,
            "input_trace": betfair_trace,
            "participates_in_score": False,
        },
    }

    modules: list[dict[str, Any]] = []
    if any(profile.p5_valid for profile in profiles.values()):
        modules.append(
            {
                "pillar_id": "pillar_5",
                "module_id": "p5_memory_engine",
                "module_name": "Exact Price Memory Engine",
                "engine_version": ENGINE_VERSION,
                "P5_STATUS": pillar_status,
                "status": pillar_status,
                "P5_TARGET_MINUTE": extraction.target_minute,
                "P5_MEMORY_PROFILES": serialized_profiles,
                "raw": raw,
            }
        )

    logger.info(
        "P5 orchestrator done event_id=%s status=%s extraction_status=%s target_minute=%s valid_profiles=%s missing_inputs=%s invalid_inputs=%s ambiguous_inputs=%s",
        event_context.event_id,
        pillar_status,
        extraction_status,
        extraction.target_minute,
        sum(profile.p5_valid for profile in profiles.values()),
        len(extraction.missing_inputs),
        len(extraction.invalid_inputs),
        len(extraction.ambiguous_inputs),
    )
    return {
        "pillar_id": "pillar_5",
        "pillar_name": "Exact Price Memory",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P5_TARGET_MINUTE": extraction.target_minute,
        "P5_STATUS": pillar_status,
        "status": pillar_status,
        "PERIODS": periods,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
        "P5_EXTRACTION_STATUS": extraction_status,
        "P5_MEMORY_PROFILES": serialized_profiles,
        "modules": modules,
        "raw": raw,
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_5"]
