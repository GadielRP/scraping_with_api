"""Independent P5 profiles using common contracts and aggregated memory."""

import logging

from infrastructure.settings import Config
from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.market_evaluation import (
    PINNACLE,
    BET365,
    FT,
    FT_OT,
    prepare_event_markets,
    context_view,
    contract_key,
)
from modules.pillars.market_snapshot_extractor import (
    MarketIdentity,
    MarketSnapshotRequest,
    ChoiceRequest,
    extract_market_snapshot,
)
from modules.pillars.market_candidate_selection import select_market_candidate
from modules.pillars.context import EventContext, EventIdentity
from .calculation_models import PopulationFilters
from .models import ThreeWayMarketSnapshot
from .ports import PriceMemoryReader
from .memory_sample import build_memory_query_key, build_memory_sample
from .memory_score import calculate_memory_profile
from .exchange_diagnostics import collect_exchange_inputs

ENGINE_VERSION = "p5_price_memory_v4_0"
logger = logging.getLogger(__name__)


def _log_assignment(name, value, *, debug_mode):
    if debug_mode:
        logger.info("P5 FORMULA | assignment | %s = %s", name, value)


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
    competition_id = getattr(event_context, "competition_id", None) or getattr(
        competition, "competition_id", None
    )
    values = {
        "competition_id": competition_id,
        "season_id": getattr(event_context, "season_id", None),
        "country": getattr(event_context, "country", None),
    }
    missing = tuple(
        name
        for name, is_enabled in enabled.items()
        if is_enabled and values[name] is None
    )
    return (
        PopulationFilters(
            competition_id=(
                values["competition_id"] if enabled["competition_id"] else None
            ),
            season_id=values["season_id"] if enabled["season_id"] else None,
            country=values["country"] if enabled["country"] else None,
        ),
        enabled,
        missing,
    )


def calculate_pillar_5(
    event_context,
    odds_trajectory_context,
    *,
    target_selection,
    memory_repository: PriceMemoryReader,
    debug_mode=False,
    market_evaluation=None,
):
    evaluation = market_evaluation or prepare_event_markets(
        odds_trajectory_context, target_selection, event_context
    )
    population, enabled, missing_filters = _population_filters(event_context)
    signals, inputs, profiles = [], {}, {}
    selected = evaluation.selected_full_time_period
    for family in ("1X2", "Home/Away"):
        lines = [
            line
            for line in evaluation.lines
            if line.market_group == family and line.market_period == selected
        ]
        for book in (PINNACLE, BET365):
            namespace = f"{book.id}:{family}:{selected or 'NO_FT'}"
            refs = tuple(contract_key(line) for line in lines)
            debug_label = f"{selected or 'NO_FT'}.{family}.{book.name.upper()}"
            if debug_mode:
                logger.info(
                    "P5 FORMULA | %s | begin profile target_minute=%s market_lines=%s",
                    debug_label,
                    target_selection.target_minute,
                    [line.market_name for line in lines],
                )
            if not lines or target_selection.target_minute is None:
                if debug_mode:
                    logger.info(
                        "P5 FORMULA | %s | unavailable: %s",
                        debug_label,
                        "market family absent" if not lines else "target minute absent",
                    )
                signals.append(
                    SignalResult(
                        namespace, "BLOCKED", reason="MISSING_INPUT", contract_refs=refs
                    )
                )
                continue
            request = MarketSnapshotRequest(
                tuple(
                    MarketIdentity(
                        line.market_group, line.market_period, line.market_name
                    )
                    for line in lines
                ),
                book.id,
                tuple(
                    ChoiceRequest(name, name, name)
                    for name in (("1", "x", "2") if family == "1X2" else ("1", "2"))
                ),
            )
            extraction = extract_market_snapshot(
                context_view(evaluation.context, lines),
                target_minute=target_selection.target_minute,
                request=request,
            )
            choice = select_market_candidate(extraction, request, allow_partial=False)
            if choice.candidate is None:
                reason = (
                    "AMBIGUOUS_CANDIDATE"
                    if choice.ambiguous
                    else "INVALID_VALUE" if choice.invalid else "MISSING_INPUT"
                )
                if debug_mode:
                    logger.info(
                        "P5 FORMULA | %s | candidate unavailable reason=%s ambiguous=%s invalid=%s",
                        debug_label,
                        reason,
                        choice.ambiguous,
                        choice.invalid,
                    )
                signals.append(
                    SignalResult(
                        namespace, "BLOCKED", reason=reason, contract_refs=refs
                    )
                )
                continue
            points = choice.candidate.choices
            vector = ThreeWayMarketSnapshot(
                home=points["1"], draw=points.get("x"), away=points["2"]
            )
            for name, point in points.items():
                outcome_name = {"1": "HOME", "x": "DRAW", "2": "AWAY"}.get(
                    name, name.upper()
                )
                _log_assignment(
                    f"{debug_label}.{outcome_name}_PRICE",
                    point.odds_price,
                    debug_mode=debug_mode,
                )
            if debug_mode:
                logger.info(
                    "P5 FORMULA | %s | current price vector=%s",
                    debug_label,
                    {
                        "HOME": vector.home.odds_price,
                        "DRAW": (
                            vector.draw.odds_price if vector.draw is not None else None
                        ),
                        "AWAY": vector.away.odds_price,
                    },
                )
            input_refs = []
            for name, point in points.items():
                ref = f"{namespace}:{name}"
                inputs[ref] = {
                    "value": float(point.odds_price),
                    "trace": point.trace.to_dict(),
                }
                input_refs.append(ref)
            if missing_filters:
                if debug_mode:
                    logger.info(
                        "P5 FORMULA | %s | unavailable: missing population filter values=%s",
                        debug_label,
                        list(missing_filters),
                    )
                signals.append(
                    SignalResult(
                        namespace,
                        "BLOCKED",
                        reason="MISSING_POPULATION_FILTER",
                        input_refs=tuple(input_refs),
                        contract_refs=refs,
                        evidence={"missing_filters": list(missing_filters)},
                    )
                )
                continue
            try:
                key = build_memory_query_key(
                    event_context, vector, expected_bookie_id=book.id
                )
            except ValueError as exc:
                if debug_mode:
                    logger.info(
                        "P5 FORMULA | %s | invalid historical query input detail=%s",
                        debug_label,
                        exc,
                    )
                signals.append(
                    SignalResult(
                        namespace,
                        "BLOCKED",
                        reason="INVALID_VALUE",
                        input_refs=tuple(input_refs),
                        contract_refs=refs,
                        evidence={"detail": str(exc)},
                    )
                )
                continue
            if debug_mode:
                logger.info(
                    "P5 FORMULA | %s | historical query key=%s population_filters=%s",
                    debug_label,
                    key.to_dict(),
                    population.to_dict(),
                )
            stage = "HISTORICAL_LOOKUP_ERROR"
            try:
                sample = build_memory_sample(
                    memory_repository,
                    key=key,
                    current_event_id=event_context.event_id,
                    current_starts_at=event_context.starts_at,
                    population_filters=population,
                )
                # Keep every captured population linked even if its calculation fails.
                profiles[namespace] = {
                    "sample_id": sample.sample_id,
                    "sample_size": sample.sample_size,
                    "query_key": key.to_dict(),
                    "exclusions": sample.exclusions,
                }
                if debug_mode:
                    logger.info(
                        "P5 FORMULA | %s | historical sample_size=%s outcomes(home=%s, draw=%s, away=%s) exclusions=%s",
                        debug_label,
                        sample.sample_size,
                        sample.wins_home,
                        sample.wins_draw,
                        sample.wins_away,
                        sample.exclusions,
                    )
                stage = "CALCULATION_ERROR"
                profile = calculate_memory_profile(
                    bookmaker=book.name,
                    target_minute=target_selection.target_minute,
                    sample=sample,
                    debug_mode=debug_mode,
                )
                profiles[namespace] = profile.to_dict()
                signals.append(
                    SignalResult(
                        namespace,
                        "COMPUTED" if profile.p5_valid else "BLOCKED",
                        float(profile.p5) if profile.p5_valid else None,
                        None if profile.p5_valid else "INSUFFICIENT_HISTORY",
                        tuple(input_refs),
                        refs,
                        {
                            "sample_size": sample.sample_size,
                            "sample_id": sample.sample_id,
                            "direction": profile.p5_direction,
                        },
                    )
                )
            except Exception as exc:
                if debug_mode:
                    logger.exception(
                        "P5 FORMULA | %s | %s failed error_class=%s",
                        debug_label,
                        stage,
                        type(exc).__name__,
                    )
                signals.append(
                    SignalResult(
                        namespace,
                        "ERROR",
                        reason=stage,
                        input_refs=tuple(input_refs),
                        contract_refs=refs,
                        evidence={"error_class": type(exc).__name__},
                    )
                )
    exchange_diagnostics = collect_exchange_inputs(evaluation, inputs)
    return EvaluationResult(
        event_context.event_id,
        "pillar_5",
        ENGINE_VERSION,
        target_selection.target_minute,
        evaluation.selection,
        tuple(signals),
        evaluation.coverage(5),
        inputs,
        evaluation.contracts(5),
        profiles,
        (
            *evaluation.diagnostics,
            *exchange_diagnostics,
            {
                "population_filters": population.to_dict(),
                "enabled_filters": enabled,
                "audit_persisted": any(p.get("sample_id") for p in profiles.values()),
            },
        ),
    ).to_dict()


__all__ = ["ENGINE_VERSION", "calculate_pillar_5"]
