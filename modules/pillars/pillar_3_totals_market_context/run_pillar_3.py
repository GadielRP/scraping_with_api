"""Composition boundary for P3's independent market readings."""

from modules.pillars.market_evaluation import prepare_event_markets
from modules.pillars.profile_evaluation import evaluate_snapshot_profile
from .signal_engine import (
    ENGINE_VERSION,
    build_p3_executive_summary,
    build_p3_signal_profile,
    interpret_p3_result,
)
from .snapshot_policy import extract_p3_market_snapshot

ANALYTICAL_FIELDS = frozenset(
    (
        "EDGE",
        "LINE_DIFF_RAW",
        "LINE_GAP",
        "RELATION",
        "GAP",
        "FT_1H_OU_RELATION",
        "FT_1H_OU_GAP",
        "BACK_LAY_RELATION",
        "EXCHANGE_INTERNAL_GAP",
    )
)


def calculate_pillar_3(
    event_context,
    odds_trajectory_context,
    *,
    target_selection,
    debug_mode=False,
    market_evaluation=None
):
    evaluation = market_evaluation or prepare_event_markets(
        odds_trajectory_context, target_selection, event_context
    )
    result = evaluate_snapshot_profile(
        event_context,
        evaluation,
        pillar=3,
        engine_version=ENGINE_VERSION,
        extract=extract_p3_market_snapshot,
        build=build_p3_signal_profile,
        metric_fields=ANALYTICAL_FIELDS,
        debug_mode=debug_mode,
    )
    result["INTERPRETATION"] = interpret_p3_result(result)
    result["EXECUTIVE_SUMMARY"] = build_p3_executive_summary(
        result, result["INTERPRETATION"]
    )
    return result


__all__ = ["ENGINE_VERSION", "calculate_pillar_3"]
