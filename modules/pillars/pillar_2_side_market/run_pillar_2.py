"""Composition boundary for P2's independent market readings."""

from modules.pillars.market_evaluation import prepare_event_markets
from modules.pillars.profile_evaluation import evaluate_snapshot_profile
from .signal_engine import ENGINE_VERSION, build_p2_signal_profile
from .snapshot_policy import extract_p2_market_snapshot

ANALYTICAL_FIELDS = frozenset(
    (
        "PIN_EDGE",
        "B365_EDGE",
        "BOOK_RELATION",
        "BOOK_GAP",
        "REP_EDGE",
        "LINE_GAP",
        "PRICE_GAP",
        "FT_1X2_HANDICAP_RELATION",
        "FT_HANDICAP_CROSS_MARKET_GAP",
        "1H_1X2_HANDICAP_RELATION",
        "1H_HANDICAP_CROSS_MARKET_GAP",
        "FT_1X2_AH_RELATION",
        "FT_CROSS_MARKET_GAP",
        "1H_1X2_AH_RELATION",
        "1H_CROSS_MARKET_GAP",
        "FT_1H_1X2_RELATION",
        "FT_1H_1X2_GAP",
        "BACK_EDGE",
        "LAY_EDGE",
        "BACK_LAY_RELATION",
        "EXCHANGE_INTERNAL_GAP",
        "HOME_SPREAD",
        "AWAY_SPREAD",
        "SIDE_SPREAD",
        "RELATION",
        "GAP",
        "LINE_DIFF_RAW",
    )
)


def calculate_pillar_2(
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
    return evaluate_snapshot_profile(
        event_context,
        evaluation,
        pillar=2,
        engine_version=ENGINE_VERSION,
        extract=extract_p2_market_snapshot,
        build=build_p2_signal_profile,
        metric_fields=ANALYTICAL_FIELDS,
        debug_mode=debug_mode,
    )


__all__ = ["ENGINE_VERSION", "calculate_pillar_2"]
