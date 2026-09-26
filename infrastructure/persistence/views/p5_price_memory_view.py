"""Modular materialized view definition and index builders for Pillar 5 Price Memory."""

from __future__ import annotations

from typing import Sequence

from modules.pillars.pillar_5.periods import (
    P5_PRICE_MEMORY_MARKET_GROUPS,
    P5_PRICE_MEMORY_MARKET_PERIODS,
)


def _sql_string_list(values: Sequence[str]) -> str:
    cleaned = []
    for value in values:
        if value is None:
            continue
        escaped = str(value).replace("'", "''")
        cleaned.append(f"'{escaped}'")
    return ", ".join(cleaned) if cleaned else "''"


def build_p5_price_memory_view_sql(
    market_groups: Sequence[str] | None = None,
    market_periods: Sequence[str] | None = None,
    *,
    include_quote_fallbacks: bool = False,
    quote_values_only: bool = False,
    current_odds_only: bool = False,
) -> str:
    """Build dynamic DDL for mv_p5_price_memory materialized view.

    ``quote_values_only`` treats persisted quote cache values as the historical
    observation for non-live markets, preferring ``current_odds`` and falling
    back to ``initial_odds``. ``current_odds_only`` selects the most recently
    updated quote with a current value and never uses ``initial_odds``. The
    other modes reproduce earlier revisions.
    """
    groups = market_groups if market_groups is not None else P5_PRICE_MEMORY_MARKET_GROUPS
    periods = market_periods if market_periods is not None else P5_PRICE_MEMORY_MARKET_PERIODS

    market_groups_sql = _sql_string_list(groups)
    market_periods_sql = _sql_string_list(periods)
    if current_odds_only:
        odds_price_sql = "mcq.current_odds::numeric(8,3)"
        quote_timestamp_sql = "COALESCE(mcq.current_updated_at, m.collected_at)"
        latest_snapshot_join_sql = ""
        quote_candidate_filter_sql = "AND quote_candidate.current_odds IS NOT NULL"
        quote_candidate_select_sql = (
            "quote_candidate.quote_id, quote_candidate.current_odds, "
            "quote_candidate.current_updated_at"
        )
        quote_candidate_order_sql = (
            "quote_candidate.current_updated_at DESC NULLS LAST, "
            "quote_candidate.quote_id DESC"
        )
    elif quote_values_only:
        odds_price_sql = "COALESCE(mcq.current_odds, mcq.initial_odds)::numeric(8,3)"
        quote_timestamp_sql = """CASE
                WHEN mcq.current_odds IS NOT NULL
                    THEN COALESCE(mcq.current_updated_at, m.collected_at)
                WHEN mcq.initial_odds IS NOT NULL
                    THEN COALESCE(mcq.initial_captured_at, m.collected_at)
                ELSE m.collected_at
            END"""
        latest_snapshot_join_sql = ""
        quote_candidate_filter_sql = ""
        quote_candidate_select_sql = "quote_candidate.*"
        quote_candidate_order_sql = "quote_candidate.quote_id"
    elif include_quote_fallbacks:
        odds_price_sql = (
            "COALESCE(latest.odds_value, mcq.current_odds, mcq.initial_odds)"
            "::numeric(8,3)"
        )
        quote_timestamp_sql = "COALESCE(\n                latest.collected_at,\n                mcq.current_updated_at,\n                mcq.initial_captured_at,\n                m.collected_at\n            )"
        latest_snapshot_join_sql = """LEFT JOIN LATERAL (
            SELECT mcs.odds_value, mcs.collected_at
            FROM market_choice_snapshots mcs
            WHERE mcs.quote_id = mcq.quote_id
              AND (mcs.collected_at <= e.starts_at OR e.starts_at IS NULL)
            ORDER BY mcs.collected_at DESC, mcs.snapshot_id DESC
            LIMIT 1
        ) latest ON TRUE"""
        quote_candidate_filter_sql = ""
        quote_candidate_select_sql = "quote_candidate.*"
        quote_candidate_order_sql = "quote_candidate.quote_id"
    else:
        odds_price_sql = "latest.odds_value::numeric(8,3)"
        quote_timestamp_sql = "latest.collected_at"
        latest_snapshot_join_sql = """LEFT JOIN LATERAL (
            SELECT mcs.odds_value, mcs.collected_at
            FROM market_choice_snapshots mcs
            WHERE mcs.quote_id = mcq.quote_id
              AND (mcs.collected_at <= e.starts_at OR e.starts_at IS NULL)
            ORDER BY mcs.collected_at DESC, mcs.snapshot_id DESC
            LIMIT 1
        ) latest ON TRUE"""
        quote_candidate_filter_sql = ""
        quote_candidate_select_sql = "quote_candidate.*"
        quote_candidate_order_sql = "quote_candidate.quote_id"

    return f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_p5_price_memory AS
    WITH choice_quotes AS (
        SELECT
            m.event_id,
            m.market_id,
            m.bookie_id,
            cmt.canonical_market_group AS market_group,
            cmt.canonical_market_period AS market_period,
            mc.choice_name,
            {odds_price_sql} AS odds_price,
            {quote_timestamp_sql} AS quote_timestamp
        FROM markets m
        JOIN events e ON e.id = m.event_id
        JOIN canonical_market_types cmt ON cmt.market_type_id = m.market_type_id
        JOIN market_choices mc ON mc.market_id = m.market_id
        JOIN LATERAL (
            SELECT {quote_candidate_select_sql}
            FROM market_choice_quotes quote_candidate
            WHERE quote_candidate.choice_id = mc.choice_id
              AND quote_candidate.exchange_side IS NULL
              AND quote_candidate.exchange_level = 0
              {quote_candidate_filter_sql}
            ORDER BY {quote_candidate_order_sql}
            LIMIT 1
        ) mcq ON TRUE
        {latest_snapshot_join_sql}
        WHERE m.is_live = false
          AND cmt.canonical_market_group IN ({market_groups_sql})
          AND cmt.canonical_market_period IN ({market_periods_sql})
          AND mc.choice_name IN ('1', 'x', '2')
    ),
    pivoted_markets AS (
        SELECT
            cq.event_id,
            cq.market_id,
            cq.bookie_id,
            cq.market_group,
            cq.market_period,
            MAX(CASE WHEN cq.choice_name = '1' THEN cq.odds_price END) AS odds_home,
            MAX(CASE WHEN cq.choice_name = 'x' THEN cq.odds_price END) AS odds_draw,
            MAX(CASE WHEN cq.choice_name = '2' THEN cq.odds_price END) AS odds_away,
            MAX(cq.quote_timestamp) AS last_sync_at
        FROM choice_quotes cq
        GROUP BY cq.event_id, cq.market_id, cq.bookie_id, cq.market_group, cq.market_period
    ),
    ranked_markets AS (
        SELECT
            pm.*,
            ROW_NUMBER() OVER (
                PARTITION BY pm.event_id, pm.bookie_id, pm.market_group, pm.market_period
                ORDER BY pm.last_sync_at DESC, pm.market_id DESC
            ) AS rn
        FROM pivoted_markets pm
        WHERE pm.odds_home IS NOT NULL
          AND pm.odds_away IS NOT NULL
    )
    SELECT
        rm.event_id,
        e.sport,
        e.competition_id,
        e.season_id,
        e.country,
        rm.bookie_id,
        rm.market_group,
        rm.market_period,
        (rm.odds_draw IS NOT NULL) AS has_draw,
        e.starts_at,
        rm.odds_home,
        rm.odds_draw,
        rm.odds_away,
        r.home_score,
        r.away_score,
        r.winner AS winner_side,
        rm.last_sync_at
    FROM ranked_markets rm
    JOIN events e ON e.id = rm.event_id
    JOIN results r ON r.event_id = rm.event_id
    WHERE rm.rn = 1
      AND r.home_score IS NOT NULL
      AND r.away_score IS NOT NULL;
    """


MV_P5_PRICE_MEMORY_INDEXES_SQL = [
    # Unique index required for REFRESH MATERIALIZED VIEW CONCURRENTLY
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_p5_event_bookie_market "
        "ON mv_p5_price_memory (event_id, bookie_id, market_group, market_period);"
    ),
    # Fast 1X2 composite lookup (with draw)
    (
        "CREATE INDEX IF NOT EXISTS idx_mv_p5_lookup_1x2 "
        "ON mv_p5_price_memory (sport, bookie_id, market_group, market_period, has_draw, odds_home, odds_draw, odds_away, starts_at DESC) "
        "WHERE odds_draw IS NOT NULL;"
    ),
    # Fast 2-way Home/Away lookup (without draw)
    (
        "CREATE INDEX IF NOT EXISTS idx_mv_p5_lookup_2way "
        "ON mv_p5_price_memory (sport, bookie_id, market_group, market_period, has_draw, odds_home, odds_away, starts_at DESC) "
        "WHERE odds_draw IS NULL;"
    ),
    # Chronological ordering index
    (
        "CREATE INDEX IF NOT EXISTS idx_mv_p5_starts_at "
        "ON mv_p5_price_memory (starts_at);"
    ),
    # Sport & competition filter index
    (
        "CREATE INDEX IF NOT EXISTS idx_mv_p5_sport_competition "
        "ON mv_p5_price_memory (sport, competition_id);"
    ),
]
