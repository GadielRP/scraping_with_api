"""Unit tests for Pillar 5 price memory materialized view and repository."""

from infrastructure.persistence.views.p5_price_memory_view import (
    MV_P5_PRICE_MEMORY_INDEXES_SQL,
    build_p5_price_memory_view_sql,
)
from modules.pillars.pillar_5.periods import (
    P5_PRICE_MEMORY_MARKET_GROUPS,
    P5_PRICE_MEMORY_MARKET_PERIODS,
)


def test_build_p5_price_memory_view_sql_default_scopes():
    sql = build_p5_price_memory_view_sql()

    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_p5_price_memory AS" in sql
    assert "FROM markets m" in sql
    assert "JOIN canonical_market_types cmt" in sql
    assert "JOIN market_choices mc" in sql
    assert "JOIN LATERAL" in sql
    assert "WHERE m.is_live = false" in sql
    assert "r.home_score IS NOT NULL" in sql
    assert "r.away_score IS NOT NULL" in sql
    assert "pm.odds_home IS NOT NULL" in sql
    assert "pm.odds_away IS NOT NULL" in sql
    assert "mcs.collected_at <= e.starts_at" in sql
    assert "e.season_id" in sql
    assert "e.country" in sql
    assert "has_draw" in sql
    assert (
        "PARTITION BY pm.event_id, pm.bookie_id, pm.market_group, pm.market_period"
        in sql
    )
    # Default groups and periods
    for group in P5_PRICE_MEMORY_MARKET_GROUPS:
        assert f"'{group}'" in sql
    for period in P5_PRICE_MEMORY_MARKET_PERIODS:
        assert f"'{period}'" in sql


def test_build_p5_price_memory_view_sql_modular_scopes():
    custom_groups = ("1X2", "Over/Under")
    custom_periods = ("1st Half", "Full Time")
    sql = build_p5_price_memory_view_sql(
        market_groups=custom_groups,
        market_periods=custom_periods,
    )

    assert "cmt.canonical_market_group IN ('1X2', 'Over/Under')" in sql
    assert "cmt.canonical_market_period IN ('1st Half', 'Full Time')" in sql


def test_build_p5_price_memory_view_current_odds_only_uses_latest_current_quote():
    sql = build_p5_price_memory_view_sql(current_odds_only=True)

    assert "mcq.current_odds::numeric(8,3) AS odds_price" in sql
    assert "COALESCE(mcq.current_odds, mcq.initial_odds)" not in sql
    assert "initial_odds" not in sql
    assert "mcq.initial_captured_at" not in sql
    assert "quote_candidate.current_odds IS NOT NULL" in sql
    assert "quote_candidate.current_updated_at DESC NULLS LAST" in sql
    assert "quote_candidate.quote_id DESC" in sql
    assert "mcs.collected_at <= e.starts_at" not in sql


def test_p5_price_memory_indexes():
    assert len(MV_P5_PRICE_MEMORY_INDEXES_SQL) == 5
    indexes_joined = " ".join(MV_P5_PRICE_MEMORY_INDEXES_SQL)

    assert "idx_mv_p5_event_bookie_market" in indexes_joined
    assert "idx_mv_p5_lookup_1x2" in indexes_joined
    assert "idx_mv_p5_lookup_2way" in indexes_joined
    assert "idx_mv_p5_starts_at" in indexes_joined
    assert "idx_mv_p5_sport_competition" in indexes_joined
    assert (
        "sport, bookie_id, market_group, market_period, has_draw, odds_home, odds_draw, odds_away, starts_at DESC"
        in indexes_joined
    )
    assert (
        "sport, bookie_id, market_group, market_period, has_draw, odds_home, odds_away, starts_at DESC"
        in indexes_joined
    )
    assert "WHERE odds_draw IS NOT NULL" in indexes_joined
    assert "WHERE odds_draw IS NULL" in indexes_joined
