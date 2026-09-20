"""Unit tests for Pillar 5 price memory materialized view and repository."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    HistoricalPriceMatch,
    Pillar5PriceMemoryRepository,
)
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


def test_p5_price_memory_indexes():
    assert len(MV_P5_PRICE_MEMORY_INDEXES_SQL) == 5
    indexes_joined = " ".join(MV_P5_PRICE_MEMORY_INDEXES_SQL)

    assert "idx_mv_p5_event_bookie_market" in indexes_joined
    assert "idx_mv_p5_lookup_1x2" in indexes_joined
    assert "idx_mv_p5_lookup_2way" in indexes_joined
    assert "idx_mv_p5_starts_at" in indexes_joined
    assert "idx_mv_p5_sport_competition" in indexes_joined
    assert "WHERE odds_draw IS NOT NULL" in indexes_joined
    assert "WHERE odds_draw IS NULL" in indexes_joined


def test_pillar_5_price_memory_repository_find_exact_matches_1x2():
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session

    now = datetime.now(timezone.utc)
    mock_row = {
        "event_id": 12345,
        "sport": "Football",
        "competition_id": 42,
        "bookie_id": 302,
        "market_group": "1X2",
        "market_period": "Full Time",
        "starts_at": now,
        "odds_home": 1.95,
        "odds_draw": 3.40,
        "odds_away": 4.10,
        "home_score": 2,
        "away_score": 1,
        "winner_side": "1",
        "last_sync_at": now,
    }
    mock_session.execute.return_value.mappings.return_value.all.return_value = [mock_row]

    repo = Pillar5PriceMemoryRepository(mock_session_factory)
    matches = repo.find_exact_matches(
        bookie_id=302,
        market_group="1X2",
        market_period="Full Time",
        odds_home=1.95,
        odds_draw=3.40,
        odds_away=4.10,
        sport="Football",
        limit=10,
    )

    assert len(matches) == 1
    match = matches[0]
    assert isinstance(match, HistoricalPriceMatch)
    assert match.event_id == 12345
    assert match.bookie_id == 302
    assert match.odds_home == 1.95
    assert match.odds_draw == 3.40
    assert match.odds_away == 4.10
    assert match.home_score == 2
    assert match.winner_side == "1"

    # Verify executed SQL contains expected parameters
    call_args = mock_session.execute.call_args
    sql_text = str(call_args[0][0])
    params = call_args[0][1]

    assert "odds_draw = :odds_draw" in sql_text
    assert "sport = :sport" in sql_text
    assert params["bookie_id"] == 302
    assert params["odds_home"] == 1.95
    assert params["odds_draw"] == 3.40
    assert params["odds_away"] == 4.10
    assert params["sport"] == "Football"
    assert params["limit"] == 10


def test_pillar_5_price_memory_repository_find_exact_matches_2way():
    mock_session = MagicMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__enter__.return_value = mock_session

    now = datetime.now(timezone.utc)
    mock_row = {
        "event_id": 99999,
        "sport": "Basketball",
        "competition_id": 10,
        "bookie_id": 1,
        "market_group": "Home/Away",
        "market_period": "Full Time",
        "starts_at": now,
        "odds_home": 1.55,
        "odds_draw": None,
        "odds_away": 2.45,
        "home_score": 102,
        "away_score": 95,
        "winner_side": "1",
        "last_sync_at": now,
    }
    mock_session.execute.return_value.mappings.return_value.all.return_value = [mock_row]

    repo = Pillar5PriceMemoryRepository(mock_session_factory)
    matches = repo.find_exact_matches(
        bookie_id=1,
        market_group="Home/Away",
        market_period="Full Time",
        odds_home=1.55,
        odds_away=2.45,
        odds_draw=None,
    )

    assert len(matches) == 1
    match = matches[0]
    assert match.odds_draw is None
    assert match.odds_home == 1.55
    assert match.odds_away == 2.45

    call_args = mock_session.execute.call_args
    sql_text = str(call_args[0][0])
    assert "odds_draw IS NULL" in sql_text
