"""
Tests for OddsPortal market-period normalization.

Covers:
  1. flatten_sport_scraping_route() produces 'Full-time' for the standard
     home/away / 1X2 full-time steps across all relevant sports.
  2. Over/Under and Asian Handicap steps are NOT changed.
  3. MarketRepository._normalize_market_period() collapses all known full-time
     variants and leaves other periods untouched.
"""

import pytest

from modules.oddsportal.oddsportal_routes import flatten_sport_scraping_route
from infrastructure.persistence.repositories.market_repository import MarketRepository


# ---------------------------------------------------------------------------
# flatten_sport_scraping_route – standard full-time steps
# ---------------------------------------------------------------------------

def test_football_1x2_full_time_period_is_normalized():
    steps = flatten_sport_scraping_route("football")
    ft_step = next(
        (s for s in steps if s["group_key"] == "1X2" and s["period_key"] == "FULL_TIME"),
        None,
    )
    assert ft_step is not None, "Expected a FULL_TIME step for football 1X2"
    assert ft_step["db_market_period"] == "Full-time"
    assert ft_step["db_market_name"] == "Full-time"


def test_basketball_home_away_ft_inc_ot_period_is_normalized():
    steps = flatten_sport_scraping_route("basketball")
    ft_step = next(
        (s for s in steps if s["group_key"] == "HOME_AWAY" and s["period_key"] == "FT_INC_OT"),
        None,
    )
    assert ft_step is not None, "Expected a FT_INC_OT step for basketball Home/Away"
    assert ft_step["db_market_period"] == "Full-time"
    assert ft_step["db_market_name"] == "Full-time"


def test_american_football_home_away_full_time_normalized():
    steps = flatten_sport_scraping_route("american-football")
    ft_step = next(
        (s for s in steps if s["group_key"] == "HOME_AWAY" and s["period_key"] == "FT_INC_OT"),
        None,
    )
    assert ft_step is not None
    assert ft_step["db_market_period"] == "Full-time"
    assert ft_step["db_market_name"] == "Full-time"


def test_baseball_home_away_full_time_normalized():
    steps = flatten_sport_scraping_route("baseball")
    ft_step = next(
        (s for s in steps if s["group_key"] == "HOME_AWAY" and s["period_key"] == "FT_INC_OT"),
        None,
    )
    assert ft_step is not None
    assert ft_step["db_market_period"] == "Full-time"
    assert ft_step["db_market_name"] == "Full-time"


def test_hockey_home_away_full_time_normalized():
    steps = flatten_sport_scraping_route("hockey")
    ft_step = next(
        (s for s in steps if s["group_key"] == "HOME_AWAY" and s["period_key"] == "FT_INC_OT"),
        None,
    )
    assert ft_step is not None
    assert ft_step["db_market_period"] == "Full-time"
    assert ft_step["db_market_name"] == "Full-time"


# ---------------------------------------------------------------------------
# flatten_sport_scraping_route – Over/Under and Asian Handicap guard tests
# ---------------------------------------------------------------------------

def test_football_over_under_full_time_period_unchanged():
    steps = flatten_sport_scraping_route("football")
    ou_ft = next(
        (s for s in steps if s["db_market_group"] == "Over/Under" and s["period_key"] == "FULL_TIME"),
        None,
    )
    assert ou_ft is not None, "Expected a FULL_TIME Over/Under step for football"
    assert ou_ft["db_market_period"] == "Over/Under"
    assert ou_ft["db_market_name"] == "Over/Under"


def test_football_asian_handicap_full_time_period_unchanged():
    steps = flatten_sport_scraping_route("football")
    ah_ft = next(
        (s for s in steps if s["db_market_group"] == "Asian Handicap" and s["period_key"] == "FULL_TIME"),
        None,
    )
    assert ah_ft is not None, "Expected a FULL_TIME Asian Handicap step for football"
    assert ah_ft["db_market_period"] == "Asian Handicap"
    assert ah_ft["db_market_name"] == "Asian Handicap"


# ---------------------------------------------------------------------------
# MarketRepository._normalize_market_period
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("variant", [
    "Full Time",
    "Full time",
    "Full-time",
    "Fulltime",
    "FT",
])
def test_normalize_market_period_collapses_full_time_variants(variant):
    assert MarketRepository._normalize_market_period(variant) == "Full-time"


def test_normalize_market_period_leaves_1st_half_unchanged():
    assert MarketRepository._normalize_market_period("1st half") == "1st half"


def test_normalize_market_period_leaves_over_under_unchanged():
    assert MarketRepository._normalize_market_period("Over/Under") == "Over/Under"


def test_normalize_market_period_leaves_asian_handicap_unchanged():
    assert MarketRepository._normalize_market_period("Asian Handicap") == "Asian Handicap"


def test_normalize_market_period_handles_none():
    assert MarketRepository._normalize_market_period(None) is None


def test_normalize_market_period_strips_whitespace():
    assert MarketRepository._normalize_market_period("  Full Time  ") == "Full-time"
