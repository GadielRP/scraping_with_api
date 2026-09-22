from modules.odds_ingestion.canonical_market_resolver import resolve_sofascore_key


def _market(name, group, period, choices):
    return {
        "marketName": name,
        "marketGroup": group,
        "marketPeriod": period,
        "choices": [{"name": choice} for choice in choices],
    }


def test_sofascore_match_period_resolves_home_away_to_ftiot():
    market = _market("Full time", "Home/Away", "Match", ("1", "2"))

    assert resolve_sofascore_key(market) == "home_away_full_time_including_overtime"


def test_sofascore_regulation_period_resolves_home_away_to_full_time():
    market = _market("Full time", "Home/Away", "Full-time", ("1", "2"))

    assert resolve_sofascore_key(market) == "home_away_full_time"


def test_sofascore_match_period_resolves_point_spread_to_ftiot():
    market = _market("Point spread", "Point spread", "Match", ("1", "2"))

    assert resolve_sofascore_key(market) == "asian_handicap_full_time_including_overtime"


def test_sofascore_match_period_resolves_game_total_to_ftiot():
    market = _market("Game total", "Over/Under", "Match", ("Over", "Under"))

    assert resolve_sofascore_key(market) == "over_under_full_time_including_overtime"


def test_sofascore_explicit_overtime_alias_resolves_to_ftiot():
    market = _market(
        "Full time (including overtime)",
        "Full time (including overtime)",
        "Full-time",
        ("1", "2"),
    )

    assert resolve_sofascore_key(market) == "home_away_full_time_including_overtime"


def test_sofascore_match_period_does_not_fallback_to_full_time():
    market = _market("Full time", "1X2", "Match", ("1", "X", "2"))

    assert resolve_sofascore_key(market) is None
