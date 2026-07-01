"""Seed catalog for the canonical market type reference table."""

from __future__ import annotations


def _seed(name, group, period, family, requires_group, trajectory, order):
    return {
        "canonical_market_name": name,
        "canonical_market_group": group,
        "canonical_market_period": period,
        "market_family": family,
        "requires_choice_group": requires_group,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": trajectory,
        "display_order": order,
    }


CANONICAL_MARKET_TYPE_SEEDS = {
    "1x2_full_time": _seed("1X2 Full Time", "1X2", "Full Time", "side_3way", False, True, 10),
    "1x2_1st_half": _seed("1X2 1st Half", "1X2", "1st Half", "side_3way", False, True, 11),
    "1x2_1st_quarter": _seed("1X2 1st Quarter", "1X2", "1st Quarter", "side_3way", False, False, 12),
    "home_away_full_time": _seed("Home/Away Full Time", "Home/Away", "Full Time", "side_2way", False, True, 20),
    "home_away_1st_half": _seed("Home/Away 1st Half", "Home/Away", "1st Half", "side_2way", False, True, 21),
    "home_away_1st_quarter": _seed("Home/Away 1st Quarter", "Home/Away", "1st Quarter", "side_2way", False, False, 22),
    "home_away_full_time_including_overtime": _seed("Home/Away Full Time Including Overtime", "Home/Away", "Full Time Including Overtime", "side_2way", False, False, 23),
    "first_set_winner_1st_set": _seed("First Set Winner 1st Set", "First Set Winner", "1st Set", "side_2way", False, False, 24),
    "current_set_winner_current_set": _seed("Current Set Winner Current Set", "Current Set Winner", "Current Set", "side_2way", False, False, 25),
    "over_under_full_time": _seed("Over/Under Full Time", "Over/Under", "Full Time", "total", True, True, 30),
    "over_under_1st_half": _seed("Over/Under 1st Half", "Over/Under", "1st Half", "total", True, True, 31),
    "over_under_1st_period": _seed("Over/Under 1st Period", "Over/Under", "1st Period", "total", True, False, 32),
    "total_cards_full_time": _seed("Total Cards Full Time", "Total Cards", "Full Time", "total", True, False, 33),
    "corners_2_way_full_time": _seed("Corners 2-Way Full Time", "Corners 2-Way", "Full Time", "total", True, False, 34),
    "total_sets_games_extra_time": _seed("Total Sets/Games Extra Time", "Total Sets/Games", "Extra Time", "total", True, False, 35),
    "team_total_home_full_time": _seed("Team Total Home Full Time", "Over/Under Team 1", "Full Time", "team_total", True, True, 36),
    "team_total_away_full_time": _seed("Team Total Away Full Time", "Over/Under Team 2", "Full Time", "team_total", True, True, 37),
    "asian_handicap_full_time": _seed("Asian Handicap Full Time", "Asian Handicap", "Full Time", "spread_2way", True, True, 40),
    "asian_handicap_1st_half": _seed("Asian Handicap 1st Half", "Asian Handicap", "1st Half", "spread_2way", True, True, 41),
    "european_handicap_full_time": _seed("European Handicap Full Time", "European Handicap", "Full Time", "side_3way", True, False, 43),
    "draw_no_bet_full_time": _seed("Draw No Bet Full Time", "Draw No Bet", "Full Time", "side_2way", False, False, 50),
    "double_chance_full_time": _seed("Double Chance Full Time", "Double Chance", "Full Time", "side_combo", False, False, 51),
    "both_teams_to_score_full_time": _seed("Both Teams To Score Full Time", "Both Teams To Score", "Full Time", "decision", False, False, 52),
    "first_goal_full_time": _seed("First Goal Full Time", "First Goal", "Full Time", "goal_team", False, False, 60),
    "last_goal_full_time": _seed("Last Goal Full Time", "Last Goal", "Full Time", "goal_team", False, False, 61),
    "first_team_to_score_full_time": _seed("First Team To Score Full Time", "First Team To Score", "Full Time", "goal_team", False, False, 62),
    "next_goal_full_time": _seed("Next Goal Full Time", "Next Goal", "Full Time", "goal_team", False, False, 63),
    "tie_break_in_match_extra_time": _seed("Tie Break In Match Extra Time", "Tie Break In Match", "Extra Time", "decision", False, False, 70),
}


def get_canonical_market_type_seed(canonical_market_key: str) -> dict | None:
    seed = CANONICAL_MARKET_TYPE_SEEDS.get(canonical_market_key)
    return dict(seed) if seed is not None else None


def get_canonical_market_type_seeds() -> dict:
    return {key: dict(value) for key, value in CANONICAL_MARKET_TYPE_SEEDS.items()}
