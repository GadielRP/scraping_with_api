# ---------------------------------------------------------------------------
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# OddsPortal URL Fragment Identifiers
# Used to navigate directly to specific market groups and periods
# URL format: {match_url}/#{group};{period}
# ---------------------------------------------------------------------------

# Market group fragment values
OP_GROUPS = {
    "1X2": "1X2",
    "HOME_AWAY": "home-away",
    "OVER_UNDER": "over-under",
    "ASIAN_HANDICAP": "ah",
}

OP_GROUPS_DISPLAY = {
    "1X2": "1X2",
    "HOME_AWAY": "Home/Away",
    "OVER_UNDER": "Over/Under",
    "ASIAN_HANDICAP": "Asian Handicap",
}

# Market period integer codes used in URL fragments
OP_PERIODS = {
    "FT_INC_OT": 1,    # Full Time including Overtime
    "FULL_TIME": 2,     # Full Time (regulation only)
    "1ST_HALF": 3,      # 1st Half
    "2ND_HALF": 4,      # 2nd Half
    "1ST_PERIOD": 5,    # 1st Period (hockey)
    "2ND_PERIOD": 6,    # 2nd Period (hockey)
    "3RD_PERIOD": 7,    # 3rd Period (hockey)
    "1ST_QUARTER": 8,
    "2ND_QUARTER": 9,
    "3RD_QUARTER": 10,
    "4TH_QUARTER": 11,
}

# ---------------------------------------------------------------------------
# Sport-Specific Scraping Routes
# Defines which market_group and periods to scrape for each sport.
# periods are defined by ('period_key', 'oddsportal_display_name', 'db_market_period')
# ---------------------------------------------------------------------------

SPORT_SCRAPING_ROUTES = {
    "football": {
        "groups": [
            {
                "group_key": "1X2",
                "db_market_group": "1X2",
                "has_draw": True,
                "periods": [
                    ("FULL_TIME", "Full Time", "Full-time"),
                    ("1ST_HALF", "1st Half", "1st half")
                ],
                "betfair_period_index": 0,
                "extract_fn": "standard",
            },
            {
                "group_key": "OVER_UNDER",
                "db_market_group": "Over/Under",
                "has_draw": False,
                "periods": [
                    ("FULL_TIME", "Full Time", "Over/Under"),
                    ("1ST_HALF", "1st Half", "Over/Under 1st half ")
                ],
                "betfair_period_index": None,
                "extract_fn": "over_under",
            },
            {
                "group_key": "ASIAN_HANDICAP",
                "db_market_group": "Asian Handicap",
                "has_draw": False,
                "periods": [
                    ("FULL_TIME", "Full Time", "Asian Handicap"),
                    ("1ST_HALF", "1st Half", "Asian Handicap 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "asian_handicap",
            },
        ],
    },
    "basketball": {
        "groups": [
            {
                "group_key": "HOME_AWAY",
                "db_market_group": "Home/Away",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Full-time"),
                    ("1ST_HALF", "1st Half", "1st half")
                ],
                "betfair_period_index": 0,
                "extract_fn": "standard",
            },
            {
                "group_key": "OVER_UNDER",
                "db_market_group": "Over/Under",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Over/Under"),
                    ("1ST_HALF", "1st Half", "Over/Under 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "over_under",
            },
            {
                "group_key": "ASIAN_HANDICAP",
                "db_market_group": "Asian Handicap",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Asian Handicap"),
                    ("1ST_HALF", "1st Half", "Asian Handicap 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "asian_handicap",
            },
        ],
    },
    "american-football": {
        "groups": [
            {
                "group_key": "HOME_AWAY",
                "db_market_group": "Home/Away",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Full-time"),
                    ("1ST_HALF", "1st Half", "1st half")
                ],
                "betfair_period_index": 0,
                "extract_fn": "standard",
            },
            {
                "group_key": "OVER_UNDER",
                "db_market_group": "Over/Under",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Over/Under"),
                    ("1ST_HALF", "1st Half", "Over/Under 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "over_under",
            },
            {
                "group_key": "ASIAN_HANDICAP",
                "db_market_group": "Asian Handicap",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Asian Handicap"),
                    ("1ST_HALF", "1st Half", "Asian Handicap 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "asian_handicap",
            },
        ],
    },
    "baseball": {
        "groups": [
            {
                "group_key": "HOME_AWAY",
                "db_market_group": "Home/Away",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Full-time"),
                    ("1ST_HALF", "1st Half", "1st half")
                ],
                "betfair_period_index": 0,
                "extract_fn": "standard",
            },
            {
                "group_key": "OVER_UNDER",
                "db_market_group": "Over/Under",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Over/Under"),
                    ("1ST_HALF", "1st Half", "Over/Under 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "over_under",
            },
            {
                "group_key": "ASIAN_HANDICAP",
                "db_market_group": "Asian Handicap",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Asian Handicap"),
                    ("1ST_HALF", "1st Half", "Asian Handicap 1st half")
                ],
                "betfair_period_index": None,
                "extract_fn": "asian_handicap",
            },
        ],
    },
    "hockey": {
        "groups": [
            {
                "group_key": "HOME_AWAY",
                "db_market_group": "Home/Away",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Full-time")
                ],
                "betfair_period_index": 0,
                "extract_fn": "standard",
            },
            {
                "group_key": "OVER_UNDER",
                "db_market_group": "Over/Under",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Over/Under"),
                ],
                "betfair_period_index": None,
                "extract_fn": "over_under",
            },
            {
                "group_key": "ASIAN_HANDICAP",
                "db_market_group": "Asian Handicap",
                "has_draw": False,
                "periods": [
                    ("FT_INC_OT", "Full Time", "Asian Handicap"),
                ],
                "betfair_period_index": None,
                "extract_fn": "asian_handicap",
            },
        ],
    },
}


def build_op_fragment(group_key: Optional[str], period_key: Optional[str]) -> Optional[str]:
    """Build a hash fragment for OddsPortal market routing."""
    if not group_key or not period_key:
        return None
    group_fragment = OP_GROUPS.get(group_key)
    period_fragment = OP_PERIODS.get(period_key)
    if group_fragment is None or period_fragment is None:
        return None
    return f":{group_fragment};{period_fragment}"


def build_match_url_with_fragment(
    match_url: str,
    group_key: Optional[str],
    period_key: Optional[str],
) -> str:
    """Return a cleaned match URL with a normalized route fragment when available."""
    # 1. Clean the base URL by removing any trailing slashes
    base_url_only = (match_url or "").split("#", 1)[0].rstrip("/")

    # 2. Extract existing fragment if present (e.g. #row_id)
    url_parts = (match_url or "").split("#", 1)
    existing_fragment = url_parts[1] if len(url_parts) > 1 else ""

    # 3. Build the market segment (e.g. ":home-away;1")
    market_segment = build_op_fragment(group_key, period_key)

    if not market_segment:
        if existing_fragment:
            return f"{base_url_only}/#{existing_fragment}"
        return base_url_only

    # 4. Construct final URL: base_url / # [existing_fragment] [market_segment]
    # This results in format: .../#TOKEN:MARKET or .../#:MARKET
    # Note: market_segment already starts with ":" from build_op_fragment
    return f"{base_url_only}/#{existing_fragment}{market_segment}"


def flatten_sport_scraping_route(sport: Optional[str]) -> List[Dict[str, Any]]:
    """Flatten the nested market-group/period structure into a linear step list."""
    route = SPORT_SCRAPING_ROUTES.get(sport) if sport else None

    if route and "groups" in route:
        groups = route.get("groups", [])
    elif route:
        groups = [{
            "group_key": route.get("primary_group"),
            "db_market_group": route.get("db_market_group", "1X2"),
            "periods": route.get("periods", [("FULL_TIME", "Full Time", "Full-time")]),
            "betfair_period_index": route.get("betfair_period_index", 0),
            "extract_fn": route.get("extract_fn", "standard"),
        }]
    else:
        groups = [{
            "group_key": None,
            "db_market_group": "1X2",
            "periods": [("FULL_TIME", "Full Time", "Full-time")],
            "betfair_period_index": 0,
            "extract_fn": "standard",
        }]

    steps: List[Dict[str, Any]] = []
    step_idx = 0
    for group in groups:
        group_key = group.get("group_key")
        db_market_group = group.get("db_market_group", "1X2")
        group_display = OP_GROUPS_DISPLAY.get(group_key, db_market_group)
        betfair_period_index = group.get("betfair_period_index")
        periods = group.get("periods") or [("FULL_TIME", "Full Time", "Full-time")]
        for period_idx, period in enumerate(periods):
            if isinstance(period, (list, tuple)):
                period_key = period[0] if len(period) > 0 else None
                period_display = period[1] if len(period) > 1 else period_key
                db_market_period = period[2] if len(period) > 2 else period_display
            else:
                period_key = period
                period_display = period
                db_market_period = period
            fragment = build_op_fragment(group_key, period_key)
            steps.append({
                "step_idx": step_idx,
                "step_key": f"{group_key or 'default'}:{period_key or 'FULL_TIME'}:{step_idx}",
                "group_key": group_key,
                "group_display": group_display,
                "period_key": period_key,
                "period_display": period_display,
                "period_idx": period_idx,
                "db_market_group": db_market_group,
                "db_market_period": db_market_period,
                "db_market_name": db_market_period,
                "fragment": f"#{fragment.lstrip(':')}" if fragment else None,
                "betfair_enabled": betfair_period_index is not None and period_idx == betfair_period_index,
                "extract_fn": group.get("extract_fn", "standard"),
            })
            step_idx += 1

    return steps
