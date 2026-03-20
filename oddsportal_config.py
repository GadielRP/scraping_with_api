"""
Configuration for OddsPortal scraping.
Maps internal season IDs to OddsPortal URL slugs.
"""

from typing import Any, Dict, List, Optional

# Maps season_id -> OddsPortal URL slug
# Based on sport_league_constants.py from OddsHarvester
SEASON_ODDSPORTAL_MAP = {
    # Premier League
    76986: {"sport": "football", "country": "england", "league": "premier-league"},
    # La Liga
    77559: {"sport": "football", "country": "spain", "league": "laliga"},
    # Serie A
    76457: {"sport": "football", "country": "italy", "league": "serie-a"},
    # Bundesliga
    77333: {"sport": "football", "country": "germany", "league": "bundesliga"},
    # NBA
    80229: {"sport": "basketball", "country": "usa", "league": "nba"},
    # NFL
    75522: {"sport": "american-football", "country": "usa", "league": "nfl"},
    # MLB
    84695: {"sport": "baseball", "country": "usa", "league": "mlb"},
    # NHL
    78476: {"sport": "hockey", "country": "usa", "league": "nhl"},
    # League 1
    77356: {"sport": "football", "country": "france", "league": "ligue-1"},
    # Saudi Pro League
    80443: {"sport": "football", "country": "saudi-arabia", "league": "saudi-professional-league"},
    # Swedish Hockey League
    75679: {"sport": "hockey", "country": "sweden", "league": "shl"},
    # Philippines Football League
    81520: {"sport": "football", "country": "philippines", "league": "pfl"},
    # Chinese Basketball Association
    85375: {"sport": "basketball", "country": "china", "league": "cba"},

}

# Normalize bookie names to match DB exact names
BOOKIE_ALIASES = {
    "10Bet": "10bet",
    "1xBet": "1xbet",
    "888sport": "888 Sport",
    "bet365": "bet365",
    "Betfair": "Betfair",
    "Betsson": "Betsson",
    "BetVictor": "BetVictor",
    "Betway": "Betway",
    "Bwin": "bwin",
    "Coral": "Coral",
    "Ladbrokes": "Ladbrokes",
    "Marathonbet": "Marathonbet",
    "Pinnacle": "Pinnacle",
    "Unibet": "Unibet",
    "William Hill": "William Hill",
}

# Common institutional suffixes/prefixes and noise tokens to remove during matching
# These are used by TeamMatcher to identify the "strong" part of a team name.
# We only include short suffixes/abbreviations to avoid destroying the team name.
INSTITUTIONAL_NOISE = [
    "fc", "cf", "if", "sc", "ac", "ud", "afc", "rc", "bk", "hk", "hc", "fk", 
    "as", "sd", "cd", "vfb", "vfl", "tsg", "ssv", "hsc", "fsv", "sv", "rb", 
    "spvg", "mtv", "vfr", "tus", "sg", "bsc", "aik", "hif", "dif", "ifk"
]

# Team name fuzzy matching overrides
# Key: The name in OUR database (SofaScore)
# Value: One or more names displayed on OddsPortal
TEAM_ALIASES = {
    # --- Premier League / English Teams ---
    "Wolverhampton": ["Wolves", "Wolverhampton"],
    "Manchester United": ["Manchester Utd", "Man United", "Man Utd"],
    "Newcastle United": ["Newcastle"],
    "Nottingham Forest": ["Nottingham"],
    "West Ham United": ["West Ham"],
    "Brighton & Hove Albion": ["Brighton"],
    "Leeds United": ["Leeds"],
    "Tottenham Hotspur": ["Tottenham", "Spurs"],
    "Leicester City": ["Leicester"],
    "Norwich City": ["Norwich"],
    "Ipswich Town": ["Ipswich"],
    "Luton Town": ["Luton"],
    "Sheffield United": ["Sheffield Utd"],
    "West Bromwich Albion": ["West Brom"],
    "Queens Park Rangers": ["QPR"],
    "Blackburn Rovers": ["Blackburn"],
    "Coventry City": ["Coventry"],
    "Stoke City": ["Stoke"],
    "Hull City": ["Hull"],
    "Cardiff City": ["Cardiff"],
    "Swansea City": ["Swansea"],
    "Middlesbrough": ["Middlesbrough"], 
    "Preston North End": ["Preston"],
    "Sheffield Wednesday": ["Sheffield Wed", "Sheffield Weds"],
    "Plymouth Argyle": ["Plymouth"],
    "Birmingham City": ["Birmingham"],
    "Huddersfield Town": ["Huddersfield"],
    "Rotherham United": ["Rotherham"],
    "Sunderland": ["Sunderland"],
    "Watford": ["Watford"],
    "Bristol City": ["Bristol City"],
    "Millwall": ["Millwall"],
    "Southampton": ["Southampton"],

    # --- La Liga / Spanish Teams ---
    "Real Betis": ["Betis"],
    "Deportivo Alavés": ["Alaves"],
    "Atlético Madrid": ["Atl. Madrid", "Atletico Madrid"],
    "Athletic Club": ["Ath Bilbao", "Athletic Bilbao"],
    "Girona FC": ["Girona"],
    "Real Oviedo": ["Oviedo"], 
    "Levante UD": ["Levante"],
    "Real Valladolid": ["Valladolid"],
    "Sporting Gijón": ["Gijon"],
    "Leganés": ["Leganes"],
    "Almería": ["Almeria"], 
    "Málaga": ["Malaga"],
    "Real Zaragoza": ["Zaragoza"],
    "Real Racing Club": ["Racing Santander"],
    "Deportivo La Coruña": ["Dep. La Coruna"],
    "Espanyol": ["Espanyol"], 
    "Mallorca": ["Mallorca"],
    "Cádiz": ["Cadiz"],
    "CD Castellón": ["Castellon"],
    "Mirandés": ["Mirandes"],
    "FC Andorra": ["Andorra"],
    "Burgos Club de Fútbol": ["Burgos CF"],
    "AD Ceuta": ["Ceuta"], 
    "Cultural Leonesa": ["Leonesa"],
    "Córdoba": ["Cordoba"],
    "Huesca": ["Huesca"],
    "Eibar": ["Eibar"],
    "Albacete Balompié": ["Albacete"],
    "Racing de Ferrol": ["Ferrol"],
    "Eldense": ["Eldense"],
    "Cartagena": ["Cartagena"],
    "Tenerife": ["Tenerife"],

    # --- Bundesliga / German Teams ---
    "FC Bayern München": ["Bayern Munich", "Bayern"],
    "Borussia Dortmund": ["Dortmund"],  
    "TSG Hoffenheim": ["Hoffenheim"],
    "VfB Stuttgart": ["Stuttgart"],
    "Bayer 04 Leverkusen": ["Bayer Leverkusen", "Bayer"], 
    "SC Freiburg": ["Freiburg"],
    "1. FC Union Berlin": ["Union Berlin"],
    "FC Augsburg": ["Augsburg"],
    "1. FC Köln": ["FC Koln"],
    "Borussia M'gladbach": ["B. Monchengladbach", "Monchengladbach"],
    "1. FSV Mainz 05": ["Mainz"],
    "VfL Wolfsburg": ["Wolfsburg"], 
    "SV Werder Bremen": ["Werder Bremen", "Bremen"],
    "FC St. Pauli": ["St. Pauli"], 
    "1. FC Heidenheim": ["Heidenheim"],
    "Hamburger SV": ["Hamburger SV", "Hamburg"],
    "RB Leipzig": ["RB Leipzig", "Leipzig"],
    "Eintracht Frankfurt": ["Eintracht Frankfurt", "Frankfurt"],
    "VfL Bochum": ["Bochum"], 
    "SV Darmstadt 98": ["Darmstadt"],
    "Holstein Kiel": ["Holstein Kiel"],
    "Fortuna Düsseldorf": ["Dusseldorf"],
    "Hannover 96": ["Hannover"],
    "Karlsruher SC": ["Karlsruher"],
    "SC Paderborn 07": ["Paderborn"], 
    "Greuther Fürth": ["Greuther Furth"],
    "Hertha BSC": ["Hertha Berlin", "Hertha"],
    "1. FC Magdeburg": ["Magdeburg"],
    "Schalke 04": ["Schalke"],
    "1. FC Nürnberg": ["Nurnberg"],
    "SSV Jahn Regensburg": ["Regensburg"],
    "SV Elversberg": ["Elversberg"], 
    "VfL Osnabrück": ["Osnabruck"],
    "Hansa Rostock": ["Hansa Rostock"],
    "Eintracht Braunschweig": ["Braunschweig"],
    "1. FC Kaiserslautern": ["Kaiserslautern"],
    "Wehen Wiesbaden": ["Wehen"],

    # --- Serie A / Italian Teams ---
    "Hellas Verona": ["Verona"],
    "Milan": ["AC Milan", "Milan"], 
    "Roma": ["AS Roma", "Roma"],
    "Torino": ["Torino"],
    "Udinese": ["Udinese"],
    "Lecce": ["Lecce"],
    "Fiorentina": ["Fiorentina"],
    "Sassuolo": ["Sassuolo"],
    "Lazio": ["Lazio"],
    "Bologna": ["Bologna"],
    "Atalanta": ["Atalanta"],
    "Monza": ["Monza"],
    "Salernitana": ["Salernitana"],
    "Empoli": ["Empoli"],
    "Frosinone": ["Frosinone"],
    "Internazionale": ["Inter"],
    "Inter": ["Inter"],

    # --- League 1 / French Teams ---
    "Paris Saint-Germain": ["PSG"],
    "AS Monaco": ["Monaco"],
    "Olympique Lyonnais": ["Lyon"],
    "Olympique Marseille": ["Marseille"],
    "Stade Rennais FC": ["Rennes"],
    "Lille OSC": ["Lille"],
    "RC Strasbourg Alsace": ["Strasbourg"],
    "OGC Nice": ["Nice"],
    "FC Nantes": ["Nantes"],
    "FC Metz": ["Metz"],
    "FC Lorient": ["Lorient"],
    "Stade Brestois 29": ["Brest"],
    "Montpellier HSC": ["Montpellier"],
    "Angers SCO": ["Angers"],
    "AS Saint-Étienne": ["St-Etienne"],
    "Toulouse FC": ["Toulouse"],
    "RC Lens": ["Lens"],
    "Girondins de Bordeaux": ["Bordeaux"],

    # --- Saudi Professional League / Saudi Arabian Teams ---
    "Al-Hilal": ["Al Hilal"],
    "Al-Nassr": ["Al Nassr"],
    "Al-Ittihad": ["Al Ittihad"],
    "Al-Ahli": ["Al Ahli"],
    "Al-Shabab": ["Al Shabab"],
    "Al-Fateh": ["Al Fateh"],
    "Al-Raed": ["Al Raed"],
    "Al-Taawoun": ["Al Taawoun"],
    "Al-Fayha": ["Al Fayha"],
    "Al-Khaleej": ["Al Khaleej"],
    "Al-Okhdood": ["Al Okhdood"],
    "Al-Wehda": ["Al Wehda"],
    "Al-Tai": ["Al Tai"],
    "Al-Hazem": ["Al Hazem"],
    "Al-Riyadh": ["Al Riyadh"],
    "Al-Najma": ["Al Najma"],
    "Al-Qadsiah": ["Al Qadsiah"],
    "Al-Jabalain": ["Al Jabalain"],
    "Al-Orobah": ["Al Orobah"],

    # --- CBA / Chinese Teams ---
    "Shanghai Sharks": ["Shanghai"],
    "Guangsha": ["Zhejiang Guangsha"],
    "Guangdong Southern Tigers": ["Guangdong"],
    "Shandong Heroes": ["Shandong"],
    "Shenzhen Leopards": ["Shenzhen"],
    "Qingdao Eagles": ["Qingdao"],
    "Beijing Ducks": ["Beijing"],
    "Flying Leopards": ["Liaoning"],
    "Shanxi Loongs": ["Shanxi Zhongyu"],
    "Ningbo Rockets": ["Ningbo Rockets"],
    "Zhejiang Golden Bulls": ["Zhejiang Chouzhou"],
    "Beijing Royal Fighters": ["Beijing Royal Fighters"],
    "Flying Tigers": ["Xinjiang"],
    "Northeast Tigers": ["Jilin"],
    "Guangzhou": ["Guangzhou"],
    "Tianjin Pioneers": ["Tianjin"],
    "Fujian Sturgeons": ["Fujian"],
    "Nanjing Monkey Kings": ["Nanjing Tongxi"],
    "Jiangsu Dragons": ["Jiangsu Dragons"],
    "Sichuan Blue Whales": ["Sichuan"],

    # --- PFL / Philippines Teams ---
    "Taguig FC": ["One Taguig"],
    "DH Cebu FC": ["Cebu FC"],
    "Kaya": ["Kaya"],
    "Manila Digger FC": ["Manila Digger"],
    "Aguilas-UMak": ["Davao Aguilas"],
    "Stallion Laguna": ["Stallion"],
    "Maharlika Manila FC": ["Maharlika"],
    "Tuloy FC": ["Tuloy"],
    "Don Bosco Garelli United": ["Don Bosco Garelli"],
    "Valenzuela PB-Mendiola": ["Mendiola FC 1991"],
    "Philippine Army": ["Philippine Army"],

    # --- SHL / Swedish Hockey League Teams ---
    "Skellefteå AIK": ["Skelleftea"],
    "Frölunda HC": ["Frolunda"],
    "Växjö Lakers": ["Vaxjo"],
    "Rögle BK": ["Rogle"],
    "Brynäs IF": ["Brynas"],
    "Färjestad BK": ["Farjestad"],
    "Luleå HF": ["Lulea"],
    "Malmö Redhawks": ["Malmo", "Malmö"],
    "Djurgårdens IF": ["Djurgarden", "Djurgården"],
    "Örebro HK": ["Orebro"],
    "Timrå IK": ["Timra"],
    "Linköping HC": ["Linkoping"],
    "Leksands IF": ["Leksand"],
    "HV71": ["HV 71"],
}

# Priority order for single-bookie initial odds extraction
# Also used to filter which bookies are stored in the database
PRIORITY_BOOKIES = ["bet365", "Pinnacle", "BetInAsia", "Megapari", "1xBet"]

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
# ---------------------------------------------------------------------------

SPORT_SCRAPING_ROUTES = {
    "football": {
        "groups": [
            {
                "group_key": "1X2",
                "db_market_group": "1X2",
                "has_draw": True,
                "periods": [
                    ("FULL_TIME", "Full Time", "Full time"),
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
                    ("1ST_HALF", "1st Half", "1st half Over/Under")
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
                    ("1ST_HALF", "1st Half", "1st half Asian Handicap")
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
                    ("FT_INC_OT", "Full Time", "Full time"),
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
                    ("1ST_HALF", "1st Half", "1st half Over/Under")
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
                    ("1ST_HALF", "1st Half", "1st half Asian Handicap")
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
                    ("FT_INC_OT", "Full Time", "Full time"),
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
                    ("1ST_HALF", "1st Half", "1st half Over/Under")
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
                    ("1ST_HALF", "1st Half", "1st half Asian Handicap")
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
                    ("FT_INC_OT", "Full Time", "Full time"),
                ],
                "betfair_period_index": 0,
                "extract_fn": "standard",
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
                    ("FT_INC_OT", "Full Time", "Full time")
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
    return f"#{group_fragment};{period_fragment}"


def build_match_url_with_fragment(
    match_url: str,
    group_key: Optional[str],
    period_key: Optional[str],
) -> str:
    """Return a cleaned match URL with a normalized route fragment when available."""
    base_url = (match_url or "").split("#", 1)[0].rstrip("/")
    fragment = build_op_fragment(group_key, period_key)
    if not fragment:
        return base_url
    return f"{base_url}/{fragment}"


def flatten_sport_scraping_route(sport: Optional[str]) -> List[Dict[str, Any]]:
    """
    Flatten the nested market-group/period structure into a deterministic, linear step list.

    Fallback behavior:
      - Unknown sport -> legacy single-step route.
      - Legacy route format (without `groups`) -> converted into one synthetic group.
    """
    route = SPORT_SCRAPING_ROUTES.get(sport) if sport else None

    if route and "groups" in route:
        groups = route.get("groups", [])
    elif route:
        groups = [{
            "group_key": route.get("primary_group"),
            "db_market_group": route.get("db_market_group", "1X2"),
            "periods": route.get("periods", [("FULL_TIME", "Full Time", "Full time")]),
            "betfair_period_index": route.get("betfair_period_index", 0),
            "extract_fn": route.get("extract_fn", "standard"),
        }]
    else:
        groups = [{
            "group_key": None,
            "db_market_group": "1X2",
            "periods": [("FULL_TIME", "Full Time", "Full time")],
            "betfair_period_index": 0,
            "extract_fn": "standard",
        }]

    steps: List[Dict[str, Any]] = []
    step_idx = 0

    for group_idx, group_cfg in enumerate(groups):
        group_key = group_cfg.get("group_key")
        periods = group_cfg.get("periods") or [("FULL_TIME", "Full Time", "Full time")]
        betfair_period_index = group_cfg.get("betfair_period_index")
        extract_fn = group_cfg.get("extract_fn", "standard")
        db_market_group = group_cfg.get("db_market_group", "1X2")
        group_display = OP_GROUPS_DISPLAY.get(group_key, group_key) if group_key else db_market_group

        for period_idx, period_tuple in enumerate(periods):
            period_key = period_tuple[0] if len(period_tuple) > 0 else "FULL_TIME"
            db_market_period = period_tuple[1] if len(period_tuple) > 1 else "Full Time"
            db_market_name = period_tuple[2] if len(period_tuple) > 2 else db_market_period
            step = {
                "step_idx": step_idx,
                "step_key": f"{group_key}:{period_key}",
                "group_idx": group_idx,
                "period_idx": period_idx,
                "group_key": group_key,
                "group_display": group_display,
                "db_market_group": db_market_group,
                "period_key": period_key,
                "period_display": db_market_period,
                "db_market_period": db_market_period,
                "db_market_name": db_market_name,
                "extract_fn": extract_fn,
                "betfair_period_index": betfair_period_index,
                "betfair_enabled": betfair_period_index == period_idx,
                "fragment": build_op_fragment(group_key, period_key),
            }
            steps.append(step)
            step_idx += 1

    return steps
