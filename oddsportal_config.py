"""
Configuration for OddsPortal scraping.
Maps internal season IDs to OddsPortal URL slugs.
"""

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

# Team name fuzzy matching overrides
# Team name fuzzy matching overrides
# Key: The name in OUR database (SofaScore)
# Value: The name displayed on OddsPortal
TEAM_ALIASES = {
    # --- Premier League / English Teams ---
    "Wolverhampton": "Wolves",
    "Manchester United": "Manchester Utd",
    "Newcastle United": "Newcastle",
    "Nottingham Forest": "Nottingham",
    "West Ham United": "West Ham",
    "Brighton & Hove Albion": "Brighton",
    "Leeds United": "Leeds",
    "Tottenham Hotspur": "Tottenham",
    "Leicester City": "Leicester",
    "Norwich City": "Norwich",
    "Ipswich Town": "Ipswich",
    "Luton Town": "Luton",
    "Sheffield United": "Sheffield Utd",
    "West Bromwich Albion": "West Brom",
    "Queens Park Rangers": "QPR",
    "Blackburn Rovers": "Blackburn",
    "Coventry City": "Coventry",
    "Stoke City": "Stoke",
    "Hull City": "Hull",
    "Cardiff City": "Cardiff",
    "Swansea City": "Swansea",
    "Middlesbrough": "Middlesbrough", 
    "Preston North End": "Preston",
    "Sheffield Wednesday": "Sheffield Wed",
    "Plymouth Argyle": "Plymouth",
    "Birmingham City": "Birmingham",
    "Huddersfield Town": "Huddersfield",
    "Rotherham United": "Rotherham",
    "Sunderland": "Sunderland",
    "Watford": "Watford",
    "Bristol City": "Bristol City",
    "Millwall": "Millwall",
    "Southampton": "Southampton",

    # --- La Liga / Spanish Teams ---
    "Real Betis": "Betis",
    "Deportivo Alavés": "Alaves",
    "Atlético Madrid": "Atl. Madrid",
    "Athletic Club": "Ath Bilbao",
    "Girona FC": "Girona",
    "Real Oviedo": "Oviedo", 
    "Levante UD": "Levante",
    "Real Valladolid": "Valladolid",
    "Sporting Gijón": "Gijon",
    "Leganés": "Leganes",
    "Almería": "Almeria", 
    "Málaga": "Malaga",
    "Real Zaragoza": "Zaragoza",
    "Real Racing Club": "Racing Santander",
    "Deportivo La Coruña": "Dep. La Coruna",
    "Espanyol": "Espanyol", 
    "Mallorca": "Mallorca",
    "Cádiz": "Cadiz",
    "CD Castellón": "Castellon",
    "Mirandés": "Mirandes",
    "FC Andorra": "Andorra",
    "Burgos Club de Fútbol": "Burgos CF",
    "AD Ceuta": "Ceuta", 
    "Cultural Leonesa": "Leonesa",
    "Córdoba": "Cordoba",
    "Huesca": "Huesca",
    "Eibar": "Eibar",
    "Albacete Balompié": "Albacete",
    "Racing de Ferrol": "Ferrol",
    "Eldense": "Eldense",
    "Cartagena": "Cartagena",
    "Tenerife": "Tenerife",

    # --- Bundesliga / German Teams ---
    "FC Bayern München": "Bayern Munich",
    "Borussia Dortmund": "Dortmund",  
    "TSG Hoffenheim": "Hoffenheim",
    "VfB Stuttgart": "Stuttgart",
    "Bayer 04 Leverkusen": "Bayer Leverkusen", 
    "SC Freiburg": "Freiburg",
    "1. FC Union Berlin": "Union Berlin",
    "FC Augsburg": "Augsburg",
    "1. FC Köln": "FC Koln",
    "Borussia M'gladbach": "B. Monchengladbach",
    "1. FSV Mainz 05": "Mainz",
    "VfL Wolfsburg": "Wolfsburg", 
    "SV Werder Bremen": "Werder Bremen",
    "FC St. Pauli": "St. Pauli", 
    "1. FC Heidenheim": "Heidenheim",
    "Hamburger SV": "Hamburger SV",
    "RB Leipzig": "RB Leipzig",
    "Eintracht Frankfurt": "Eintracht Frankfurt",
    "VfL Bochum": "Bochum", 
    "SV Darmstadt 98": "Darmstadt",
    "Holstein Kiel": "Holstein Kiel",
    "Fortuna Düsseldorf": "Dusseldorf",
    "Hannover 96": "Hannover",
    "Karlsruher SC": "Karlsruher",
    "SC Paderborn 07": "Paderborn", 
    "Greuther Fürth": "Greuther Furth",
    "Hertha BSC": "Hertha Berlin",
    "1. FC Magdeburg": "Magdeburg",
    "Schalke 04": "Schalke",
    "1. FC Nürnberg": "Nurnberg",
    "SSV Jahn Regensburg": "Regensburg",
    "SV Elversberg": "Elversberg", 
    "VfL Osnabrück": "Osnabruck",
    "Hansa Rostock": "Hansa Rostock",
    "Eintracht Braunschweig": "Braunschweig",
    "1. FC Kaiserslautern": "Kaiserslautern",
    "Wehen Wiesbaden": "Wehen",

    # --- Serie A / Italian Teams ---
    "Hellas Verona": "Verona",
    "Milan": "AC Milan", 
    "Roma": "AS Roma",
    "Torino": "Torino", # Direct match usually fine but good to be explicit
    "Udinese": "Udinese",
    "Lecce": "Lecce",
    "Fiorentina": "Fiorentina",
    "Sassuolo": "Sassuolo",
    "Lazio": "Lazio",
    "Bologna": "Bologna",
    "Atalanta": "Atalanta",
    "Monza": "Monza",
    "Salernitana": "Salernitana",
    "Empoli": "Empoli",
    "Frosinone": "Frosinone",
    "Internazionale": "Inter", # Just in case DB uses full name
    "Inter": "Inter",

    # --- League 1 / French Teams ---
    "Paris Saint-Germain": "PSG",
    "AS Monaco": "Monaco",
    "Olympique Lyonnais": "Lyon",
    "Olympique Marseille": "Marseille",
    "Stade Rennais FC": "Rennes",
    "Lille OSC": "Lille",
    "RC Strasbourg Alsace": "Strasbourg",
    "OGC Nice": "Nice",
    "FC Nantes": "Nantes",
    "FC Metz": "Metz",
    "FC Lorient": "Lorient",
    "Stade Brestois 29": "Brest",
    "Montpellier HSC": "Montpellier",
    "Angers SCO": "Angers",
    "AS Saint-Étienne": "St-Etienne",
    "Toulouse FC": "Toulouse",
    "RC Lens": "Lens",
    "Girondins de Bordeaux": "Bordeaux",
    "AS Monaco": "Monaco",
    "AS Monaco": "Monaco",

    # --- Saudi Professional League / Saudi Arabian Teams ---
    "Al-Hilal": "Al Hilal",
    "Al-Nassr": "Al Nassr",
    "Al-Ittihad": "Al Ittihad",
    "Al-Ahli": "Al Ahli",
    "Al-Shabab": "Al Shabab",
    "Al-Fateh": "Al Fateh",
    "Al-Raed": "Al Raed",
    "Al-Taawoun": "Al Taawoun",
    "Al-Fayha": "Al Fayha",
    "Al-Khaleej": "Al Khaleej",
    "Al-Okhdood": "Al Okhdood",
    "Al-Wehda": "Al Wehda",
    "Al-Tai": "Al Tai",
    "Al-Hazem": "Al Hazem",
    "Al-Riyadh": "Al Riyadh",
    "Al-Najma": "Al Najma",
    "Al-Qadsiah": "Al Qadsiah",
    "Al-Jabalain": "Al Jabalain",
    "Al-Orobah": "Al Orobah",
    "Al-Ahli": "Al Ahli",
    "Al-Shabab": "Al Shabab",
    "Al-Fateh": "Al Fateh",
    "Al-Raed": "Al Raed",
    "Al-Taawoun": "Al Taawoun",
    "Al-Fayha": "Al Fayha",
    "Al-Khaleej": "Al Khaleej",
    "Al-Okhdood": "Al Okhdood",
    "Al-Wehda": "Al Wehda",
    "Al-Tai": "Al Tai",
    "Al-Hazem": "Al Hazem",
    "Al-Riyadh": "Al Riyadh",
    "Al-Najma": "Al Najma",
    "Al-Qadsiah": "Al Qadsiah",
    "Al-Jabalain": "Al Jabalain",
    "Al-Orobah": "Al Orobah",
}

# Priority order for single-bookie initial odds extraction
# Also used to filter which bookies are stored in the database
PRIORITY_BOOKIES = ["bet365", "Pinnacle", "BettingAsia", "Megapari", "1xBet"]

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
