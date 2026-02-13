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
TEAM_ALIASES = {
    "Man Utd": "Manchester United",
    "Man City": "Manchester City",
    "Sheff Utd": "Sheffield United",
    "Nottm Forest": "Nottingham Forest",
    "Wolves": "Wolverhampton Wanderers",
}
