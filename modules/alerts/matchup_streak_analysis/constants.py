"""
Constants for Matchup Streak Analysis module.

Contains all static data (season IDs, conference/league mappings, thresholds)
and pure lookup functions that operate solely on those constants.
Extracted from historical_standings.py.
"""

from typing import Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Collected Season IDs - seasons we have fully collected in our database
# From sport_seasons_processing.py season_to_process list
# ---------------------------------------------------------------------------

# Human-readable configuration with metadata
# Each entry represents a "full season" which may include multiple season_ids
# (e.g., NBA regular season + NBA Cup are part of the same collected package)
COLLECTED_SEASON_IDS: List[Dict] = [
    # NBA seasons (includes In-Season Tournament / NBA Cup since 2023)
    {"season_name": "NBA 25/26", "season_id": 80229, "additional_season_id": 84238},  # NBA 25/26, NBA Cup 2025
    {"season_name": "NBA 24/25", "season_id": 65360, "additional_season_id": 69143},  # NBA 24/25, NBA Cup 2024
    {"season_name": "NBA 23/24", "season_id": 54105, "additional_season_id": 56094},  # NBA 23/24, NBA Cup 2023
    {"season_name": "NBA 22/23", "season_id": 45096, "additional_season_id": None},   # NBA 22/23
    {"season_name": "NBA 21/22", "season_id": 38191, "additional_season_id": None},   # NBA 21/22
    {"season_name": "NBA 20/21", "season_id": 34951, "additional_season_id": None},   # NBA 20/21
    # La Liga seasons
    {"season_name": "La Liga 2025", "season_id": 77559, "additional_season_id": None},
    {"season_name": "La Liga 2024", "season_id": 61643, "additional_season_id": None},
    {"season_name": "La Liga 2023", "season_id": 52376, "additional_season_id": None},
    {"season_name": "La Liga 2022", "season_id": 42409, "additional_season_id": None},
    {"season_name": "La Liga 2021", "season_id": 37223, "additional_season_id": None},
    {"season_name": "La Liga 2020", "season_id": 32501, "additional_season_id": None},
    # Premier League seasons
    {"season_name": "Premier League 2025", "season_id": 76986, "additional_season_id": None},
    {"season_name": "Premier League 2024", "season_id": 61627, "additional_season_id": None},
    {"season_name": "Premier League 2023", "season_id": 52186, "additional_season_id": None},
    {"season_name": "Premier League 2022", "season_id": 41886, "additional_season_id": None},
    {"season_name": "Premier League 2021", "season_id": 37036, "additional_season_id": None},
    {"season_name": "Premier League 2020", "season_id": 29415, "additional_season_id": None},
    # NFL seasons
    {"season_name": "NFL 2025", "season_id": 75522, "additional_season_id": None},
    {"season_name": "NFL 2024", "season_id": 60592, "additional_season_id": None},
    {"season_name": "NFL 2023", "season_id": 51361, "additional_season_id": None},
    {"season_name": "NFL 2022", "season_id": 46786, "additional_season_id": None},
    {"season_name": "NFL 2021", "season_id": 36422, "additional_season_id": None},
    {"season_name": "NFL 2020", "season_id": 27719, "additional_season_id": None},
    # MLB (Baseball) seasons
    {"season_name": "MLB 2026", "season_id": 84695, "additional_season_id": None},
    {"season_name": "MLB 2025", "season_id": 68611, "additional_season_id": None},
    {"season_name": "MLB 2024", "season_id": 57577, "additional_season_id": None},
    # NHL (Ice Hockey) seasons
    {"season_name": "NHL 2025", "season_id": 78476, "additional_season_id": None},
    # Serie A seasons
    {"season_name": "Serie A 2025", "season_id": 76457, "additional_season_id": None},
    # Bundesliga seasons
    {"season_name": "Bundesliga 2025", "season_id": 77333, "additional_season_id": None},
    # league 1 2025
    {"season_name": "League 1 2025", "season_id": 77356, "additional_season_id": None},
    # saudi professional league 2025
    {"season_name": "Saudi Pro League 2025", "season_id": 80443, "additional_season_id": None},
    # SHL sweden hockey league 2025
    {"season_name": "SHL 2025", "season_id": 75679, "additional_season_id": None},
    # PFL 2025 philipphines football league
    {"season_name": "PFL 2025", "season_id": 81520, "additional_season_id": None},
    # China basketball association
    {"season_name": "CBA 2025", "season_id": 85375, "additional_season_id": None},
]

# ---------------------------------------------------------------------------
# Pre-computed flat set for O(1) lookups
# This "Quick Index" contains ALL season_ids (main + additional) for instant checking
# ---------------------------------------------------------------------------
_ALL_COLLECTED_IDS: Set[int] = set()
for _entry in COLLECTED_SEASON_IDS:
    _ALL_COLLECTED_IDS.add(_entry["season_id"])
    if _entry.get("additional_season_id"):
        _ALL_COLLECTED_IDS.add(_entry["additional_season_id"])


# ---------------------------------------------------------------------------
# Standings computation families (points / percentage methods)
# ---------------------------------------------------------------------------

POINTS_METHOD_WIN_PCT_SEASON_IDS = {
    # NBA seasons (regular + additional seasons already collected)
    80229, 84238,  # NBA 25/26 + NBA Cup 2025
    65360, 69143,  # NBA 24/25 + NBA Cup 2024
    54105, 56094,  # NBA 23/24 + NBA Cup 2023
    45096,         # NBA 22/23
    38191,         # NBA 21/22
    34951,         # NBA 20/21
    # MLB seasons
    84695,         # MLB 2026
    68611,         # MLB 2025
    57577,         # MLB 2024
    # CBA
    85375,         # CBA 2025
}

POINTS_METHOD_WIN_PCT_HALF_TIE_SEASON_IDS = {
    75522,  # NFL 2025
    60592,  # NFL 2024
    51361,  # NFL 2023
    46786,  # NFL 2022
    36422,  # NFL 2021
    27719,  # NFL 2020
}

POINTS_METHOD_FOOTBALL_3_1_0_SEASON_IDS = {
    # La Liga
    77559, 61643, 52376, 42409, 37223, 32501,
    # Premier League
    76986, 61627, 52186, 41886, 37036, 29415,
    # Serie A
    76457,
    # Bundesliga
    77333,
    # Ligue 1
    77356,
    # Saudi Pro League
    80443,
    # PFL
    81520,
}

POINTS_METHOD_NHL_2_1_0_OTL_SEASON_IDS = {
    78476,  # NHL 2025
}

POINTS_METHOD_HOCKEY_3_2_1_0_SEASON_IDS = {
    75679,  # SHL 2025
}

# Grouping/ranking scope (separate from points method)
GROUPING_NBA_CONFERENCE_SEASON_IDS = {
    80229, 84238, 65360, 69143, 54105, 56094, 45096, 38191, 34951
}

GROUPING_NFL_CONFERENCE_SEASON_IDS = {
    75522, 60592, 51361, 46786, 36422, 27719
}

GROUPING_MLB_LEAGUE_SEASON_IDS = {
    84695, 68611, 57577
}

GROUPING_NHL_CONFERENCE_SEASON_IDS = {
    78476
}

# ---------------------------------------------------------------------------
# Conference/Division Mappings for NBA, NFL, MLB, and NHL
# ---------------------------------------------------------------------------

NBA_EASTERN_CONFERENCE = {
    'Atlanta Hawks', 'Boston Celtics', 'Brooklyn Nets', 'Charlotte Hornets',
    'Chicago Bulls', 'Cleveland Cavaliers', 'Detroit Pistons', 'Indiana Pacers',
    'Miami Heat', 'Milwaukee Bucks', 'New York Knicks', 'Orlando Magic',
    'Philadelphia 76ers', 'Toronto Raptors', 'Washington Wizards'
}

NBA_WESTERN_CONFERENCE = {
    'Dallas Mavericks', 'Denver Nuggets', 'Golden State Warriors', 'Houston Rockets',
    'Los Angeles Clippers', 'Los Angeles Lakers', 'Memphis Grizzlies', 'Minnesota Timberwolves',
    'New Orleans Pelicans', 'Oklahoma City Thunder', 'Phoenix Suns', 'Portland Trail Blazers',
    'Sacramento Kings', 'San Antonio Spurs', 'Utah Jazz'
}

NFL_AFC = {
    'Baltimore Ravens', 'Buffalo Bills', 'Cincinnati Bengals', 'Cleveland Browns',
    'Denver Broncos', 'Houston Texans', 'Indianapolis Colts', 'Jacksonville Jaguars',
    'Kansas City Chiefs', 'Las Vegas Raiders', 'Los Angeles Chargers', 'Miami Dolphins',
    'New England Patriots', 'New York Jets', 'Pittsburgh Steelers', 'Tennessee Titans'
}

NFL_NFC = {
    'Arizona Cardinals', 'Atlanta Falcons', 'Carolina Panthers', 'Chicago Bears',
    'Dallas Cowboys', 'Detroit Lions', 'Green Bay Packers', 'Los Angeles Rams',
    'Minnesota Vikings', 'New Orleans Saints', 'New York Giants', 'Philadelphia Eagles',
    'San Francisco 49ers', 'Seattle Seahawks', 'Tampa Bay Buccaneers', 'Washington Commanders'
}

# MLB - American League (AL) and National League (NL)
MLB_AMERICAN_LEAGUE = {
    # AL East
    'Baltimore Orioles', 'Boston Red Sox', 'New York Yankees', 'Tampa Bay Rays', 'Toronto Blue Jays',
    # AL Central
    'Chicago White Sox', 'Cleveland Guardians', 'Detroit Tigers', 'Kansas City Royals', 'Minnesota Twins',
    # AL West
    'Houston Astros', 'Los Angeles Angels', 'Athletics', 'Seattle Mariners', 'Texas Rangers'
}

MLB_NATIONAL_LEAGUE = {
    # NL East
    'Atlanta Braves', 'Miami Marlins', 'New York Mets', 'Philadelphia Phillies', 'Washington Nationals',
    # NL Central
    'Chicago Cubs', 'Cincinnati Reds', 'Milwaukee Brewers', 'Pittsburgh Pirates', 'St. Louis Cardinals',
    # NL West
    'Arizona Diamondbacks', 'Colorado Rockies', 'Los Angeles Dodgers', 'San Diego Padres', 'San Francisco Giants'
}

# NHL - Eastern and Western Conferences
NHL_EASTERN_CONFERENCE = {
    # Atlantic Division
    'Boston Bruins', 'Buffalo Sabres', 'Detroit Red Wings', 'Florida Panthers',
    'Montréal Canadiens', 'Ottawa Senators', 'Tampa Bay Lightning', 'Toronto Maple Leafs',
    # Metropolitan Division
    'Carolina Hurricanes', 'Columbus Blue Jackets', 'New Jersey Devils', 'New York Islanders',
    'New York Rangers', 'Philadelphia Flyers', 'Pittsburgh Penguins', 'Washington Capitals'
}

NHL_WESTERN_CONFERENCE = {
    # Central Division
    'Utah Mammoth', 'Chicago Blackhawks', 'Colorado Avalanche', 'Dallas Stars',
    'Minnesota Wild', 'Nashville Predators', 'St. Louis Blues', 'Winnipeg Jets',
    # Pacific Division
    'Anaheim Ducks', 'Calgary Flames', 'Edmonton Oilers', 'Los Angeles Kings',
    'San Jose Sharks', 'Seattle Kraken', 'Vancouver Canucks', 'Vegas Golden Knights'
}


# ---------------------------------------------------------------------------
# Pure lookup functions (operate only on constants above)
# ---------------------------------------------------------------------------

def get_all_season_ids(season_id: int) -> List[int]:
    """
    Get all related season IDs for a given season (main + additional).

    For example, NBA 25/26 (80229) also includes NBA Cup 2025 (84238).
    This function returns [80229, 84238] so we can query the FULL season.

    Args:
        season_id: Any season ID (main or additional)

    Returns:
        List of all season IDs that belong to the same "full season"
    """
    for entry in COLLECTED_SEASON_IDS:
        main_id = entry["season_id"]
        additional_id = entry.get("additional_season_id")

        # Check if this entry matches our season_id
        if main_id == season_id or additional_id == season_id:
            if additional_id:
                return [main_id, additional_id]
            else:
                return [main_id]

    # Not found in collected seasons, return just the input
    return [season_id]


def get_canonical_season_id(season_id: int) -> int:
    """
    Get the canonical (main) season ID for caching and identification.
    If season_id belongs to a bundle with a main + additional season,
    always return the main season_id.
    """
    for entry in COLLECTED_SEASON_IDS:
        main_id = entry["season_id"]
        # Check if this entry matches our season_id
        if main_id == season_id or entry.get("additional_season_id") == season_id:
            return main_id

    # Not found in collected seasons, return just the input
    return season_id


def is_season_collected(season_id: int) -> bool:
    """
    Check if a season is in our collected set.

    Uses the pre-computed _ALL_COLLECTED_IDS set for O(1) lookup.
    This includes both main season_ids AND additional_season_ids (e.g., NBA Cup).

    Args:
        season_id: The season ID to check

    Returns:
        True if we have fully collected this season's data
    """
    if season_id is None:
        return False
    return int(season_id) in _ALL_COLLECTED_IDS


def get_standings_method(season_id: int, sport: str = None) -> str:
    """
    Determine standings computation method for a season.

    Returns one of:
    - "win_pct"
    - "win_pct_half_tie"
    - "football_3_1_0"
    - "nhl_2_1_0_otl"
    - "hockey_3_2_1_0"
    """
    sid = int(season_id) if season_id else 0

    if sid in POINTS_METHOD_WIN_PCT_SEASON_IDS:
        return "win_pct"
    if sid in POINTS_METHOD_WIN_PCT_HALF_TIE_SEASON_IDS:
        return "win_pct_half_tie"
    if sid in POINTS_METHOD_FOOTBALL_3_1_0_SEASON_IDS:
        return "football_3_1_0"
    if sid in POINTS_METHOD_NHL_2_1_0_OTL_SEASON_IDS:
        return "nhl_2_1_0_otl"
    if sid in POINTS_METHOD_HOCKEY_3_2_1_0_SEASON_IDS:
        return "hockey_3_2_1_0"

    if sport:
        sport_lower = sport.lower()
        if "american football" in sport_lower:
            return "win_pct_half_tie"
        if "football" in sport_lower or "soccer" in sport_lower:
            return "football_3_1_0"
        if "ice hockey" in sport_lower or "hockey" in sport_lower:
            return "nhl_2_1_0_otl"
        if "basketball" in sport_lower or "baseball" in sport_lower:
            return "win_pct"

    return "win_pct"


def get_grouping_method(season_id: int, sport: str = None) -> str:
    """
    Determine standings grouping method for a season.

    Returns one of:
    - "league_wide"
    - "nba_conference"
    - "nfl_conference"
    - "mlb_league"
    - "nhl_conference"
    """
    sid = int(season_id) if season_id else 0

    if sid in GROUPING_NBA_CONFERENCE_SEASON_IDS:
        return "nba_conference"
    if sid in GROUPING_NFL_CONFERENCE_SEASON_IDS:
        return "nfl_conference"
    if sid in GROUPING_MLB_LEAGUE_SEASON_IDS:
        return "mlb_league"
    if sid in GROUPING_NHL_CONFERENCE_SEASON_IDS:
        return "nhl_conference"

    return "league_wide"


def get_team_group(team_name: str, grouping_method: str) -> Optional[str]:
    """Get the grouping bucket for a team under a grouping method."""
    if grouping_method == "nba_conference":
        if team_name in NBA_EASTERN_CONFERENCE:
            return "Eastern"
        if team_name in NBA_WESTERN_CONFERENCE:
            return "Western"
    elif grouping_method == "nfl_conference":
        if team_name in NFL_AFC:
            return "AFC"
        if team_name in NFL_NFC:
            return "NFC"
    elif grouping_method == "mlb_league":
        if team_name in MLB_AMERICAN_LEAGUE:
            return "AL"
        if team_name in MLB_NATIONAL_LEAGUE:
            return "NL"
    elif grouping_method == "nhl_conference":
        if team_name in NHL_EASTERN_CONFERENCE:
            return "Eastern"
        if team_name in NHL_WESTERN_CONFERENCE:
            return "Western"
    elif grouping_method == "league_wide":
        return None
    return None
