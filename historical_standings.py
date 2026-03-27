"""
Historical Standings Module

Provides functionality to compute historical standings from collected season data.
For seasons we've fully collected (NBA, La Liga, Premier League, NFL), this module
enables querying team standings at any point in time during the season.

This avoids API calls by using locally stored event/result data.
"""

import logging
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime

from database import db_manager
from sqlalchemy import text

logger = logging.getLogger(__name__)

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


# Standings computation families (points / percentage methods)
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
    'Houston Astros', 'Los Angeles Angels', 'Oakland Athletics', 'Seattle Mariners', 'Texas Rangers'
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


def normalize_result_subtype(raw_subtype: Optional[str], winner: Optional[str]) -> str:
    """Normalize raw result subtype values to REG/OT/SO/DRAW."""
    if winner == "X":
        return "DRAW"
    if raw_subtype is None:
        return "REG"

    value = str(raw_subtype).strip().upper()
    if not value:
        return "REG"
    if value in {"DRAW", "X", "TIE"}:
        return "DRAW"
    if "SO" in value or "SHOOT" in value:
        return "SO"
    if value in {"OT", "OVERTIME"} or "OT" in value or "EXTRA" in value:
        return "OT"
    if value in {"REG", "REGULATION"} or "REG" in value:
        return "REG"
    return "REG"


def _normalize_team_name_for_sort(team_name: str) -> str:
    """Normalize team name for deterministic sorting."""
    return team_name.lower().strip()


def build_primary_rank_key(stats: Dict, standings_method: str) -> Tuple:
    """Build primary metric key for determining official standings position including ties."""
    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (stats.get("pct", 0.0) or 0.0,)
    
    # Fallback to existing sort key behavior for other methods to remain compatible
    return build_sort_key(stats, standings_method)


def build_display_sort_key(team_name: str, stats: Dict, standings_method: str) -> Tuple:
    """Build deterministic sort key for stable ordering inside a tie cluster."""
    inverted_name = tuple(-ord(c) for c in _normalize_team_name_for_sort(team_name))
    
    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (
            stats.get("pct", 0.0) or 0.0,
            inverted_name
        )
    
    # Existing behavior + name fallback
    base_key = build_sort_key(stats, standings_method)
    return base_key + (inverted_name,)


def assign_positions_with_ties(sorted_items: List[Tuple[str, Dict]], standings_method: str) -> Dict[str, Dict]:
    """
    Given a list of (team_name, stats) ALREADY sorted by display_sort_key,
    assigns shared positions using competition ranking based on primary_rank_key.
    Returns mapping of team_name -> metadata dict with position.
    """
    result = {}
    current_pos = 1
    tied_count = 0
    prev_rank_key = None
    
    for i, (team_name, stats) in enumerate(sorted_items):
        rank_key = build_primary_rank_key(stats, standings_method)
        
        if i == 0:
            result[team_name] = {'position': 1, 'is_primary_tie': False, 'primary_rank_key': rank_key}
            prev_rank_key = rank_key
            continue
            
        if rank_key == prev_rank_key:
            tied_count += 1
            # Backtrack and mark previous team in this tie cluster as well if it's the first tie match
            if tied_count == 1:
                prev_team_name = sorted_items[i-1][0]
                result[prev_team_name]['is_primary_tie'] = True
                
            result[team_name] = {'position': current_pos, 'is_primary_tie': True, 'primary_rank_key': rank_key}
        else:
            current_pos += 1 + tied_count
            tied_count = 0
            result[team_name] = {'position': current_pos, 'is_primary_tie': False, 'primary_rank_key': rank_key}
            prev_rank_key = rank_key
            
    return result


def build_sort_key(stats: Dict, standings_method: str) -> Tuple:
    """Build method-specific sort key for deterministic simplified ranking."""
    if standings_method == "football_3_1_0":
        return (
            stats.get("points", 0),
            stats.get("goal_diff", 0),
            stats.get("wins", 0),
        )

    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (
            stats.get("pct", 0.0) or 0.0,
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    if standings_method == "nhl_2_1_0_otl":
        if stats.get("regulation_wins") is not None:
            return (
                stats.get("points", 0),
                stats.get("pct", 0.0) or 0.0,
                stats.get("regulation_wins", 0),
                stats.get("wins", 0),
                stats.get("goal_diff", 0),
            )
        return (
            stats.get("points", 0),
            stats.get("pct", 0.0) or 0.0,
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    if standings_method == "hockey_3_2_1_0":
        return (
            stats.get("points", 0),
            stats.get("regulation_wins", 0),
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    return (
        stats.get("points", 0),
        stats.get("goal_diff", 0),
        stats.get("wins", 0),
    )


class StandingsSimulator:
    """
    Computes standings at any point in time for a collected season.
    
    Uses the season_events_with_results view to query all finished events
    before a cutoff timestamp, then aggregates W/L/D per team.
    """
    
    def __init__(self):
        # Simple cache for repeated queries
        self._cache: Dict[Tuple[int, float], Dict[str, Dict]] = {}
    
    def compute_standings(
        self, 
        season_id: int, 
        cutoff_timestamp: float,
        sport: str = None,
        send_debug_standings: bool = False
    ) -> Dict[str, Dict]:
        """
        Compute standings as they were at a specific point in time.
        
        Args:
            season_id: The season ID to compute standings for
            cutoff_timestamp: Unix timestamp - only events before this are counted
            sport: Sport name for points system detection
            
        Returns:
            Dict keyed by team_name with values:
            {
                'position': int,
                'points': int,
                'wins': int,
                'losses': int,
                'draws': int,
                'goal_diff': int,
                'games_played': int
            }
        """
        canonical_season_id = get_canonical_season_id(season_id)
        cache_key = (canonical_season_id, cutoff_timestamp, sport or "")
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            standings = self._compute_standings_internal(season_id, cutoff_timestamp, sport, send_debug_standings)
            self._cache[cache_key] = standings
            return standings
        except Exception as e:
            logger.error(f"Error computing standings for season {season_id}: {e}")
            return {}
    
    def _compute_standings_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        send_debug_standings: bool = False
    ) -> Dict[str, Dict]:
        """Internal method to compute standings from database."""
        cutoff_dt = datetime.fromtimestamp(cutoff_timestamp)
        standings_method = get_standings_method(season_id, sport)
        grouping_method = get_grouping_method(season_id, sport)

        all_season_ids = get_all_season_ids(season_id)
        canonical_season_id = get_canonical_season_id(season_id)

        query = text("""
            SELECT
                home_team,
                away_team,
                home_score,
                away_score,
                winner,
                result_subtype
            FROM season_events_with_results
            WHERE season_id = ANY(:season_ids)
              AND round = 'regular_season'
              AND start_time_utc < :cutoff_dt
            ORDER BY start_time_utc
        """)
        
        team_stats: Dict[str, Dict] = {}
        warned_unknown_group_teams = set()

        with db_manager.get_session() as session:
            result = session.execute(query, {
                'season_ids': all_season_ids,
                'cutoff_dt': cutoff_dt
            })

            all_rows = result.fetchall()
            if send_debug_standings:
                logger.info(
                    "🔍 STANDINGS DEBUG: Found %s events. Input season: %s, Canonical: %s, Bundle: %s before %s",
                    len(all_rows),
                    season_id,
                    canonical_season_id,
                    all_season_ids,
                    cutoff_dt.strftime('%Y-%m-%d %H:%M:%S')
                )

            for row in all_rows:
                home_team = row.home_team
                away_team = row.away_team
                winner = row.winner
                result_subtype = normalize_result_subtype(
                    getattr(row, "result_subtype", None),
                    winner
                )

                for team in [home_team, away_team]:
                    if team not in team_stats:
                        group = None
                        if grouping_method != "league_wide":
                            group = get_team_group(team, grouping_method)
                            if not group:
                                if team not in warned_unknown_group_teams:
                                    logger.warning(
                                        "Standings group mapping missing for season %s team '%s'; keeping in UNKNOWN group",
                                        season_id,
                                        team
                                    )
                                    warned_unknown_group_teams.add(team)
                                group = "UNKNOWN"

                        team_stats[team] = {
                            'wins': 0,
                            'losses': 0,
                            'draws': 0,
                            'ties': 0,
                            'ot_losses': 0,
                            'regulation_wins': 0,
                            'ot_so_wins': 0,
                            'goals_for': 0,
                            'goals_against': 0,
                            'games_played': 0,
                            'group': group,
                            'points': 0,
                            'pct': None,
                        }

                team_stats[home_team]['goals_for'] += row.home_score
                team_stats[home_team]['goals_against'] += row.away_score
                team_stats[home_team]['games_played'] += 1

                team_stats[away_team]['goals_for'] += row.away_score
                team_stats[away_team]['goals_against'] += row.home_score
                team_stats[away_team]['games_played'] += 1

                if standings_method == "football_3_1_0":
                    if winner == '1':
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                    elif winner == '2':
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                    elif winner == 'X':
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

                elif standings_method == "win_pct":
                    if winner == '1':
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                    elif winner == '2':
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                    elif winner == 'X':
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

                elif standings_method == "win_pct_half_tie":
                    if winner == '1':
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                    elif winner == '2':
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                    elif winner == 'X':
                        team_stats[home_team]['ties'] += 1
                        team_stats[away_team]['ties'] += 1
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

                elif standings_method == "nhl_2_1_0_otl":
                    if winner == '1':
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                        if result_subtype in {"OT", "SO"}:
                            team_stats[home_team]['ot_so_wins'] += 1
                            team_stats[away_team]['ot_losses'] += 1
                        else:
                            team_stats[home_team]['regulation_wins'] += 1
                    elif winner == '2':
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                        if result_subtype in {"OT", "SO"}:
                            team_stats[away_team]['ot_so_wins'] += 1
                            team_stats[home_team]['ot_losses'] += 1
                        else:
                            team_stats[away_team]['regulation_wins'] += 1
                    elif winner == 'X':
                        team_stats[home_team]['ties'] += 1
                        team_stats[away_team]['ties'] += 1
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

                elif standings_method == "hockey_3_2_1_0":
                    if winner == '1':
                        team_stats[away_team]['losses'] += 1
                        if result_subtype in {"OT", "SO"}:
                            team_stats[home_team]['ot_so_wins'] += 1
                            team_stats[away_team]['ot_losses'] += 1
                        else:
                            team_stats[home_team]['regulation_wins'] += 1
                    elif winner == '2':
                        team_stats[home_team]['losses'] += 1
                        if result_subtype in {"OT", "SO"}:
                            team_stats[away_team]['ot_so_wins'] += 1
                            team_stats[home_team]['ot_losses'] += 1
                        else:
                            team_stats[away_team]['regulation_wins'] += 1
                    elif winner == 'X':
                        team_stats[home_team]['ties'] += 1
                        team_stats[away_team]['ties'] += 1
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

                else:
                    if winner == '1':
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                    elif winner == '2':
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                    elif winner == 'X':
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

        for stats in team_stats.values():
            stats['goal_diff'] = stats['goals_for'] - stats['goals_against']

            if standings_method == "football_3_1_0":
                stats['points'] = (stats['wins'] * 3) + stats['draws']
                stats['pct'] = None
            elif standings_method == "win_pct":
                stats['points'] = stats['wins']
                stats['pct'] = (
                    stats['wins'] / stats['games_played']
                    if stats['games_played'] > 0 else 0.0
                )
            elif standings_method == "win_pct_half_tie":
                standing_points = stats['wins'] + (0.5 * stats['ties'])
                stats['points'] = standing_points
                stats['pct'] = (
                    standing_points / stats['games_played']
                    if stats['games_played'] > 0 else 0.0
                )
            elif standings_method == "nhl_2_1_0_otl":
                stats['points'] = (stats['wins'] * 2) + stats['ot_losses']
                stats['pct'] = (
                    stats['points'] / (stats['games_played'] * 2)
                    if stats['games_played'] > 0 else 0.0
                )
            elif standings_method == "hockey_3_2_1_0":
                stats['wins'] = stats['regulation_wins'] + stats['ot_so_wins']
                stats['points'] = (
                    (stats['regulation_wins'] * 3) +
                    (stats['ot_so_wins'] * 2) +
                    stats['ot_losses']
                )
                stats['pct'] = (
                    stats['points'] / (stats['games_played'] * 3)
                    if stats['games_played'] > 0 else 0.0
                )
            else:
                stats['points'] = stats['wins']
                stats['pct'] = (
                    stats['wins'] / stats['games_played']
                    if stats['games_played'] > 0 else 0.0
                )

        standings: Dict[str, Dict] = {}
        if grouping_method == "league_wide":
            sorted_teams = sorted(
                team_stats.items(),
                key=lambda item: build_display_sort_key(item[0], item[1], standings_method),
                reverse=True
            )
            positions = assign_positions_with_ties(sorted_teams, standings_method)
            for team_name, stats in sorted_teams:
                pos_meta = positions[team_name]
                standings[team_name] = {
                    'position': pos_meta['position'],
                    'is_primary_tie': pos_meta['is_primary_tie'],
                    'primary_rank_key': pos_meta['primary_rank_key'],
                    'points': stats['points'],
                    'wins': stats['wins'],
                    'losses': stats['losses'],
                    'draws': stats['draws'],
                    'goal_diff': stats['goal_diff'],
                    'games_played': stats['games_played'],
                    'pct': stats['pct'],
                    'ties': stats['ties'],
                    'ot_losses': stats['ot_losses'],
                    'group': None,
                    'conference': None,
                }
        else:
            grouped_teams: Dict[str, List[Tuple[str, Dict]]] = {}
            for team_name, stats in team_stats.items():
                group_name = stats.get('group') or "UNKNOWN"
                grouped_teams.setdefault(group_name, []).append((team_name, stats))

            for group_name, teams in grouped_teams.items():
                sorted_teams = sorted(
                    teams,
                    key=lambda item: build_display_sort_key(item[0], item[1], standings_method),
                    reverse=True
                )
                positions = assign_positions_with_ties(sorted_teams, standings_method)
                for team_name, stats in sorted_teams:
                    pos_meta = positions[team_name]
                    standings[team_name] = {
                        'position': pos_meta['position'],
                        'is_primary_tie': pos_meta['is_primary_tie'],
                        'primary_rank_key': pos_meta['primary_rank_key'],
                        'points': stats['points'],
                        'wins': stats['wins'],
                        'losses': stats['losses'],
                        'draws': stats['draws'],
                        'goal_diff': stats['goal_diff'],
                        'games_played': stats['games_played'],
                        'pct': stats['pct'],
                        'ties': stats['ties'],
                        'ot_losses': stats['ot_losses'],
                        'group': group_name,
                        'conference': group_name,
                    }

        return standings
    
    def clear_cache(self):
        """Clear the internal cache."""
        self._cache.clear()


class HistoricalFormProcessor:
    """
    Processes team form data from the database for collected seasons.
    
    Instead of fetching from API, queries the local database and attaches
    standings data to each historical game.
    """
    
    def __init__(self):
        self.standings_simulator = StandingsSimulator()
    
    def _format_standings_table_for_telegram(
        self,
        standings: Dict[str, Dict],
        title: str,
        standings_method: Optional[str] = None
    ) -> str:
        """Format standings table for Telegram message."""
        message = f"📊 <b>{title}</b>\n\n"
        
        # Sort by position
        sorted_standings = sorted(
            standings.items(),
            key=lambda x: x[1].get('position', 999)
        )
        
        for team_name, stats in sorted_standings:
            pos = stats.get('position', '?')
            pts = stats.get('points', 0)
            wins = stats.get('wins', 0)
            draws = stats.get('draws', 0)
            losses = stats.get('losses', 0)
            gd = stats.get('goal_diff', 0)
            gd_str = f"+{gd}" if gd > 0 else str(gd)
            games = stats.get('games_played', 0)
            pct = stats.get('pct')

            method = standings_method or ""
            ot_losses = stats.get('ot_losses', 0)
            ties = stats.get('ties', 0)

            if pct is not None and method == "win_pct":
                message += f"#{pos} {team_name}: .{int(pct*1000):03d} ({wins}W-{losses}L) PD:{gd_str}\n"
            elif pct is not None and method == "win_pct_half_tie":
                message += f"#{pos} {team_name}: .{int(pct*1000):03d} ({wins}W-{losses}L-{ties}T) PD:{gd_str}\n"
            elif method in {"nhl_2_1_0_otl", "hockey_3_2_1_0"} and ot_losses > 0:
                message += f"#{pos} {team_name}: {pts}pts ({wins}W-{losses}L-{ot_losses}OTL) GD:{gd_str}\n"
            else:
                message += f"#{pos} {team_name}: {pts}pts ({wins}W-{draws}D-{losses}L, {games} played) GD:{gd_str}\n"
        
        return message
    
    def _send_debug_telegram(self, message: str, chat_id: str):
        """Send debug message to personal Telegram chat."""
        import requests
        from config import Config
        
        if not Config.TELEGRAM_BOT_TOKEN or not chat_id:
            logger.debug("Telegram not configured for debug messages")
            return
        
        try:
            url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Debug standings sent to personal chat")
            else:
                logger.warning(f"Failed to send debug standings: {response.status_code}")
        except Exception as e:
            logger.error(f"Error sending debug Telegram: {e}")
    
    def _get_round_cutoff_timestamps(self, season_id: int, rounds: List[int]) -> Dict[int, float]:
        """
        Find the timestamp when ALL teams in the season have completed exactly N games.
        
        Scans events chronologically, tracks per-team game counts, and records 
        the timestamp of the event that completes a round (i.e., when the last 
        team reaches N games played).
        
        Args:
            season_id: Season ID to query
            rounds: List of round numbers to find (e.g., [5, 10, 15, 20, 25])
            
        Returns:
            Dict mapping round number -> cutoff timestamp (right after the last game of that round)
        """
        all_season_ids = get_all_season_ids(season_id)

        query = text("""
            SELECT 
                home_team, 
                away_team, 
                start_time_utc
            FROM season_events_with_results
            WHERE season_id = ANY(:season_ids)
              AND round = 'regular_season'
            ORDER BY start_time_utc ASC
        """)
        
        team_game_counts: Dict[str, int] = {}
        round_cutoffs: Dict[int, float] = {}
        max_round = max(rounds)
        
        with db_manager.get_session() as session:
            result = session.execute(query, {'season_ids': all_season_ids})
            all_rows = result.fetchall()
            
            logger.info(f"🔍 ROUND CUTOFF: Scanning {len(all_rows)} events in season {season_id} for rounds {rounds}")
            
            for row in all_rows:
                home_team = row.home_team
                away_team = row.away_team
                event_ts = row.start_time_utc.timestamp()
                
                # Increment game counts
                team_game_counts[home_team] = team_game_counts.get(home_team, 0) + 1
                team_game_counts[away_team] = team_game_counts.get(away_team, 0) + 1
                
                # Check if all teams have reached exactly N games for any pending round
                if team_game_counts:
                    min_games = min(team_game_counts.values())
                    
                    for round_num in rounds:
                        if round_num not in round_cutoffs and min_games >= round_num:
                            # Use timestamp right after this event (+1 second) so
                            # compute_standings includes this game
                            round_cutoffs[round_num] = event_ts + 1
                            logger.info(f"🔍 ROUND CUTOFF: Round {round_num} completed at {row.start_time_utc} ({len(team_game_counts)} teams)")
                
                # Stop if we've found all rounds
                if len(round_cutoffs) == len(rounds):
                    break
        
        return round_cutoffs
    
    def get_team_form_from_db(
        self,
        team_name: str,
        season_id: int,
        sport: str,
        exclude_event_id: int = None,
        current_event_timestamp: float = None,
        # set to true for debug standings logs and message
        send_debug_standings: bool = True
    ) -> Tuple[List[Dict], int]:
        """
        Get team's past results from the database with standings at each game.
        
        Args:
            team_name: Name of the team
            season_id: Season ID to query
            sport: Sport name for points system
            exclude_event_id: Event ID to exclude (current/upcoming event)
            current_event_timestamp: Timestamp of current event (for filtering)
            
        Returns:
            Tuple of (results_list, overall_win_streak)
            Each result dict includes standings_position and standings_points
        """
        try:
            # Query events where this team participated
            query = text("""
                SELECT 
                    event_id,
                    home_team,
                    away_team,
                    home_score,
                    away_score,
                    winner,
                    start_time_utc
                FROM season_events_with_results
                WHERE season_id = ANY(:season_ids)
                  AND round = 'regular_season'
                  AND (home_team = :team_name OR away_team = :team_name)
                ORDER BY start_time_utc DESC
            """)
            
            results = []
            
            # Get all related season IDs (e.g., NBA regular + NBA Cup)
            all_season_ids = get_all_season_ids(season_id)
            
            with db_manager.get_session() as session:
                db_result = session.execute(query, {
                    'season_ids': all_season_ids,  # Pass as list, not tuple
                    'team_name': team_name
                })
                
                all_rows = db_result.fetchall()
                logger.info(f"📊 DB query returned {len(all_rows)} events for {team_name} in seasons {all_season_ids}")
                
                for row in all_rows:
                    event_id = row.event_id
                    
                    # Skip excluded event
                    if exclude_event_id and event_id == exclude_event_id:
                        continue
                    
                    # Skip events after current event if timestamp provided
                    if current_event_timestamp:
                        event_ts = row.start_time_utc.timestamp()
                        if event_ts >= current_event_timestamp:
                            continue
                    
                    is_team_home = row.home_team == team_name
                    opponent_name = row.away_team if is_team_home else row.home_team
                    
                    # Determine result relative to team
                    if row.winner == '1':
                        team_result = '1' if is_team_home else '2'
                    elif row.winner == '2':
                        team_result = '1' if not is_team_home else '2'
                    else:
                        team_result = 'X'
                    
                    # Scores relative to team
                    if is_team_home:
                        team_score = row.home_score
                        opponent_score = row.away_score
                        team_role = 'home'
                    else:
                        team_score = row.away_score
                        opponent_score = row.home_score
                        team_role = 'away'
                    
                    # Get standings at the moment of this game
                    game_timestamp = row.start_time_utc.timestamp()
                    standings = self.standings_simulator.compute_standings(
                        season_id, game_timestamp, sport, send_debug_standings
                    )
                    
                    team_standing = standings.get(team_name, {})
                    opponent_standing = standings.get(opponent_name, {})
                    
                    result_dict = {
                        'event_id': event_id,
                        'winner': team_result,
                        'home_score': team_score,
                        'away_score': opponent_score,
                        'team_name': team_name,
                        'opponent_name': opponent_name,
                        'startTimestamp': int(game_timestamp),
                        'role': team_role,
                        'opponent_ranking': 0,  # Not available from DB
                        'own_ranking': 0,  # Not available from DB
                        # Standings at the moment of this game
                        'standings_position': team_standing.get('position'),
                        'standings_points': team_standing.get('points'),
                        'opponent_standings_position': opponent_standing.get('position'),
                        'opponent_standings_points': opponent_standing.get('points')
                    }
                    
                    results.append(result_dict)
            
            # Calculate overall win streak (from most recent games)
            win_streak = 0
            for result in results:
                if result['winner'] == '1':
                    win_streak += 1
                else:
                    break
            
            logger.info(f"📊 DB-based form: {team_name} - {len(results)} games from season {season_id}, win streak: {win_streak}")
            
            # Send debug standings at 5-game intervals (round-based) and current standings
            if send_debug_standings:
                import os
                personal_chat_id = os.getenv('PERSONAL_CHAT_ID', '')
                
                if personal_chat_id:
                    # Find round-based cutoffs: when ALL teams have played 5, 10, 15, 20, 25 games
                    rounds_to_check = [5, 10, 15, 20, 25]
                    round_cutoffs = self._get_round_cutoff_timestamps(season_id, rounds_to_check)
                    
                    for round_num in sorted(round_cutoffs.keys()):
                        cutoff_ts = round_cutoffs[round_num]
                        cutoff_date = datetime.fromtimestamp(cutoff_ts).strftime('%Y-%m-%d')
                        
                        # Compute standings at the round cutoff
                        standings = self.standings_simulator.compute_standings(
                            season_id, cutoff_ts, sport
                        )
                        
                        title = f"Round {round_num} Standings ({cutoff_date})"
                        message = self._format_standings_table_for_telegram(
                            standings,
                            title,
                            standings_method=get_standings_method(season_id, sport)
                        )
                        self._send_debug_telegram(message, personal_chat_id)
                    
                    # Send current/final standings (at the moment of the upcoming event)
                    if current_event_timestamp:
                        current_standings = self.standings_simulator.compute_standings(
                            season_id, current_event_timestamp, sport
                        )
                        current_date = datetime.fromtimestamp(current_event_timestamp).strftime('%Y-%m-%d %H:%M')
                        title = f"CURRENT Standings (at {current_date})"
                        message = self._format_standings_table_for_telegram(
                            current_standings,
                            title,
                            standings_method=get_standings_method(season_id, sport)
                        )
                        self._send_debug_telegram(message, personal_chat_id)
                else:
                    logger.warning("PERSONAL_CHAT_ID not configured - skipping debug standings")
            
            return results, win_streak
            
        except Exception as e:
            logger.error(f"Error getting team form from DB for {team_name}: {e}")
            return [], 0


# Global instances for easy access
standings_simulator = StandingsSimulator()
historical_form_processor = HistoricalFormProcessor()
