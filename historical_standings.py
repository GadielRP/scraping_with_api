"""
Historical Standings Module

Provides functionality to compute historical standings from collected season data.
For seasons we've fully collected (NBA, La Liga, Premier League, NFL), this module
enables querying team standings at any point in time during the season.

This avoids API calls by using locally stored event/result data.
"""

import logging
from typing import Dict, List, Optional, Tuple, Set
from functools import lru_cache
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
    {"season_name": "MLB 2025", "season_id": 84695, "additional_season_id": None},
    {"season_name": "MLB 2024", "season_id": 68611, "additional_season_id": None},
    # NHL (Ice Hockey) seasons
    {"season_name": "NHL 2025", "season_id": 78476, "additional_season_id": None},
    # Serie A seasons
    {"season_name": "Serie A 2025", "season_id": 76457, "additional_season_id": None},
    # Bundesliga seasons
    {"season_name": "Bundesliga 2025", "season_id": 77333, "additional_season_id": None},
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


# Sport detection based on season ID - derived from COLLECTED_SEASON_IDS
# These sets are used for determining the points system and conference handling
# They should include ALL seasons we have collected for that sport

# NBA seasons (regular season + NBA Cup/In-Season Tournament)
# NBA uses wins-only standings with conference splits (Eastern/Western)
NBA_SEASON_IDS = {
    80229, 84238,  # NBA 25/26 + NBA Cup 2025
    65360, 69143,  # NBA 24/25 + NBA Cup 2024
    54105, 56094,  # NBA 23/24 + NBA Cup 2023
    45096,         # NBA 22/23
    38191,         # NBA 21/22
    34951,         # NBA 20/21
}

# NFL seasons
# NFL uses wins-only standings with conference splits (AFC/NFC)
NFL_SEASON_IDS = {
    75522,  # NFL 2025
    60592,  # NFL 2024
    51361,  # NFL 2023
    46786,  # NFL 2022
    36422,  # NFL 2021
    27719,  # NFL 2020
}

# Football (Soccer) seasons - La Liga, Premier League, Serie A, Bundesliga
# Football uses 3pts-win/1pt-draw system with league-wide standings
FOOTBALL_SEASON_IDS = {
    # La Liga
    77559, 61643, 52376, 42409, 37223, 32501,
    # Premier League
    76986, 61627, 52186, 41886, 37036, 29415,
    # Serie A
    76457,
    # Bundesliga
    77333,
}

# MLB (Baseball) seasons
# Baseball uses wins-only standings (no divisions/conferences for simplicity)
BASEBALL_MLB_SEASON_IDS = {
    84695,  # MLB 2025
    68611,  # MLB 2024
}

# NHL (Ice Hockey) seasons
# Hockey uses wins-only standings (points could vary but wins is the primary metric)
HOCKEY_NHL_SEASON_IDS = {
    78476,  # NHL 2025
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
    'Montreal Canadiens', 'Ottawa Senators', 'Tampa Bay Lightning', 'Toronto Maple Leafs',
    # Metropolitan Division
    'Carolina Hurricanes', 'Columbus Blue Jackets', 'New Jersey Devils', 'New York Islanders',
    'New York Rangers', 'Philadelphia Flyers', 'Pittsburgh Penguins', 'Washington Capitals'
}

NHL_WESTERN_CONFERENCE = {
    # Central Division
    'Arizona Coyotes', 'Chicago Blackhawks', 'Colorado Avalanche', 'Dallas Stars',
    'Minnesota Wild', 'Nashville Predators', 'St. Louis Blues', 'Winnipeg Jets',
    # Pacific Division
    'Anaheim Ducks', 'Calgary Flames', 'Edmonton Oilers', 'Los Angeles Kings',
    'San Jose Sharks', 'Seattle Kraken', 'Vancouver Canucks', 'Vegas Golden Knights'
}


def get_team_conference(team_name: str, sport_system: str) -> Optional[str]:
    """
    Get the conference/division for a team.
    
    Args:
        team_name: Name of the team
        sport_system: 'nba', 'nfl', 'mlb', 'nhl', or 'football'
        
    Returns:
        Conference/League name or None
    """
    if sport_system == 'nba':
        if team_name in NBA_EASTERN_CONFERENCE:
            return 'Eastern'
        elif team_name in NBA_WESTERN_CONFERENCE:
            return 'Western'
    elif sport_system == 'nfl':
        if team_name in NFL_AFC:
            return 'AFC'
        elif team_name in NFL_NFC:
            return 'NFC'
    elif sport_system == 'mlb':
        if team_name in MLB_AMERICAN_LEAGUE:
            return 'AL'
        elif team_name in MLB_NATIONAL_LEAGUE:
            return 'NL'
    elif sport_system == 'nhl':
        if team_name in NHL_EASTERN_CONFERENCE:
            return 'Eastern'
        elif team_name in NHL_WESTERN_CONFERENCE:
            return 'Western'
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


def get_sport_points_system(season_id: int, sport: str = None) -> str:
    """
    Determine the points system for a season.
    
    Args:
        season_id: The season ID
        sport: Optional sport name for fallback
        
    Returns:
        'wins_only' for NBA/NFL/MLB/NHL, 'football' for La Liga/Premier League/Serie A/Bundesliga
    """
    sid = int(season_id) if season_id else 0
    
    # Check season ID sets in order
    if sid in NBA_SEASON_IDS or sid in NFL_SEASON_IDS:
        return 'wins_only'
    elif sid in BASEBALL_MLB_SEASON_IDS or sid in HOCKEY_NHL_SEASON_IDS:
        return 'wins_only'
    elif sid in FOOTBALL_SEASON_IDS:
        return 'football'
    
    # Fallback to sport name
    if sport:
        sport_lower = sport.lower()
        if any(kw in sport_lower for kw in ['basketball', 'american football', 'baseball', 'hockey', 'ice hockey']):
            return 'wins_only'
        elif any(kw in sport_lower for kw in ['football', 'soccer']):
            return 'football'
    
    return 'wins_only'  # Default


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
        sport: str = None
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
        cache_key = (season_id, cutoff_timestamp)
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            standings = self._compute_standings_internal(season_id, cutoff_timestamp, sport)
            self._cache[cache_key] = standings
            return standings
        except Exception as e:
            logger.error(f"Error computing standings for season {season_id}: {e}")
            return {}
    
    def _compute_standings_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str
    ) -> Dict[str, Dict]:
        """Internal method to compute standings from database."""
        
        # Convert timestamp to datetime for SQL query
        cutoff_dt = datetime.fromtimestamp(cutoff_timestamp)
        points_system = get_sport_points_system(season_id, sport)
        
        # Determine if this is a conference-based sport
        is_nba = int(season_id) in NBA_SEASON_IDS
        is_nfl = int(season_id) in NFL_SEASON_IDS
        is_mlb = int(season_id) in BASEBALL_MLB_SEASON_IDS
        is_nhl = int(season_id) in HOCKEY_NHL_SEASON_IDS
        
        # NBA, NFL, MLB, and NHL all use conference/league splits
        use_conferences = is_nba or is_nfl or is_mlb or is_nhl
        
        # Determine sport system for conference lookup
        if is_nba:
            sport_system = 'nba'
        elif is_nfl:
            sport_system = 'nfl'
        elif is_mlb:
            sport_system = 'mlb'
        elif is_nhl:
            sport_system = 'nhl'
        else:
            sport_system = 'football'
        
        # Query all events in the season before the cutoff
        query = text("""
            SELECT 
                home_team, 
                away_team, 
                home_score, 
                away_score, 
                winner
            FROM season_events_with_results
            WHERE season_id = :season_id
              AND start_time_utc < :cutoff_dt
            ORDER BY start_time_utc
        """)
        
        team_stats: Dict[str, Dict] = {}
        
        with db_manager.get_session() as session:
            result = session.execute(query, {
                'season_id': season_id,
                'cutoff_dt': cutoff_dt
            })
            
            for row in result:
                home_team = row.home_team
                away_team = row.away_team
                home_score = row.home_score
                away_score = row.away_score
                winner = row.winner
                
                # Initialize team stats if not exists
                for team in [home_team, away_team]:
                    if team not in team_stats:
                        team_stats[team] = {
                            'wins': 0,
                            'losses': 0,
                            'draws': 0,
                            'goals_for': 0,
                            'goals_against': 0,
                            'games_played': 0,
                            'conference': get_team_conference(team, sport_system) if use_conferences else None
                        }
                
                # Update stats based on result
                team_stats[home_team]['goals_for'] += home_score
                team_stats[home_team]['goals_against'] += away_score
                team_stats[home_team]['games_played'] += 1
                
                team_stats[away_team]['goals_for'] += away_score
                team_stats[away_team]['goals_against'] += home_score
                team_stats[away_team]['games_played'] += 1
                
                if winner == '1':  # Home team won
                    team_stats[home_team]['wins'] += 1
                    team_stats[away_team]['losses'] += 1
                elif winner == '2':  # Away team won
                    team_stats[away_team]['wins'] += 1
                    team_stats[home_team]['losses'] += 1
                elif winner == 'X':  # Draw
                    team_stats[home_team]['draws'] += 1
                    team_stats[away_team]['draws'] += 1
        
        # Calculate points and goal difference
        for team, stats in team_stats.items():
            stats['goal_diff'] = stats['goals_for'] - stats['goals_against']
            
            if points_system == 'football':
                # Football: 3 points for win, 1 for draw, 0 for loss
                stats['points'] = (stats['wins'] * 3) + stats['draws']
            else:
                # NBA/NFL: Points = wins only
                stats['points'] = stats['wins']
        
        # If using conferences, rank within each conference separately
        if use_conferences:
            standings = {}
            
            # Group teams by conference
            conferences = {}
            for team_name, stats in team_stats.items():
                conf = stats.get('conference')
                if conf:
                    if conf not in conferences:
                        conferences[conf] = []
                    conferences[conf].append((team_name, stats))
            
            # Rank within each conference
            for conf, teams in conferences.items():
                sorted_teams = sorted(
                    teams,
                    key=lambda x: (x[1]['points'], x[1]['goal_diff'], x[1]['wins']),
                    reverse=True
                )
                
                for position, (team_name, stats) in enumerate(sorted_teams, start=1):
                    standings[team_name] = {
                        'position': position,
                        'points': stats['points'],
                        'wins': stats['wins'],
                        'losses': stats['losses'],
                        'draws': stats['draws'],
                        'goal_diff': stats['goal_diff'],
                        'games_played': stats['games_played'],
                        'conference': conf
                    }
        else:
            # League-wide ranking (football)
            sorted_teams = sorted(
                team_stats.items(),
                key=lambda x: (x[1]['points'], x[1]['goal_diff'], x[1]['wins']),
                reverse=True
            )
            
            standings = {}
            for position, (team_name, stats) in enumerate(sorted_teams, start=1):
                standings[team_name] = {
                    'position': position,
                    'points': stats['points'],
                    'wins': stats['wins'],
                    'losses': stats['losses'],
                    'draws': stats['draws'],
                    'goal_diff': stats['goal_diff'],
                    'games_played': stats['games_played']
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
    
    def get_team_form_from_db(
        self,
        team_name: str,
        season_id: int,
        sport: str,
        exclude_event_id: int = None,
        current_event_timestamp: float = None
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
                        season_id, game_timestamp, sport
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
            
            return results, win_streak
            
        except Exception as e:
            logger.error(f"Error getting team form from DB for {team_name}: {e}")
            return [], 0


# Global instances for easy access
standings_simulator = StandingsSimulator()
historical_form_processor = HistoricalFormProcessor()
