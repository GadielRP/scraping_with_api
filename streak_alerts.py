"""
Streak Alert System - H2H streak analysis for upcoming events

Imports proven result extraction logic from sofascore_api.py
to ensure consistency with existing result collection system.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from sofascore_api import api_client
import sofascore_api2
from odds_utils import fractional_to_decimal
from repository import SeasonRepository
logger = logging.getLogger(__name__)

@dataclass
class H2HStreak:
    """Represents H2H streak analysis between two teams (relative to upcoming event)"""
    event_id: int
    custom_id: str
    participants: str
    discovery_source: str
    competition_name: str
    competition_slug: str
    season_id: Optional[str]
    season_name: Optional[str]
    observations: Optional[List[Dict]]
    sport: str
    home_team_name: str  # Upcoming event home team
    away_team_name: str  # Upcoming event away team
    home_team_ranking: Optional[int]  # Ranking of upcoming event's home team
    away_team_ranking: Optional[int]  # Ranking of upcoming event's away team
    total_h2h_matches: int
    matches_analyzed: int  # Matches within 2-year window
    home_wins: int  # Wins for upcoming event's home team
    away_wins: int  # Wins for upcoming event's away team
    draws: int
    home_win_rate: float  # Percentage
    away_win_rate: float  # Percentage
    draw_rate: float  # Percentage
    all_matches: List[Dict]  # All H2H matches with detailed information (most recent first)
    h2h_home_net_points: int  # Net points when upcoming home team was home
    h2h_away_net_points: int  # Net points when upcoming home team was away
    current_streak: str  # e.g., "Home team won last 3 matches"
    current_streak_count: int
    avg_home_score: float  # Avg score for upcoming home team
    avg_away_score: float  # Avg score for upcoming away team
    minutes_until_start: int
    # NEW: Team results data
    home_team_results: List[Dict]  # Last 10 results for home team
    away_team_results: List[Dict]  # Last 10 results for away team
    home_team_wins: int  # Wins in last 10 games for home team
    away_team_wins: int  # Wins in last 10 games for away team
    home_team_losses: int  # Losses in last 10 games for home team
    away_team_losses: int  # Losses in last 10 games for away team
    home_team_draws: int  # Draws in last 10 games for home team
    away_team_draws: int  # Draws in last 10 games for away team
    # NEW: Batched team form data
    home_team_batches: List[Dict]  # Home team results in batches of 5
    away_team_batches: List[Dict]  # Away team results in batches of 5
    # NEW: Final real rankings (average of batch real rankings)
    home_team_final_real_ranking: float = 0  # Final real ranking for home team
    away_team_final_real_ranking: float = 0  # Final real ranking for away team
    # NEW: Standings snapshots
    home_team_standing: Optional[Dict] = None
    away_team_standing: Optional[Dict] = None
    # NEW: Winning odds data
    winning_odds_data: Optional[Dict] = None  # Winning odds response data
    # NEW: Current event odds (for display in H2H streak messages)
    one_open: Optional[float] = None
    x_open: Optional[float] = None
    two_open: Optional[float] = None
    one_final: Optional[float] = None
    x_final: Optional[float] = None
    two_final: Optional[float] = None
    # NEW: Overall win streaks (unfiltered)
    home_current_win_streak: int = 0
    away_current_win_streak: int = 0


class StreakAlertEngine:
    """Engine for analyzing H2H streaks and generating alerts"""
    
    # Default minimum number of results to fetch for team form analysis
    DEFAULT_MIN_RESULTS = 10
    
    # Enable fallback filtering by season year (e.g. for NBA Regular vs NBA Cup)
    ENABLE_SEASON_YEAR_FILTERING = True
    
    def __init__(self):
        self.two_year_window = timedelta(days=730)  # 2 years
    

    
    
    def process_team_results_into_batches(self, team_results: List[Dict], team_name: str) -> List[Dict]:
        """
        Process team results into batches of 5 matches with individual game results and batch totals.
        
        For tennis, uses period points instead of sets for net differential calculation.
        
        Args:
            team_results: List of team results from get_team_last_results_by_id
            team_name: Name of the team for context
            
        Returns:
            List of batch dictionaries, each containing:
            - batch_number: Sequential batch number (1, 2, 3, etc.)
            - games: List of individual game results in this batch
            - batch_wins: Number of wins in this batch
            - batch_losses: Number of losses in this batch  
            - batch_draws: Number of draws in this batch
            - batch_points_for: Total points scored by team in this batch (sets or total game points)
            - batch_points_against: Total points scored against team in this batch
            - batch_net_points: Net points (for - against) in this batch
        """
        if not team_results:
            return []
        
        batches = []
        batch_size = 5
        
        # Check if this is tennis data (has period points)
        is_tennis = any('team_period1' in game for game in team_results)
        
        # Process results in batches of 5 (most recent first)
        for i in range(0, len(team_results), batch_size):
            batch_games = team_results[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            
            # Calculate batch statistics
            batch_wins = sum(1 for game in batch_games if game['winner'] == '1')
            batch_losses = sum(1 for game in batch_games if game['winner'] == '2')
            batch_draws = sum(1 for game in batch_games if game['winner'] == 'X')
            
            # Calculate points totals
            if is_tennis:
                # For tennis, use total points from all periods instead of sets
                batch_points_for = 0
                batch_points_against = 0
                for game in batch_games:
                    team_total = self._calculate_tennis_total_points(game, is_team=True)
                    opponent_total = self._calculate_tennis_total_points(game, is_team=False)
                    batch_points_for += team_total
                    batch_points_against += opponent_total
                batch_net_points = batch_points_for - batch_points_against
            else:
                # For other sports, use regular scores
                batch_points_for = sum(game['home_score'] for game in batch_games)
                batch_points_against = sum(game['away_score'] for game in batch_games)
                batch_net_points = batch_points_for - batch_points_against
            
            # Calculate net points by role
            batch_home_net_points = 0
            batch_away_net_points = 0
            for game in batch_games:
                if is_tennis:
                    # For tennis, calculate net points from period points
                    team_total = self._calculate_tennis_total_points(game, is_team=True)
                    opponent_total = self._calculate_tennis_total_points(game, is_team=False)
                    net = team_total - opponent_total
                else:
                    # For other sports, use regular scores
                    net = game['home_score'] - game['away_score']
                
                if game.get('role') == 'home':
                    batch_home_net_points += net
                elif game.get('role') == 'away':
                    batch_away_net_points += net
            
            # Calculate real ranking (average of single rankings)
            single_rankings = []
            for game in batch_games:
                own_ranking = game.get('own_ranking', 0)
                opponent_ranking = game.get('opponent_ranking', 0)
                result = game.get('winner')
                single_ranking = self._calculate_single_ranking(result, own_ranking, opponent_ranking)
                if single_ranking > 0:
                    single_rankings.append(single_ranking)
            
            # Calculate average real ranking for this batch
            batch_real_ranking = round(int(sum(single_rankings)) / len(single_rankings)) if single_rankings else 0
            
            # Format individual games for display
            formatted_games = []
            for game in batch_games:
                # Convert winner to display format
                if game['winner'] == '1':
                    result_symbol = 'W'
                elif game['winner'] == '2':
                    result_symbol = 'L'
                else:  # 'X'
                    result_symbol = 'D'
                
                # Format opponent name (truncate if too long)
                opponent = game['opponent_name']
                if len(opponent) > 15:
                    opponent = opponent[:12] + "..."
                
                # Get role for this game
                team_role = game.get('role', 'home')
                
                # Calculate score_for and score_against based on sport
                if is_tennis:
                    # For tennis, use total points from all periods
                    score_for = self._calculate_tennis_total_points(game, is_team=True)
                    score_against = self._calculate_tennis_total_points(game, is_team=False)
                else:
                    # For other sports, use regular scores
                    score_for = game['home_score']
                    score_against = game['away_score']
                
                # Calculate single ranking for this game
                single_ranking = self._calculate_single_ranking(
                    game['winner'],
                    game.get('own_ranking', 0),
                    game.get('opponent_ranking', 0)
                )
                
                formatted_games.append({
                    'result': result_symbol,
                    'opponent': opponent,
                    'score_for': score_for,
                    'score_against': score_against,
                    'net_score': score_for - score_against,
                    'startTimestamp': game.get('startTimestamp', 0),  # Include timestamp for date display
                    'role': team_role,  # Track role for display
                    'opponent_ranking': game.get('opponent_ranking', 0),
                    'own_ranking': game.get('own_ranking', 0),
                    'home_score': game.get('home_score'),  # Original sets for tennis display
                    'away_score': game.get('away_score'),  # Original sets for tennis display
                    'single_ranking': single_ranking,  # Single ranking for this game
                    'standings_position': game.get('standings_position'),  # Position at time of game (from DB)
                    'standings_points': game.get('standings_points'),  # Points at time of game (from DB)
                    'opponent_standings_position': game.get('opponent_standings_position')  # Opponent position (from DB)
                })
            
            batches.append({
                'batch_number': batch_number,
                'games': formatted_games,
                'batch_wins': batch_wins,
                'batch_losses': batch_losses,
                'batch_draws': batch_draws,
                'batch_points_for': batch_points_for,
                'batch_points_against': batch_points_against,
                'batch_net_points': batch_net_points,
                'batch_home_net_points': batch_home_net_points,
                'batch_away_net_points': batch_away_net_points,
                'batch_real_ranking': batch_real_ranking
            })
        
        return batches
    
    def _calculate_tennis_total_points(self, game: Dict, is_team: bool) -> int:
        """
        Calculate total tennis points from all periods (sets) for a game.
        
        Args:
            game: Game dictionary with period point data
            is_team: True to calculate for team, False for opponent
            
        Returns:
            Total points across all periods
        """
        prefix = 'team' if is_team else 'opponent'
        
        period1 = game.get(f'{prefix}_period1', 0) or 0
        period2 = game.get(f'{prefix}_period2', 0) or 0
        period3 = game.get(f'{prefix}_period3', 0) or 0  # May be 0 for 2-set matches
        
        return period1 + period2 + period3
    
    def _calculate_single_ranking(self, result: str, own_ranking: int, opponent_ranking: int) -> int:
        """
        Calculate the single ranking for a game based on result and rankings.
        
        Logic:
        - Win + opponent lower ranked (higher number): use opponent's ranking
        - Win + opponent higher ranked (lower number): use player's ranking
        - Loss + opponent lower ranked (higher number): use player's ranking
        - Loss + opponent higher ranked (lower number): use opponent's ranking
        
        Simplified: Win = min(own, opp), Loss = max(own, opp)
        
        Args:
            result: '1' for win, '2' for loss, 'X' for draw
            own_ranking: Player's ranking
            opponent_ranking: Opponent's ranking
            
        Returns:
            Single ranking for this game (0 if rankings not available)
        """
        if own_ranking == 0 or opponent_ranking == 0:
            return 0
        
        if result == '1':  # Win
            return min(own_ranking, opponent_ranking)
        elif result == '2':  # Loss
            return max(own_ranking, opponent_ranking)
        else:  # Draw
            # For draws, use average of both rankings
            return (own_ranking + opponent_ranking) // 2
    
    def _calculate_final_real_ranking(self, batches: List[Dict]) -> float:
        """
        Calculate final real ranking as average of all batch real rankings.
        Excludes batches with 0 real ranking.
        
        Args:
            batches: List of batch dictionaries with batch_real_ranking
            
        Returns:
            Final real ranking (0 if no valid rankings)
        """
        valid_rankings = [b['batch_real_ranking'] for b in batches if b.get('batch_real_ranking', 0) > 0]
        return float(sum(valid_rankings) / len(valid_rankings)) if valid_rankings else 0

    def _calculate_current_win_streak(self, results: List[Dict]) -> int:
        """
        Calculate the current win streak (no filters) from a chronological list of results.
        
        Args:
            results: List of result dictionaries sorted by most recent first.
            
        Returns:
            Count of consecutive wins from the most recent result.
        """
        if not results:
            return 0
        
        streak_count = 0
        for result in results:
            winner = result.get('winner')
            if winner == '1':
                streak_count += 1
            else:
                break
        
        return streak_count


    def _get_filtering_criteria(self, sport: str, competition_slug: str, observations: Optional[List[Dict]] = None) -> Tuple[str, str]:
        """
        Determine the filtering criteria and value for logging purposes.
        
        Args:
            sport: Sport type
            competition_slug: Competition slug for non-tennis sports
            observations: Observations for sport-specific filtering
            
        Returns:
            Tuple of (filter_type, filter_value) for logging
        """
        if (sport == 'Tennis' or sport == 'Tennis Doubles') and observations:
            # For tennis, search for ground_type anywhere in observations (not just first item)
            ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None)
            if ground_type_obs:
                return ('ground_type', ground_type_obs.get('value', 'unknown'))
        
        # For other sports, use competition filtering
        return ('competition', competition_slug)
    
    def _extract_ranking_from_team(self, team_data: Dict) -> int:
        """
        Extract ranking from team data, handling both singles and doubles teams.
        
        Args:
            team_data: Team data dictionary from event
            
        Returns:
            Ranking as integer (0 if not found)
        """
        # First check if team has direct ranking (singles)
        ranking = team_data.get('ranking', None)
        if ranking is not None:
            return ranking
        
        # If no direct ranking, check subTeams for doubles teams
        sub_teams = team_data.get('subTeams', [])
        if sub_teams:
            # Get all rankings from subTeams and return the best (lowest) ranking
            rankings = [sub.get('ranking') for sub in sub_teams if sub.get('ranking') is not None]
            if rankings:
                return min(rankings)
        
        return 0

    def _process_events_into_results(
        self,
        events: List[Dict],
        team_id: int,
        team_name: str,
        competition_slug: str,
        sport: str,
        season_id: Optional[str] = None,
        season_year: Optional[int] = None,
        observations: Optional[List[Dict]] = None,
        exclude_event_id: Optional[int] = None,
        apply_filters: bool = True,
        overall_results: Optional[List[Dict]] = None,
        overall_seen_event_ids: Optional[Set] = None
    ) -> List[Dict]:
        """
        Process events into results list with filtering applied.
        Helper method to avoid code duplication between first and second fetch.
        
        Args:
            events: List of event dictionaries from API
            team_id: ID of the team
            team_name: Name of the team for context
            competition_slug: Slug of the competition
            sport: Sport type for filtering logic
            season_id: Season ID for filtering (non-tennis sports)
            season_year: Season Year for fallback filtering (e.g. 2023)
            observations: List of dicts containing filtering observations
            exclude_event_id: Optional event ID to exclude from results
            apply_filters: Whether to apply sport-specific filters
            overall_results: Optional list to collect unfiltered results
            overall_seen_event_ids: Optional set of seen event IDs for overall collection
            
        Returns:
            List of processed result dictionaries
        """
        results = []
        for event in events:
            try:
                # Exclude current event if exclude_event_id is provided
                event_id_from_api = event.get('id')
                if exclude_event_id and event_id_from_api == exclude_event_id:
                    logger.debug(f"Skipping event {event_id_from_api} for {team_name} - this is the current/upcoming event being analyzed")
                    continue
                
                # Extract result using proven logic
                # For tennis, also extract period points for points-based tracking
                extract_points = sport in ['Tennis', 'Tennis Doubles']
                result_data = api_client.extract_results_from_response({'event': event}, extract_tennis_points=extract_points, for_streaks=True)
                if not result_data:
                    continue
                # Skip canceled/postponed events
                if result_data.get('_canceled'):
                    continue
                
                # Get team names and data
                home_team_data = event.get('homeTeam', {})
                away_team_data = event.get('awayTeam', {})
                home_team_name = home_team_data.get('name', '')
                away_team_name = away_team_data.get('name', '')

                if home_team_name != team_name and away_team_name != team_name:
                    logger.debug(f"Skipping event {event_id_from_api} for {team_name} - team not found among participants")
                    continue

                is_team_home = home_team_name == team_name
                opponent_name = away_team_name if is_team_home else home_team_name
                opponent_ranking = self._extract_ranking_from_team(away_team_data if is_team_home else home_team_data)
                own_ranking = self._extract_ranking_from_team(home_team_data if is_team_home else away_team_data)

                winner_code = event.get('winnerCode')
                winner_position = result_data.get('winner')

                if winner_code == 1:
                    team_result = '1' if is_team_home else '2'
                elif winner_code == 2:
                    team_result = '1' if not is_team_home else '2'
                else:
                    if winner_position == '1':
                        team_result = '1' if is_team_home else '2'
                    elif winner_position == '2':
                        team_result = '1' if not is_team_home else '2'
                    else:
                        team_result = 'X'

                if is_team_home:
                    team_score = result_data['home_score']
                    opponent_score = result_data['away_score']
                    team_role = 'home'
                else:
                    team_score = result_data['away_score']
                    opponent_score = result_data['home_score']
                    team_role = 'away'

                passes_filters = True
                if apply_filters:
                    if sport in ['Tennis', 'Tennis Doubles']:
                        if observations:
                            ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None)
                            if ground_type_obs:
                                ground_type_value = ground_type_obs.get('value')
                                if ground_type_value != event.get('groundType'):
                                    logger.debug(f"Skipping event for {team_name} - different ground type: {ground_type_value} vs {event.get('groundType')}")
                                    passes_filters = False
                        if passes_filters:
                            home_team_type = home_team_data.get('type', 1)
                            away_team_type = away_team_data.get('type', 1)
                            event_is_doubles = home_team_type == 2 and away_team_type == 2
                            sport_is_doubles = sport == 'Tennis Doubles'
                            if event_is_doubles != sport_is_doubles:
                                logger.debug(f"Skipping event for {team_name} - different match type (event is {'doubles' if event_is_doubles else 'singles'}, sport is {'doubles' if sport_is_doubles else 'singles'})")
                                passes_filters = False
                    else:
                        # For non-tennis sports: filter by competition slug AND season_id
                        if competition_slug:
                            event_competition_slug = event.get('tournament', {}).get('uniqueTournament', {}).get('slug', '')
                            if event_competition_slug != competition_slug:
                                logger.debug(f"Skipping event for {team_name} - different competition: {event_competition_slug} vs {competition_slug}")
                                passes_filters = False
                        
                        # Also filter by season_id if provided
                        if passes_filters and season_id:
                            event_season_id = str(event.get('season', {}).get('id', ''))
                            
                            # ID Check (Primary)
                            id_match = event_season_id and event_season_id == season_id
                            
                            # Year Check (Fallback)
                            year_match = False
                            event_year = None
                            if not id_match and self.ENABLE_SEASON_YEAR_FILTERING and season_year:
                                raw_year = event.get('season', {}).get('year')
                                event_year = SeasonRepository._parse_year(raw_year)
                                if event_year and event_year == season_year:
                                    year_match = True
                                    logger.debug(f"✅ Year match for {team_name}: event_year={event_year} == target_season_year={season_year} (ID mismatch: {event_season_id} != {season_id})")
                            
                            # Apply filter if neither ID nor Year matches
                            if not (id_match or year_match):
                                if self.ENABLE_SEASON_YEAR_FILTERING and season_year:
                                    logger.debug(f"Skipping event for {team_name} - different season: ID {event_season_id} vs {season_id}, Year {event_year} vs {season_year}")
                                else:
                                    logger.debug(f"Skipping event for {team_name} - different season: ID {event_season_id} vs {season_id} (year filtering disabled or no year provided)")
                                passes_filters = False
                
                result_dict = {
                    'event_id': event_id_from_api,
                    'winner': team_result,
                    'home_score': team_score,
                    'away_score': opponent_score,
                    'team_name': team_name,
                    'opponent_name': opponent_name,
                    'opponent_ranking': opponent_ranking,
                    'own_ranking': own_ranking,
                    'startTimestamp': event.get('startTimestamp', 0),
                    'role': team_role,
                    'winner_code': winner_code
                }
                
                # Add tennis period points if available (for points-based tracking)
                if sport in ['Tennis', 'Tennis Doubles'] and 'home_period1' in result_data:
                    if is_team_home:
                        result_dict['team_period1'] = result_data.get('home_period1')
                        result_dict['team_period2'] = result_data.get('home_period2')
                        result_dict['team_period3'] = result_data.get('home_period3')
                        result_dict['opponent_period1'] = result_data.get('away_period1')
                        result_dict['opponent_period2'] = result_data.get('away_period2')
                        result_dict['opponent_period3'] = result_data.get('away_period3')
                    else:
                        result_dict['team_period1'] = result_data.get('away_period1')
                        result_dict['team_period2'] = result_data.get('away_period2')
                        result_dict['team_period3'] = result_data.get('away_period3')
                        result_dict['opponent_period1'] = result_data.get('home_period1')
                        result_dict['opponent_period2'] = result_data.get('home_period2')
                        result_dict['opponent_period3'] = result_data.get('home_period3')

                if overall_results is not None and overall_seen_event_ids is not None:
                    if event_id_from_api not in overall_seen_event_ids:
                        overall_results.append(dict(result_dict))
                        overall_seen_event_ids.add(event_id_from_api)

                if apply_filters and not passes_filters:
                    continue

                results.append(result_dict)
                
            except Exception as e:
                logger.debug(f"Error processing team result event: {e}")
                continue
        
        # Sort by startTimestamp to get most recent first (API returns inverted order)
        results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)
        
        return results

    def get_team_last_results_by_id(self, team_id: int, team_name: str, competition_slug: str, sport: str, season_id: Optional[str] = None, season_year: Optional[int] = None, observations: Optional[List[Dict]] = None, exclude_event_id: Optional[int] = None, min_results: int = None, event_start_timestamp: float = None) -> Tuple[List[Dict], int]:
        """
        Get team results using team_id with flexible filtering.
        
        Season-based filtering (non-tennis):
        - When season_id is provided, fetches ALL games from that season (no minimum)
        - Stops fetching when encountering events from a different season
        
        Legacy filtering (tennis or no season):
        - Uses min_results to fetch a minimum number of games
        - Applies ground_type filtering for tennis
        
        Args:
            team_id: ID of the team
            team_name: Name of the team for context
            competition_slug: Slug of the competition
            sport: Sport type for filtering logic
            season_id: Season ID for filtering (non-tennis sports only)
            season_year: Season Year for fallback filtering (e.g. 2023)
            observations: List of dicts containing filtering observations
            exclude_event_id: Optional event ID to exclude from results (current/upcoming event)
            min_results: Minimum number of results to fetch (ignored when season_id is provided)
            event_start_timestamp: Timestamp of the current event (for DB-based filtering)
        Returns:
            Tuple where:
                - List of dicts with keys: winner, home_score, away_score, team_name, opponent_name (filtered results)
                - Integer representing the overall (unfiltered) current win streak
        """
        # =====================================================================
        # ROUTE 1: DB-based form retrieval for collected seasons
        # Uses historical_standings module instead of API calls
        # =====================================================================
        from historical_standings import is_season_collected, historical_form_processor
        
        if season_id and is_season_collected(int(season_id)):
            logger.info(f"📊 Using DB-based form retrieval for {team_name} (season {season_id} is collected)")
            return historical_form_processor.get_team_form_from_db(
                team_name=team_name,
                season_id=int(season_id),
                sport=sport,
                exclude_event_id=exclude_event_id,
                current_event_timestamp=event_start_timestamp,
                send_debug_standings=False  # Toggle: set True to send debug standings to personal chat
            )
        
        # =====================================================================
        # ROUTE 2: API-based form retrieval (existing logic - UNCHANGED)
        # =====================================================================
        
        # Season-based filtering: fetch all games from current season (no minimum)
        use_season_filtering = season_id and sport not in ['Tennis', 'Tennis Doubles']
        
        # Use class constant if min_results not provided and not using season filtering
        if min_results is None and not use_season_filtering:
            min_results = self.DEFAULT_MIN_RESULTS
        elif use_season_filtering:
            # For season filtering, we don't have a minimum - we fetch all available
            min_results = 0  # Will be ignored in the loop logic
        
        try:
            # Log filtering criteria at the start
            filter_type, filter_value = self._get_filtering_criteria(sport, competition_slug, observations)
            match_type_info = ""
            if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                match_type_info = f" and match_type '{match_type}'"
            
            # Log detailed filtering information
            if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None) if observations else None
                if ground_type_obs:
                    logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: ground_type='{ground_type_obs.get('value')}'{match_type_info}")
                else:
                    logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: NO ground_type filter (ground_type not found in observations){match_type_info}")
            else:
                if use_season_filtering:
                    year_filter_info = ""
                    if self.ENABLE_SEASON_YEAR_FILTERING and season_year:
                        year_filter_info = f" OR season_year='{season_year}' (fallback)"
                    logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: competition='{competition_slug}' AND season_id='{season_id}'{year_filter_info} (fetching all season games)")
                else:
                    logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: competition='{competition_slug}'")
            
            # Determine tennis parameters
            is_tennis_singles = sport == 'Tennis'
            is_tennis_doubles = sport == 'Tennis Doubles'
            
            # First fetch
            logger.debug(f"TEST DEBUG: Sport-> {sport}")
            full_response = api_client.get_team_last_results_response(
                team_id, 
                is_tennis_singles=is_tennis_singles, 
                is_tennis_doubles=is_tennis_doubles,
                fetch_index=0
            )
            
            all_results = []
            seen_event_ids = set()
            overall_results: List[Dict] = []
            overall_seen_event_ids: Set[int] = set()
            
            if full_response and 'events' in full_response:
                events = full_response['events']
                if events:
                    logger.info(f"🔍 FILTERING: Processing {len(events)} total events from API (first fetch), filtering by {filter_type} '{filter_value}'{match_type_info}")
                    first_fetch_results = self._process_events_into_results(
                        events,
                        team_id,
                        team_name,
                        competition_slug,
                        sport,
                        season_id=season_id,
                        season_year=season_year,
                        observations=observations,
                        exclude_event_id=exclude_event_id,
                        apply_filters=True,
                        overall_results=overall_results,
                        overall_seen_event_ids=overall_seen_event_ids
                    )
                    
                    # Track seen event IDs to avoid duplicates
                    for result in first_fetch_results:
                        result_key = result.get('event_id')
                        if result_key not in seen_event_ids:
                            all_results.append(result)
                            seen_event_ids.add(result_key)
            
            # Check if we need additional fetches
            # Season filtering: keep fetching until we exhaust the season (no minimum)
            # Legacy filtering: fetch until we reach min_results
            fetch_index = 1
            MAX_FETCH_INDEX = 3
            
            # For season filtering, we continue fetching until we run out of events
            # For min_results filtering, we stop when we have enough
            should_continue_fetching = use_season_filtering or len(all_results) < min_results
            
            while should_continue_fetching and fetch_index < MAX_FETCH_INDEX:
                if use_season_filtering:
                    logger.info(f"📊 Team {team_name} - Collected {len(all_results)} season results so far, fetching more (index {fetch_index})")
                else:
                    logger.info(f"📊 Team {team_name} - Only {len(all_results)} results after fetch {fetch_index}, requesting additional batch (index {fetch_index}) to reach minimum of {min_results}")
                
                additional_response = api_client.get_team_last_results_response(
                    team_id,
                    is_tennis_singles=is_tennis_singles,
                    is_tennis_doubles=is_tennis_doubles,
                    fetch_index=fetch_index
                )
                
                if additional_response and 'events' in additional_response:
                    additional_events = additional_response['events']
                    if additional_events:
                        logger.info(f"🔍 FILTERING: Processing {len(additional_events)} total events from API (fetch index {fetch_index}), filtering by {filter_type} '{filter_value}'{match_type_info}")
                        additional_fetch_results = self._process_events_into_results(
                            additional_events,
                            team_id,
                            team_name,
                            competition_slug,
                            sport,
                            season_id=season_id,
                            season_year=season_year,
                            observations=observations,
                            exclude_event_id=exclude_event_id,
                            apply_filters=True,
                            overall_results=overall_results,
                            overall_seen_event_ids=overall_seen_event_ids
                        )
                        
                        # Add results from additional fetch, avoiding duplicates
                        added_count = 0
                        for result in additional_fetch_results:
                            result_key = result.get('event_id')
                            if result_key not in seen_event_ids:
                                all_results.append(result)
                                seen_event_ids.add(result_key)
                                added_count += 1
                        
                        if added_count == 0:
                            logger.info(f"📊 Team {team_name} - No new results added in fetch index {fetch_index}, continuing to next fetch (max {MAX_FETCH_INDEX})")
                    else:
                        logger.info(f"📊 Team {team_name} - Fetch index {fetch_index} returned no events, continuing to next fetch (max {MAX_FETCH_INDEX})")
                else:
                    logger.info(f"📊 Team {team_name} - Fetch index {fetch_index} returned no response, continuing to next fetch (max {MAX_FETCH_INDEX})")
                
                fetch_index += 1
                
                # Update loop condition
                should_continue_fetching = use_season_filtering or len(all_results) < min_results
            
            # Sort results by timestamp (most recent first)
            all_results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)
            overall_results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)
            
            # Calculate overall (unfiltered) win streak
            current_win_streak = self._calculate_current_win_streak(overall_results)
            logger.info(f"📈 Overall win streak for {team_name} (ID: {team_id}): {current_win_streak}")

            # Debug: Log the results to verify processing
            if all_results:
                filter_type, filter_value = self._get_filtering_criteria(sport, competition_slug, observations)
                match_type_info = ""
                if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                    match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                    match_type_info = f" and match_type '{match_type}'"
                logger.info(f"📊 Team {team_name} processed {len(all_results)} total results from {filter_type} '{filter_value}'{match_type_info}:")
                for i, result in enumerate(all_results[:5]):  # Show first 5
                    timestamp = result.get('startTimestamp', 0)
                    from datetime import datetime
                    date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M') if timestamp > 0 else 'Invalid'
                    own_rank = result.get('own_ranking', 0)
                    opp_rank = result.get('opponent_ranking', 0)
                    logger.info(f"  {i+1}. {date_str} - Winner: {result.get('winner', 'N/A')} ~{own_rank} vs ~{opp_rank} {result.get('opponent_name', 'N/A')}")
            else:
                filter_type, filter_value = self._get_filtering_criteria(sport, competition_slug, observations)
                match_type_info = ""
                if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                    match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                    match_type_info = f" and match_type '{match_type}'"
                logger.info(f"📊 Team {team_name} - no results found in {filter_type} '{filter_value}'{match_type_info}")
            
            # Return filtered results:
            # - Season filtering: all collected season results (no slicing)
            # - Legacy/min_results filtering: only the most recent min_results entries
            results_to_return = all_results if use_season_filtering else all_results[:min_results]
            return results_to_return, current_win_streak
            
        except Exception as e:
            logger.error(f"Error getting team last results for {team_name} (ID: {team_id}): {e}")
            return [], 0

    def get_winning_odds_data(self, event_id: int) -> Optional[Dict]:
        """
        Fetch winning odds data for an event using the new API endpoint.
        
        Args:
            event_id: ID of the event
            
        Returns:
            Dict with winning odds data or None if error
        """
        try:
            logger.debug(f"🎯 Fetching winning odds for event {event_id}")
            response = api_client.get_winning_odds_response(event_id)
            
            if not response:
                logger.debug(f"No winning odds data found for event {event_id}")
                return None
            
            # Check if response is an error or empty
            if isinstance(response, dict) and 'error' in response:
                logger.debug(f"API returned error for event {event_id}: {response.get('error')}")
                return None
            
            # Process the response to include decimal odds
            processed_data = {}
            
            # Check if response has the expected structure
            if not isinstance(response, dict):
                logger.debug(f"Invalid response format for event {event_id}: {type(response)}")
                return None
            
            for team_key in ['home', 'away']:
                if team_key in response and response[team_key] is not None:
                    team_data = response[team_key]
                    
                    # Validate that team_data is a dictionary with expected fields
                    if isinstance(team_data, dict) and 'fractionalValue' in team_data:
                        fractional_value = team_data.get('fractionalValue', '')
                        
                        # Convert fractional to decimal
                        decimal_odds = fractional_to_decimal(fractional_value)
                        
                        processed_data[team_key] = {
                            'fractionalValue': fractional_value,
                            'decimalValue': decimal_odds,
                            'expected': team_data.get('expected', 0),
                            'actual': team_data.get('actual', 0),
                            'id': team_data.get('id', 0)
                        }
                        
                        logger.debug(f"📊 {team_key.title()} team odds: {fractional_value} → {decimal_odds} (Expected: {team_data.get('expected')}%, Actual: {team_data.get('actual')}%)")
                    else:
                        logger.debug(f"📊 {team_key.title()} team: Invalid odds data structure")
                else:
                    logger.debug(f"📊 {team_key.title()} team: No odds data available (null or missing)")
            
            # Only return data if we have at least one valid team's odds
            if processed_data:
                logger.info(f"✅ Winning odds processed for event {event_id}: {list(processed_data.keys())}")
                return processed_data
            else:
                logger.info(f"ℹ️ No valid winning odds data found for event {event_id}")
                return None
            
        except Exception as e:
            logger.error(f"Error fetching winning odds for event {event_id}: {e}")
            return None

    def analyze_h2h_events(self, event_id: int, event_custom_id: str, 
                          event_start_time: datetime, sport: str, discovery_source: str, tournament_id: str, competition_name: str, competition_slug: str, season_id: str, season_name: str, participants: str,
                          home_team_name: str, away_team_name: str,
                          h2h_events: List[Dict], minutes_until_start: int, season_year: Optional[int] = None, observations: Optional[List[Dict]] = None,
                          home_team_id: int = None, away_team_id: int = None,
                          event_odds: Optional[Any] = None) -> Optional[H2HStreak]:
        """
        Analyze H2H events using proven result extraction from sofascore_api.py.
        Tracks wins relative to ACTUAL TEAMS (not home/away positions which change historically).
        """
        try:
            # Initialize results list - will be empty if no H2H events or no past events
            results = []
            
            # Log filtering criteria at the start
            filter_type, filter_value = self._get_filtering_criteria(sport, competition_slug, observations)
            match_type_info = ""
            if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                match_type_info = f" and match_type '{match_type}'"
            
            # Log detailed filtering information for H2H
            if h2h_events:
                logger.info(f"🔍 FILTERING H2H: Processing {len(h2h_events)} H2H events for {participants}")
                if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                    ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None) if observations else None
                    if ground_type_obs:
                        logger.info(f"🔍 FILTERING H2H: Applying filters: ground_type='{ground_type_obs.get('value')}'{match_type_info}")
                    else:
                        logger.info(f"🔍 FILTERING H2H: Applying filters: NO ground_type filter (ground_type not found in observations){match_type_info}")
                else:
                    logger.info(f"🔍 FILTERING H2H: Applying filters: competition='{competition_slug}'")
            
            if h2h_events:
                # Filter events within 2-year window
                cutoff_timestamp = (event_start_time - self.two_year_window).timestamp()
                
                # Extract results using proven logic and map to actual teams
                for event in h2h_events:
                    # Exclude current event from H2H analysis
                    h2h_event_id = event.get('id')
                    if h2h_event_id == event_id:
                        logger.debug(f"Skipping H2H event {h2h_event_id} - this is the current/upcoming event being analyzed")
                        continue
                    
                    event_timestamp = event.get('startTimestamp', 0)
                    
                    # Only past events within 2-year window
                    if event_timestamp >= cutoff_timestamp and event_timestamp < event_start_time.timestamp():
                        # Extract team data and names for later use in results mapping
                        hist_home_team_data = event.get('homeTeam', {})
                        hist_away_team_data = event.get('awayTeam', {})
                        hist_home = hist_home_team_data.get('name', '')
                        hist_away = hist_away_team_data.get('name', '')
                        
                        # SPORT-SPECIFIC FILTERING: Apply appropriate filter based on sport
                        if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                            # For tennis, filter by ground_type AND singles/doubles
                            if observations:
                                # Search for ground_type anywhere in observations (not just first item)
                                ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None)
                                if ground_type_obs:
                                    ground_type_value = ground_type_obs.get('value')
                                    if ground_type_value != event.get('groundType'):
                                        logger.debug(f"Skipping H2H event - different ground type: {ground_type_value} vs {event.get('groundType')}")
                                        continue
                            
                            # Filter by singles/doubles match type (use type field from API response)
                            hist_home_team_type = hist_home_team_data.get('type', 1)
                            hist_away_team_type = hist_away_team_data.get('type', 1)
                            event_is_doubles = hist_home_team_type == 2 and hist_away_team_type == 2
                            sport_is_doubles = sport == 'Tennis Doubles'
                            
                            if event_is_doubles != sport_is_doubles:
                                logger.debug(f"Skipping H2H event - different match type (event is {'doubles' if event_is_doubles else 'singles'}, sport is {'doubles' if sport_is_doubles else 'singles'})")
                                continue
                        else: 
                            # For other sports, filter by competition
                            if competition_slug:
                                event_competition_slug = event.get('tournament', {}).get('uniqueTournament', {}).get('slug', '')
                                if event_competition_slug != competition_slug:
                                    logger.debug(f"Skipping H2H event - different competition: {event_competition_slug} vs {competition_slug}")
                                    continue
                        
                        # Use proven extraction logic
                        # For tennis, also extract period points for points-based H2H tracking
                        extract_points = sport in ['Tennis', 'Tennis Doubles']
                        result_data = api_client.extract_results_from_response({'event': event}, extract_tennis_points=extract_points, for_streaks=True)
                        if result_data:
                            # Skip canceled/postponed events in H2H history
                            if result_data.get('_canceled'):
                                logger.debug(f"Skipping H2H event {h2h_event_id} - event was canceled/postponed")
                                continue
                                
                            # Map winner to actual teams (not positions)
                            winner_position = result_data['winner']  # '1', '2', or 'X'
                            
                            # Determine which team won relative to upcoming event's home team
                            if winner_position == 'X':
                                winner_relative = 'X'
                                upcoming_home_score = result_data['home_score'] if hist_home == home_team_name else result_data['away_score']
                                upcoming_away_score = result_data['away_score'] if hist_home == home_team_name else result_data['home_score']
                            elif winner_position == '1':  # Historical home team won
                                if hist_home == home_team_name:
                                    winner_relative = '1'  # Upcoming home team won
                                    upcoming_home_score = result_data['home_score']
                                    upcoming_away_score = result_data['away_score']
                                else:  # Historical home was upcoming away team
                                    winner_relative = '2'  # Upcoming away team won
                                    upcoming_home_score = result_data['away_score']
                                    upcoming_away_score = result_data['home_score']
                            else:  # winner_position == '2' - Historical away team won
                                if hist_away == away_team_name:
                                    winner_relative = '2'  # Upcoming away team won
                                    upcoming_home_score = result_data['home_score']
                                    upcoming_away_score = result_data['away_score']
                                else:  # Historical away was upcoming home team
                                    winner_relative = '1'  # Upcoming home team won
                                    upcoming_home_score = result_data['away_score']
                                    upcoming_away_score = result_data['home_score']
                            
                            # Determine role of upcoming home team in historical match
                            upcoming_home_role = 'home' if hist_home == home_team_name else 'away'
                            
                            result_entry = {
                                'winner': winner_relative,
                                'home_score': upcoming_home_score,
                                'away_score': upcoming_away_score,
                                # Add detailed match info
                                'hist_home': hist_home,
                                'hist_away': hist_away,
                                'hist_home_score': result_data['home_score'],
                                'hist_away_score': result_data['away_score'],
                                'hist_home_penalties': result_data.get('home_penalties', None),
                                'hist_away_penalties': result_data.get('away_penalties', None),
                                'winner_position': winner_position,
                                'startTimestamp': event_timestamp,  # Add timestamp for date display
                                'upcoming_home_role': upcoming_home_role  # Track role for net points calculation
                            }
                            
                            # Add tennis period points if available (for points-based H2H tracking)
                            if sport in ['Tennis', 'Tennis Doubles'] and 'home_period1' in result_data:
                                result_entry['hist_home_period1'] = result_data.get('home_period1')
                                result_entry['hist_home_period2'] = result_data.get('home_period2')
                                result_entry['hist_home_period3'] = result_data.get('home_period3')
                                result_entry['hist_away_period1'] = result_data.get('away_period1')
                                result_entry['hist_away_period2'] = result_data.get('away_period2')
                                result_entry['hist_away_period3'] = result_data.get('away_period3')
                            
                            results.append(result_entry)
            
            # Continue processing even if no H2H results found
            # This allows us to still show team form and winning odds data
            
            # Log H2H filtering results
            filter_type, filter_value = self._get_filtering_criteria(sport, competition_slug, observations)
            if filter_value:
                match_type_info = ""
                if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                    match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                    match_type_info = f" and match_type '{match_type}'"
                logger.info(f"📊 H2H analysis: Found {len(results)} matches in {filter_type} '{filter_value}'{match_type_info} (filtered by {filter_type})")
            else:
                logger.info(f"📊 H2H analysis: Found {len(results)} matches (no filtering)")
            
            # Calculate statistics relative to upcoming event teams
            home_wins = sum(1 for r in results if r['winner'] == '1')
            away_wins = sum(1 for r in results if r['winner'] == '2')
            draws = sum(1 for r in results if r['winner'] == 'X')
            total = len(results)
            
            # All matches with detailed information (most recent first)
            all_matches = results  # Already in most recent first order
            
            # Extract results list from matches for streak detection
            all_results = [match['winner'] for match in all_matches]
            
            # Detect streak with team names
            streak_text, streak_count = self._detect_streak(all_results, home_team_name, away_team_name)
            
            # Calculate averages relative to upcoming event teams
            avg_home = sum(r['home_score'] for r in results) / total if total > 0 else 0
            avg_away = sum(r['away_score'] for r in results) / total if total > 0 else 0
            
            # Calculate H2H net points by role
            h2h_home_net_points = 0
            h2h_away_net_points = 0
            for r in results:
                if r.get('upcoming_home_role') == 'home':
                    # Upcoming home team was home in this match
                    h2h_home_net_points += (r['home_score'] - r['away_score'])
                else:
                    # Upcoming home team was away in this match
                    h2h_away_net_points += (r['home_score'] - r['away_score'])
            
            # Get team results (last 10 games for each team)
            home_team_results = []
            away_team_results = []
            home_team_wins = 0
            away_team_wins = 0
            home_team_losses = 0
            away_team_losses = 0
            home_team_draws = 0
            away_team_draws = 0
            home_team_batches = []
            away_team_batches = []
            home_overall_win_streak = 0
            away_overall_win_streak = 0
            
            # Use provided season_year, or fallback to parsing season_name if not provided
            target_season_year = season_year
            if target_season_year is None and season_name:
                target_season_year = SeasonRepository._parse_year(season_name)
            
            if self.ENABLE_SEASON_YEAR_FILTERING and target_season_year:
                logger.info(f"📅 Using season year filtering: {target_season_year} (fallback enabled: {self.ENABLE_SEASON_YEAR_FILTERING})")
            
            if home_team_id:
                home_team_results, home_overall_win_streak = self.get_team_last_results_by_id(
                    home_team_id,
                    home_team_name,
                    competition_slug,
                    sport,
                    season_id=season_id,
                    season_year=target_season_year,
                    observations=observations,
                    exclude_event_id=event_id,
                    event_start_timestamp=event_start_time.timestamp() if event_start_time else None
                )
            else:
                logger.debug(f"No home_team_id provided for {home_team_name}")
            
            if away_team_id:
                away_team_results, away_overall_win_streak = self.get_team_last_results_by_id(
                    away_team_id,
                    away_team_name,
                    competition_slug,
                    sport,
                    season_id=season_id,
                    season_year=target_season_year,
                    observations=observations,
                    exclude_event_id=event_id,
                    event_start_timestamp=event_start_time.timestamp() if event_start_time else None
                )
            else:
                logger.debug(f"No away_team_id provided for {away_team_name}")
            
            # Trim results to match the player with fewer results (for fair ranking comparison)
            # But try to maintain at least 10 results for each player when possible
            if home_team_results and away_team_results:
                home_count = len(home_team_results)
                away_count = len(away_team_results)
                min_results = min(home_count, away_count)
                
                # Only trim if counts differ, and try to maintain at least 10 results
                if home_count != away_count:
                    # If both have 10+, trim to the minimum (but that will be at least 10)
                    # If one has less than 10, trim to that (but that's necessary for fair comparison)
                    target_count = min_results
                    logger.info(f"📊 Trimming results to {target_count} matches (home had {home_count}, away had {away_count})")
                    home_team_results = home_team_results[:target_count]
                    away_team_results = away_team_results[:target_count]
            
            # Calculate stats and process batches for home team
            home_team_final_real_ranking = 0
            if home_team_results:
                home_team_wins = sum(1 for r in home_team_results if r['winner'] == '1')
                home_team_losses = sum(1 for r in home_team_results if r['winner'] == '2')
                home_team_draws = sum(1 for r in home_team_results if r['winner'] == 'X')
                home_team_batches = self.process_team_results_into_batches(home_team_results, home_team_name)
                home_team_final_real_ranking = self._calculate_final_real_ranking(home_team_batches)
                logger.info(f"📊 Home team {home_team_name} form: {home_team_wins}W-{home_team_losses}L-{home_team_draws}D ({len(home_team_batches)} batches, final ranking: {home_team_final_real_ranking})")
            
            # Calculate stats and process batches for away team
            away_team_final_real_ranking = 0
            if away_team_results:
                away_team_wins = sum(1 for r in away_team_results if r['winner'] == '1')
                away_team_losses = sum(1 for r in away_team_results if r['winner'] == '2')
                away_team_draws = sum(1 for r in away_team_results if r['winner'] == 'X')
                away_team_batches = self.process_team_results_into_batches(away_team_results, away_team_name)
                away_team_final_real_ranking = self._calculate_final_real_ranking(away_team_batches)
                logger.info(f"📊 Away team {away_team_name} form: {away_team_wins}W-{away_team_losses}L-{away_team_draws}D ({len(away_team_batches)} batches, final ranking: {away_team_final_real_ranking})")
            
            # Fetch winning odds data
            winning_odds_data = self.get_winning_odds_data(event_id)
            
            # Log summary of what data we have
            data_summary = []
            if total > 0:
                data_summary.append(f"H2H: {total} matches")
            if home_team_wins + home_team_losses + home_team_draws > 0:
                data_summary.append(f"Home form: {home_team_wins}W-{home_team_losses}L-{home_team_draws}D")
            if away_team_wins + away_team_losses + away_team_draws > 0:
                data_summary.append(f"Away form: {away_team_wins}W-{away_team_losses}L-{away_team_draws}D")
            if winning_odds_data:
                data_summary.append("Winning odds: available")
            
            if data_summary:
                logger.info(f"📊 H2H analysis for {participants}: {', '.join(data_summary)}")
            else:
                logger.info(f"📊 H2H analysis for {participants}: No data available (no H2H matches, no team form, no winning odds)")
            
            # Extract rankings safely from observations
            home_team_ranking = None
            away_team_ranking = None

            home_team_standing = None
            away_team_standing = None

            if sport not in ['Tennis', 'Tennis Doubles'] and season_id and tournament_id and (home_team_id or away_team_id):
                raw_standings = api_client.get_standings_response(season_id, tournament_id)
                home_team_standing, away_team_standing = api_client.process_standings_response(
                    raw_standings,
                    home_team_id,
                    away_team_id
                )

                if home_team_standing and home_team_ranking is None:
                    home_team_ranking = home_team_standing.get('position')
                if away_team_standing and away_team_ranking is None:
                    away_team_ranking = away_team_standing.get('position')

                if home_team_standing or away_team_standing:
                    logger.debug(
                        f"Extracted standings snapshots for event {event_id}: "
                        f"home={home_team_standing}, away={away_team_standing}"
                    )
                else:
                    logger.debug(f"No standings found for event {event_id}")

            if observations:
                # Buscar rankings en cualquier posición de la lista (no asumir posición fija)
                rankings_obs = next((obs for obs in observations if isinstance(obs, dict) and obs.get('type') == 'rankings'), None)
                if rankings_obs:
                    home_team_ranking = rankings_obs.get('home_ranking')
                    away_team_ranking = rankings_obs.get('away_ranking')
                    logger.debug(f"Extracted rankings from observations: home={home_team_ranking}, away={away_team_ranking}")
                else:
                    logger.debug(f"No rankings found in observations for event {event_id}")
            
            # Extract odds from event_odds if available
            one_open = None
            x_open = None
            two_open = None
            one_final = None
            x_final = None
            two_final = None
            
            if event_odds:
                try:
                    # Convert Decimal to float for consistency
                    one_open = float(event_odds.one_open) if event_odds.one_open is not None else None
                    x_open = float(event_odds.x_open) if event_odds.x_open is not None else None
                    two_open = float(event_odds.two_open) if event_odds.two_open is not None else None
                    one_final = float(event_odds.one_final) if event_odds.one_final is not None else None
                    x_final = float(event_odds.x_final) if event_odds.x_final is not None else None
                    two_final = float(event_odds.two_final) if event_odds.two_final is not None else None
                    logger.debug(f"Extracted odds for event {event_id}: 1={one_open}→{one_final}, X={x_open}→{x_final}, 2={two_open}→{two_final}")
                except (AttributeError, ValueError, TypeError) as e:
                    logger.warning(f"Error extracting odds from event_odds for event {event_id}: {e}")
            
            return H2HStreak(
                event_id=event_id,
                custom_id=event_custom_id,
                participants=participants,
                discovery_source=discovery_source,
                competition_name=competition_name,
                competition_slug=competition_slug,
                season_id=season_id,
                season_name=season_name,
                observations=observations,
                sport=sport,
                home_team_name=home_team_name,
                away_team_name=away_team_name,
                home_team_ranking=home_team_ranking,
                away_team_ranking=away_team_ranking,
                total_h2h_matches=len(h2h_events),
                matches_analyzed=total,
                home_wins=home_wins,
                away_wins=away_wins,
                draws=draws,
                home_win_rate=round(home_wins / total * 100, 1) if total > 0 else 0,
                away_win_rate=round(away_wins / total * 100, 1) if total > 0 else 0,
                draw_rate=round(draws / total * 100, 1) if total > 0 else 0,
                all_matches=all_matches,
                h2h_home_net_points=h2h_home_net_points,
                h2h_away_net_points=h2h_away_net_points,
                current_streak=streak_text,
                current_streak_count=streak_count,
                avg_home_score=round(avg_home, 1),
                avg_away_score=round(avg_away, 1),
                minutes_until_start=minutes_until_start,
                # NEW: Team results data
                home_team_results=home_team_results,
                away_team_results=away_team_results,
                home_team_wins=home_team_wins,
                away_team_wins=away_team_wins,
                home_team_losses=home_team_losses,
                away_team_losses=away_team_losses,
                home_team_draws=home_team_draws,
                away_team_draws=away_team_draws,
                # NEW: Batched team form data
                home_team_batches=home_team_batches,
                away_team_batches=away_team_batches,
                # NEW: Final real rankings
                home_team_final_real_ranking=home_team_final_real_ranking,
                away_team_final_real_ranking=away_team_final_real_ranking,
                # NEW: Standings snapshots
                home_team_standing=home_team_standing,
                away_team_standing=away_team_standing,
                # NEW: Winning odds data
                winning_odds_data=winning_odds_data,
                # NEW: Current event odds
                one_open=one_open,
                x_open=x_open,
                two_open=two_open,
                one_final=one_final,
                x_final=x_final,
                two_final=two_final,
                # NEW: Overall win streaks
                home_current_win_streak=home_overall_win_streak,
                away_current_win_streak=away_overall_win_streak
            )
            
        except Exception as e:
            logger.error(f"Error analyzing H2H events: {e}")
            return None
    
    def _detect_streak(self, results: List[str], home_team_name: str = None, 
                      away_team_name: str = None) -> Tuple[str, int]:
        """
        Detect the current streak from recent results.
        
        Args:
            results: List of recent results ['1', '2', 'X'] (most recent first)
            home_team_name: Optional home team name for detailed streak text
            away_team_name: Optional away team name for detailed streak text
            
        Returns:
            Tuple of (streak_text, streak_count)
        """
        if not results:
            return "No H2H matches found between these players", 0
        
        # Get the most recent result
        current_result = results[0]
        streak_count = 1
        
        # Count consecutive occurrences
        for result in results[1:]:
            if result == current_result:
                streak_count += 1
            else:
                break
        
        # Format streak text with team names if available
        if current_result == '1':
            team_name = home_team_name if home_team_name else "Home"
            streak_text = f"{team_name} won last {streak_count} match{'es' if streak_count > 1 else ''}"
        elif current_result == '2':
            team_name = away_team_name if away_team_name else "Away"
            streak_text = f"{team_name} won last {streak_count} match{'es' if streak_count > 1 else ''}"
        elif current_result == 'X':
            streak_text = f"Last {streak_count} match{'es' if streak_count > 1 else ''} ended in draw"
        else:
            streak_text = "Unknown streak"
        
        return streak_text, streak_count
    
    def should_send_streak_alert(self, streak: H2HStreak) -> bool:
        """
        Determine if a streak alert should be sent.
        
        UPDATED: Now requires at least one team to have ≥STREAK_ALERT_MIN_RESULTS past results.
        This filters out events with insufficient historical data.
        
        Send alerts if we have sufficient data AND at least one of:
        1. H2H data (at least 1 match analyzed), OR
        2. Team form data (at least one team has results), OR  
        3. Winning odds data
        
        This ensures we send alerts even when there are no H2H matches
        but we have team form or winning odds information.
        
        Args:
            streak: H2HStreak object
            
        Returns:
            True if we have any meaningful data to show AND sufficient historical data
        """
        from config import Config
        
        # Calculate total games for each team
        home_total_games = len(streak.home_team_results) if hasattr(streak, 'home_team_results') and streak.home_team_results else 0
        away_total_games = len(streak.away_team_results) if hasattr(streak, 'away_team_results') and streak.away_team_results else 0
        
        # NEW REQUIREMENT: At least one team must have ≥STREAK_ALERT_MIN_RESULTS past results
        min_results_threshold = Config.STREAK_ALERT_MIN_RESULTS
        has_sufficient_data = home_total_games >= min_results_threshold or away_total_games >= min_results_threshold
        
        if not has_sufficient_data:
            logger.info(f"⏭️ H2H streak alert skipped for {streak.participants}: Insufficient data (home: {home_total_games}, away: {away_total_games}, need ≥{min_results_threshold})")
            return False
        
        # Check if we have H2H data
        has_h2h_data = streak.matches_analyzed >= 1
        
        # Check if we have team form data
        has_team_form = (streak.home_team_wins + streak.home_team_losses + streak.home_team_draws > 0) or \
                       (streak.away_team_wins + streak.away_team_losses + streak.away_team_draws > 0)
        
        # Check if we have winning odds data
        has_winning_odds = streak.winning_odds_data is not None
        
        should_send = has_h2h_data or has_team_form or has_winning_odds
        
        # Log the decision
        if should_send:
            reasons = []
            if has_h2h_data:
                reasons.append(f"H2H data ({streak.matches_analyzed} matches)")
            if has_team_form:
                reasons.append("team form data")
            if has_winning_odds:
                reasons.append("winning odds data")
            logger.info(f"✅ H2H streak alert will send for {streak.participants}: {', '.join(reasons)} (data: home={home_total_games}, away={away_total_games})")
        else:
            logger.info(f"⏭️ H2H streak alert skipped for {streak.participants}: No meaningful data (H2H: {streak.matches_analyzed}, Home form: {streak.home_team_wins + streak.home_team_losses + streak.home_team_draws}, Away form: {streak.away_team_wins + streak.away_team_losses + streak.away_team_draws}, Winning odds: {has_winning_odds})")
        
        return should_send


# Global streak alert engine instance
streak_alert_engine = StreakAlertEngine()

