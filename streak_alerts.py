"""
Streak Alert System - H2H streak analysis for upcoming events

Imports proven result extraction logic from sofascore_api.py
to ensure consistency with existing result collection system.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from sofascore_api import api_client
import sofascore_api2
from odds_utils import fractional_to_decimal
logger = logging.getLogger(__name__)

@dataclass
class H2HStreak:
    """Represents H2H streak analysis between two teams (relative to upcoming event)"""
    event_id: int
    custom_id: str
    participants: str
    sport: str
    home_team_name: str  # Upcoming event home team
    away_team_name: str  # Upcoming event away team
    total_h2h_matches: int
    matches_analyzed: int  # Matches within 2-year window
    home_wins: int  # Wins for upcoming event's home team
    away_wins: int  # Wins for upcoming event's away team
    draws: int
    home_win_rate: float  # Percentage
    away_win_rate: float  # Percentage
    draw_rate: float  # Percentage
    all_results: List[str]  # All results relative to upcoming home team (most recent first)
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
    # NEW: Winning odds data
    winning_odds_data: Optional[Dict] = None  # Winning odds response data


class StreakAlertEngine:
    """Engine for analyzing H2H streaks and generating alerts"""
    
    def __init__(self):
        self.two_year_window = timedelta(days=730)  # 2 years
    

    def get_team_last_10_results(self, team_name: str) -> List[Dict]:
        """
        Get the last 10 results for a team using team name.
        Processes team results response and returns standardized format.
        
        Args:
            team_name: Name of the team (e.g., "Inter Miami CF")
            
        Returns:
            List of dicts with keys: winner, home_score, away_score, team_name, opponent_name
            Format: [
                {
                    "winner": "1",  # "1" = team won, "2" = opponent won, "X" = draw
                    "home_score": 2,
                    "away_score": 1,
                    "team_name": "Inter Miami CF",
                    "opponent_name": "CF Montréal"
                }
            ]
        """
        try:
            # Get team ID from team name (we need to find this team in our database or API)
            # For now, we'll use a placeholder - in production, you'd look up the team_id
            # This is a limitation of the current approach - we need team_id, not team_name
            
            logger.warning(f"get_team_last_10_results called with team_name='{team_name}' but needs team_id. This function needs team_id lookup implementation.")
            return []
            
        except Exception as e:
            logger.error(f"Error getting team last 10 results for {team_name}: {e}")
            return []
    
    def get_team_last_10_results_by_id(self, team_id: int, team_name: str) -> List[Dict]:
        """
        Get the last 10 results for a team using team_id.
        Processes team results response and returns standardized format.
        
        Args:
            team_id: ID of the team
            team_name: Name of the team for context
            
        Returns:
            List of dicts with keys: winner, home_score, away_score, team_name, opponent_name
        """
        try:
            # Fetch team results from API
            full_response = api_client.get_team_last_10_results_response(team_id)
            if not full_response or 'events' not in full_response:
                logger.debug(f"No team results found for team {team_name} (ID: {team_id})")
                return []
            
            events = full_response['events']
            if not events:
                logger.debug(f"No events in team results for team {team_name} (ID: {team_id})")
                return []
            
            # Process events (they are in inverted order - last event is most recent)
            results = []
            for event in events:
                try:
                    # Extract result using proven logic
                    result_data = api_client.extract_results_from_response({'event': event})
                    if not result_data:
                        continue
                    
                    # Get team names
                    home_team = event.get('homeTeam', {}).get('name', '')
                    away_team = event.get('awayTeam', {}).get('name', '')
                    
                    # Determine if our team won, lost, or drew
                    winner_position = result_data['winner']  # '1', '2', or 'X'
                    
                    if winner_position == 'X':
                        # Draw
                        team_result = 'X'
                        opponent_name = away_team if home_team == team_name else home_team
                    elif winner_position == '1':
                        # Home team won
                        if home_team == team_name:
                            team_result = '1'  # Our team won
                            opponent_name = away_team
                        else:
                            team_result = '2'  # Our team lost
                            opponent_name = home_team
                    else:  # winner_position == '2'
                        # Away team won
                        if away_team == team_name:
                            team_result = '1'  # Our team won
                            opponent_name = home_team
                        else:
                            team_result = '2'  # Our team lost
                            opponent_name = away_team
                    
                    # Map scores to our team's perspective
                    if home_team == team_name:
                        team_score = result_data['home_score']
                        opponent_score = result_data['away_score']
                    else:
                        team_score = result_data['away_score']
                        opponent_score = result_data['home_score']
                    
                    results.append({
                        'winner': team_result,
                        'home_score': team_score,
                        'away_score': opponent_score,
                        'team_name': team_name,
                        'opponent_name': opponent_name,
                        'startTimestamp': event.get('startTimestamp', 0)  # Include for sorting
                    })
                    
                except Exception as e:
                    logger.debug(f"Error processing team result event: {e}")
                    continue
            
            # Sort by startTimestamp to get most recent first (API returns inverted order)
            results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)
            
            # Debug: Log the results to verify processing
            if results:
                logger.info(f"📊 Team {team_name} processed {len(results)} results:")
                for i, result in enumerate(results[:5]):  # Show first 5
                    timestamp = result.get('startTimestamp', 0)
                    from datetime import datetime
                    date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M') if timestamp > 0 else 'Invalid'
                    logger.info(f"  {i+1}. {date_str} - Winner: {result.get('winner', 'N/A')} vs {result.get('opponent_name', 'N/A')}")
            
            # Return only the 10 most recent
            return results[:10]
            
        except Exception as e:
            logger.error(f"Error getting team last 10 results for {team_name} (ID: {team_id}): {e}")
            return []

    def get_winning_odds_data(self, event_id: int) -> Optional[Dict]:
        """
        Fetch winning odds data for an event using the new API endpoint.
        
        Args:
            event_id: ID of the event
            
        Returns:
            Dict with winning odds data or None if error
        """
        try:
            logger.info(f"🎯 Fetching winning odds for event {event_id}")
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
                          event_start_time: datetime, sport: str, participants: str,
                          home_team_name: str, away_team_name: str,
                          h2h_events: List[Dict], minutes_until_start: int,
                          home_team_id: int = None, away_team_id: int = None) -> Optional[H2HStreak]:
        """
        Analyze H2H events using proven result extraction from sofascore_api.py.
        Tracks wins relative to ACTUAL TEAMS (not home/away positions which change historically).
        """
        try:
            if not h2h_events:
                return None
            
            # Filter events within 2-year window
            cutoff_timestamp = (event_start_time - self.two_year_window).timestamp()
            
            # Extract results using proven logic and map to actual teams
            results = []
            for event in h2h_events:
                event_timestamp = event.get('startTimestamp', 0)
                
                # Only past events within 2-year window
                if event_timestamp >= cutoff_timestamp and event_timestamp < event_start_time.timestamp():
                    # Use proven extraction logic
                    result_data = api_client.extract_results_from_response({'event': event})
                    if result_data:
                        # Get team names from historical event
                        hist_home = event.get('homeTeam', {}).get('name', '')
                        hist_away = event.get('awayTeam', {}).get('name', '')
                        
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
                        
                        results.append({
                            'winner': winner_relative,
                            'home_score': upcoming_home_score,
                            'away_score': upcoming_away_score
                        })
            
            if not results:
                return None
            
            # Calculate statistics relative to upcoming event teams
            home_wins = sum(1 for r in results if r['winner'] == '1')
            away_wins = sum(1 for r in results if r['winner'] == '2')
            draws = sum(1 for r in results if r['winner'] == 'X')
            total = len(results)
            
            # All results for flexible display (most recent first)
            all_results = [r['winner'] for r in results]
            
            # Detect streak with team names
            streak_text, streak_count = self._detect_streak(all_results, home_team_name, away_team_name)
            
            # Calculate averages relative to upcoming event teams
            avg_home = sum(r['home_score'] for r in results) / total if total > 0 else 0
            avg_away = sum(r['away_score'] for r in results) / total if total > 0 else 0
            
            # Get team results (last 10 games for each team)
            home_team_results = []
            away_team_results = []
            home_team_wins = 0
            away_team_wins = 0
            home_team_losses = 0
            away_team_losses = 0
            home_team_draws = 0
            away_team_draws = 0
            
            if home_team_id:
                home_team_results = self.get_team_last_10_results_by_id(home_team_id, home_team_name)
                home_team_wins = sum(1 for r in home_team_results if r['winner'] == '1')
                home_team_losses = sum(1 for r in home_team_results if r['winner'] == '2')
                home_team_draws = sum(1 for r in home_team_results if r['winner'] == 'X')
                logger.info(f"📊 Home team {home_team_name} form: {home_team_wins}W-{home_team_losses}L-{home_team_draws}D")
            else:
                logger.debug(f"No home_team_id provided for {home_team_name}")
            
            if away_team_id:
                away_team_results = self.get_team_last_10_results_by_id(away_team_id, away_team_name)
                away_team_wins = sum(1 for r in away_team_results if r['winner'] == '1')
                away_team_losses = sum(1 for r in away_team_results if r['winner'] == '2')
                away_team_draws = sum(1 for r in away_team_results if r['winner'] == 'X')
                logger.info(f"📊 Away team {away_team_name} form: {away_team_wins}W-{away_team_losses}L-{away_team_draws}D")
            else:
                logger.debug(f"No away_team_id provided for {away_team_name}")
            
            # Fetch winning odds data
            winning_odds_data = self.get_winning_odds_data(event_id)
            
            return H2HStreak(
                event_id=event_id,
                custom_id=event_custom_id,
                participants=participants,
                sport=sport,
                home_team_name=home_team_name,
                away_team_name=away_team_name,
                total_h2h_matches=len(h2h_events),
                matches_analyzed=total,
                home_wins=home_wins,
                away_wins=away_wins,
                draws=draws,
                home_win_rate=round(home_wins / total * 100, 1) if total > 0 else 0,
                away_win_rate=round(away_wins / total * 100, 1) if total > 0 else 0,
                draw_rate=round(draws / total * 100, 1) if total > 0 else 0,
                all_results=all_results,
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
                # NEW: Winning odds data
                winning_odds_data=winning_odds_data
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
            return "No recent matches", 0
        
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
        
        Always send alerts if we have H2H data (no significance filtering).
        Only requirement: at least 1 H2H match analyzed.
        
        Args:
            streak: H2HStreak object
            
        Returns:
            True if at least 1 match was analyzed
        """
        return streak.matches_analyzed >= 1


# Global streak alert engine instance
streak_alert_engine = StreakAlertEngine()

