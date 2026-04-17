"""
Head-to-Head analysis for Matchup Streak Analysis.

Processes H2H event history and detects streaks between two teams.
Extracted from streak_alerts.py (StreakAlertEngine.build_matchup_streak_context H2H section
and StreakAlertEngine._detect_streak).
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import timedelta

from modules.sofascore import api_client

logger = logging.getLogger(__name__)

# 2-year window constant (mirrors StreakAlertEngine.two_year_window)
TWO_YEAR_WINDOW = timedelta(days=730)


def analyze_head_to_head_events(
    matchup_events: List[Dict],
    event_id: int,
    event_start_time,
    home_team_name: str,
    away_team_name: str,
    sport: str,
    competition_slug: str,
    participants: str,
    observations: Optional[List[Dict]] = None,
) -> List[Dict]:
    """
    Filter and process H2H events into normalized result entries.

    Applies 2-year window, competition/ground-type filtering, and maps
    historical wins to the perspective of the upcoming event's home/away teams.

    Args:
        matchup_events: List of raw H2H event dicts from the API
        event_id: Current event ID to exclude from analysis
        event_start_time: datetime of the upcoming event
        home_team_name: Upcoming event home team name
        away_team_name: Upcoming event away team name
        sport: Sport type for filtering logic
        competition_slug: Competition slug for non-tennis sports
        participants: Human-readable matchup string (for logging)
        observations: Optional list of observation dicts for sport-specific filtering

    Returns:
        List of result entry dicts (most-recent-first order from the caller loop).
        Each entry contains: winner, home_score, away_score, hist_home, hist_away,
        hist_home_score, hist_away_score, winner_position, startTimestamp,
        upcoming_home_role, and tennis period fields when applicable.
    """
    results: List[Dict] = []

    if not matchup_events:
        return results

    # Log filtering info
    _log_h2h_filtering_info(matchup_events, participants, sport, competition_slug, observations)

    cutoff_timestamp = (event_start_time - TWO_YEAR_WINDOW).timestamp()

    for event in matchup_events:
        # Exclude current event from H2H analysis
        matchup_event_id = event.get('id')
        if matchup_event_id == event_id:
            logger.debug(f"Skipping H2H event {matchup_event_id} - this is the current/upcoming event being analyzed")
            continue

        event_timestamp = event.get('startTimestamp', 0)

        # Only past events within 2-year window
        if not (event_timestamp >= cutoff_timestamp and event_timestamp < event_start_time.timestamp()):
            continue

        # Extract team data and names for results mapping
        hist_home_team_data = event.get('homeTeam', {})
        hist_away_team_data = event.get('awayTeam', {})
        hist_home = hist_home_team_data.get('name', '')
        hist_away = hist_away_team_data.get('name', '')

        # SPORT-SPECIFIC FILTERING
        if sport in ('Tennis', 'Tennis Doubles'):
            # Filter by ground_type
            if observations:
                ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None)
                if ground_type_obs:
                    ground_type_value = ground_type_obs.get('value')
                    if ground_type_value != event.get('groundType'):
                        logger.debug(f"Skipping H2H event - different ground type: {ground_type_value} vs {event.get('groundType')}")
                        continue

            # Filter by singles/doubles match type
            hist_home_team_type = hist_home_team_data.get('type', 1)
            hist_away_team_type = hist_away_team_data.get('type', 1)
            event_is_doubles = hist_home_team_type == 2 and hist_away_team_type == 2
            sport_is_doubles = sport == 'Tennis Doubles'

            if event_is_doubles != sport_is_doubles:
                logger.debug(
                    f"Skipping H2H event - different match type "
                    f"(event is {'doubles' if event_is_doubles else 'singles'}, "
                    f"sport is {'doubles' if sport_is_doubles else 'singles'})"
                )
                continue
        else:
            # For other sports, filter by competition
            if competition_slug:
                event_competition_slug = event.get('tournament', {}).get('uniqueTournament', {}).get('slug', '')
                if event_competition_slug != competition_slug:
                    logger.debug(f"Skipping H2H event - different competition: {event_competition_slug} vs {competition_slug}")
                    continue

        # Use proven extraction logic
        extract_points = sport in ['Tennis', 'Tennis Doubles']
        result_data = api_client.extract_results_from_response(
            {'event': event},
            extract_tennis_points=extract_points,
            for_streaks=True
        )
        if not result_data:
            continue

        # Skip canceled/postponed events in H2H history
        if result_data.get('_canceled'):
            logger.debug(f"Skipping H2H event {matchup_event_id} - event was canceled/postponed")
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
            # Detailed match info
            'hist_home': hist_home,
            'hist_away': hist_away,
            'hist_home_score': result_data['home_score'],
            'hist_away_score': result_data['away_score'],
            'hist_home_penalties': result_data.get('home_penalties', None),
            'hist_away_penalties': result_data.get('away_penalties', None),
            'winner_position': winner_position,
            'startTimestamp': event_timestamp,
            'upcoming_home_role': upcoming_home_role
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

    return results


def detect_streak(
    results: List[str],
    home_team_name: str = None,
    away_team_name: str = None
) -> Tuple[str, int]:
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
        return "No matchup events found between these players", 0

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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_h2h_filtering_info(
    matchup_events: List[Dict],
    participants: str,
    sport: str,
    competition_slug: str,
    observations: Optional[List[Dict]]
) -> None:
    """Log H2H filtering criteria at the start of processing."""
    match_type_info = ""
    if sport in ('Tennis', 'Tennis Doubles'):
        match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
        match_type_info = f" and match_type '{match_type}'"

    logger.info(f"🔍 FILTERING MATCHUP: Processing {len(matchup_events)} H2H events for {participants}")

    if sport in ('Tennis', 'Tennis Doubles'):
        ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None) if observations else None
        if ground_type_obs:
            logger.info(f"🔍 FILTERING MATCHUP: Applying filters: ground_type='{ground_type_obs.get('value')}'{match_type_info}")
        else:
            logger.info(f"🔍 FILTERING MATCHUP: Applying filters: NO ground_type filter (ground_type not found in observations){match_type_info}")
    else:
        logger.info(f"🔍 FILTERING MATCHUP: Applying filters: competition='{competition_slug}'")
