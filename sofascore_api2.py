# sofascore_api2.py
# meant to process events that are not covered by the main sofascore_api.py file, like streaks, team streaks, h2h and winning odds events.
from sofascore_api import SofaScoreAPI, api_client
import logging
from typing import Dict, List, Tuple, Optional
from timezone_utils import get_local_now_aware

logger = logging.getLogger(__name__)

# --- Define your new methods ---

# API fetching methods
def get_high_value_streaks_events(self):
    """fetches list of top 20 high value streaks events"""
    logger.info(f"Fetching top 20 high value streaks events list")
    endpoint = f"/odds/1/high-value-streaks"
    return self._make_request(endpoint)

def get_team_streaks_events(self):
    """fetches list of top 20 team streaks events"""
    logger.info(f"Fetching top 20 team streaks events list")
    endpoint = f"/odds/top-team-streaks/wins/all"
    return self._make_request(endpoint)

def get_h2h_events(self):
    """fetches list of top 20 h2h events"""
    logger.info(f"Fetching top 20 h2h events list")
    endpoint = f"/odds/1/top-h2h/all"
    return self._make_request(endpoint)

def get_winning_odds_events(self):
    """fetches list of top 20 winning odds events"""
    logger.info(f"Fetching top 20 winning odds events list")
    endpoint = f"/odds/1/winning/all"
    return self._make_request(endpoint)

def get_team_ids_from_team_streaks(self, response: Dict) -> List[int]:
    """extracts team ids from team streaks response"""
    team_ids = []
    for item in response['topTeamStreaks']:
        if 'team' in item and 'id' in item['team']:
            team_ids.append(item['team']['id'])
    return team_ids

def get_nearest_event_for_team(self, team_id: int) -> Optional[int]:
    """Fetch the nearest event ID for a team based on startTimestamp."""
    endpoint = f"/team/{team_id}/events/next/0"
    response = self._make_request(endpoint)

    events = response.get('events', [])
    if not events:
        return None  # No events found

    # Use timezone_utils for consistent timezone handling
    
    now_aware = get_local_now_aware()
    now_ts = now_aware.timestamp()

    # Filter out events that already occurred and sort by time difference
    future_events = [e for e in events if e.get('startTimestamp', 0) >= now_ts]

    if not future_events:
        # fallback: return the latest past event
        events.sort(key=lambda e: abs(e.get('startTimestamp', 0) - now_ts))
        return events[0].get('id')

    # Sort future events by how soon they'll start
    nearest_event = min(future_events, key=lambda e: e.get('startTimestamp', float('inf')))
    return nearest_event.get('id')

def get_event_details(self, event_id: int) -> Optional[Dict]:
    """Fetch full event details from the /event/{id} endpoint."""
    endpoint = f"/event/{event_id}"
    response = self._make_request(endpoint)
    
    if not response or 'event' not in response:
        return None
    return response['event']

def get_team_last_10_results_response(self, team_id: int) -> Optional[Dict]:
    """Fetch the last 10 results for a team from the /team/{id}/events/last/0 endpoint."""
    endpoint = f"/team/{team_id}/events/last/0"
    response = self._make_request(endpoint)
    if not response or 'events' not in response:
        logger.error(f"No results found for team {team_id}")
        return None
    return response

def get_winning_odds_response(self, event_id: int) -> Optional[Dict]:
    """Fetch the winning odds for an event from the /event/{event_id}/provider/1/winning-odds endpoint."""
    endpoint = f"/event/{event_id}/provider/1/winning-odds"
    logger.debug(f"Making API request to: {endpoint}")
    # Use no_retry_on_404=True because 404s are common for winning odds (not all events have this data)
    response = self._make_request(endpoint, no_retry_on_404=True)
    logger.debug(f"API response type: {type(response)}, content: {response}")
    if not response:
        logger.debug(f"No winning odds found for event {event_id}")
        return None
    return response

# Event extraction methods
def extract_events_from_high_value_streaks(self, response: Dict) -> List[Dict]:
    """
    Extract events from high value streaks response which has a different structure.
    
    This function extracts the nested events and returns them as a flat list.
    The returned list can then be wrapped in a response dict with "events" key
    to be processed by extract_events_and_odds_from_dropping_response.
    
    Args:
        response: The API response dictionary containing general and head2head arrays
        
    Returns:
        List[Dict]: list of event dictionaries
    """
    events = []
    events_high_value_streaks_h2h = []
    try:
        # Extract from 'general' array
        if 'general' in response:
            for item in response['general']:
                if 'event' in item:
                    events.append(item['event'])
                    logger.debug(f"Extracted event from general: {item['event'].get('id')} - {item['event'].get('slug')}")
        
        # Extract from 'head2head' array
        if 'head2head' in response:
            for item in response['head2head']:
                if 'event' in item:
                    events_high_value_streaks_h2h.append(item['event'])
                    logger.debug(f"Extracted event from head2head: {item['event'].get('id')} - {item['event'].get('slug')}")
        
        logger.info(f"✅ Extracted {len(events)} events from high value streaks response (general + head2head)")
        return events, events_high_value_streaks_h2h
        
    except Exception as e:
        logger.error(f"❌ Error extracting events from high value streaks response: {e}")
        return []


def get_h2h_events_for_event(self, custom_id: str) -> Optional[Dict]:
    """
    Fetch H2H events for a specific custom_id using /event/{custom_id}/h2h/events endpoint.
    
    This endpoint returns all historical and future matches between the two teams
    associated with the custom_id.
    
    Args:
        custom_id: Custom ID of the event (e.g., 'ccKcsmcKc')
        
    Returns:
        Dict containing 'events' list with all H2H matches, or None if error
    """
    logger.info(f"Fetching H2H events for custom_id: {custom_id}")
    endpoint = f"/event/{custom_id}/h2h/events"
    return self._make_request(endpoint)


# --- Dynamically attach these methods to the existing class ---
SofaScoreAPI.get_high_value_streaks_events = get_high_value_streaks_events
SofaScoreAPI.get_team_streaks_events = get_team_streaks_events
SofaScoreAPI.get_h2h_events = get_h2h_events
SofaScoreAPI.get_winning_odds_events = get_winning_odds_events
SofaScoreAPI.get_team_ids_from_team_streaks = get_team_ids_from_team_streaks
SofaScoreAPI.get_nearest_event_for_team = get_nearest_event_for_team
SofaScoreAPI.get_event_details = get_event_details
SofaScoreAPI.extract_events_from_high_value_streaks = extract_events_from_high_value_streaks
SofaScoreAPI.get_h2h_events_for_event = get_h2h_events_for_event
SofaScoreAPI.get_team_last_10_results_response = get_team_last_10_results_response
SofaScoreAPI.get_winning_odds_response = get_winning_odds_response

logger.info("✅ sofascore_api2 methods successfully loaded and attached to SofaScoreAPI")
