from curl_cffi import requests
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from config import Config
from odds_utils import fractional_to_decimal
from sport_observations import sport_observations_manager
from alert_system import pre_start_notifier
from repository import EventRepository
from sport_classifier import sport_classifier

logger = logging.getLogger(__name__)

class SofaScoreAPI:
    def __init__(self):
        self.base_url = Config.SOFASCORE_BASE_URL
        # Use curl-cffi with Chrome impersonation - THIS IS THE KEY CHANGE!
        self.session = requests.Session(impersonate="chrome120")
        self.last_request_time = 0
        self._rate_limit_lock = threading.Lock()  # Thread-safe rate limiting
        
        # Add proxy configuration
        self.proxy_enabled = Config.PROXY_ENABLED
        self.proxy_username = Config.PROXY_USERNAME
        self.proxy_password = Config.PROXY_PASSWORD
        self.proxy_endpoint = Config.PROXY_ENDPOINT
        
        # Initialize session with proxy support
        self._setup_session()
    
    def _setup_session(self):
        """Setup session with proxy configuration"""
        self.session = requests.Session(impersonate="chrome120")
        
        if self.proxy_enabled and self.proxy_username and self.proxy_password:
            # Format: username:password@proxy_endpoint
            proxy_url = f"http://{self.proxy_username}:{self.proxy_password}@{self.proxy_endpoint}"
            self.session.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            logger.info(f"Proxy enabled: {self.proxy_endpoint}")
        else:
            logger.info("Proxy disabled - using direct connection")
            
    def get_gender(self, home_team: Dict, away_team: Dict) -> str:
        """Get the gender of the event from the home and away team"""
        gender_home_team = home_team.get('gender', 'unknown')
        gender_away_team = away_team.get('gender', 'unknown')

        # Case 1: both are known and equal
        if gender_home_team == gender_away_team and gender_home_team != 'unknown':
            return gender_home_team

        # Case 2: one is unknown, return the known one
        if gender_home_team == 'unknown' and gender_away_team != 'unknown':
            return gender_away_team
        if gender_away_team == 'unknown' and gender_home_team != 'unknown':
            return gender_home_team

        # Case 3: both unknown
        if gender_home_team == 'unknown' and gender_away_team == 'unknown':
            return 'unknown'

        # Case 4: both known but different
        logger.info(f"🔍 DEBUG: Gender mismatch detected: {gender_home_team} != {gender_away_team}")
        return "mixed"

    def get_event_information(self, event: Dict, discovery_source: str = 'dropping_odds') -> Dict:
        """
        Extract and structure event information from a single event object.
        
        Args:
            event: The event object from the API response
            discovery_source: The source that discovered this event (default: 'dropping_odds')
            
        Returns:
            Dict containing structured event data with keys: id, customId, slug, startTimestamp,
            sport, competition, country, homeTeam, awayTeam, gender, discovery_source
        """
        try:
            # Extract basic event information
            original_sport = event.get('tournament', {}).get('category', {}).get('sport', {}).get('name')
            home_team = event.get('homeTeam', {}).get('name')
            away_team = event.get('awayTeam', {}).get('name')
            
            # Get gender information
            gender = self.get_gender(event.get('homeTeam', {}), event.get('awayTeam', {}))
            
            # Classify sport using the sport classifier
            classified_sport = sport_classifier.classify_sport(
                sport=original_sport,
                home_team=home_team,
                away_team=away_team
            )
            
            # Build structured event data
            event_data = {
                'id': event.get('id'),
                'customId': event.get('customId'),
                'slug': event.get('slug'),
                'startTimestamp': event.get('startTimestamp'),
                'sport': classified_sport,
                'competition': f"{event.get('tournament', {}).get('category', {}).get('name')}, {event.get('tournament', {}).get('name')}, {event.get('tournament', {}).get('uniqueTournament', {}).get('name')}",
                'country': event.get('venue', {}).get('country', {}).get('name') or event.get('tournament', {}).get('category', {}).get('country', {}).get('name'),
                'homeTeam': home_team,
                'awayTeam': away_team,
                'gender': gender,
                'discovery_source': discovery_source,
            }
            
            # Log sport classification if it changed
            if classified_sport != original_sport:
                logger.info(f"🎾 Sport classified: '{original_sport}' → '{classified_sport}' for {home_team} vs {away_team}")
            
            return event_data
            
        except Exception as e:
            logger.error(f"Error extracting event information: {e}")
            return {}


    def _rate_limit(self):
        """Implement thread-safe rate limiting between requests"""
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            min_interval = Config.REQUEST_DELAY_SECONDS
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, no_retry_on_404: bool = False) -> Optional[Dict]:
        """Make an HTTP request with enhanced browser impersonation and proxy support"""
        url = f"{self.base_url}{endpoint}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                self._rate_limit()
                
                logger.debug(f"Making enhanced request to: {url}")
                response = self.session.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    return data
                
                # Handle specific error codes with appropriate retry strategies
                elif response.status_code == 407:
                    # Proxy authentication error - retry with exponential backoff
                    wait_time = min(30 * (2 ** attempt), 300)  # 30s, 60s, 120s, 240s, max 300s
                    logger.warning(f"Proxy authentication error (407) for {endpoint}, waiting {wait_time}s, attempt {attempt + 1}/{Config.MAX_RETRIES}")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Proxy authentication failed after {Config.MAX_RETRIES} attempts for {endpoint}")
                        break
                
                elif response.status_code == 429:
                    # Rate limiting - longer wait with exponential backoff
                    wait_time = min(60 * (2 ** attempt), 600)  # 60s, 120s, 240s, 480s, max 600s
                    logger.warning(f"Rate limited (429) for {endpoint}, waiting {wait_time}s, attempt {attempt + 1}/{Config.MAX_RETRIES}")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {Config.MAX_RETRIES} attempts for {endpoint}")
                        break
                
                elif response.status_code == 404:
                    # 404 errors - handle based on no_retry_on_404 flag
                    if no_retry_on_404:
                        logger.debug(f"HTTP 404 for {endpoint} - no retry requested")
                        return None
                    else:
                        # Standard 404 retry logic
                        wait_time = min(5 * (2 ** attempt), 60)  # 5s, 10s, 20s, 40s, max 60s
                        logger.warning(f"HTTP 404 for {endpoint}, waiting {wait_time}s, attempt {attempt + 1}/{Config.MAX_RETRIES}")
                        if attempt < Config.MAX_RETRIES - 1:
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"HTTP 404 failed after {Config.MAX_RETRIES} attempts for {endpoint}")
                            break
                
                elif response.status_code in [500, 502, 503, 504, 522, 525]:
                    # Server errors - retry with exponential backoff
                    wait_time = min(5 * (2 ** attempt), 60)  # 5s, 10s, 20s, 40s, max 60s
                    logger.warning(f"HTTP {response.status_code} for {endpoint}, waiting {wait_time}s, attempt {attempt + 1}/{Config.MAX_RETRIES}")
                    if attempt < Config.MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"HTTP {response.status_code} failed after {Config.MAX_RETRIES} attempts for {endpoint}")
                        break
                
                else:
                    # Other HTTP errors - log and break (no retry for client errors like 400, 401, 403)
                    logger.error(f"HTTP {response.status_code} for {endpoint}: {response.text}")
                    if response.status_code >= 400 and response.status_code < 500:
                        logger.info(f"Client error {response.status_code} - not retrying")
                    break
                    
            except requests.exceptions.RequestException as e:
                # Network/proxy connection errors - retry with exponential backoff
                wait_time = min(10 * (2 ** attempt), 120)  # 10s, 20s, 40s, 80s, max 120s
                logger.error(f"Request error for {endpoint} (attempt {attempt + 1}/{Config.MAX_RETRIES}): {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Request failed after {Config.MAX_RETRIES} attempts for {endpoint}")
                    break
            except Exception as e:
                # Unexpected errors - log and break
                logger.error(f"Unexpected error for {endpoint}: {e}")
                break
        
        return None
    
    def get_dropping_odds_with_odds(self) -> Optional[Dict]:
        """Get dropping odds events AND their odds data in a single call with enhanced browser impersonation"""
        logger.info("Fetching dropping odds events with odds data using browser impersonation")
        result = self._make_request("/odds/1/dropping/all")

        return result
    
    def get_event_final_odds(self, id: int, slug: str=None, no_retry_on_404: bool = False) -> Optional[Dict]:
        """Get final odds for a specific event using the dedicated endpoint"""
        if not slug:
            return self._make_request(f"/event/{id}/odds/1/all", no_retry_on_404=no_retry_on_404)
        else:
            logger.info(f"Fetching final odds for event {slug} using dedicated endpoint")
            return self._make_request(f"/event/{id}/odds/1/all", no_retry_on_404=no_retry_on_404)
    
    def update_event_information_from_response(self, response: Dict) -> bool:
        """
        Extract and update event information from /event/{id} API response.
        Uses the refactored get_event_information function to ensure consistency.
        This is called during midnight sync to update event info using existing API calls.
        
        Note: discovery_source is NOT updated - it preserves the original discovery source.
        
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            if not response or 'event' not in response:
                logger.warning("No event data in response for information update")
                return False
            
            event_response = response['event']
            
            # Extract event information using our refactored function
            # Note: The response structure from /event/{id} is the same as dropping odds
            event_data = self.get_event_information(event_response, discovery_source='results_sync')
            
            if not event_data or not event_data.get('id'):
                logger.warning("Could not extract event information from response")
                return False
            
            # Remove discovery_source from event_data to preserve the original discovery source
            # The discovery_source should only be set during initial discovery, not during updates
            event_data.pop('discovery_source', None)
            
            # Update event in database (without changing discovery_source)
            updated_event = EventRepository.upsert_event(event_data)
            if updated_event:
                logger.info(f"✅ Event information updated for event {event_data['id']} from results sync")
                return True
            else:
                logger.warning(f"Failed to update event information for event {event_data.get('id')}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating event information from response: {e}")
            return False

    def get_event_results(self, event_id: int, update_time: bool = False, update_court_type: bool = False, minutes_until_start: int = 0) -> Optional[Dict]:
        """ 
        Fetch event results from /event/{id} endpoint.
        Returns structured result data ready for database upsert.
        """
        try:
            endpoint = f"/event/{event_id}"
            if update_court_type:
                logger.info(f"Fetching /event/{event_id} endpoint to update court type")
            if update_time:
                logger.info(f"Fetching /event/{event_id} endpoint to update time")
            else:
                logger.info(f"Fetching event results for event {event_id}")
            
            response = self._make_request(endpoint)
            if not response:
                logger.warning(f"No response received for event {event_id}")
                return None
            
            # Update event information from response (midnight sync optimization)
            # This ensures we always have complete event data from the authoritative /event/{id} endpoint
            if not update_time and not update_court_type:
                # Only update during normal results collection (not during time checks or court type extraction)
                self.update_event_information_from_response(response)
            
            if update_court_type:
                # Extraer rankings de forma segura
                event_data = response.get('event', {})
                home_team_ranking = event_data.get('homeTeam', {}).get('ranking')
                away_team_ranking = event_data.get('awayTeam', {}).get('ranking')
                
                return [
                            {
                                "type": "ground_type",
                                "value": sport_observations_manager.extract_tennis_ground_type(event_id, response)
                            },
                            {
                                "type": "rankings",
                                "home_ranking": home_team_ranking,
                                "away_ranking": away_team_ranking
                            }
                        ]
       
            if update_time:
                event_data = response.get('event', {})
                # SofaScore uses 'startTimestamp' (camelCase), not 'startTimeStamp'
                start_timestamp = event_data.get('startTimestamp')
                if start_timestamp is None:
                    logger.warning(f"No startTimestamp found in API response for event {event_id}")
                    logger.debug(f"Available event fields: {list(event_data.keys())}")
                    return None  # Return None to indicate API error, not time change
                
                # Check if this is for a recently started event (negative minutes) - send alert
                if minutes_until_start < 0:
                    return self.check_and_update_starting_time(event_id, start_timestamp, send_alert=True)
                else:
                    # Regular timestamp check for upcoming events
                    return self.check_and_update_starting_time(event_id, start_timestamp)
            return self.extract_results_from_response(response)
            
        except Exception as e:
            logger.error(f"Error fetching event results for {event_id}: {e}")
            return None
    

    def extract_results_from_response(self, response: Dict) -> Optional[Dict]:
        """
        Extract results data from event API response.
        Works with any sport (football, tennis, etc.) by analyzing the response structure.
        IMPROVED: Handles all finished event status codes, not just status_code=100.
        """
        try:
            if not response or 'event' not in response:
                logger.warning("No event data found in results response")
                return None
            
            event_data = response['event']
            
            # Check if event is finished - IMPROVED LOGIC
            status = event_data.get('status', {})
            status_code = status.get('code')
            status_type = status.get('type', '').lower()
            status_description = status.get('description', '')
            
            # Define finished status codes (expanded list)
            FINISHED_STATUS_CODES = {
                100,  # Ended (normal finish)
                110,  # AET (After Extra Time)
                92,   # Retired (Tennis)
                120,  # AP (After Penalties)
                130,  # WO (Walkover)
                140,  # ABD (Abandoned but with result)
            }
            
            # Define canceled/postponed status codes (should be skipped)
            CANCELED_STATUS_CODES = {
                70,   # Canceled
                80,   # Postponed
                90,   # Suspended
            }
            
            # Check if event is finished
            if status_code in CANCELED_STATUS_CODES:
                logger.info(f"Event canceled/postponed - status: {status_description}")
                return None
            
            if status_code not in FINISHED_STATUS_CODES or status_type != 'finished':
                logger.info(f"Event not finished yet - status: {status_description}")
                return None
            
            # Extract scores
            home_score_data = event_data.get('homeScore', {})
            away_score_data = event_data.get('awayScore', {})
            
            if not home_score_data or not away_score_data:
                logger.warning("Score data not found in response")
                return None
            
            # IMPROVED: Better score extraction logic with penalty handling
            # For penalty shootouts (status code 120), use normaltime instead of current
            # to get only regular time results, not including penalty scores
            
            if status_code == 120:  # AP (After Penalties)
                logger.info(f"Penalty shootout detected (status 120) - using normaltime scores instead of current")
                home_score = home_score_data.get('normaltime')
                away_score = away_score_data.get('normaltime')
            else:
                # Try multiple score fields in order of preference for non-penalty games
                # Use 'is not None' to handle 0 scores correctly
                home_score = (
                    home_score_data.get('current') if home_score_data.get('current') is not None else
                    home_score_data.get('display') if home_score_data.get('display') is not None else
                    home_score_data.get('normaltime') if home_score_data.get('normaltime') is not None else
                    home_score_data.get('overtime') if home_score_data.get('overtime') is not None else
                    home_score_data.get('penalties') if home_score_data.get('penalties') is not None else
                    None
                )
                
                away_score = (
                    away_score_data.get('current') if away_score_data.get('current') is not None else
                    away_score_data.get('display') if away_score_data.get('display') is not None else
                    away_score_data.get('normaltime') if away_score_data.get('normaltime') is not None else
                    away_score_data.get('overtime') if away_score_data.get('overtime') is not None else
                    away_score_data.get('penalties') if away_score_data.get('penalties') is not None else
                    None
                )
            
            # For tennis, try 'point' field if scores are None
            if home_score is None and 'point' in home_score_data:
                try:
                    home_score = int(home_score_data['point'])
                except (ValueError, TypeError):
                    pass
            
            if away_score is None and 'point' in away_score_data:
                try:
                    away_score = int(away_score_data['point'])
                except (ValueError, TypeError):
                    pass
            
            # If still no scores, check if it's a valid 0-0 result
            if home_score is None and away_score is None:
                # Check if both teams have score data but with 0 values
                if (home_score_data.get('current') == 0 and away_score_data.get('current') == 0):
                    home_score = 0
                    away_score = 0
                else:
                    logger.warning("Could not extract valid scores from response")
                    return None
            
            # Final validation - ensure we have valid scores
            if home_score is None or away_score is None:
                logger.warning("Could not extract valid scores from response")
                return None
            
            # Determine winner
            winner = None
            winner_code = event_data.get('winnerCode')
            
            if status_code == 120:  # AP (After Penalties)
                # For penalty shootouts, determine winner based on regular time scores only
                if home_score == away_score:
                    winner = 'X'  # Draw in regular time (penalties don't count for betting analysis)
                    logger.info(f"Penalty shootout result: Regular time was {home_score}-{away_score} (draw)")
                elif home_score > away_score:
                    winner = '1'  # Home team wins in regular time
                    logger.info(f"Penalty shootout result: Regular time was {home_score}-{away_score} (home wins)")
                else:
                    winner = '2'  # Away team wins in regular time
                    logger.info(f"Penalty shootout result: Regular time was {home_score}-{away_score} (away wins)")
            else:
                # For non-penalty games, use standard logic
                if winner_code == 1:
                    winner = '1'  # Home team wins
                elif winner_code == 2:
                    winner = '2'  # Away team wins
                elif winner_code == 3:
                    winner = 'X'  # Draw (for sports that support draws)
                elif home_score == away_score:
                    winner = 'X'  # Draw based on equal scores
                elif home_score > away_score:
                    winner = '1'  # Home team wins
                else:
                    winner = '2'  # Away team wins
            
            # Extract end time
            ended_at = None
            changes = event_data.get('changes', {})
            if changes and 'changeTimestamp' in changes:
                ended_at = datetime.fromtimestamp(changes['changeTimestamp'])
            
            result_data = {
                'home_score': int(home_score),
                'away_score': int(away_score),
                'winner': winner,
                'ended_at': ended_at
            }
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error extracting results from response: {e}")
            return None
    
    def _extract_observations_from_response(self, response: Dict) -> Optional[List[Dict]]:
        """
        Extract sport-specific observations from event response.
        COMPLETELY FAIL-SAFE: Returns None on any error, doesn't break main processing.
        """
        try:
            if not response or 'event' not in response:
                return None
            
            event_data = response['event']
            observations = []
            
            # Extract sport information for context
            sport = None
            if 'tournament' in event_data:
                tournament = event_data['tournament']
                if 'category' in tournament and 'sport' in tournament['category']:
                    sport = tournament['category']['sport'].get('name')
            
            if not sport:
                logger.debug("No sport information found, skipping observations")
                return None
            
            # TENNIS: Extract ground type
            if sport.lower() == 'tennis' or sport.lower() == 'tennis doubles':
                
                ground_type = event_data.get('groundType')
                
                if ground_type:
                    observations.append({
                        'type': 'ground_type',
                        'value': ground_type,
                        'sport': sport
                    })
                    logger.info(f"📍 Tennis ground type extracted: {ground_type}")
                else:
                    logger.info(f"🔍 DEBUG: No groundType found in event data")
            
            # FUTURE: Add other sports observations here
            # if sport.lower() == 'football':
            #     weather = event_data.get('weather')
            #     if weather:
            #         observations.append({'type': 'weather', 'value': weather, 'sport': sport})
            
            if observations:
                logger.info(f"✅ Extracted {len(observations)} observations for {sport}")
                return observations
            else:
                logger.debug(f"No observations extracted for {sport}")
                return None
                
        except Exception as e:
            logger.warning(f"Error extracting observations (FAIL-SAFE): {e}")
            # FAIL-SAFE: Return None, don't break main processing
            return None

    def extract_final_odds_from_response(self, response: Dict, initial_odds_extraction: bool = False) -> Optional[Dict]:
        """
        Extract final/current odds data from full time market structure automatically.
        This function is completely sport-agnostic and captures full time market structure.
        based purely on the actual choices available.
        
        Args:
            response: The API response containing market data
            initial_odds_extraction: If True, also extract initial odds alongside final odds
        """
        try:
            if not response or 'markets' not in response:
                logger.warning("No markets found in final odds response")
                return None
            
            # Look for any market with choices
            for market in response['markets']:
                if market.get('isLive') == False and market.get('marketName') == 'Full time':
                    choices = market.get('choices', [])
                    if not choices:
                        continue
                    
                    # Extract final odds for each choice dynamically
                    odds_data = {}
                    available_choices = []
                    
                    # Only create initial_odds_data if we need it
                    initial_odds_data = {} if initial_odds_extraction else None
                    
                    for choice in choices:
                        name = choice.get('name')
                        current_fractional = choice.get('fractionalValue')
                        initial_fractional = choice.get('initialFractionalValue')
                        
                        if not current_fractional:
                            logger.warning(f"Missing fractional value for choice {name}")
                            continue
                        
                        # Store the choice name and convert odds
                        available_choices.append(name)
                        odds_data[f'{name}_final'] = fractional_to_decimal(current_fractional)
                        
                        # Extract initial odds if requested and available
                        if initial_odds_extraction and initial_fractional:
                            initial_odds_data[f'{name}_initial'] = fractional_to_decimal(initial_fractional)
                        elif initial_odds_extraction and not initial_fractional:
                            logger.warning(f"Missing initial fractional value for choice {name}")
                    
                    # Analyze market structure automatically based on available choices
                    if len(available_choices) >= 2:
                        # Determine market type based on actual choices, not hardcoded sport names
                        if len(available_choices) == 3:
                            # 3-choice market - check if any choice could be a draw equivalent
                            # Look for patterns that suggest a draw option (middle choice, or any non-extreme choice)
                            if len(odds_data) == 3:
                                # Extract the three choices in order
                                choice_names = list(odds_data.keys())
                                # Map to our standard format: first choice = one, middle choice = x, last choice = two
                                result = {
                                    'one_final': odds_data[choice_names[0]],
                                    'x_final': odds_data[choice_names[1]],  # Middle choice (draw equivalent)
                                    'two_final': odds_data[choice_names[2]]
                                }
                                
                                # Add initial odds if requested
                                if initial_odds_extraction and initial_odds_data and len(initial_odds_data) == 3:
                                    initial_choice_names = list(initial_odds_data.keys())
                                    result.update({
                                        'one_initial': initial_odds_data[initial_choice_names[0]],
                                        'x_initial': initial_odds_data[initial_choice_names[1]],
                                        'two_initial': initial_odds_data[initial_choice_names[2]]
                                    })
                                    logger.info(f"✅ Odds extracted (3-choice market)")
                                else:
                                    logger.info(f"✅ Final odds extracted (3-choice market)")
                                
                                return result
                            else:
                                logger.warning(f"Incomplete 3-choice market data: {odds_data}")
                                continue
                                
                        elif len(available_choices) == 2:
                            # 2-choice market - any sport without draw option
                            if len(odds_data) == 2:
                                # Map to our standard format, setting X to None
                                choice_names = list(odds_data.keys())
                                result = {
                                    'one_final': odds_data[choice_names[0]],
                                    'x_final': None,  # No draw option in 2-choice markets
                                    'two_final': odds_data[choice_names[1]]
                                }
                                
                                # Add initial odds if requested
                                if initial_odds_extraction and initial_odds_data and len(initial_odds_data) == 2:
                                    initial_choice_names = list(initial_odds_data.keys())
                                    result.update({
                                        'one_initial': initial_odds_data[initial_choice_names[0]],
                                        'x_initial': None,  # No draw option in 2-choice markets
                                        'two_initial': initial_odds_data[initial_choice_names[1]]
                                    })
                                    logger.info(f"✅ Odds extracted (2-choice market)")
                                else:
                                    logger.info(f"✅ Final odds extracted (2-choice market)")
                                
                                return result
                            else:
                                logger.warning(f"Incomplete 2-choice market data: {odds_data}")
                                continue
                                
                        elif len(available_choices) > 3:
                            # Multi-choice market - extract first two choices as main competitors
                            if len(odds_data) >= 2:
                                choice_names = list(odds_data.keys())
                                result = {
                                    'one_final': odds_data[choice_names[0]],
                                    'x_final': None,  # Multi-choice markets typically don't have draws
                                    'two_final': odds_data[choice_names[1]]
                                }
                                
                                # Add initial odds if requested
                                if initial_odds_extraction and initial_odds_data and len(initial_odds_data) >= 2:
                                    initial_choice_names = list(initial_odds_data.keys())
                                    result.update({
                                        'one_initial': initial_odds_data[initial_choice_names[0]],
                                        'x_initial': None,  # Multi-choice markets typically don't have draws
                                        'two_initial': initial_odds_data[initial_choice_names[1]]
                                    })
                                    logger.info(f"✅ Odds extracted (multi-choice market)")
                                else:
                                    logger.info(f"✅ Final odds extracted (multi-choice market)")
                                
                                return result
                            else:
                                logger.warning(f"Multi-choice market without extractable first two choices: {odds_data}")
                                continue
                        else:
                            logger.warning(f"Market with insufficient choices: {available_choices}")
                            continue
                    else:
                        logger.warning(f"Market with insufficient choices: {available_choices}")
                        continue
            
            logger.warning("No suitable market found for final odds extraction")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting final odds: {e}")
            return None
    
    def extract_events_and_odds_from_dropping_response(self, response: Dict, odds_extraction: bool = True, discovery_source: str = 'dropping_odds') -> Tuple[List[Dict], Dict]:
        """Extract both events and their odds data from dropping odds response with sport classification"""
        events: List[Dict] = []
        odds_map: Dict = {}

        try:
            if not response or 'events' not in response:
                logger.warning("No events found in dropping odds response")
                return events, odds_map

            for event in response['events']:
                try:
                    # Use the new get_event_information function
                    event_data = self.get_event_information(event, discovery_source)

                    required_fields = ['id', 'slug', 'startTimestamp', 'sport', 'competition', 'homeTeam', 'awayTeam', 'gender']
                    if all(event_data.get(field) for field in required_fields):
                        events.append(event_data)
                    else:
                        logger.warning(f"Event {event.get('id')} missing required fields")

                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    continue

            if odds_extraction and 'oddsMap' in response:
                odds_map = response['oddsMap']
                logger.info(f"Extracted {len(odds_map)} odds entries from response")

            return events, odds_map

        except Exception as e:
            logger.error(f"Error extracting events and odds: {e}")
            return events, odds_map

    

    def check_and_update_starting_time(self, event_id: int, startTimeStamp: int, send_alert: bool = False) -> bool:
        """
        Compares the stored starting time with the new starting time that was passed in,
        if they are different, it updates the starting time of the event in the database
        """
        try:
            # Query the database for the starting_time_utc of the event and store it in current_starting_time
            event = EventRepository.get_event_by_id(event_id)
            if not event:
                logger.warning(f"Event {event_id} not found in database")
                return False
            
            current_starting_time = event.start_time_utc
            new_starting_time = self.convert_timestamp_to_datetime(startTimeStamp)
            
            # Compare the current_starting_time with the new starting time that was passed in startTimeStamp
            if current_starting_time == new_starting_time:
                logger.debug(f"Starting time unchanged for event {event_id}: {current_starting_time}")
                return True  # Same time, process should continue
            else:
                logger.info(f"Starting time changed for event {event_id}: {current_starting_time} -> {new_starting_time}")
                
                # If they are different, update the starting time of the event in the database
                if EventRepository.update_event_starting_time(event_id, new_starting_time):
                    logger.info(f"✅ Successfully updated starting time for event {event_id}")
                    if send_alert:
                        logger.info(f"🕐 Sending alert for event {event_id} - starting time changed")
                        pre_start_notifier.send_time_correction_message(event_id, current_starting_time, new_starting_time)
                        return False # Send alert and don't continue with odds extraction, time was updated
                    else:
                        logger.info(f"🕐 Starting time changed for event {event_id} - not sending alert")
                        return False # Time was updated, don't continue with odds extraction
                    return False  # Time was updated, don't continue with odds extraction
                else:
                    logger.error(f"Failed to update starting time for event {event_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error in check_and_update_starting_time for event {event_id}: {e}")
            return False

    def convert_timestamp_to_datetime(self, timestamp: int) -> datetime:
        """Convert Unix timestamp to datetime object"""
        return datetime.fromtimestamp(timestamp)
    
    def is_event_starting_soon(self, start_timestamp: int, window_minutes: int = 30) -> bool:
        """Check if an event is starting within the specified window"""
        now = datetime.now()
        event_time = self.convert_timestamp_to_datetime(start_timestamp)
        
        # Calculate time difference in minutes
        delta_min = (event_time - now).total_seconds() / 60
        
        # Return True if event is within the window (0 to window_minutes minutes from now)
        return 0 <= delta_min <= window_minutes

    

# Global API client instance
api_client = SofaScoreAPI()

