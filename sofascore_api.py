from curl_cffi import requests
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from config import Config
from odds_utils import fractional_to_decimal

logger = logging.getLogger(__name__)

class SofaScoreAPI:
    def __init__(self):
        self.base_url = Config.SOFASCORE_BASE_URL
        # Use curl-cffi with Chrome impersonation - THIS IS THE KEY CHANGE!
        self.session = requests.Session(impersonate="chrome120")
        self.last_request_time = 0
        
        # Add proxy configuration
        self.proxy_enabled = Config.PROXY_ENABLED
        self.proxy_username = Config.PROXY_USERNAME
        self.proxy_password = Config.PROXY_PASSWORD
        self.proxy_endpoint = Config.PROXY_ENDPOINT
        
        # Initialize session with proxy support
        self._setup_session()
    
    def _setup_session(self):
        """Setup session with proxy configuration"""
        if self.proxy_enabled and self.proxy_username and self.proxy_password:
            # Format: username:password@proxy_endpoint
            proxy_url = f"http://{self.proxy_username}:{self.proxy_password}@{self.proxy_endpoint}"
            
            self.session = requests.Session(impersonate="chrome120")
            self.session.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            
            logging.info(f"Proxy enabled: {self.proxy_endpoint}")
        else:
            self.session = requests.Session(impersonate="chrome120")
            logging.info("Proxy disabled - using direct connection")
    
    def _rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = Config.REQUEST_DELAY_SECONDS
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
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
                    logger.info(f"API request successful: {endpoint}")
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
                
                elif response.status_code in [404, 500, 502, 503, 504, 522, 525]:
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
    
    def get_event_final_odds(self, id: int, slug: str) -> Optional[Dict]:
        """Get final odds for a specific event using the dedicated endpoint"""
        logger.info(f"Fetching final odds for event {slug} using dedicated endpoint")
        return self._make_request(f"/event/{id}/odds/1/all")
    
    def get_event_results(self, event_id: int) -> Optional[Dict]:
        """
        Fetch event results from /event/{id} endpoint.
        Returns structured result data ready for database upsert.
        """
        try:
            endpoint = f"/event/{event_id}"
            logger.info(f"Fetching event results for event {event_id}")
            
            response = self._make_request(endpoint)
            if not response:
                logger.warning(f"No response received for event {event_id}")
                return None
            
            return self.extract_results_from_response(response)
            
        except Exception as e:
            logger.error(f"Error fetching event results for {event_id}: {e}")
            return None
    
    def extract_results_from_response(self, response: Dict) -> Optional[Dict]:
        """
        Extract results data from event API response.
        Works with any sport (football, tennis, etc.) by analyzing the response structure.
        """
        try:
            if not response or 'event' not in response:
                logger.warning("No event data found in results response")
                return None
            
            event_data = response['event']
            
            # Check if event is finished
            status = event_data.get('status', {})
            status_code = status.get('code')
            status_type = status.get('type', '').lower()
            
            # Only process finished events
            if status_code != 100 or status_type != 'finished':
                logger.info(f"Event not finished yet - status: {status.get('description', 'Unknown')}")
                return None
            
            # Extract scores
            home_score_data = event_data.get('homeScore', {})
            away_score_data = event_data.get('awayScore', {})
            
            if not home_score_data or not away_score_data:
                logger.warning("Score data not found in response")
                return None
            
            # Get final scores - use 'current' or 'display' as primary, 'normaltime' as fallback
            home_score = (
                home_score_data.get('current') or 
                home_score_data.get('display') or 
                home_score_data.get('normaltime')
            )
            away_score = (
                away_score_data.get('current') or 
                away_score_data.get('display') or 
                away_score_data.get('normaltime')
            )
            
            if home_score is None or away_score is None:
                logger.warning("Could not extract valid scores from response")
                return None
            
            # Determine winner
            winner = None
            winner_code = event_data.get('winnerCode')
            
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
            
            logger.info(f"✅ Results extracted: {home_score}-{away_score}, Winner: {winner}")
            return result_data
            
        except Exception as e:
            logger.error(f"Error extracting results from response: {e}")
            return None

    def extract_final_odds_from_response(self, response: Dict) -> Optional[Dict]:
        """
        Extract final/current odds data from any market structure automatically.
        This function is completely sport-agnostic and adapts to any market type
        based purely on the actual choices available.
        """
        try:
            if not response or 'markets' not in response:
                logger.warning("No markets found in final odds response")
                return None
            
            # Look for any market with choices
            for market in response['markets']:
                choices = market.get('choices', [])
                if not choices:
                    continue
                
                # Extract final odds for each choice dynamically
                odds_data = {}
                available_choices = []
                
                for choice in choices:
                    name = choice.get('name')
                    current_fractional = choice.get('fractionalValue')
                    
                    if not current_fractional:
                        logger.warning(f"Missing fractional value for choice {name}")
                        continue
                    
                    # Store the choice name and convert odds
                    available_choices.append(name)
                    odds_data[f'{name}_final'] = fractional_to_decimal(current_fractional)
                
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
                            logger.info(f"✅ Final odds extracted (3-choice market): 1={result['one_final']}, X={result['x_final']}, 2={result['two_final']}")
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
                            logger.info(f"✅ Final odds extracted (2-choice market): 1={result['one_final']}, X=None, 2={result['two_final']}")
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
                            logger.info(f"✅ Final odds extracted (multi-choice market): 1={result['one_final']}, X=None, 2={result['two_final']}")
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
    
    def extract_events_and_odds_from_dropping_response(self, response: Dict) -> Tuple[List[Dict], Dict]:
        """Extract both events and their odds data from dropping odds response"""
        events = []
        odds_map = {}
        
        try:
            if not response or 'events' not in response:
                logger.warning("No events found in dropping odds response")
                return events, odds_map
            
            # Extract events
            for event in response['events']:
                try:
                    event_data = {
                        'id': event.get('id'),
                        'customId': event.get('customId'),
                        'slug': event.get('slug'),
                        'startTimestamp': event.get('startTimestamp'),
                        'sport': event.get('tournament', {}).get('category', {}).get('sport', {}).get('name'),
                        'competition': f"{event.get('tournament', {}).get('category', {}).get('name')}, {event.get('tournament', {}).get('name')}",
                        'country': event.get('tournament', {}).get('category', {}).get('country', {}).get('name'),
                        'homeTeam': event.get('homeTeam', {}).get('name'),
                        'awayTeam': event.get('awayTeam', {}).get('name'),
                    }
                    
                    required_fields = ['id', 'slug', 'startTimestamp', 'sport', 'competition', 'homeTeam', 'awayTeam']
                    if all(event_data.get(field) for field in required_fields):
                        events.append(event_data)
                    else:
                        logger.warning(f"Event {event.get('id')} missing required fields")
                        
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    continue
            
            # Extract odds map
            if 'oddsMap' in response:
                odds_map = response['oddsMap']
                logger.info(f" Extracted {len(odds_map)} odds entries from response")
            else:
                logger.warning("No oddsMap found in response")
            
            logger.info(f" Extracted {len(events)} valid events from dropping odds")
            return events, odds_map
            
        except Exception as e:
            logger.error(f"Error extracting events and odds: {e}")
            return events, odds_map
    
    def convert_timestamp_to_datetime(self, timestamp: int) -> datetime:
        """Convert Unix timestamp to datetime object"""
        return datetime.fromtimestamp(timestamp)
    
    def is_event_starting_soon(self, start_timestamp: int, window_minutes: int = 30) -> bool:
        """Check if an event is starting within the specified window"""
        now = datetime.now()
        event_time = self.convert_timestamp_to_datetime(start_timestamp)
        window_start = event_time.replace(minute=event_time.minute - window_minutes)
        
        return window_start <= now <= event_time

# Global API client instance
api_client = SofaScoreAPI()

