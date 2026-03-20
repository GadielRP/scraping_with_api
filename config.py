import os
import ast
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Helper to parse environment lists
def _parse_env_list(env_name, default_value):
    value = os.getenv(env_name)
    if not value:
        return default_value
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return parsed
        return [str(parsed)]
    except (ValueError, SyntaxError):
        return [item.strip() for item in value.split(',') if item.strip()]

# Discovery sources to allow for alert sending
DISCOVERY_SOURCES_FOR_ALERTS = _parse_env_list('DISCOVERY_SOURCES_FOR_ALERTS', ['dropping_odds'])

class Config:
    # Database
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///sofascore_odds.db')
    # Connection timeout in seconds for PostgreSQL (prevents long OS-level waits)
    DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', '5'))
    
    # API Configuration
    SOFASCORE_BASE_URL = 'https://api.sofascore.com/api/v1'
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    # Scheduler Configuration
    POLL_INTERVAL_MINUTES = int(os.getenv('POLL_INTERVAL_MINUTES', '5'))
    DISCOVERY_INTERVAL_HOURS = int(os.getenv('DISCOVERY_INTERVAL_HOURS', '6'))
    DISCOVERY2_INTERVAL_HOURS = int(os.getenv('DISCOVERY2_INTERVAL_HOURS', '6'))  # Separate interval for Discovery2
    PRE_START_WINDOW_MINUTES = int(os.getenv('PRE_START_WINDOW_MINUTES', '30'))

    # Daily Discovery Log/Queue Configuration
    DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES = int(os.getenv('DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES', '240'))
    DAILY_DISCOVERY_DAYS_TO_KEEP = int(os.getenv('DAILY_DISCOVERY_DAYS_TO_KEEP', '1'))
    
    # Discovery Schedule Times (dynamically generated based on DISCOVERY_INTERVAL_HOURS)
    # Runs at exact hours: 00:00, 06:00, 12:00, 18:00 (if interval is 6)
    @staticmethod
    def _generate_discovery_times():
        interval_hours = int(os.getenv('DISCOVERY_INTERVAL_HOURS', '6'))
        times = []
        for hour in range(0, 24, interval_hours):
            times.append(f"{hour:02d}:00")
        return times
    
    DISCOVERY_TIMES = _generate_discovery_times.__func__() if hasattr(_generate_discovery_times, "__func__") else _generate_discovery_times()
    
    # Discovery2 Schedule Times (runs at hh:02 to avoid blocking pre-start checks at hh:00)
    # Runs at 2 minutes past the hour: 00:02, 06:02, 12:02, 18:02 (if interval is 6)
    @staticmethod
    def _generate_discovery2_times():
        interval_hours = int(os.getenv('DISCOVERY2_INTERVAL_HOURS', '6'))
        times = []
        for hour in range(0, 24, interval_hours):
            times.append(f"{hour:02d}:02")  # Run at hh:02 instead of hh:00
        return times
    
    DISCOVERY2_TIMES = _generate_discovery2_times.__func__() if hasattr(_generate_discovery2_times, "__func__") else _generate_discovery2_times()
    
    # Timezone
    TIMEZONE = os.getenv('TIMEZONE', 'America/Mexico_City')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')  # Reset to INFO after debugging
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Rate limiting
    REQUEST_DELAY_SECONDS = float(os.getenv('REQUEST_DELAY_SECONDS', '0.5'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    
    # Notification Configuration
    NOTIFICATIONS_ENABLED = os.getenv('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'
    
    # Timestamp Correction Configuration
    ENABLE_TIMESTAMP_CORRECTION = os.getenv('ENABLE_TIMESTAMP_CORRECTION', 'true').lower() == 'true'
    
    # Odds Extraction Configuration (for testing)
    ENABLE_ODDS_EXTRACTION = os.getenv('ENABLE_ODDS_EXTRACTION', 'true').lower() == 'true'
    
    # Filter by tracked seasons only (OddsPortal leagues)
    TRACKED_SEASONS_ONLY = os.getenv('TRACKED_SEASONS_TOGGLE', 'true').lower() == 'true'

    # Filter alerts by OP Season ID
    FILTER_ALERTS_BY_OP_SEASON = os.getenv('FILTER_ALERTS_BY_OP_SEASON', 'false').lower() == 'true'
    
    # Telegram Settings
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    PERSONAL_CHAT_ID = os.getenv('PERSONAL_CHAT_ID', '')  # For debug messages
    
    # Proxy configuration
    PROXY_ENABLED = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
    PROXY_USERNAME = os.getenv('PROXY_USERNAME', '')
    PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', '')
    PROXY_ENDPOINT = os.getenv('PROXY_ENDPOINT', '')
    
    PROXY_ROTATION_INTERVAL = int(os.getenv('PROXY_ROTATION_INTERVAL', '5'))
    PROXY_MAX_RETRIES = int(os.getenv('PROXY_MAX_RETRIES', '3'))
    
    # OddsPortal resource blocking toggle (disable if it causes scraping instability)
    ODDSPORTAL_BLOCK_RESOURCES = os.getenv('ODDSPORTAL_BLOCK_RESOURCES', 'true').lower() == 'true'
    
    # OddsPortal parallel scraping (requires 2GB+ RAM)
    ODDSPORTAL_PARALLEL_BROWSERS = int(os.getenv('ODDSPORTAL_PARALLEL_BROWSERS', '1'))
    
    # OddsPortal Fast-Fail & Diagnostics
    ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS = int(os.getenv('ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS', '30000'))
    ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS = int(os.getenv('ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS', '15000'))
    ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS = int(os.getenv('ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS', '60000'))
    ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS = int(os.getenv('ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS', '8000'))
    ODDSPORTAL_TAB_WAIT_TIMEOUT = int(os.getenv('ODDSPORTAL_TAB_WAIT_TIMEOUT', '20'))
    ODDSPORTAL_SAVE_DEBUG_ON_GOTO_TIMEOUT = os.getenv('ODDSPORTAL_SAVE_DEBUG_ON_GOTO_TIMEOUT', 'true').lower() == 'true'
    ODDSPORTAL_ENABLE_SHELL_GRACE = os.getenv('ODDSPORTAL_ENABLE_SHELL_GRACE', 'true').lower() == 'true'
    
    # OddsPortal Context Lifecycle
    ODDSPORTAL_IGNORE_HTTPS_ERRORS = os.getenv('ODDSPORTAL_IGNORE_HTTPS_ERRORS', 'true').lower() == 'true'
    ODDSPORTAL_FRESH_CONTEXT_PER_EVENT = os.getenv('ODDSPORTAL_FRESH_CONTEXT_PER_EVENT', 'true').lower() == 'true'
    
    # Max seconds to wait for a previous OP cycle to finish before proceeding
    ODDSPORTAL_PREVIOUS_CYCLE_TIMEOUT = int(os.getenv('ODDSPORTAL_PREVIOUS_CYCLE_TIMEOUT', '120'))

    # Max seconds an odds alert thread will wait for OddsPortal scraping to finish for a specific event
    # before proceeding without the OddsPortal section.
    ODDSPORTAL_ALERT_WAIT_TIMEOUT = int(os.getenv('ODDSPORTAL_ALERT_WAIT_TIMEOUT', '180'))    
    # Smart Alert Filtering Configuration
    # Minimum number of past results required for at least one team to send streak alerts
    STREAK_ALERT_MIN_RESULTS = int(os.getenv('STREAK_ALERT_MIN_RESULTS', '15'))

    # Sports to exclude from alert evaluation (but not odds extraction)
    EXCLUDED_SPORTS = _parse_env_list('EXCLUDED_SPORTS', ['Table tennis', 'Darts'])

    # Discovery sources to allow for alert sending
    DISCOVERY_SOURCES_FOR_ALERTS = DISCOVERY_SOURCES_FOR_ALERTS
