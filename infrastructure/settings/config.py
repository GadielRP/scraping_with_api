import os
import ast
import logging
import re
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


def _parse_optional_env_list(env_name, default_value=None):
    value = os.getenv(env_name)
    if value is None:
        return default_value

    if not value.strip():
        return None

    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            cleaned = [str(item).strip() for item in parsed if str(item).strip()]
            return cleaned or None
        cleaned_value = str(parsed).strip()
        return [cleaned_value] if cleaned_value else None
    except (ValueError, SyntaxError):
        cleaned = [item.strip() for item in value.split(',') if item.strip()]
        return cleaned or None


def _parse_env_int_list(env_name, default_value):
    value = os.getenv(env_name)
    if not value:
        return default_value

    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return [int(item) for item in parsed]
        return [int(parsed)]
    except (ValueError, SyntaxError, TypeError):
        return [int(item.strip()) for item in value.split(',') if item.strip()]


def _parse_env_list_alias(primary_name, alias_name, default_value):
    if os.getenv(primary_name):
        return _parse_env_list(primary_name, default_value)
    if os.getenv(alias_name):
        return _parse_env_list(alias_name, default_value)
    return default_value


def _parse_env_bool(env_name, default_value=False):
    value = os.getenv(env_name)
    if value is None:
        return default_value
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


_X_REQUESTED_WITH_SAFE_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def _parse_x_requested_with_tokens(
    env_name="x_requested_with_header_tokens",
    default_value=None,
) -> list[str]:
    raw_tokens = _parse_env_list(env_name, default_value or [])
    cleaned = []
    seen = set()

    for raw in raw_tokens:
        token = str(raw or "").strip()
        if not token:
            continue

        # Reject values that could break headers or be abused for header injection.
        if not _X_REQUESTED_WITH_SAFE_PATTERN.match(token):
            logging.getLogger(__name__).warning(
                "Ignoring invalid X-Requested-With token from env: invalid format"
            )
            continue

        if token not in seen:
            seen.add(token)
            cleaned.append(token)

    return cleaned


_SOFASCORE_X_REQUESTED_WITH_SAFE_PATTERN = re.compile(r"^[A-Za-z0-9._-]{0,128}$")


def _parse_x_requested_with_value(
    env_name="SOFASCORE_X_REQUESTED_WITH",
    default_value="XMLHttpRequest",
):
    value = os.getenv(env_name, default_value)

    # None should not happen with a default, but keep the parser defensive.
    if value is None:
        return default_value

    value = str(value).strip()

    # Allow empty strings only when explicitly configured; reject risky header values.
    if not _SOFASCORE_X_REQUESTED_WITH_SAFE_PATTERN.match(value):
        logging.getLogger(__name__).warning(
            "Invalid SOFASCORE_X_REQUESTED_WITH value; falling back to XMLHttpRequest"
        )
        return "XMLHttpRequest"

    return value

class Config:
    # Database
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///sofascore_odds.db')
    # Connection timeout in seconds for PostgreSQL (prevents long OS-level waits)
    DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', '5'))
    
    # SOFASCORE API Configuration
    SOFASCORE_BASE_URL = 'https://api.sofascore.com/api/v1'
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    SOFASCORE_X_REQUESTED_WITH = _parse_x_requested_with_value()

    # Deprecated for production use.
    # Kept only for diagnostic A/B testing and rollback experiments.
    # Production client uses SOFASCORE_X_REQUESTED_WITH.
    X_REQUESTED_WITH_HEADER_TOKENS = _parse_x_requested_with_tokens(
        "x_requested_with_header_tokens",
        ["4a6089", "17cb4a"],
    )
    
    # Scheduler Configuration
    POLL_INTERVAL_MINUTES = int(os.getenv('POLL_INTERVAL_MINUTES', '5'))
    DISCOVERY_INTERVAL_HOURS = int(os.getenv('DISCOVERY_INTERVAL_HOURS', '6'))
    DISCOVERY2_INTERVAL_HOURS = int(os.getenv('DISCOVERY2_INTERVAL_HOURS', '6'))  # Separate interval for Discovery2
    PRE_START_WINDOW_MINUTES = int(os.getenv('PRE_START_WINDOW_MINUTES', '30'))
    PRE_START_WORKERS = int(os.getenv('PRE_START_WORKERS', '5'))  # Number of parallel workers for pre-start checks
    INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES = int(os.getenv("INTRADAY_RESULT_FRESHNESS_WINDOW_MINUTES", "390"))
    INTRADAY_RESULT_FRESHNESS_WORKERS = int(os.getenv("INTRADAY_RESULT_FRESHNESS_WORKERS", str(PRE_START_WORKERS)))
    PRE_START_ODDS_MOMENTS = _parse_env_int_list(
        "PRE_START_ODDS_MOMENTS",
        [120, 30, 5, 0, -5],
    )
    PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES = int(
        os.getenv("PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES", "3")
    )

    # Daily Discovery Log/Queue Configuration
    DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES = int(os.getenv('DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES', '240'))
    DAILY_DISCOVERY_CHECK_INTERVAL_MINUTES = int(
        os.getenv(
            'DAILY_DISCOVERY_CHECK_INTERVAL_MINUTES',
            str(DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES),
        )
    )
    DAILY_DISCOVERY_AM_OPEN_HOUR = int(os.getenv('DAILY_DISCOVERY_AM_OPEN_HOUR', '5'))
    DAILY_DISCOVERY_PM_OPEN_HOUR = int(os.getenv('DAILY_DISCOVERY_PM_OPEN_HOUR', '16'))
    DAILY_DISCOVERY_SLOTS = _parse_env_list('DAILY_DISCOVERY_SLOTS', ['AM', 'PM'])
    DAILY_DISCOVERY_DAYS_TO_KEEP = int(os.getenv('DAILY_DISCOVERY_DAYS_TO_KEEP', '1'))
    ODDSPAPI_FIXTURE_DISCOVERY_TIMES = _parse_env_list(
        'ODDSPAPI_FIXTURE_DISCOVERY_TIMES',
        ['02:35'],
    )
    
    # Discovery Schedule Times (dynamically generated based on DISCOVERY_INTERVAL_HOURS)
    # Runs at exact hours: 00:00, 06:00, 12:00, 18:00 (if interval is 6)
    @staticmethod
    def _generate_discovery_times():
        interval_hours = int(os.getenv('DISCOVERY_INTERVAL_HOURS', '6'))
        times = []
        for hour in range(0, 24, interval_hours):
            times.append(f"{hour:02d}:12")
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
    
    # pre start check settings
    global_debug_mode = os.getenv('global_debug_mode', 'true').lower() == 'true'

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')  # Reset to INFO after debugging
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Rate limiting
    REQUEST_DELAY_SECONDS = float(os.getenv('REQUEST_DELAY_SECONDS', '0.5'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

    # OddsPapi API Configuration
    ODDSPAPI_BASE_URL = os.getenv('ODDSPAPI_BASE_URL', 'https://api.oddspapi.io').rstrip('/')
    ODDSPAPI_KEY = os.getenv('ODDSPAPI_KEY') or os.getenv('ODDSpapi_KEY', '')
    ODDSPAPI_TIMEOUT_SECONDS = float(os.getenv('ODDSPAPI_TIMEOUT_SECONDS', '15'))
    ODDSPAPI_MAX_RETRIES = int(os.getenv('ODDSPAPI_MAX_RETRIES', str(MAX_RETRIES)))
    ODDSPAPI_REQUEST_DELAY_SECONDS = float(
        os.getenv('ODDSPAPI_REQUEST_DELAY_SECONDS', str(REQUEST_DELAY_SECONDS))
    )
    ODDSPAPI_FIXTURES_COOLDOWN_SECONDS = float(
        os.getenv('ODDSPAPI_FIXTURES_COOLDOWN_SECONDS', '2.0')
    )
    ODDSPAPI_DEFAULT_BOOKMAKERS = _parse_optional_env_list(
        'ODDSPAPI_DEFAULT_BOOKMAKERS',
        ['pinnacle'],
    )
    ODDSPAPI_DEFAULT_LANGUAGE = os.getenv('ODDSPAPI_DEFAULT_LANGUAGE', 'en')
    ODDSPAPI_DEFAULT_ODDS_FORMAT = os.getenv('ODDSPAPI_DEFAULT_ODDS_FORMAT', 'decimal')
    ODDSPAPI_DEFAULT_VERBOSITY = int(os.getenv('ODDSPAPI_DEFAULT_VERBOSITY', '3'))
    ODDSPAPI_DEFAULT_MARKET_KEYS = _parse_env_list(
        'ODDSPAPI_DEFAULT_MARKET_KEYS',
        ['1x2_full_time', 'over_under_full_time', 'asian_handicap_full_time'],
    )
    
    # Notification Configuration
    NOTIFICATIONS_ENABLED = os.getenv('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'
    
    # Timestamp Correction Configuration
    ENABLE_TIMESTAMP_CORRECTION = os.getenv('ENABLE_TIMESTAMP_CORRECTION', 'true').lower() == 'true'
    
    # Odds Extraction Configuration (for testing)
    ENABLE_ODDS_EXTRACTION = os.getenv('ENABLE_ODDS_EXTRACTION', 'true').lower() == 'true'

    # Dual Process market odds read configuration
    MARKETS_DUAL_PROCESS = _parse_env_list_alias('MARKETS_DUAL_PROCESS', 'markets_dual_process', ['1X2', 'Home/Away'])

    PERIODS_DUAL_PROCESS = _parse_env_list_alias('PERIODS_DUAL_PROCESS', 'periods_dual_process', ['Full Time'])

    # OddsPortal scraping activation toggle for the pre-start flow
    ODDSPORTAL_SCRAPING_ENABLED = _parse_env_bool('ODDSPORTAL_SCRAPING_ENABLED', True)
    
    # ODDSPORTAL LANGUAGE (REGIONAL DOMAIN)
    ODDSPORTAL_UI_LANGUAGE = os.getenv('ODDSPORTAL_UI_LANGUAGE', 'es')
    if ODDSPORTAL_UI_LANGUAGE == 'es':
        ODDSPORTAL_DOMAIN = 'cuotasahora.com'
    elif ODDSPORTAL_UI_LANGUAGE == 'en':
        ODDSPORTAL_DOMAIN = 'oddsportal.com'
    else:
        ODDSPORTAL_DOMAIN = 'cuotasahora.com'
    
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
    PROXY_PROVIDER = (os.getenv('PROXY_PROVIDER', '').strip().lower() or ('decodo' if 'decodo' in PROXY_ENDPOINT.lower() else 'legacy'))
    PROXY_PROTOCOL = (os.getenv('PROXY_PROTOCOL', 'http').strip().lower() or 'http')
    PROXY_USERNAME_BASE = os.getenv('PROXY_USERNAME_BASE', PROXY_USERNAME)
    PROXY_COUNTRY = os.getenv('PROXY_COUNTRY', 'mx').strip().lower()
    PROXY_CITY = os.getenv('PROXY_CITY', '').strip().lower()
    PROXY_SESSION_DURATION_MINUTES = int(os.getenv('PROXY_SESSION_DURATION_MINUTES', '10'))
    PROXY_MODE_ODDSPORTAL = os.getenv('PROXY_MODE_ODDSPORTAL', 'sticky').strip().lower()
    PROXY_MODE_SOFASCORE = os.getenv('PROXY_MODE_SOFASCORE', 'rotating').strip().lower()
    PROXY_ROTATE_ON_ODDSPORTAL_BROWSER_RESTART = _parse_env_bool('PROXY_ROTATE_ON_ODDSPORTAL_BROWSER_RESTART', True)
    PROXY_ROTATE_ON_SOFASCORE_PROXY_ERROR = _parse_env_bool('PROXY_ROTATE_ON_SOFASCORE_PROXY_ERROR', True)
    PROXY_LOG_SAFE = _parse_env_bool('PROXY_LOG_SAFE', True)
    
    PROXY_ROTATION_INTERVAL = int(os.getenv('PROXY_ROTATION_INTERVAL', '5'))
    PROXY_MAX_RETRIES = int(os.getenv('PROXY_MAX_RETRIES', '3'))
    _ODDSPORTAL_PROXY_ALIGNMENT_WARNED = False
    
    # OddsPortal resource blocking toggle (disable if it causes scraping instability)
    ODDSPORTAL_BLOCK_RESOURCES = os.getenv('ODDSPORTAL_BLOCK_RESOURCES', 'true').lower() == 'true'
    ODDSPORTAL_BLOCK_SERVICE_WORKERS = _parse_env_bool('ODDSPORTAL_BLOCK_SERVICE_WORKERS', True)
    ODDSPORTAL_PRE_NAVIGATION_CLEAR_STATE = _parse_env_bool('ODDSPORTAL_PRE_NAVIGATION_CLEAR_STATE', True)
    
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

    # Matchup streak standings grouping toggle.
    # True keeps natural conference/league grouping when available.
    # False forces league-wide standings.
    MATCHUP_STANDINGS_GROUP_BY_CONFERENCE = _parse_env_bool(
        'MATCHUP_STANDINGS_GROUP_BY_CONFERENCE',
        True
    )

    # Sports to exclude from alert evaluation (but not odds extraction)
    EXCLUDED_SPORTS = _parse_env_list('EXCLUDED_SPORTS', ['Table tennis', 'Darts'])

    # Pipeline toggles
    ENABLE_PILLAR_PIPELINE = _parse_env_bool('ENABLE_PILLAR_PIPELINE', True)
    ENABLE_LEGACY_ALERT_PIPELINE = _parse_env_bool('ENABLE_LEGACY_ALERT_PIPELINE', True)
    ENABLE_STANDINGS_COMPETITION_METADATA_ENRICHMENT = _parse_env_bool(
        'ENABLE_STANDINGS_COMPETITION_METADATA_ENRICHMENT',
        True,
    )
    FORCE_STANDINGS_COMPETITION_METADATA_REFRESH = _parse_env_bool(
        'FORCE_STANDINGS_COMPETITION_METADATA_REFRESH',
        False,
    )

    # Discovery sources to allow for alert sending
    DISCOVERY_SOURCES_FOR_ALERTS = _parse_env_list('DISCOVERY_SOURCES_FOR_ALERTS', ['dropping_odds'])

    @staticmethod
    def validate_oddsportal_proxy_alignment(logger: logging.Logger) -> None:
        """Warn when sticky OddsPortal sessions outlive the polling cadence."""
        if Config._ODDSPORTAL_PROXY_ALIGNMENT_WARNED:
            return

        if (
            Config.PROXY_ENABLED
            and Config.PROXY_MODE_ODDSPORTAL == 'sticky'
            and Config.PROXY_SESSION_DURATION_MINUTES > Config.POLL_INTERVAL_MINUTES
        ):
            logger.warning(
                "OddsPortal sticky proxy session duration (%s min) exceeds polling cadence (%s min) "
                "and may increase stale-edge reuse risk.",
                Config.PROXY_SESSION_DURATION_MINUTES,
                Config.POLL_INTERVAL_MINUTES,
            )
            Config._ODDSPORTAL_PROXY_ALIGNMENT_WARNED = True
