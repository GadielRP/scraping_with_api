import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
    PRE_START_WINDOW_MINUTES = int(os.getenv('PRE_START_WINDOW_MINUTES', '30'))
    
    # Discovery Schedule Times (dynamically generated based on DISCOVERY_INTERVAL_HOURS)
    def _generate_discovery_times():
        interval_hours = int(os.getenv('DISCOVERY_INTERVAL_HOURS', '6'))
        times = []
        for hour in range(0, 24, interval_hours):
            times.append(f"{hour:02d}:00")
        return times
    
    DISCOVERY_TIMES = _generate_discovery_times()
    
    # Timezone
    TIMEZONE = os.getenv('TIMEZONE', 'America/Mexico_City')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')  # Reset to INFO after debugging
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Rate limiting
    REQUEST_DELAY_SECONDS = float(os.getenv('REQUEST_DELAY_SECONDS', '1.0'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    
    # Notification Configuration
    NOTIFICATIONS_ENABLED = os.getenv('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'
    
    # Timestamp Correction Configuration
    ENABLE_TIMESTAMP_CORRECTION = os.getenv('ENABLE_TIMESTAMP_CORRECTION', 'true').lower() == 'true'
    
    # Telegram Settings
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
    # Proxy configuration
    PROXY_ENABLED = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
    PROXY_USERNAME = os.getenv('PROXY_USERNAME', '')
    PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', '')
    PROXY_ENDPOINT = os.getenv('PROXY_ENDPOINT', '')
    PROXY_ROTATION_INTERVAL = int(os.getenv('PROXY_ROTATION_INTERVAL', '5'))
    PROXY_MAX_RETRIES = int(os.getenv('PROXY_MAX_RETRIES', '3'))
