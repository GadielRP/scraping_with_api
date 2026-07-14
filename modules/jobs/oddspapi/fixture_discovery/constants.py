"""Configuration defaults for the Oddspapi fixture discovery job."""

DISCOVERY_SPORT_IDS = {
    "soccer": 10,
    "basketball": 11,
    "tennis": 12,
    "baseball": 13,
    "american-football": 14,
    "ice-hockey": 15,
}

DEFAULT_STATUS_ID = 0
DEFAULT_HAS_ODDS = False
DEFAULT_LANGUAGE = "en"
DEFAULT_WINDOW_HOURS = 24
DEFAULT_LOOKAHEAD_DAYS = 1
DEFAULT_PERSIST_QUEUE = False
DEFAULT_CREATE_MAPPINGS = True
DEFAULT_MAX_REQUEST_WINDOW_HOURS = 48
