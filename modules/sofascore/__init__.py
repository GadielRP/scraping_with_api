"""Public API for the SofaScore package."""

from .client import SofaScoreAPI, api_client
from .event_normalizer import get_event_information
from .exceptions import SofaScoreNotFoundException, SofaScoreRateLimitException

__all__ = [
    "SofaScoreAPI",
    "api_client",
    "SofaScoreNotFoundException",
    "SofaScoreRateLimitException",
    "get_event_information",
]
