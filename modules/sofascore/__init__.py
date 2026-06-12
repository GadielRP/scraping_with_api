"""Public API for the SofaScore package."""

from .client import SofaScoreAPI, api_client
from .event_normalizer import get_event_information
from .exceptions import SofaScoreChallengeException, SofaScoreNotFoundException, SofaScoreRateLimitException
from .sport_classifier import SPORT_TENNIS, SPORT_TENNIS_DOUBLES, SPORT_UNKNOWN, SportClassifier, sport_classifier

__all__ = [
    "SofaScoreAPI",
    "api_client",
    "SofaScoreChallengeException",
    "SofaScoreNotFoundException",
    "SofaScoreRateLimitException",
    "get_event_information",
    "SPORT_TENNIS",
    "SPORT_TENNIS_DOUBLES",
    "SPORT_UNKNOWN",
    "SportClassifier",
    "sport_classifier",
]
