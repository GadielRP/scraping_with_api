"""Public API for the SofaScore package."""

from .client import SofaScoreAPI, api_client
from .event_normalizer import normalize_event_payload
from .event_identity import resolve_sofascore_event_id
from .exceptions import (
    SofaScoreChallengeException,
    SofaScoreError,
    SofaScoreHttpError,
    SofaScoreNotFoundException,
    SofaScoreRateLimitException,
)
from .sport_classifier import SPORT_TENNIS, SPORT_TENNIS_DOUBLES, SPORT_UNKNOWN, SportClassifier, sport_classifier

__all__ = [
    "SofaScoreAPI",
    "api_client",
    "resolve_sofascore_event_id",
    "SofaScoreChallengeException",
    "SofaScoreError",
    "SofaScoreHttpError",
    "SofaScoreNotFoundException",
    "SofaScoreRateLimitException",
    "normalize_event_payload",
    "SPORT_TENNIS",
    "SPORT_TENNIS_DOUBLES",
    "SPORT_UNKNOWN",
    "SportClassifier",
    "sport_classifier",
]
