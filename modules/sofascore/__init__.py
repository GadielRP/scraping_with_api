"""Public API for the SofaScore package."""

from .client import SofaScoreAPI, api_client
from .event_normalizer import normalize_event_payload
from .event_identity import SofaScoreEventIdentity, resolve_sofascore_event_identity
from .exceptions import (
    SofaScoreChallengeCircuitOpenException,
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
    "SofaScoreEventIdentity",
    "resolve_sofascore_event_identity",
    "SofaScoreChallengeException",
    "SofaScoreChallengeCircuitOpenException",
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
