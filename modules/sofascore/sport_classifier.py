"""Pure sport classification utilities for SofaScore payloads."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict

logger = logging.getLogger(__name__)

SPORT_TENNIS = "Tennis"
SPORT_TENNIS_DOUBLES = "Tennis Doubles"
SPORT_UNKNOWN = "Unknown"


@dataclass
class SportClassifier:
    classification_stats: Dict[str, int] = field(
        default_factory=lambda: {
            "tennis_singles": 0,
            "tennis_doubles": 0,
            "other_sports": 0,
        }
    )

    def classify_sport(self, sport: str, home_team: str, away_team: str) -> str:
        if not sport or not home_team or not away_team:
            logger.warning(
                "Missing data for classification: sport='%s', home='%s', away='%s'",
                sport,
                home_team,
                away_team,
            )
            return sport or SPORT_UNKNOWN

        if str(sport).lower() == "tennis":
            return self.classify_tennis_match_format(home_team, away_team)

        self.classification_stats["other_sports"] += 1
        return sport

    def classify_tennis_match_format(self, home_team: str, away_team: str) -> str:
        try:
            home_has_slash = "/" in str(home_team or "")
            away_has_slash = "/" in str(away_team or "")

            if home_has_slash and away_has_slash:
                self.classification_stats["tennis_doubles"] += 1
                logger.debug("Tennis Doubles detected: '%s' vs '%s'", home_team, away_team)
                return SPORT_TENNIS_DOUBLES

            self.classification_stats["tennis_singles"] += 1
            logger.debug("Tennis Singles detected: '%s' vs '%s'", home_team, away_team)
            return SPORT_TENNIS
        except Exception as exc:
            logger.error("Error classifying tennis event '%s' vs '%s': %s", home_team, away_team, exc)
            self.classification_stats["tennis_singles"] += 1
            return SPORT_TENNIS

    def get_classification_stats(self) -> Dict[str, int]:
        return self.classification_stats.copy()


sport_classifier = SportClassifier()

__all__ = [
    "SPORT_TENNIS",
    "SPORT_TENNIS_DOUBLES",
    "SPORT_UNKNOWN",
    "SportClassifier",
    "sport_classifier",
]
