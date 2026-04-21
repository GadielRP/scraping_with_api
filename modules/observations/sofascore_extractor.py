"""Extract observations from SofaScore responses."""

from __future__ import annotations

import logging
from typing import Dict

logger = logging.getLogger(__name__)


def extract_observations_from_sofascore_response(response: Dict) -> list[dict] | None:
    try:
        if not response or "event" not in response:
            return None

        event_data = response["event"]
        tournament = event_data.get("tournament", {})
        sport = tournament.get("category", {}).get("sport", {}).get("name")
        if not sport:
            logger.debug("No sport information found, skipping observations")
            return None

        observations = []
        if sport.lower() in {"tennis", "tennis doubles"}:
            ground_type = event_data.get("groundType")
            if ground_type:
                observations.append({"type": "ground_type", "value": ground_type, "sport": sport})

        return observations or None
    except Exception as exc:
        logger.warning("Error extracting observations: %s", exc)
        return None
