"""Schedule and live feed helpers for SofaScore.
    dates are in format YYYY-MM-DD."""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_live_events_response_per_sport(client, sport: str) -> Optional[Dict]:
    endpoint = f"/sport/{sport}/events/live"
    logger.info(f"✈️ Fetching live events {sport}. Endpoint: {endpoint}")
    response = client.request_json_or_none(endpoint)
    if not response or "events" not in response:
        return None
    return response
