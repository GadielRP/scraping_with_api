"""Head-to-head SofaScore helpers."""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_h2h_events_for_event(client, custom_id: str) -> Optional[Dict]:
    logger.info("Fetching H2H events for custom_id %s", custom_id)
    return client._request_json(f"/event/{custom_id}/h2h/events")
