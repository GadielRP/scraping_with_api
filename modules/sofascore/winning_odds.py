"""Winning odds helpers for SofaScore."""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_winning_odds_response(client, event_id: int) -> Optional[Dict]:
    endpoint = f"/event/{event_id}/provider/1/winning-odds"
    logger.debug("Fetching winning odds response from %s", endpoint)
    return client._request_json(endpoint, no_retry_on_404=True)
