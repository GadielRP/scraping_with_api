"""SofaScore odds endpoint adapter with provider-neutral outcomes."""

from __future__ import annotations

import logging

from modules.odds_ingestion.fetch_result import OddsFetchResult

from .client import SofaScoreAPI
from .exceptions import SofaScoreNotFoundException

logger = logging.getLogger(__name__)


class SofaScoreOddsFetcher:
    """Translate SofaScore transport behavior into expected odds outcomes."""

    def __init__(self, client: SofaScoreAPI):
        self.client = client

    def fetch_odds(self, source_event_id: int, slug: str | None = None) -> OddsFetchResult:
        if slug:
            logger.info(
                "✈️ Fetching final SofaScore odds source_event_id=%s slug=%s",
                source_event_id,
                slug,
            )
        try:
            payload = self.client.request_json(f"/event/{source_event_id}/odds/1/all")
        except SofaScoreNotFoundException:
            return OddsFetchResult.endpoint_not_found()
        return OddsFetchResult.from_payload(payload)
