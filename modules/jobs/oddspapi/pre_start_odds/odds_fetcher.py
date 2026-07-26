"""HTTP adapter for the Oddspapi odds endpoint."""

from __future__ import annotations

from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiHttpError


class OddspapiOddsFetcher:
    EMPTY_ODDS_ERROR_CODES = {"NO_ODDS", "ODDS_NOT_FOUND"}

    def __init__(self, client: OddsPapiClient | None = None):
        self.client = client or OddsPapiClient()

    def fetch_odds(
        self,
        fixture_id: str,
        bookmakers: list[str] | None = None,
        odds_format: str | None = None,
        language: str | None = None,
        verbosity: int | None = None,
    ) -> OddsFetchResult:
        try:
            payload = self.client.get_odds(
                fixture_id=fixture_id,
                bookmakers=bookmakers,
                odds_format=odds_format,
                language=language,
                verbosity=verbosity,
            )
        except OddsPapiHttpError as exc:
            if exc.status_code == 404:
                return OddsFetchResult.endpoint_not_found()
            if exc.error_code in self.EMPTY_ODDS_ERROR_CODES:
                return OddsFetchResult.from_payload(None)
            raise
        return OddsFetchResult.from_payload(payload)
