"""HTTP adapter for the Oddspapi odds endpoint."""

from __future__ import annotations

from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiHttpError
from modules.oddspapi.historical_odds_normalizer import (
    OddspapiHistoricalOddsNormalizer,
)

from .constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
    ODDSPAPI_PRE_START_ODDS_ENDPOINTS,
)


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
        endpoint: str = ODDSPAPI_CURRENT_ODDS_ENDPOINT,
        source_sport_id: str | int | None = None,
        outcome_id: int | None = None,
        minimum_initial_span_minutes: float = 0.0,
        require_active_quotes: bool = True,
        capture_raw_response: bool = False,
    ) -> OddsFetchResult:
        selected_endpoint = str(endpoint or "").strip().lower()
        if selected_endpoint not in ODDSPAPI_PRE_START_ODDS_ENDPOINTS:
            supported = ", ".join(sorted(ODDSPAPI_PRE_START_ODDS_ENDPOINTS))
            raise ValueError(
                f"Unsupported Oddspapi odds endpoint '{endpoint}'. "
                f"Expected one of: {supported}"
            )

        raw_payload = None
        try:
            if selected_endpoint == ODDSPAPI_HISTORICAL_ODDS_ENDPOINT:
                historical_payload = self.client.get_historical_odds(
                    fixture_id=fixture_id,
                    bookmakers=bookmakers,
                    outcome_id=outcome_id,
                )
                if capture_raw_response:
                    raw_payload = historical_payload
                payload = OddspapiHistoricalOddsNormalizer.normalize(
                    historical_payload,
                    source_sport_id=source_sport_id,
                    minimum_initial_span_minutes=minimum_initial_span_minutes,
                    require_active_quotes=require_active_quotes,
                )
            else:
                payload = self.client.get_odds(
                    fixture_id=fixture_id,
                    bookmakers=bookmakers,
                    odds_format=odds_format,
                    language=language,
                    verbosity=verbosity,
                )
                if capture_raw_response:
                    raw_payload = payload
        except OddsPapiHttpError as exc:
            if exc.status_code == 404:
                return OddsFetchResult.endpoint_not_found()
            if exc.error_code in self.EMPTY_ODDS_ERROR_CODES:
                return OddsFetchResult.from_payload(None)
            raise
        return OddsFetchResult.from_payload(
            payload,
            raw_payload=raw_payload,
        )
