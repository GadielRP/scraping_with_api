"""HTTP adapter for the Oddspapi odds endpoint."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.oddspapi.api_keys import (
    api_key_for_slot,
    free_endpoint_api_keys,
    odds_endpoint_api_keys,
)
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiHttpError
from modules.oddspapi.historical_odds_reader import OddspapiHistoricalOddsReader

from .constants import (
    ODDSPAPI_CURRENT_ODDS_ENDPOINT,
    ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
    ODDSPAPI_PRE_START_ODDS_ENDPOINTS,
)


class OddspapiOddsFetcher:
    EMPTY_ODDS_ERROR_CODES = {"NO_ODDS", "ODDS_NOT_FOUND"}

    def __init__(
        self,
        client: OddsPapiClient | None = None,
        *,
        odds_client: OddsPapiClient | None = None,
        historical_client: OddsPapiClient | None = None,
    ):
        if client is not None and (odds_client is not None or historical_client is not None):
            raise ValueError(
                "Pass either client= (both endpoints) or "
                "odds_client=/historical_client=, not both"
            )
        shared = client
        if shared is None and odds_client is None and historical_client is None:
            odds_key = api_key_for_slot(0, odds_endpoint_api_keys())
            historical_key = api_key_for_slot(0, free_endpoint_api_keys())
            odds_client = OddsPapiClient(api_key=odds_key)
            historical_client = (
                odds_client
                if historical_key == odds_key
                else OddsPapiClient(api_key=historical_key)
            )
            shared = odds_client
        self.odds_client = odds_client or shared
        self.historical_client = historical_client or shared or self.odds_client
        self.client = shared or self.odds_client

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
        as_of_targets: Sequence[tuple[int, datetime, datetime]] | None = None,
    ) -> OddsFetchResult:
        selected_endpoint = str(endpoint or "").strip().lower()
        if selected_endpoint not in ODDSPAPI_PRE_START_ODDS_ENDPOINTS:
            supported = ", ".join(sorted(ODDSPAPI_PRE_START_ODDS_ENDPOINTS))
            raise ValueError(
                f"Unsupported Oddspapi odds endpoint '{endpoint}'. "
                f"Expected one of: {supported}"
            )

        raw_payload = None
        as_of_quotes: tuple = ()
        try:
            if selected_endpoint == ODDSPAPI_HISTORICAL_ODDS_ENDPOINT:
                historical_payload = self.historical_client.get_historical_odds(
                    fixture_id=fixture_id,
                    bookmakers=bookmakers,
                    outcome_id=outcome_id,
                )
                if capture_raw_response:
                    raw_payload = historical_payload
                read_result = OddspapiHistoricalOddsReader.read(
                    historical_payload,
                    source_sport_id=source_sport_id,
                    as_of_targets=as_of_targets or (),
                    minimum_initial_span_minutes=minimum_initial_span_minutes,
                    require_active_quotes=require_active_quotes,
                )
                payload = read_result.normalized_payload
                as_of_quotes = read_result.as_of_quotes
            else:
                payload = self.odds_client.get_odds(
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
            as_of_quotes=as_of_quotes,
        )
