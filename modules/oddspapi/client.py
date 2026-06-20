"""Small, proxy-free HTTP client for the OddsPapi v4 API."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from infrastructure.settings import Config

logger = logging.getLogger(__name__)


class OddsPapiError(RuntimeError):
    """Raised when OddsPapi returns an unusable response."""


class OddsPapiClient:
    TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        request_delay_seconds: float | None = None,
    ) -> None:
        self.base_url = (base_url or Config.ODDSPAPI_BASE_URL).rstrip("/")
        self.api_key = Config.ODDSPAPI_KEY if api_key is None else api_key
        self.timeout = Config.ODDSPAPI_TIMEOUT_SECONDS if timeout is None else timeout
        self.max_retries = Config.ODDSPAPI_MAX_RETRIES if max_retries is None else max_retries
        self.request_delay_seconds = (
            Config.ODDSPAPI_REQUEST_DELAY_SECONDS
            if request_delay_seconds is None
            else request_delay_seconds
        )
        self.session = requests.Session()
        # OddsPapi must never inherit HTTP(S)_PROXY or other request settings from env.
        self.session.trust_env = False

    @staticmethod
    def _comma_separated(values: list[str] | tuple[str, ...] | str | None) -> str | None:
        if isinstance(values, str):
            return values.strip() or None
        if not values:
            return None
        cleaned = [str(value).strip() for value in values if str(value).strip()]
        return ",".join(cleaned) or None

    def _request(self, endpoint: str, params: dict | None = None) -> dict | list:
        if not str(self.api_key or "").strip():
            raise ValueError("ODDSPAPI_KEY is required to make an OddsPapi request")

        normalized_endpoint = str(endpoint or "").strip().lstrip("/")
        if normalized_endpoint.startswith("v4/"):
            normalized_endpoint = normalized_endpoint[3:]
        if not normalized_endpoint:
            raise ValueError("OddsPapi endpoint is required")

        url = f"{self.base_url}/v4/{normalized_endpoint}"
        request_params = {
            key: value
            for key, value in (params or {}).items()
            if value is not None and str(key).lower() != "apikey"
        }
        safe_params = dict(request_params)
        request_params["apiKey"] = self.api_key
        logger.info("OddsPapi GET /v4/%s params=%s", normalized_endpoint, safe_params)

        attempts = max(1, int(self.max_retries))
        for attempt in range(attempts):
            if attempt and self.request_delay_seconds > 0:
                time.sleep(self.request_delay_seconds)

            try:
                # Do not add a proxies argument: trust_env=False is the single source of truth.
                response = self.session.get(url, params=request_params, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt < attempts - 1:
                    logger.warning(
                        "Transient OddsPapi request error for /v4/%s (attempt %s/%s): %s",
                        normalized_endpoint,
                        attempt + 1,
                        attempts,
                        type(exc).__name__,
                    )
                    continue
                raise OddsPapiError(
                    f"OddsPapi request failed endpoint=/v4/{normalized_endpoint} "
                    f"error={type(exc).__name__}"
                ) from exc

            status_code = response.status_code
            if status_code in self.TRANSIENT_STATUS_CODES and attempt < attempts - 1:
                logger.warning(
                    "Transient OddsPapi HTTP %s for /v4/%s (attempt %s/%s)",
                    status_code,
                    normalized_endpoint,
                    attempt + 1,
                    attempts,
                )
                continue

            if status_code < 200 or status_code >= 300:
                response_text = str(getattr(response, "text", "") or "").replace("\n", " ")[:500]
                response_text = response_text.replace(str(self.api_key), "***")
                raise OddsPapiError(
                    f"OddsPapi HTTP error status_code={status_code} "
                    f"endpoint=/v4/{normalized_endpoint} response={response_text!r}"
                )

            try:
                payload: Any = response.json()
            except (ValueError, requests.exceptions.JSONDecodeError) as exc:
                raise OddsPapiError(
                    f"Invalid JSON from OddsPapi endpoint=/v4/{normalized_endpoint} "
                    f"status_code={status_code}"
                ) from exc

            if not isinstance(payload, (dict, list)):
                raise OddsPapiError(
                    f"Invalid JSON payload type from OddsPapi endpoint=/v4/{normalized_endpoint}: "
                    f"{type(payload).__name__}"
                )
            return payload

        raise OddsPapiError(f"OddsPapi request exhausted retries endpoint=/v4/{normalized_endpoint}")

    def get_fixture(self, fixture_id: str, language: str | None = None) -> dict:
        return self._request("fixture", {"fixtureId": fixture_id, "language": language})

    def get_fixtures(
        self,
        tournament_id: str | int | None = None,
        sport_id: str | int | None = None,
        participant_id: str | int | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        language: str | None = None,
        status_id: str | int | None = None,
        has_odds: bool | str | None = None,
        bookmakers: list[str] | None = None,
    ) -> dict | list:
        params = {
            "tournamentId": tournament_id,
            "sportId": sport_id,
            "participantId": participant_id,
            "from": from_date,
            "to": to_date,
            "language": language,
            "statusId": status_id,
            "hasOdds": has_odds,
            "bookmakers": self._comma_separated(bookmakers),
        }
        return self._request("fixtures", params)

    def get_odds(
        self,
        fixture_id: str,
        bookmakers: list[str] | None = None,
        odds_format: str | None = None,
        language: str | None = None,
        verbosity: int | None = None,
    ) -> dict:
        params = {
            "fixtureId": fixture_id,
            "bookmakers": self._comma_separated(
                Config.ODDSPAPI_DEFAULT_BOOKMAKERS if bookmakers is None else bookmakers
            ),
            "oddsFormat": odds_format or Config.ODDSPAPI_DEFAULT_ODDS_FORMAT,
            "language": language or Config.ODDSPAPI_DEFAULT_LANGUAGE,
            "verbosity": Config.ODDSPAPI_DEFAULT_VERBOSITY if verbosity is None else verbosity,
        }
        return self._request("odds", params)

    def get_odds_by_tournaments(
        self,
        tournament_ids: list[str | int] | str,
        bookmakers: list[str] | None = None,
        odds_format: str | None = None,
        language: str | None = None,
        verbosity: int | None = None,
    ) -> dict | list:
        params = {
            "tournamentIds": self._comma_separated(tournament_ids),
            "bookmakers": self._comma_separated(
                Config.ODDSPAPI_DEFAULT_BOOKMAKERS if bookmakers is None else bookmakers
            ),
            "oddsFormat": odds_format or Config.ODDSPAPI_DEFAULT_ODDS_FORMAT,
            "language": language or Config.ODDSPAPI_DEFAULT_LANGUAGE,
            "verbosity": Config.ODDSPAPI_DEFAULT_VERBOSITY if verbosity is None else verbosity,
        }
        return self._request("odds-by-tournaments", params)

    def get_markets(self, language: str | None = None) -> list[dict]:
        payload = self._request("markets", {"language": language})
        if not isinstance(payload, list):
            raise OddsPapiError("OddsPapi /v4/markets response must be a list")
        return payload

    def get_bookmakers(self) -> list[dict]:
        payload = self._request("bookmakers")
        if not isinstance(payload, list):
            raise OddsPapiError("OddsPapi /v4/bookmakers response must be a list")
        return payload
