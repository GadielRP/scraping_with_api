"""Small, proxy-free HTTP client for the OddsPapi v4 API."""

from __future__ import annotations

import logging
import random
import threading
import time
from contextlib import contextmanager
from typing import Any

import requests

from infrastructure.settings import Config
from modules.oddspapi.exceptions import OddsPapiError, OddsPapiHttpError

logger = logging.getLogger(__name__)


class OddsPapiClient:
    TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        request_delay_seconds: float | None = None,
        fixtures_cooldown_seconds: float | None = None,
        endpoint_cooldowns: dict[str, float] | None = None,
    ) -> None:
        self.base_url = (base_url or Config.ODDSPAPI_BASE_URL).rstrip("/")
        if api_key is None:
            keys = getattr(Config, "ODDSPAPI_KEYS", [])
            self.api_key = random.choice(keys) if keys else Config.ODDSPAPI_KEY
        else:
            self.api_key = api_key
        self.timeout = Config.ODDSPAPI_TIMEOUT_SECONDS if timeout is None else timeout
        self.max_retries = Config.ODDSPAPI_MAX_RETRIES if max_retries is None else max_retries
        self.request_delay_seconds = (
            Config.ODDSPAPI_REQUEST_DELAY_SECONDS
            if request_delay_seconds is None
            else request_delay_seconds
        )
        configured_cooldowns = getattr(Config, "ODDSPAPI_ENDPOINT_COOLDOWNS", {})
        self.endpoint_cooldowns = self._normalize_endpoint_cooldowns(
            configured_cooldowns
        )
        # Keep the old constructor argument working for callers/tests that
        # configured only the fixtures endpoint before the generic map existed.
        if fixtures_cooldown_seconds is not None:
            self.endpoint_cooldowns["fixtures"] = max(
                0.0,
                float(fixtures_cooldown_seconds),
            )
        if endpoint_cooldowns is not None:
            self.endpoint_cooldowns.update(
                self._normalize_endpoint_cooldowns(endpoint_cooldowns)
            )
        self._last_request_completed_at: dict[str, float] = {}
        self._endpoint_locks: dict[str, threading.Lock] = {}
        self._endpoint_locks_guard = threading.Lock()
        self.session = requests.Session()
        # OddsPapi must never inherit HTTP(S)_PROXY or other request settings from env.
        self.session.trust_env = False

    def close(self) -> None:
        """Release the connection pool owned by this client."""
        self.session.close()

    @staticmethod
    def _comma_separated(values: list[str] | tuple[str, ...] | str | None) -> str | None:
        if isinstance(values, str):
            return values.strip() or None
        if not values:
            return None
        cleaned = [str(value).strip() for value in values if str(value).strip()]
        return ",".join(cleaned) or None

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        normalized = str(endpoint or "").strip().lstrip("/")
        if normalized.startswith("v4/"):
            normalized = normalized[3:]
        return normalized.rstrip("/").lower()

    @staticmethod
    def _normalize_endpoint_cooldowns(
        cooldowns: dict[str, float] | None,
    ) -> dict[str, float]:
        normalized: dict[str, float] = {}
        for endpoint, seconds in (cooldowns or {}).items():
            key = OddsPapiClient._normalize_endpoint(endpoint)
            if not key:
                continue
            try:
                normalized[key] = max(0.0, float(seconds))
            except (TypeError, ValueError):
                logger.warning(
                    "Ignoring invalid OddsPapi cooldown for endpoint=%s",
                    endpoint,
                )
        return normalized

    def _endpoint_lock(self, endpoint: str) -> threading.Lock:
        with self._endpoint_locks_guard:
            return self._endpoint_locks.setdefault(endpoint, threading.Lock())

    @contextmanager
    def _endpoint_request_slot(self, endpoint: str):
        """Serialize requests per endpoint and enforce completion-to-start spacing."""
        cooldown_seconds = self.endpoint_cooldowns.get(endpoint, 0.0)
        if cooldown_seconds <= 0:
            yield
            return

        with self._endpoint_lock(endpoint):
            now = time.monotonic()
            last_completed_at = self._last_request_completed_at.get(endpoint)
            if last_completed_at is not None:
                remaining = cooldown_seconds - (
                    now - last_completed_at
                )
                if remaining > 0:
                    time.sleep(remaining)
            try:
                yield
            finally:
                # OddsPapi's documented cooldown is effectively response-to-next
                # request for sequential calls, so record completion, not start.
                self._last_request_completed_at[endpoint] = time.monotonic()

    @staticmethod
    def _retry_after_seconds(response) -> float | None:
        header_value = getattr(response, "headers", {}).get("Retry-After")
        try:
            if header_value is not None:
                return max(float(header_value), 0.0)
        except (TypeError, ValueError):
            pass

        try:
            body = response.json()
        except (ValueError, TypeError, AttributeError):
            return None
        if not isinstance(body, dict):
            return None

        error = body.get("error") if isinstance(body.get("error"), dict) else body
        retry_ms = error.get("retryMs") if isinstance(error, dict) else None
        try:
            if retry_ms is not None:
                return max(float(retry_ms) / 1000.0, 0.0)
        except (TypeError, ValueError):
            pass
        return None

    @staticmethod
    def _error_code(response) -> str | None:
        try:
            body = response.json()
        except (ValueError, TypeError, AttributeError):
            return None
        if not isinstance(body, dict):
            return None
        error = body.get("error") if isinstance(body.get("error"), dict) else body
        if not isinstance(error, dict):
            return None
        code = error.get("code") or error.get("errorCode")
        return str(code).strip().upper() if code else None

    def _request(self, endpoint: str, params: dict | None = None) -> dict | list:
        if not str(self.api_key or "").strip():
            raise ValueError("ODDSPAPI_KEY is required to make an OddsPapi request")

        normalized_endpoint = self._normalize_endpoint(endpoint)
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
        logger.info("✈️ OddsPapi GET /v4/%s params=%s", normalized_endpoint, safe_params)

        attempts = max(1, int(self.max_retries))
        retry_delay_seconds = self.request_delay_seconds
        for attempt in range(attempts):
            if attempt and self.request_delay_seconds > 0:
                time.sleep(retry_delay_seconds)

            try:
                # Do not add a proxies argument: trust_env=False is the single source of truth.
                with self._endpoint_request_slot(normalized_endpoint):
                    response = self.session.get(
                        url,
                        params=request_params,
                        timeout=self.timeout,
                    )
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
                retry_after_seconds = self._retry_after_seconds(response)
                retry_delay_seconds = max(
                    self.request_delay_seconds,
                    retry_after_seconds or 0.0,
                )
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
                raise OddsPapiHttpError(
                    status_code=status_code,
                    endpoint=f"/v4/{normalized_endpoint}",
                    response_text=response_text,
                    error_code=self._error_code(response),
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
        logger.info("✈️ Fetching oddspapi odds for fixture_id: %s", fixture_id)
        return self._request("odds", params)

    def get_historical_odds(
        self,
        fixture_id: str,
        bookmakers: list[str] | None = None,
        *,
        historical_id: int | None = None,
        player_id: int | None = None,
        outcome_id: int | None = None,
        active: bool | None = None,
    ) -> dict:
        selected_bookmakers = (
            Config.ODDSPAPI_DEFAULT_BOOKMAKERS
            if bookmakers is None
            else bookmakers
        )
        cleaned_bookmakers = [
            str(bookmaker).strip()
            for bookmaker in (selected_bookmakers or [])
            if str(bookmaker).strip()
        ]
        if not cleaned_bookmakers:
            raise ValueError(
                "At least one bookmaker is required for OddsPapi historical odds"
            )
        if len(cleaned_bookmakers) > 3:
            raise ValueError(
                "OddsPapi historical odds supports at most 3 bookmakers"
            )
        normalized_bookmakers = {
            bookmaker.lower()
            for bookmaker in cleaned_bookmakers
        }
        if "betfair-ex" in normalized_bookmakers:
            if len(cleaned_bookmakers) != 1 or outcome_id is None:
                raise ValueError(
                    "OddsPapi historical odds requires betfair-ex to be the "
                    "only bookmaker and exactly one outcome_id"
                )

        payload = self._request(
            "historical-odds",
            {
                "fixtureId": fixture_id,
                "bookmakers": self._comma_separated(cleaned_bookmakers),
                "id": historical_id,
                "playerId": player_id,
                "outcomeId": outcome_id,
                "active": active,
            },
        )
        if not isinstance(payload, dict):
            raise OddsPapiError(
                "OddsPapi /v4/historical-odds response must be an object"
            )
        return payload

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
