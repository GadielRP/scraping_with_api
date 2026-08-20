"""Small, proxy-free HTTP client for the OddsPapi v4 API."""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any

import requests

from infrastructure.settings import Config
from modules.oddspapi.api_key_inventory import api_key_fingerprint
from modules.oddspapi.api_key_scheduler import (
    ApiKeyLease,
    OddsPapiApiKeyScheduler,
    RequestOutcome,
)
from modules.oddspapi.endpoint_policy import EndpointPolicyRegistry
from modules.oddspapi.exceptions import OddsPapiError, OddsPapiHttpError

logger = logging.getLogger(__name__)


class OddsPapiClient:
    TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
    _key_endpoint_locks_guard = threading.Lock()
    _key_endpoint_locks: dict[tuple[str, str], threading.Lock] = {}
    _last_request_completed_at: dict[tuple[str, str], float] = {}

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        request_delay_seconds: float | None = None,
        fixtures_cooldown_seconds: float | None = None,
        endpoint_cooldowns: dict[str, float] | None = None,
        key_scheduler: OddsPapiApiKeyScheduler | None = None,
    ) -> None:
        self.base_url = (base_url or Config.ODDSPAPI_BASE_URL).rstrip("/")
        self.api_key = api_key
        self._key_scheduler = key_scheduler
        self._uses_dynamic_key = api_key is None
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

    def _scheduler(self) -> OddsPapiApiKeyScheduler:
        if self._key_scheduler is None:
            from modules.oddspapi.runtime import get_oddspapi_key_scheduler

            self._key_scheduler = get_oddspapi_key_scheduler()
        return self._key_scheduler

    def _cooldown_identity(self, endpoint: str, api_key: str) -> tuple[str, str]:
        return (api_key_fingerprint(api_key), endpoint)

    def _endpoint_lock(self, endpoint: str, api_key: str) -> threading.Lock:
        identity = self._cooldown_identity(endpoint, api_key)
        with OddsPapiClient._key_endpoint_locks_guard:
            return OddsPapiClient._key_endpoint_locks.setdefault(
                identity, threading.Lock()
            )

    @contextmanager
    def _endpoint_request_slot(self, endpoint: str, api_key: str):
        """Serialize requests per endpoint and enforce completion-to-start spacing."""
        cooldown_seconds = self.endpoint_cooldowns.get(endpoint, 0.0)
        if cooldown_seconds <= 0:
            yield
            return

        with self._endpoint_lock(endpoint, api_key):
            now = time.monotonic()
            identity = self._cooldown_identity(endpoint, api_key)
            last_completed_at = OddsPapiClient._last_request_completed_at.get(
                identity
            )
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
                OddsPapiClient._last_request_completed_at[identity] = (
                    time.monotonic()
                )

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

    def _execute_http_attempt(
        self,
        *,
        endpoint: str,
        url: str,
        base_request_params: dict,
        safe_params: dict,
    ):
        """Execute one physical request and always return its lease."""
        lease = self._acquire_lease(endpoint)
        outcome = RequestOutcome()
        try:
            request_params = dict(base_request_params)
            request_params["apiKey"] = lease.api_key
            logger.info(
                "✈️ OddsPapi GET /v4/%s key_id=%s params=%s",
                endpoint,
                lease.key_id,
                safe_params,
            )
            if lease.wait_seconds > 0:
                time.sleep(lease.wait_seconds)

            # Do not add a proxies argument: trust_env=False is the single
            # source of truth for the proxy-free OddsPapi session.
            with self._endpoint_request_slot(endpoint, lease.api_key):
                response = self.session.get(
                    url,
                    params=request_params,
                    timeout=self.timeout,
                )

            status_code = int(response.status_code)
            # Successful odds payloads can be large. Decode them only once in
            # the normal return path; error metadata is relevant only for a
            # non-2xx response and those bodies are small.
            if 200 <= status_code < 300:
                error_code = None
                retry_after_seconds = None
            else:
                error_code = self._error_code(response)
                retry_after_seconds = self._retry_after_seconds(response)
            outcome = RequestOutcome(
                status_code=status_code,
                error_code=error_code,
                response_received=True,
                retry_after_seconds=retry_after_seconds,
            )
            return (
                lease,
                response,
                status_code,
                error_code,
                retry_after_seconds,
            )
        except requests.RequestException:
            # Conservatively count network ambiguity for metered endpoints:
            # the server may have completed the call before the disconnect.
            outcome = RequestOutcome(network_error=True)
            raise
        finally:
            self._complete_lease(lease, outcome)

    def _request(self, endpoint: str, params: dict | None = None) -> dict | list:
        if not self._uses_dynamic_key and not str(self.api_key or "").strip():
            raise ValueError("ODDSPAPI_KEY is required to make an OddsPapi request")

        normalized_endpoint = self._normalize_endpoint(endpoint)
        if not normalized_endpoint:
            raise ValueError("OddsPapi endpoint is required")

        url = f"{self.base_url}/v4/{normalized_endpoint}"
        base_request_params = {
            key: value
            for key, value in (params or {}).items()
            if value is not None and str(key).lower() != "apikey"
        }
        safe_params = dict(base_request_params)

        attempts = max(1, int(self.max_retries))
        transient_attempt = 0
        credential_failovers = 0
        max_credential_failovers = (
            self._scheduler().available_key_count(normalized_endpoint)
            if self._uses_dynamic_key
            else 0
        )
        retry_delay_seconds = self.request_delay_seconds
        while transient_attempt < attempts:
            if transient_attempt and self.request_delay_seconds > 0:
                time.sleep(retry_delay_seconds)

            try:
                (
                    lease,
                    response,
                    status_code,
                    error_code,
                    retry_after_seconds,
                ) = self._execute_http_attempt(
                    endpoint=normalized_endpoint,
                    url=url,
                    base_request_params=base_request_params,
                    safe_params=safe_params,
                )
            except requests.RequestException as exc:
                transient_attempt += 1
                if transient_attempt < attempts:
                    logger.warning(
                        "Transient OddsPapi request error for /v4/%s (attempt %s/%s): %s",
                        normalized_endpoint,
                        transient_attempt,
                        attempts,
                        type(exc).__name__,
                    )
                    continue
                raise OddsPapiError(
                    f"OddsPapi request failed endpoint=/v4/{normalized_endpoint} "
                    f"error={type(exc).__name__}"
                ) from exc

            rejected_credential = (
                error_code == "REQUEST_LIMIT_EXCEEDED"
                or status_code == 401
                or error_code in {"INVALID_API_KEY", "INVALID_KEY", "UNAUTHORIZED"}
            )
            if (
                self._uses_dynamic_key
                and rejected_credential
                and credential_failovers < max_credential_failovers
            ):
                credential_failovers += 1
                logger.warning(
                    "OddsPapi credential rejected endpoint=/v4/%s key_id=%s "
                    "error_code=%s; selecting another key",
                    normalized_endpoint,
                    lease.key_id,
                    error_code or f"HTTP_{status_code}",
                )
                continue

            transient_attempt += 1
            if status_code in self.TRANSIENT_STATUS_CODES and transient_attempt < attempts:
                retry_delay_seconds = max(
                    self.request_delay_seconds,
                    retry_after_seconds or 0.0,
                )
                logger.warning(
                    "Transient OddsPapi HTTP %s for /v4/%s (attempt %s/%s)",
                    status_code,
                    normalized_endpoint,
                    transient_attempt,
                    attempts,
                )
                continue

            if status_code < 200 or status_code >= 300:
                response_text = str(getattr(response, "text", "") or "").replace("\n", " ")[:500]
                response_text = response_text.replace(str(lease.api_key), "***")
                raise OddsPapiHttpError(
                    status_code=status_code,
                    endpoint=f"/v4/{normalized_endpoint}",
                    response_text=response_text,
                    error_code=error_code,
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

    def _acquire_lease(self, endpoint: str) -> ApiKeyLease:
        if self._uses_dynamic_key:
            return self._scheduler().acquire(endpoint)
        api_key = str(self.api_key or "").strip()
        return ApiKeyLease(
            api_key=api_key,
            key_fingerprint=api_key_fingerprint(api_key),
            endpoint=endpoint,
            quota_policy=EndpointPolicyRegistry().policy_for(endpoint),
            sequence=0,
        )

    def _complete_lease(self, lease: ApiKeyLease, outcome: RequestOutcome) -> None:
        if self._uses_dynamic_key:
            self._scheduler().complete(lease, outcome)

    def get_fixture(self, fixture_id: str, language: str | None = None) -> dict:
        return self._request("fixture", {"fixtureId": fixture_id, "language": language})

    def get_account(self) -> dict:
        payload = self._request("account")
        if not isinstance(payload, dict):
            raise OddsPapiError("OddsPapi /v4/account response must be an object")
        return payload

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
        logger.info("💰 Fetching oddspapi odds for fixture_id: %s", fixture_id)
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
