"""HTTP transport and retry policy for SofaScore requests."""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Dict, Optional, Protocol

from infrastructure.settings import Config
from shared.shutdown import is_shutdown_requested

from .challenge import (
    body_preview,
    build_challenge_evidence,
    get_challenge_reason,
    is_sofascore_challenge_response,
    write_challenge_evidence,
)
from .exceptions import (
    SofaScoreChallengeException,
    SofaScoreNotFoundException,
    SofaScoreRateLimitException,
)

logger = logging.getLogger(__name__)


class SofaScoreTransportClient(Protocol):
    """Client capabilities required by the transport policy."""

    base_url: str
    challenge_evidence_enabled: bool
    proxy_identity: object
    proxy_manager: object
    session: object
    _proxy_error_streak: int

    def _build_headers(self) -> Dict[str, str]: ...
    def _extract_endpoint_event_id(self, endpoint: str) -> int: ...
    def _rate_limit(self) -> None: ...
    def _rotate_proxy_identity(self, reason: str) -> None: ...
    def _should_capture_challenge_evidence(self) -> bool: ...


def _safe_token_fingerprint(token: str | None) -> str:
    if not token:
        return "none"
    digest = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    return f"sha256:{digest[:10]}"


def _safe_token_suffix(token: str | None) -> str:
    if not token:
        return "none"
    normalized_token = str(token)
    return normalized_token[-2:] if len(normalized_token) >= 2 else "**"


def _safe_token_context(token: str | None, header_sent: bool) -> dict:
    return {
        "x_requested_with_header_sent": bool(header_sent),
        "x_requested_with_value_non_empty": bool(token),
        "x_requested_with_fingerprint": _safe_token_fingerprint(token),
        "x_requested_with_suffix": _safe_token_suffix(token),
    }


def request_json(
    client: SofaScoreTransportClient,
    endpoint: str,
    params: Optional[Dict] = None,
) -> Optional[Dict]:
    """Execute one JSON request according to the SofaScore retry policy."""
    url = f"{client.base_url}{endpoint}"
    headers = client._build_headers()
    x_requested_with_token = headers.get("X-Requested-With")

    for attempt in range(Config.MAX_RETRIES):
        try:
            if is_shutdown_requested():
                raise KeyboardInterrupt()

            client._rate_limit()
            if is_shutdown_requested():
                raise KeyboardInterrupt()

            logger.debug("Making request to: %s", url)
            response = client.session.get(
                url,
                headers=headers,
                params=params,
                timeout=30,
            )

            if response.status_code == 200:
                client._proxy_error_streak = 0
                return response.json()

            if is_sofascore_challenge_response(response):
                reason = get_challenge_reason(response)
                token_context = _safe_token_context(
                    x_requested_with_token,
                    "X-Requested-With" in headers,
                )
                evidence = {"request_token_context": token_context}

                logger.info(
                    "SofaScore challenge token context: "
                    "token_fingerprint=%s token_suffix=%s endpoint=%s",
                    token_context["x_requested_with_fingerprint"],
                    token_context["x_requested_with_suffix"],
                    endpoint,
                )

                if client._should_capture_challenge_evidence():
                    challenge_evidence = build_challenge_evidence(
                        response=response,
                        endpoint=endpoint,
                        base_url=client.base_url,
                        attempt=attempt + 1,
                        max_retries=Config.MAX_RETRIES,
                        params=params,
                        proxy_identity=client.proxy_identity,
                        request_url=url,
                    )
                    challenge_evidence["request_token_context"] = token_context
                    evidence = challenge_evidence
                    write_challenge_evidence(evidence)
                else:
                    logger.debug(
                        "SofaScore challenge evidence capture disabled for %s "
                        "(debug_mode=%s)",
                        endpoint,
                        client.challenge_evidence_enabled,
                    )

                logger.error(
                    "SofaScore challenge detected for %s, reason=%s, attempt %s/%s",
                    endpoint,
                    reason,
                    attempt + 1,
                    Config.MAX_RETRIES,
                )

                if (
                    attempt == 0
                    and client.proxy_manager.should_rotate_on_sofascore_error()
                    and Config.MAX_RETRIES > 1
                ):
                    client._rotate_proxy_identity(
                        reason=f"http_403_challenge_attempt_{attempt + 1}_{endpoint}"
                    )
                    continue

                raise SofaScoreChallengeException(
                    client._extract_endpoint_event_id(endpoint),
                    endpoint=endpoint,
                    reason=reason,
                    evidence=evidence,
                )

            if response.status_code == 407:
                wait_time = min(30 * (2**attempt), 300)
                logger.warning(
                    "Proxy authentication error (407) for %s, waiting %ss, attempt %s/%s",
                    endpoint,
                    wait_time,
                    attempt + 1,
                    Config.MAX_RETRIES,
                )
                if client.proxy_manager.should_rotate_on_sofascore_error():
                    client._rotate_proxy_identity(
                        reason=f"http_407_attempt_{attempt + 1}_{endpoint}"
                    )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
                break

            if response.status_code == 429:
                wait_time = min(60 * (2**attempt), 600)
                logger.warning(
                    "Rate limited (429) for %s, waiting %ss, attempt %s/%s",
                    endpoint,
                    wait_time,
                    attempt + 1,
                    Config.MAX_RETRIES,
                )
                if client.proxy_manager.should_rotate_on_sofascore_error():
                    client._rotate_proxy_identity(
                        reason=f"http_429_attempt_{attempt + 1}_{endpoint}"
                    )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
                raise SofaScoreRateLimitException(
                    client._extract_endpoint_event_id(endpoint),
                    endpoint=endpoint,
                    status_code=response.status_code,
                )

            if response.status_code == 404:
                logger.debug("HTTP 404 for %s - skipping retries", endpoint)
                raise SofaScoreNotFoundException(
                    client._extract_endpoint_event_id(endpoint),
                    endpoint=endpoint,
                )

            if response.status_code == 403:
                wait_time = min(30 * (2**attempt), 300)
                logger.warning(
                    "HTTP 403 for %s, waiting %ss, attempt %s/%s, body=%s",
                    endpoint,
                    wait_time,
                    attempt + 1,
                    Config.MAX_RETRIES,
                    body_preview(getattr(response, "text", "") or ""),
                )
                if client.proxy_manager.should_rotate_on_sofascore_error():
                    client._rotate_proxy_identity(
                        reason=f"http_403_attempt_{attempt + 1}_{endpoint}"
                    )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
                raise SofaScoreRateLimitException(
                    client._extract_endpoint_event_id(endpoint),
                    endpoint=endpoint,
                    status_code=response.status_code,
                )

            if response.status_code in [500, 502, 503, 504, 522, 525]:
                wait_time = min(5 * (2**attempt), 60)
                logger.warning(
                    "HTTP %s for %s, waiting %ss, attempt %s/%s",
                    response.status_code,
                    endpoint,
                    wait_time,
                    attempt + 1,
                    Config.MAX_RETRIES,
                )
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
                break

            logger.error(
                "HTTP %s for %s: %s",
                response.status_code,
                endpoint,
                response.text,
            )
            break
        except (
            SofaScoreChallengeException,
            SofaScoreNotFoundException,
            SofaScoreRateLimitException,
        ):
            raise
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            if is_shutdown_requested():
                logger.info("Shutdown requested while requesting %s", endpoint)
                raise KeyboardInterrupt() from exc
            logger.error("Unexpected error for %s: %s", endpoint, exc)
            break

    return None
