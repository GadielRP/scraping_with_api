"""Probe the public country endpoint through the configured SofaScore client.

Unlike the offline diagnostics in this package, this command makes a live API
request. It deliberately uses the application's existing client, proxy policy,
rate limit, challenge handling, and evidence logging.

Run with::

    python -m scripts.diagnostics.sofascore_country_endpoint
"""

from __future__ import annotations

import ipaddress
import logging
from typing import Any


ENDPOINT = "/country/alpha2"
logger = logging.getLogger(__name__)


def _summarize_payload(payload: Any) -> str:
    if isinstance(payload, dict):
        keys = sorted(str(key) for key in payload.keys())
        location = {
            key: payload[key]
            for key in ("alpha2", "country", "city", "continent_code", "region_code")
            if isinstance(payload.get(key), (str, int, float, bool))
        }
        raw_ip = payload.get("ip")
        if isinstance(raw_ip, str):
            try:
                parsed_ip = ipaddress.ip_address(raw_ip)
                location["ip_masked"] = (
                    ".".join(str(parsed_ip).split(".")[:3]) + ".x"
                    if parsed_ip.version == 4
                    else ":".join(parsed_ip.exploded.split(":")[:3]) + ":…"
                )
            except ValueError:
                location["ip_masked"] = "unavailable"
        return (
            f"payload_type=dict top_level_keys={keys[:12]} "
            f"location_fields={location}"
        )
    if isinstance(payload, list):
        return f"payload_type=list item_count={len(payload)}"
    return f"payload_type={type(payload).__name__}"


def main() -> int:
    from app.logging_setup import setup_logging

    setup_logging()

    from modules.sofascore import (
        SofaScoreChallengeCircuitOpenException,
        SofaScoreChallengeException,
        SofaScoreNotFoundException,
        SofaScoreRateLimitException,
        api_client,
    )

    logger.info("Probing SofaScore endpoint %s with the configured client", ENDPOINT)
    try:
        payload = api_client.request_json(ENDPOINT)
    except SofaScoreChallengeCircuitOpenException as exc:
        logger.warning(
            "Probe skipped because the challenge circuit is open: retry_after=%ss",
            exc.retry_after_seconds,
        )
        return 2
    except SofaScoreChallengeException as exc:
        headers = exc.evidence.get("response_headers", {})
        logger.error(
            "Probe result: HTTP 403 challenge; reason=%s server=%s endpoint=%s",
            exc.reason,
            headers.get("server", "unavailable"),
            exc.endpoint,
        )
        return 1
    except SofaScoreNotFoundException as exc:
        logger.warning("Probe result: endpoint not found: %s", exc.endpoint)
        return 1
    except SofaScoreRateLimitException as exc:
        logger.warning("Probe result: rate limited: %s", exc.endpoint)
        return 1

    if payload is None:
        logger.error(
            "Probe result: no JSON response; see the SofaScore transport diagnostics above"
        )
        return 1

    logger.info("Probe result: HTTP 200; %s", _summarize_payload(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
