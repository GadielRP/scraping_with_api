"""Probe an event's public details endpoint through the configured client.

Run with::

    python -m scripts.diagnostics.sofascore_event_endpoint 17108553

The script uses the normal SofaScore client, proxy, rate limiter, and challenge
handling; it does not alter browser or TLS fingerprints.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _summarize_payload(payload: Any) -> str:
    if isinstance(payload, dict):
        return f"payload_type=dict top_level_keys={sorted(str(key) for key in payload)[:12]}"
    if isinstance(payload, list):
        return f"payload_type=list item_count={len(payload)}"
    return f"payload_type={type(payload).__name__}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe /event/{id}, distinct from the event odds endpoint."
    )
    parser.add_argument("event_id", type=int, help="SofaScore event ID")
    args = parser.parse_args()

    from app.logging_setup import setup_logging

    setup_logging()

    from modules.sofascore import (
        SofaScoreChallengeCircuitOpenException,
        SofaScoreChallengeException,
        SofaScoreNotFoundException,
        SofaScoreRateLimitException,
        api_client,
    )

    endpoint = f"/event/{args.event_id}"
    logger.info("Probing SofaScore event details endpoint %s", endpoint)
    try:
        payload = api_client.request_json(endpoint)
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
        logger.warning("Probe result: event endpoint not found: %s", exc.endpoint)
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
