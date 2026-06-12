"""Detection and safe evidence capture for SofaScore challenge responses."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CHALLENGE_REASON = "challenge"
DEFAULT_EVIDENCE_PATH = "logs/sofascore_challenge_evidence.jsonl"

SAFE_CHALLENGE_HEADERS = {
    "server",
    "cf-ray",
    "cf-cache-status",
    "cf-mitigated",
    "content-type",
    "content-length",
    "set-cookie",
    "date",
    "vary",
    "x-frame-options",
    "strict-transport-security",
}

_COOKIE_SPLIT_PATTERN = re.compile(r",\s*(?=[!#$%&'*+\-.^_`|~0-9A-Za-z]+=)")
_COOKIE_NAME_PATTERN = re.compile(r"^\s*([!#$%&'*+\-.^_`|~0-9A-Za-z]+)=")

logger = logging.getLogger(__name__)


def safe_json_loads(text: str) -> dict | None:
    if not text or not isinstance(text, str):
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def extract_cookie_names(set_cookie_header: str | None) -> list[str]:
    if not set_cookie_header:
        return []

    cookie_names: list[str] = []
    seen: set[str] = set()
    for segment in _COOKIE_SPLIT_PATTERN.split(set_cookie_header):
        first_part = segment.split(";", 1)[0]
        match = _COOKIE_NAME_PATTERN.match(first_part)
        if not match:
            continue
        name = match.group(1)
        if name not in seen:
            seen.add(name)
            cookie_names.append(name)
    return cookie_names


def extract_relevant_headers(headers: Any) -> dict:
    if not headers:
        return {}

    if hasattr(headers, "items"):
        header_items = list(headers.items())
    else:
        header_items = list(dict(headers).items())

    result: dict[str, Any] = {}
    for key, value in header_items:
        lower_key = str(key).lower()
        if lower_key not in SAFE_CHALLENGE_HEADERS:
            continue
        if lower_key == "set-cookie":
            result["set-cookie-names"] = extract_cookie_names(str(value) if value is not None else None)
            continue
        result[lower_key] = value
    return result


def body_preview(text: str, max_chars: int = 500) -> str:
    if not text or not isinstance(text, str):
        return ""
    preview = re.sub(r"\s+", " ", text).strip()
    return preview[:max_chars]


def _headers_look_like_cloudflare(headers: dict) -> bool:
    server = str(headers.get("server", "")).lower()
    cf_ray = headers.get("cf-ray")
    cf_cache_status = headers.get("cf-cache-status")
    strict_transport_security = headers.get("strict-transport-security")
    x_frame_options = headers.get("x-frame-options")

    return bool(
        "cloudflare" in server
        or cf_ray
        or cf_cache_status
        or strict_transport_security
        or x_frame_options
    )


def is_sofascore_challenge_response(response) -> bool:
    try:
        status_code = int(getattr(response, "status_code", 0) or 0)
    except Exception:
        return False

    if status_code != 403:
        return False

    parsed = safe_json_loads(getattr(response, "text", ""))
    if parsed:
        nested_error = parsed.get("error")
        if isinstance(nested_error, dict):
            if nested_error.get("code") == 403 and str(nested_error.get("reason", "")).lower() == CHALLENGE_REASON:
                return True
        if parsed.get("code") == 403 and str(parsed.get("reason", "")).lower() == CHALLENGE_REASON:
            return True

    headers = extract_relevant_headers(getattr(response, "headers", {}) or {})
    if "cf-mitigated" in headers:
        return True

    text = str(getattr(response, "text", "") or "").lower()
    if "challenge" in text and _headers_look_like_cloudflare(headers):
        return True

    return False


def get_challenge_reason(response) -> str:
    parsed = safe_json_loads(getattr(response, "text", ""))
    if parsed:
        nested_error = parsed.get("error")
        if isinstance(nested_error, dict):
            reason = nested_error.get("reason")
            if reason:
                return str(reason)
        reason = parsed.get("reason")
        if reason:
            return str(reason)

    if is_sofascore_challenge_response(response):
        return CHALLENGE_REASON

    return "unknown"


def _sanitize_proxy_identity(proxy_identity: Any) -> dict:
    if proxy_identity is None:
        return {"enabled": False}

    proxy: dict[str, Any] = {
        "enabled": bool(getattr(proxy_identity, "enabled", False)),
        "provider": getattr(proxy_identity, "provider", None),
        "mode": getattr(proxy_identity, "mode", None),
        "endpoint": getattr(proxy_identity, "endpoint", None),
        "generation": getattr(proxy_identity, "generation", None),
        "client_name": getattr(proxy_identity, "client_name", None),
        "country": getattr(proxy_identity, "country", None),
        "city": getattr(proxy_identity, "city", None),
    }

    session_label = getattr(proxy_identity, "session_label", None)
    if session_label:
        proxy["session_label"] = session_label
    return proxy


def build_challenge_evidence(
    *,
    response,
    endpoint: str,
    base_url: str,
    attempt: int,
    max_retries: int,
    params: dict | None,
    proxy_identity=None,
    request_url: str | None = None,
) -> dict:
    headers = extract_relevant_headers(getattr(response, "headers", {}) or {})
    evidence = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "type": "sofascore_challenge",
        "endpoint": endpoint,
        "base_url": base_url,
        "request_url": request_url or f"{base_url}{endpoint}",
        "status_code": int(getattr(response, "status_code", 0) or 0),
        "reason": get_challenge_reason(response),
        "attempt": attempt,
        "max_retries": max_retries,
        "response_headers": headers,
        "body_preview": body_preview(getattr(response, "text", "") or ""),
        "proxy": _sanitize_proxy_identity(proxy_identity),
        "params_keys": sorted(list((params or {}).keys())),
    }
    return evidence


def write_challenge_evidence(
    evidence: dict,
    evidence_path: str = DEFAULT_EVIDENCE_PATH,
) -> None:
    try:
        path = Path(evidence_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(payload + "\n")
    except Exception as exc:
        logger.warning("Unable to write SofaScore challenge evidence to %s: %s", evidence_path, exc)
