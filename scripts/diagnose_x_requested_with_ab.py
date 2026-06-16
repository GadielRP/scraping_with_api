from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests

from infrastructure.network import ProxyIdentityManager
from infrastructure.settings import Config
from modules.sofascore.challenge import extract_relevant_headers, is_sofascore_challenge_response


DEFAULT_ENDPOINT = "/sport/football/events/live"
DEFAULT_OUTPUT = "logs/sofascore_x_requested_with_ab_metrics.jsonl"


def safe_token_fingerprint(token: str | None) -> str:
    if not token:
        return "none"
    digest = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    return f"sha256:{digest[:10]}"


def safe_token_suffix(token: str | None) -> str:
    if not token:
        return "none"
    token = str(token)
    return token[-2:] if len(token) >= 2 else "**"


def safe_token_context(token: str | None, kind: str, header_sent: bool) -> dict:
    return {
        "header_sent": bool(header_sent),
        "value_non_empty": bool(token),
        "kind": kind,
        "fingerprint": safe_token_fingerprint(token),
        "suffix": safe_token_suffix(token),
    }


def build_test_headers(x_requested_with: str | None) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Origin": "https://www.sofascore.com",
        "Referer": "https://www.sofascore.com/",
        "Sec-Ch-Ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if x_requested_with is not None:
        headers["X-Requested-With"] = x_requested_with
    return headers


def build_proxy_context(proxy_manager: ProxyIdentityManager, identity) -> dict:
    return {
        "enabled": bool(getattr(identity, "enabled", False)),
        "provider": getattr(proxy_manager, "provider", None),
        "mode": getattr(proxy_manager, "mode", None),
        "endpoint": getattr(proxy_manager, "endpoint", None),
        "generation": getattr(identity, "generation", 0) if identity is not None else 0,
    }


def safe_response_headers(response_headers) -> dict:
    allowed = {
        "server",
        "cf-ray",
        "cf-cache-status",
        "cf-mitigated",
        "content-type",
        "date",
    }
    extracted = extract_relevant_headers(response_headers or {})
    return {key: extracted[key] for key in allowed if key in extracted}


def classify_response(response) -> tuple[str, bool, dict | None, str, dict]:
    status_code = int(getattr(response, "status_code", 0) or 0)
    body_text = str(getattr(response, "text", "") or "")
    body_kind = "empty" if not body_text.strip() else "text"
    challenge_detected = bool(is_sofascore_challenge_response(response))
    parsed_json = None

    if status_code == 200:
        try:
            parsed_json = response.json()
            body_kind = "json"
            return "OK_JSON", challenge_detected, parsed_json, body_kind, safe_response_headers(getattr(response, "headers", {}) or {})
        except Exception:
            return "ERROR", challenge_detected, None, body_kind, safe_response_headers(getattr(response, "headers", {}) or {})

    if challenge_detected:
        return "CHALLENGE", challenge_detected, None, body_kind, safe_response_headers(getattr(response, "headers", {}) or {})

    if status_code == 429:
        return "RATE_LIMIT", challenge_detected, None, body_kind, safe_response_headers(getattr(response, "headers", {}) or {})

    if status_code == 404:
        return "NOT_FOUND", challenge_detected, None, body_kind, safe_response_headers(getattr(response, "headers", {}) or {})

    return "ERROR", challenge_detected, None, body_kind, safe_response_headers(getattr(response, "headers", {}) or {})


def build_metric_record(
    *,
    case_name: str,
    endpoint: str,
    status_code: int | None,
    logical_result: str,
    response_ms: float,
    body_kind: str,
    parsed_json,
    challenge_detected: bool,
    token_context: dict,
    response_headers: dict,
    proxy_context: dict,
    error_message: str | None = None,
    skipped_reason: str | None = None,
) -> dict:
    has_events_key = isinstance(parsed_json, dict) and "events" in parsed_json
    has_error_key = isinstance(parsed_json, dict) and "error" in parsed_json

    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "case_name": case_name,
        "endpoint": endpoint,
        "status_code": status_code,
        "logical_result": logical_result,
        "response_ms": round(response_ms, 2),
        "body_kind": body_kind,
        "has_events_key": has_events_key,
        "has_error_key": has_error_key,
        "challenge_detected": challenge_detected,
        "token_context": token_context,
        "response_headers": response_headers,
        "proxy": proxy_context,
    }
    if error_message:
        record["error"] = error_message
    if skipped_reason:
        record["skipped_reason"] = skipped_reason
    return record


def print_summary(records: list[dict], endpoints: list[str]) -> None:
    print(f"Endpoints: {', '.join(endpoints)}")
    if not records:
        print("No records collected.")
        return

    columns = [
        ("case_name", "CASE"),
        ("logical_result", "RESULT"),
        ("status_code", "STATUS"),
        ("response_ms", "MS"),
        ("body_kind", "BODY"),
        ("challenge_detected", "CHALLENGE"),
        ("token_context", "TOKEN"),
    ]

    rows = []
    for record in records:
        token_ctx = record.get("token_context", {})
        token_label = (
            f"{token_ctx.get('kind', 'unknown')}:"
            f"{'yes' if token_ctx.get('header_sent') else 'no'}/"
            f"{'yes' if token_ctx.get('value_non_empty') else 'no'}"
        )
        rows.append(
            {
                "case_name": record.get("case_name", ""),
                "logical_result": record.get("logical_result", ""),
                "status_code": "" if record.get("status_code") is None else str(record.get("status_code")),
                "response_ms": f"{record.get('response_ms', 0):.1f}",
                "body_kind": record.get("body_kind", ""),
                "challenge_detected": "yes" if record.get("challenge_detected") else "no",
                "token_context": token_label,
            }
        )

    widths = {}
    for key, label in columns:
        widths[key] = max(len(label), max(len(str(row[key])) for row in rows))

    header_line = " | ".join(label.ljust(widths[key]) for key, label in columns)
    separator = "-+-".join("-" * widths[key] for key, _ in columns)
    print(header_line)
    print(separator)
    for row in rows:
        print(
            " | ".join(
                str(row[key]).ljust(widths[key])
                for key, _ in columns
            )
        )

    counts = Counter(record.get("logical_result", "") for record in records)
    print("Summary:")
    for result, count in sorted(counts.items()):
        print(f"  {result}: {count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose SofaScore X-Requested-With behavior.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--endpoints", default=None)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def prepare_test_cases() -> list[tuple[str, str | None, str, bool]]:
    valid_tokens = list(getattr(Config, "X_REQUESTED_WITH_HEADER_TOKENS", []) or [])
    first_configured_token_or_none = valid_tokens[0] if valid_tokens else None

    cases: list[tuple[str, str | None, str, bool]] = [
        ("missing_header", None, "missing", False),
        ("empty_header", "", "empty", True),
        ("classic_ajax", "XMLHttpRequest", "classic_ajax", True),
        ("known_frontend_token", first_configured_token_or_none, "known_frontend_token", True),
        ("random_hex", secrets.token_hex(3), "random_hex", True),
        ("plain_text", "hello-world", "plain_text", True),
    ]
    return cases


def parse_endpoint_list(args: argparse.Namespace) -> list[str]:
    if args.endpoints:
        return [
            endpoint.strip() if endpoint.strip().startswith("/") else f"/{endpoint.strip()}"
            for endpoint in args.endpoints.split(",")
            if endpoint.strip()
        ]

    endpoint = args.endpoint if args.endpoint.startswith("/") else f"/{args.endpoint}"
    return [endpoint]



def main() -> int:
    args = parse_args()
    base_url = Config.SOFASCORE_BASE_URL.rstrip("/")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    repeat_total = max(1, int(args.repeat))

    proxy_manager = ProxyIdentityManager(Config, client_name="sofascore")
    session = requests.Session(impersonate="chrome136")
    identity = None
    if getattr(Config, "PROXY_ENABLED", False):
        identity = proxy_manager.get_identity(reason="diagnostic_ab")
        proxies = proxy_manager.build_requests_proxies(identity)
        if proxies:
            session.proxies = proxies

    proxy_context = build_proxy_context(proxy_manager, identity)
    records: list[dict] = []
    cases = prepare_test_cases()
    endpoints = parse_endpoint_list(args)

    try:
        for repeat_index in range(repeat_total):
            for endpoint in endpoints:
                url = f"{base_url}{endpoint}"
                for case_name, token, kind, header_sent in cases:
                    is_last_request = (
                        repeat_index == repeat_total - 1
                        and endpoint == endpoints[-1]
                        and case_name == cases[-1][0]
                    )
                    if case_name == "known_frontend_token" and token is None:
                        record = build_metric_record(
                            case_name=case_name,
                            endpoint=endpoint,
                            status_code=None,
                            logical_result="SKIPPED",
                            response_ms=0.0,
                            body_kind="empty",
                            parsed_json=None,
                            challenge_detected=False,
                            token_context=safe_token_context(None, kind, False),
                            response_headers={},
                            proxy_context=proxy_context,
                            skipped_reason="No valid configured X-Requested-With token",
                        )
                        records.append(record)
                        with output_path.open("a", encoding="utf-8") as handle:
                            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                        if not is_last_request:
                            time.sleep(max(0.0, float(args.sleep)))
                        continue

                    headers = build_test_headers(token if header_sent else None)
                    start_time = time.perf_counter()
                    error_message = None
                    try:
                        response = session.get(url, headers=headers, timeout=30)
                        logical_result, challenge_detected, parsed_json, body_kind, response_headers = classify_response(response)
                        status_code = int(getattr(response, "status_code", 0) or 0)
                    except Exception as exc:
                        response = None
                        logical_result = "ERROR"
                        challenge_detected = False
                        parsed_json = None
                        body_kind = "empty"
                        response_headers = {}
                        status_code = None
                        error_message = f"{type(exc).__name__}: {exc}"
                    response_ms = (time.perf_counter() - start_time) * 1000

                    record = build_metric_record(
                        case_name=case_name,
                        endpoint=endpoint,
                        status_code=status_code,
                        logical_result=logical_result,
                        response_ms=response_ms,
                        body_kind=body_kind,
                        parsed_json=parsed_json,
                        challenge_detected=challenge_detected,
                        token_context=safe_token_context(token, kind, header_sent),
                        response_headers=response_headers,
                        proxy_context=proxy_context,
                        error_message=error_message,
                    )
                    records.append(record)

                    with output_path.open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

                    if not is_last_request:
                        time.sleep(max(0.0, float(args.sleep)))
    finally:
        session.close()

    print_summary(records, endpoints)
    print(f"Wrote {len(records)} records to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
