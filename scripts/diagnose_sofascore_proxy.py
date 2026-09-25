"""Separate Decodo gateway connectivity from SofaScore's HTTP response."""

from __future__ import annotations

import argparse
import ipaddress
import socket
import time

from curl_cffi import requests
from curl_cffi.const import CurlECode

from infrastructure.network import ProxyIdentityManager
from infrastructure.settings import Config


def _curl_error_fields(error: Exception) -> str:
    code = getattr(error, "code", None)
    try:
        code_number = int(code) if code is not None else None
        code_name = CurlECode(code_number).name if code_number is not None else "unavailable"
    except (TypeError, ValueError):
        code_number, code_name = code, "unknown"
    return (
        f"exception={type(error).__module__}.{type(error).__qualname__} "
        f"curl_code={code_number} curl_code_name={code_name}"
    )


def _masked_ip(value: object) -> str:
    if not isinstance(value, str):
        return "unavailable"
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        return "unavailable"
    if parsed.version == 4:
        return ".".join(str(parsed).split(".")[:3]) + ".x"
    return ":".join(parsed.exploded.split(":")[:3]) + ":…"


def _test_gateway_tcp(host: str, port: int) -> None:
    print(f"[1/3] Gateway DNS/TCP: {host}:{port}")
    try:
        addresses = sorted(
            {item[4][0] for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}
        )
        print(f"  DNS: OK ({len(addresses)} address(es))")
    except Exception as exc:
        print(f"  DNS: FAIL ({type(exc).__name__}: {exc})")
        return

    try:
        with socket.create_connection((host, port), timeout=5):
            pass
        print("  TCP: OK; this machine can reach the Decodo gateway port")
    except PermissionError as exc:
        print(f"  TCP: INCONCLUSIVE; this runtime blocks raw socket checks ({exc})")
    except OSError as exc:
        print(f"  TCP: FAIL ({type(exc).__name__}: {exc})")


def _test_http(session, url: str, label: str, *, headers=None) -> object | None:
    print(f"[{label}] GET {url}")
    started = time.perf_counter()
    try:
        response = session.get(url, headers=headers, timeout=15)
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        print(f"  HTTP: NO RESPONSE ({elapsed_ms} ms; {_curl_error_fields(exc)})")
        return None

    elapsed_ms = round((time.perf_counter() - started) * 1000)
    print(
        f"  HTTP: {response.status_code} ({elapsed_ms} ms); "
        f"server={response.headers.get('server', 'unavailable')}; "
        f"content_type={response.headers.get('content-type', 'unavailable')}"
    )
    return response


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test Decodo gateway reachability, proxy egress, and SofaScore response separately."
    )
    parser.add_argument(
        "event_id",
        nargs="?",
        default="17108553",
        help="SofaScore event ID to check (default: 17108553)",
    )
    args = parser.parse_args()

    manager = ProxyIdentityManager(Config, client_name="sofascore")
    identity = manager.get_identity()
    print(
        "Proxy config: "
        f"configured={manager.proxy_enabled} active={identity.enabled} "
        f"provider={identity.provider} mode={identity.mode} "
        f"endpoint={identity.endpoint} country={identity.country or 'default'} "
        f"generation={identity.generation} "
        f"sticky_session_present={bool(identity.session_token)}"
    )
    if not identity.enabled:
        print("Proxy is disabled or credentials/configuration are incomplete; no HTTP checks sent.")
        return 2

    host, separator, raw_port = identity.endpoint.rpartition(":")
    if not separator or not raw_port.isdigit():
        print("Gateway endpoint must be configured as host:port; HTTP checks not sent.")
        return 2
    _test_gateway_tcp(host, int(raw_port))

    session = requests.Session(impersonate="chrome136")
    session.proxies = manager.build_requests_proxies(identity)
    try:
        ip_response = _test_http(
            session,
            "https://ip.decodo.com/json",
            "2/3 Decodo proxy egress",
        )
        if ip_response is not None and ip_response.status_code == 200:
            try:
                payload = ip_response.json()
                proxy_info = payload.get("proxy", payload) if isinstance(payload, dict) else {}
                print(
                    f"  Exit IP: {_masked_ip(proxy_info.get('ip'))}; "
                    f"country={proxy_info.get('country', 'unavailable')}"
                )
            except Exception:
                print("  Exit IP: response was not valid JSON")

        event_id = str(args.event_id).strip()
        if not event_id.isdigit():
            parser.error("event_id must contain digits only")
        sofascore_url = (
            f"{Config.SOFASCORE_BASE_URL}/event/{event_id}/odds/1/all"
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
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
        x_requested_with = getattr(Config, "SOFASCORE_X_REQUESTED_WITH", None)
        if x_requested_with:
            headers["X-Requested-With"] = x_requested_with
        score_response = _test_http(
            session,
            sofascore_url,
            "3/3 SofaScore through the same proxy",
            headers=headers,
        )
        if score_response is not None:
            print(f"  Varnish: {score_response.headers.get('server', '').lower() == 'varnish'}")
            print(f"  Retry-After: {score_response.headers.get('retry-after', 'unavailable')}")
            try:
                body = score_response.json()
            except Exception:
                print("  Body: non-JSON response (not printed)")
            else:
                if isinstance(body, dict) and isinstance(body.get("error"), dict):
                    print(
                        "  SofaScore error: "
                        f"code={body['error'].get('code', 'unavailable')} "
                        f"reason={body['error'].get('reason', 'unavailable')}"
                    )
                else:
                    print("  Body: valid JSON response")
    finally:
        session.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
