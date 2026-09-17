#!/usr/bin/env python3
import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

from curl_cffi import requests
from playwright.async_api import async_playwright


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import Config
from proxy_manager import ProxyIdentityManager


IP_INFO_URL = "https://ipwho.is/"
IP_ONLY_URL = "https://api.ipify.org/?format=json"


def _fetch_identity_info(proxy_manager: ProxyIdentityManager, *, rotate: bool = False) -> Tuple[Dict, str]:
    identity = proxy_manager.get_identity(
        rotate_session=rotate,
        reason="manual_test_rotation" if rotate else "manual_test_initial",
    )
    proxies = proxy_manager.build_requests_proxies(identity)
    session = requests.Session(impersonate="chrome120")
    session.proxies = proxies
    response = session.get(IP_INFO_URL, timeout=45)
    response.raise_for_status()
    return response.json(), proxy_manager.session_label(identity)


async def _playwright_ip_check(proxy_manager: ProxyIdentityManager) -> Optional[str]:
    identity = proxy_manager.get_identity(rotate_session=False, reason="playwright_check")
    launch_proxy = proxy_manager.build_playwright_proxy(identity)
    if not launch_proxy:
        return None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, proxy=launch_proxy)
        try:
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(IP_ONLY_URL, wait_until="domcontentloaded", timeout=45000)
            payload = json.loads(await page.text_content("body") or "{}")
            return payload.get("ip")
        finally:
            await browser.close()


def _status_line(name: str, ok: bool, details: str) -> str:
    state = "PASS" if ok else "FAIL"
    return f"[{state}] {name}: {details}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Decodo proxy auth, stickiness, and Playwright integration.")
    parser.add_argument("--expected-country", default=(Config.PROXY_COUNTRY or "mx"), help="Expected country code (default: PROXY_COUNTRY or mx).")
    parser.add_argument(
        "--allow-same-ip-on-rotate",
        action="store_true",
        help="Do not fail when rotated session keeps the same IP (useful for non-deterministic pools).",
    )
    args = parser.parse_args()

    expected_country = (args.expected_country or "mx").strip().upper()
    proxy_manager = ProxyIdentityManager(Config, client_name="oddsportal")

    if not proxy_manager.proxy_enabled:
        print("[FAIL] Proxy is disabled (PROXY_ENABLED=false).")
        return 2
    if proxy_manager.provider != "decodo":
        print(f"[FAIL] PROXY_PROVIDER is '{proxy_manager.provider}', expected 'decodo' for this test.")
        return 2

    results = []
    exit_code = 0

    try:
        first_info, first_session = _fetch_identity_info(proxy_manager, rotate=False)
        second_info, second_session = _fetch_identity_info(proxy_manager, rotate=False)
        rotated_info, rotated_session = _fetch_identity_info(proxy_manager, rotate=True)
    except Exception as exc:
        print(f"[FAIL] Decodo request test failed: {exc}")
        return 1

    first_ip = first_info.get("ip")
    second_ip = second_info.get("ip")
    rotated_ip = rotated_info.get("ip")
    first_country = (first_info.get("country_code") or "").upper()

    auth_ok = bool(first_ip)
    results.append(_status_line("Decodo auth over requests", auth_ok, f"ip={first_ip or 'missing'}, session={first_session}"))

    country_ok = first_country == expected_country
    results.append(_status_line("Mexico targeting", country_ok, f"country={first_country or 'unknown'}, expected={expected_country}"))

    sticky_ok = bool(first_ip and second_ip and first_ip == second_ip and first_session == second_session)
    results.append(_status_line("Sticky session same token", sticky_ok, f"first_ip={first_ip}, second_ip={second_ip}, session={first_session}"))

    rotated_changed = bool(rotated_ip and first_ip and rotated_ip != first_ip and rotated_session != first_session)
    rotation_detail = f"first_ip={first_ip}, rotated_ip={rotated_ip}, first_session={first_session}, rotated_session={rotated_session}"
    session_changed = rotated_session != first_session
    results.append(_status_line("Session token rotates", session_changed, rotation_detail))

    if args.allow_same_ip_on_rotate:
        ip_changed = bool(rotated_ip and first_ip and rotated_ip != first_ip)
        results.append(_status_line("Session rotation changed IP", ip_changed, rotation_detail))
    else:
        results.append(_status_line("Session rotation changes proxy identity", rotated_changed, rotation_detail))

    playwright_ip = None
    try:
        playwright_ip = asyncio.run(_playwright_ip_check(proxy_manager))
    except Exception as exc:
        results.append(_status_line("Playwright proxy launch", False, str(exc)))
        exit_code = 1
    else:
        playwright_ok = bool(playwright_ip)
        results.append(_status_line("Playwright proxy launch", playwright_ok, f"ip={playwright_ip or 'missing'}"))

    for line in results:
        print(line)
        if line.startswith("[FAIL]"):
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
