from __future__ import annotations

from dataclasses import dataclass

from modules.sofascore.challenge import (
    build_challenge_evidence,
    extract_cookie_names,
    get_challenge_reason,
    is_sofascore_challenge_response,
)
from modules.sofascore.exceptions import SofaScoreChallengeException


@dataclass
class FakeResponse:
    status_code: int
    text: str
    headers: dict


def test_detects_json_error_challenge_nested():
    response = FakeResponse(
        status_code=403,
        text='{"error":{"code":403,"reason":"challenge"}}',
        headers={"server": "cloudflare", "cf-ray": "abc"},
    )

    assert is_sofascore_challenge_response(response) is True
    assert get_challenge_reason(response) == "challenge"


def test_detects_json_error_challenge_flat():
    response = FakeResponse(
        status_code=403,
        text='{"code":403,"reason":"challenge"}',
        headers={},
    )

    assert is_sofascore_challenge_response(response) is True


def test_does_not_detect_429_as_challenge():
    response = FakeResponse(
        status_code=429,
        text='{"error":{"code":429,"reason":"rate_limit"}}',
        headers={},
    )

    assert is_sofascore_challenge_response(response) is False


def test_does_not_detect_404_as_challenge():
    response = FakeResponse(status_code=404, text="{}", headers={})

    assert is_sofascore_challenge_response(response) is False


def test_extract_cookie_names_redacts_values():
    header = "__cf_bm=abc123; Path=/; Secure, cf_clearance=secretvalue; Path=/"

    assert extract_cookie_names(header) == ["__cf_bm", "cf_clearance"]


def test_build_challenge_evidence_redacts_sensitive_cookie_values():
    response = FakeResponse(
        status_code=403,
        text='{"error":{"code":403,"reason":"challenge"}}',
        headers={
            "server": "cloudflare",
            "cf-ray": "ray-123",
            "set-cookie": "__cf_bm=abc123; Path=/; Secure, cf_clearance=secretvalue; Path=/",
        },
    )

    evidence = build_challenge_evidence(
        response=response,
        endpoint="/sport/football/events/live",
        base_url="https://api.sofascore.com/api/v1",
        attempt=1,
        max_retries=3,
        params={"sport": "football", "page": 1},
        proxy_identity=None,
        request_url="https://api.sofascore.com/api/v1/sport/football/events/live",
    )

    headers = evidence["response_headers"]
    assert "set-cookie" not in headers
    assert headers["set-cookie-names"] == ["__cf_bm", "cf_clearance"]
    assert evidence["body_preview"]
    assert evidence["params_keys"] == ["page", "sport"]


def test_exception_message_contains_endpoint_reason_and_cf_ray():
    exc = SofaScoreChallengeException(
        123,
        endpoint="/sport/football/events/live",
        reason="challenge",
        evidence={"response_headers": {"cf-ray": "ray-123"}},
    )

    assert "/sport/football/events/live" in str(exc)
    assert "challenge" in str(exc)
    assert "cf-ray" in str(exc)
