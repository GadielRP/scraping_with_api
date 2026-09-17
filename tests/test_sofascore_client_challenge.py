from __future__ import annotations

import pytest

from infrastructure.settings import Config
from modules.sofascore.client import SofaScoreAPI
from modules.sofascore.exceptions import SofaScoreChallengeException


class FakeResponse:
    status_code = 403
    text = '{"error":{"code":403,"reason":"challenge"}}'
    headers = {"server": "cloudflare", "cf-ray": "test-ray"}


class FakeSession:
    def __init__(self):
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        return FakeResponse()


def test_make_request_raises_challenge_exception_without_rotating(monkeypatch):
    api = SofaScoreAPI()
    api.session = FakeSession()
    api.proxy_manager.should_rotate_on_sofascore_error = lambda: False
    monkeypatch.setattr(Config, "MAX_RETRIES", 3)
    monkeypatch.setattr(Config, "REQUEST_DELAY_SECONDS", 0)
    monkeypatch.setattr("modules.sofascore.client.write_challenge_evidence", lambda evidence: None)

    with pytest.raises(SofaScoreChallengeException):
        api.request_json("/sport/football/events/live")

    assert api.session.calls == 1


def test_make_request_rotates_once_then_raises_challenge_exception(monkeypatch):
    api = SofaScoreAPI()
    api.session = FakeSession()
    rotate_calls: list[str] = []
    api.proxy_manager.should_rotate_on_sofascore_error = lambda: True
    monkeypatch.setattr(Config, "MAX_RETRIES", 2)
    monkeypatch.setattr(Config, "REQUEST_DELAY_SECONDS", 0)
    monkeypatch.setattr("modules.sofascore.client.write_challenge_evidence", lambda evidence: None)
    monkeypatch.setattr(
        api,
        "_rotate_proxy_identity",
        lambda reason: rotate_calls.append(reason),
    )

    with pytest.raises(SofaScoreChallengeException):
        api.request_json("/sport/football/events/live")

    assert api.session.calls == 2
    assert rotate_calls == ["http_403_challenge_attempt_1_/sport/football/events/live"]


def test_request_json_returns_none_for_challenge(monkeypatch):
    api = SofaScoreAPI()

    def raise_challenge(*args, **kwargs):
        raise SofaScoreChallengeException(123, endpoint="/sport/football/events/live")

    monkeypatch.setattr(api, "request_json", raise_challenge)

    assert api.request_json_or_none("/sport/football/events/live") is None


def test_make_request_skips_evidence_when_capture_disabled(monkeypatch):
    api = SofaScoreAPI()
    api.session = FakeSession()
    api.proxy_manager.should_rotate_on_sofascore_error = lambda: False
    api.set_challenge_evidence_enabled(False)
    monkeypatch.setattr(Config, "MAX_RETRIES", 1)
    monkeypatch.setattr(Config, "REQUEST_DELAY_SECONDS", 0)
    monkeypatch.setattr(
        "modules.sofascore.client.write_challenge_evidence",
        lambda evidence: (_ for _ in ()).throw(RuntimeError("evidence write should not run")),
    )

    with pytest.raises(SofaScoreChallengeException):
        api.request_json("/sport/football/events/live")
