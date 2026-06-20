import logging
from unittest.mock import Mock, patch

import pytest

from modules.oddspapi.client import OddsPapiClient, OddsPapiError


def response(status_code=200, payload=None, text=""):
    result = Mock()
    result.status_code = status_code
    result.text = text
    result.json.return_value = {} if payload is None else payload
    return result


def client(**overrides):
    defaults = {
        "base_url": "https://example.test",
        "api_key": "top-secret",
        "timeout": 7,
        "max_retries": 3,
        "request_delay_seconds": 0,
    }
    defaults.update(overrides)
    return OddsPapiClient(**defaults)


def test_session_ignores_environment_proxies():
    api = client()
    assert api.session.trust_env is False


def test_get_fixtures_builds_expected_params_without_proxies():
    api = client()
    api.session.get = Mock(return_value=response(payload={"fixtures": []}))

    api.get_fixtures(
        tournament_id=17,
        sport_id=10,
        participant_id=42,
        from_date="2026-06-01",
        to_date="2026-06-02",
        language="en",
        status_id=0,
        has_odds=True,
        bookmakers=["pinnacle", "bet365"],
    )

    _, kwargs = api.session.get.call_args
    assert kwargs["params"] == {
        "tournamentId": 17,
        "sportId": 10,
        "participantId": 42,
        "from": "2026-06-01",
        "to": "2026-06-02",
        "language": "en",
        "statusId": 0,
        "hasOdds": True,
        "bookmakers": "pinnacle,bet365",
        "apiKey": "top-secret",
    }
    assert kwargs["timeout"] == 7
    assert "proxies" not in kwargs


def test_api_key_is_never_logged(caplog):
    api = client()
    api.session.get = Mock(return_value=response(payload={}))
    with caplog.at_level(logging.INFO):
        api._request("fixture", {"fixtureId": "f1", "apiKey": "also-secret"})
    assert "top-secret" not in caplog.text
    assert "also-secret" not in caplog.text


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_transient_http_statuses_retry(status_code):
    api = client()
    api.session.get = Mock(
        side_effect=[response(status_code, text="retry"), response(200, {"ok": True})]
    )
    assert api._request("fixture") == {"ok": True}
    assert api.session.get.call_count == 2


def test_404_does_not_retry():
    api = client()
    api.session.get = Mock(return_value=response(404, text="missing"))
    with pytest.raises(OddsPapiError, match=r"status_code=404.*endpoint=/v4/fixture"):
        api._request("fixture")
    assert api.session.get.call_count == 1


def test_invalid_json_has_clear_error():
    api = client()
    invalid = response(200)
    invalid.json.side_effect = ValueError("bad json")
    api.session.get = Mock(return_value=invalid)
    with pytest.raises(OddsPapiError, match=r"Invalid JSON.*endpoint=/v4/fixture"):
        api._request("fixture")


def test_missing_key_fails_only_when_request_is_made():
    api = client(api_key="")
    with pytest.raises(ValueError, match="ODDSPAPI_KEY"):
        api.get_bookmakers()
