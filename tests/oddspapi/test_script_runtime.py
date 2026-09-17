import pytest

from odds_papi import _runtime


def test_runtime_selects_first_configured_key_by_default(monkeypatch):
    monkeypatch.setattr(
        _runtime.Config,
        "ODDSPAPI_KEYS",
        ["key-1", "key-2"],
    )
    monkeypatch.delenv("ODDSPAPI_SCRIPT_KEY_INDEX", raising=False)

    assert _runtime.get_api_key() == "key-1"


def test_runtime_can_select_second_key_without_exposing_it(monkeypatch):
    monkeypatch.setattr(
        _runtime.Config,
        "ODDSPAPI_KEYS",
        ["key-1", "key-2"],
    )
    monkeypatch.setenv("ODDSPAPI_SCRIPT_KEY_INDEX", "2")

    assert _runtime.get_api_key() == "key-2"


def test_runtime_rejects_missing_or_out_of_range_key(monkeypatch):
    monkeypatch.setattr(_runtime.Config, "ODDSPAPI_KEYS", [])

    with pytest.raises(RuntimeError, match="ODDSPAPI_KEY is not configured"):
        _runtime.get_api_key()

    monkeypatch.setattr(
        _runtime.Config,
        "ODDSPAPI_KEYS",
        ["key-1", "key-2"],
    )
    monkeypatch.setenv("ODDSPAPI_SCRIPT_KEY_INDEX", "3")
    with pytest.raises(RuntimeError, match="outside the configured key range"):
        _runtime.get_api_key()


def test_runtime_builds_url_from_configured_base(monkeypatch):
    monkeypatch.setattr(
        _runtime.Config,
        "ODDSPAPI_BASE_URL",
        "https://example.test/api/",
    )

    assert _runtime.oddspapi_url("/v4/fixtures") == (
        "https://example.test/api/v4/fixtures"
    )
