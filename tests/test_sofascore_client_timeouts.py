"""Regression: discovery transport timeouts must use the existing retry loop."""

import logging
from types import SimpleNamespace

import pytest
from curl_cffi.requests.exceptions import Timeout

from infrastructure.settings import Config
from modules.sofascore import client as client_module
from infrastructure.network.json_document import open_json_document, document_entries
from infrastructure.network import json_document
from infrastructure.settings.job_execution import JobExecutionSettings
from shared.execution_context import WorkDeferred


ENDPOINT = "/unique-tournament/18643/scheduled-events/2026-10-04"


@pytest.fixture
def api(monkeypatch, caplog):
    monkeypatch.setattr(Config, "MAX_RETRIES", 3)
    monkeypatch.setattr(client_module.SofaScoreAPI, "_setup_session", lambda *a, **k: None)
    api = client_module.SofaScoreAPI()
    monkeypatch.setattr(api, "_rate_limit", lambda: None)
    caplog.set_level(logging.ERROR, logger="modules.sofascore.client")
    return api


def test_streamed_timeout_retries_and_discards_the_partial_body(api, monkeypatch, caplog):
    calls, waits = [], []

    def get(url, *, content_callback, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            content_callback(b'{"events":[{"incomplete":')
            raise Timeout("Operation timed out", code=28)
        content_callback(b'{"events":[{"id":1}]}')
        return SimpleNamespace(status_code=200)

    api.session = SimpleNamespace(get=get)
    monkeypatch.setattr(client_module, "wait_interruptibly", waits.append)
    with open_json_document(api, ENDPOINT) as document:
        assert list(document_entries(document, "events")) == [{"id": 1}]
    assert len(calls) == 2
    assert waits == [5]
    assert "attempt=1/3" in caplog.text
    assert "retrying=true" in caplog.text


def test_persistent_timeout_stops_after_configured_attempts(api, monkeypatch, caplog):
    calls, waits = [], []

    def get(url, **kwargs):
        calls.append(url)
        raise Timeout("Operation timed out", code=28)

    api.session = SimpleNamespace(get=get)
    monkeypatch.setattr(client_module, "wait_interruptibly", waits.append)
    with pytest.raises(RuntimeError, match="Incomplete provider response"):
        with open_json_document(api, ENDPOINT):
            pytest.fail("An incomplete response must not reach discovery parsing")
    assert len(calls) == 3
    assert waits == [5, 10]
    assert "attempt=3/3" in caplog.records[-1].getMessage()
    assert "retrying=false" in caplog.records[-1].getMessage()


def test_retry_wait_preserves_execution_deferral(api, monkeypatch):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        raise Timeout("Operation timed out", code=28)

    def defer(seconds):
        raise WorkDeferred("Execution deadline reached")

    api.session = SimpleNamespace(get=get)
    monkeypatch.setattr(client_module, "wait_interruptibly", defer)
    with pytest.raises(WorkDeferred, match="Execution deadline reached"):
        api.request_json(ENDPOINT)
    assert len(calls) == 1


def test_callback_abort_preserves_original_size_error_instead_of_curl_failure(api, monkeypatch):
    from curl_cffi.requests.exceptions import RequestException

    monkeypatch.setattr(json_document, "JobExecutionSettings", lambda: JobExecutionSettings(response_max_bytes=5))
    calls = []

    def get(url, *, content_callback, **kwargs):
        calls.append(url)
        assert content_callback(b"123456") == 0
        raise RequestException("write callback aborted", code=23)

    api.session = SimpleNamespace(get=get)
    with pytest.raises(ValueError, match="byte budget"):
        with open_json_document(api, ENDPOINT):
            pytest.fail("An oversized body must not reach parsing")
    assert len(calls) == 1


def test_each_actual_attempt_logs_airplane_and_endpoint_at_info(api, caplog):
    api.session = SimpleNamespace(get=lambda *args, **kwargs: SimpleNamespace(
        status_code=200, json=lambda: {"event": {"id": 1}},
    ))
    caplog.set_level(logging.INFO, logger="modules.sofascore.client")
    assert api.request_json("/event/1") == {"event": {"id": 1}}
    assert "✈️ SofaScore GET" in caplog.text and "/event/1 attempt=1/3" in caplog.text
