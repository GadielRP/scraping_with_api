"""Exercise the real HTTP adapter without external requests or whole-body decoding."""

import json
from unittest.mock import Mock

import pytest
import requests

from infrastructure.network import json_document
from infrastructure.settings.job_execution import JobExecutionSettings
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiError, OddsPapiHttpError
from shared.execution_context import WorkDeferred


class StreamingResponse:
    def __init__(self, chunks, status=200):
        self.chunks = chunks
        self.status_code = status
        self.headers = {}
        self.encoding = "utf-8"
        self.closed = False

    def iter_content(self, chunk_size):
        assert chunk_size == 65536
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    def json(self):
        pytest.fail("A streamed response must not be decoded wholesale")

    @property
    def content(self):
        pytest.fail("A streamed response must not access Response.content")

    @property
    def text(self):
        pytest.fail("A streamed response must not access Response.text")

    def close(self):
        self.closed = True


@pytest.fixture
def api():
    client = OddsPapiClient(
        base_url="https://example.test", api_key="secret", max_retries=3,
        request_delay_seconds=0, endpoint_cooldowns={"fixtures": 0},
    )
    client.outcomes = []
    client._complete_lease = lambda lease, outcome: client.outcomes.append(outcome)
    yield client
    client.close()


@pytest.mark.parametrize("field", [None, "fixtures", "data", "items"])
def test_raw_and_wrapped_fixtures_preserve_nested_data_without_json_decoding(api, field, caplog):
    fixture = {"fixtureId": "f-1", "externalProviders": {"sofascoreId": 123}}
    rows = [fixture, None, 7]
    payload = rows if field is None else {field: rows}
    response = StreamingResponse([json.dumps(payload).encode()])
    api.session.get = Mock(return_value=response)
    assert list(api.iter_fixtures(sport_id=10, has_odds=True)) == [fixture]
    assert api.session.get.call_args.kwargs["stream"] is True
    assert api.session.get.call_args.kwargs["params"]["sportId"] == 10
    assert "Ignored 2 non-object" in caplog.text
    assert response.closed
    assert len(api.outcomes) == 1 and api.outcomes[0].status_code == 200


def test_mid_body_timeout_resets_document_closes_responses_and_completes_leases(api):
    partial = StreamingResponse([b'[{"broken":', requests.Timeout("read timeout")])
    success = StreamingResponse([b'[{"fixtureId":"f-2"}]'])
    api.session.get = Mock(side_effect=[partial, success])
    assert list(api.iter_fixtures(sport_id=10)) == [{"fixtureId": "f-2"}]
    assert api.session.get.call_count == 2
    assert partial.closed and success.closed
    assert len(api.outcomes) == 2
    assert api.outcomes[0].network_error
    assert api.outcomes[1].status_code == 200


def test_streamed_not_found_preserves_error_code_without_reading_response_text(api):
    response = StreamingResponse([b'{"error":{"code":"FIXTURE_NOT_FOUND"}}'], status=404)
    api.session.get = Mock(return_value=response)
    with pytest.raises(OddsPapiHttpError) as caught:
        list(api.iter_fixtures(sport_id=10))
    assert caught.value.error_code == "FIXTURE_NOT_FOUND"
    assert api.session.get.call_count == 1
    assert response.closed and api.outcomes[0].status_code == 404


@pytest.mark.parametrize("failure", ["size", "deadline"])
def test_download_abort_is_not_retried_and_releases_response_and_lease(api, monkeypatch, failure):
    response = StreamingResponse([b'[{"fixtureId":"f-1"}]'])
    api.session.get = Mock(return_value=response)
    if failure == "size":
        monkeypatch.setattr(json_document, "JobExecutionSettings", lambda: JobExecutionSettings(response_max_bytes=5))
        expected = ValueError
    else:
        def defer():
            raise WorkDeferred("stop reading")
        monkeypatch.setattr(json_document, "check_execution_budget", defer)
        expected = WorkDeferred
    with pytest.raises(expected):
        list(api.iter_fixtures(sport_id=10))
    assert response.closed and api.session.get.call_count == 1
    assert len(api.outcomes) == 1


def test_invalid_shape_is_an_error_instead_of_silent_empty_success(api):
    api.session.get = Mock(return_value=StreamingResponse([b'{"unexpected":[]}']))
    with pytest.raises(OddsPapiError, match="Invalid JSON collection"):
        list(api.iter_fixtures(sport_id=10))


def test_closing_iterator_releases_its_temporary_document(api):
    documents = []
    download = api.download_json

    def capture(endpoint, document, params=None):
        documents.append(document)
        download(endpoint, document, params)

    api.download_json = capture
    api.session.get = Mock(return_value=StreamingResponse([b'[{"fixtureId":"1"},{"fixtureId":"2"}]']))
    fixtures = api.iter_fixtures(sport_id=10)
    assert next(fixtures) == {"fixtureId": "1"}
    assert not documents[0].file.closed
    fixtures.close()
    assert documents[0].file.closed
