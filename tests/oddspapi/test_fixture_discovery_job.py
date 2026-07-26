from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from modules.jobs.oddspapi.fixture_discovery.fixture_discovery_job import OddspapiFixtureDiscoveryJob
from modules.jobs.oddspapi.fixture_discovery.response_utils import (
    extract_fixture_list,
    split_time_window,
)
from modules.oddspapi.exceptions import OddsPapiHttpError
from modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery import _resolve_sports


def _fixture(fixture_id: str = "f-1") -> dict:
    return {
        "fixtureId": fixture_id,
        "sportId": 10,
        "sportName": "Soccer",
        "startTime": "2026-07-15T12:00:00Z",
        "participant1Name": "Home",
        "participant2Name": "Away",
    }


@pytest.mark.parametrize(
    "payload",
    [[_fixture()], {"fixtures": [_fixture()]}, {"data": [_fixture()]}, {"items": [_fixture()]}],
)
def test_extract_fixture_list_supports_raw_and_wrapped_shapes(payload):
    assert extract_fixture_list(payload) == [_fixture()]


def test_extract_fixture_list_unsupported_shape_returns_empty(caplog):
    assert extract_fixture_list({"unexpected": []}) == []
    assert "Unsupported Oddspapi" in caplog.text


def test_window_splitting_keeps_chunks_within_limit():
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    chunks = split_time_window(start, start + timedelta(hours=121), max_window_hours=48)
    assert len(chunks) == 3
    assert all((end - begin) <= timedelta(hours=48) for begin, end in chunks)
    assert chunks[0][0] == start
    assert chunks[-1][1] == start + timedelta(hours=121)


def test_cli_sport_subset_and_unknown_slug():
    assert _resolve_sports("soccer, baseball") == {"soccer": 10, "baseball": 13}
    with pytest.raises(ValueError, match="unknown sport"):
        _resolve_sports("soccer,quidditch")


class _Client:
    def __init__(self):
        self.calls = []

    def get_fixtures(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["sport_id"] == 11:
            raise RuntimeError("basketball unavailable")
        return [_fixture(f"{kwargs['sport_id']}-fixture")]


class _BatchResult:
    fixtures_valid = 1
    fixtures_deduplicated = 0
    invalid_payloads = 0
    resolved_existing_oddspapi = 1
    resolved_external_sofascore = 0
    resolved_candidate_match = 0
    mappings_created = 0
    unresolved_no_candidates = 0
    needs_review = 0
    queue_rows_written = 0


class _BatchProcessor:
    def process_batch(self, **kwargs):
        return _BatchResult()


@contextmanager
def _session():
    yield object()


def test_api_error_for_one_sport_does_not_stop_other_sports(monkeypatch):
    client = _Client()
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_discovery_job.db_manager.get_session",
        _session,
    )
    job = OddspapiFixtureDiscoveryJob(
        client=client,
        sports={"soccer": 10, "basketball": 11},
        create_mappings=False,
        batch_processor=_BatchProcessor(),
    )
    summary = job.run(
        datetime(2026, 7, 15, tzinfo=timezone.utc),
        datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert [sport.sport_slug for sport in summary.sports] == ["soccer", "basketball"]
    assert summary.sports[0].fixtures_fetched == 1
    assert summary.sports[1].errors == 1
    assert summary.total_fixtures_fetched == 1


def test_fixture_not_found_is_treated_as_empty_success(monkeypatch):
    class _NoFixturesClient:
        def get_fixtures(self, **kwargs):
            raise OddsPapiHttpError(
                status_code=404,
                endpoint="/v4/fixtures",
                response_text="fixture not found",
                error_code="FIXTURE_NOT_FOUND",
            )

    job = OddspapiFixtureDiscoveryJob(
        client=_NoFixturesClient(),
        sports={"american-football": 14},
        create_mappings=False,
    )
    summary = job.run(
        datetime(2026, 7, 15, tzinfo=timezone.utc),
        datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert summary.sports[0].errors == 0
    assert summary.sports[0].fixtures_fetched == 0
