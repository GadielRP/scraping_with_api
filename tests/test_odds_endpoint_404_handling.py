from types import SimpleNamespace

import pytest

from infrastructure.persistence.repositories import EventOddsSourceState
from modules.jobs.oddspapi.pre_start_odds.event_selector import (
    OddspapiPreStartCandidate,
)
from modules.jobs.oddspapi.pre_start_odds.odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
)
from modules.jobs.oddspapi.pre_start_odds.odds_fetcher import (
    OddspapiOddsFetcher,
)
from modules.jobs.oddspapi.pre_start_odds import pre_start_odds_job
from modules.jobs.pre_start_check_job import event_candidate_builder
from modules.jobs.pre_start_check_job import run_pre_start_check_job as pre_start_job_runner
from modules.jobs.pre_start_check_job import sofascore_odds_processor
from modules.odds_ingestion.fetch_result import OddsFetchResult, OddsFetchStatus
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiHttpError
from modules.sofascore.client import SofaScoreAPI
from modules.sofascore.exceptions import SofaScoreNotFoundException
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher
from scripts.development import pre_start_odds_simulation


def _event_info(event_id=101):
    return {
        "event_id": event_id,
        "event_data": {
            "id": event_id,
            "slug": "home-away",
            "sport": "Football",
            "home_team": "Home",
            "away_team": "Away",
            "start_time_utc": None,
        },
        "minutes_until_start": 30,
        "should_extract_odds": True,
        "sofascore_event_id": 9001,
        "metadata_snapshot": None,
    }


def _state(event_id, source, source_event_id, has_odds):
    return EventOddsSourceState(
        event_id=event_id,
        source=source,
        source_event_id=source_event_id,
        has_odds=has_odds,
    )


def test_sofascore_false_state_skips_entire_odds_flow(monkeypatch):
    fetcher = SimpleNamespace(
        fetch_odds=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("HTTP request must be skipped")
        )
    )
    monkeypatch.setattr(
        sofascore_odds_processor.MarketOddsIngestionService,
        "save_from_event_odds_response",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("ingestion must be skipped")
        ),
    )

    result = sofascore_odds_processor.process_sofascore_pre_start_odds(
        [_event_info()],
        {101: {"sofascore": _state(101, "sofascore", "9001", False)}},
        odds_fetcher=fetcher,
    )

    assert result.requests_attempted == 0
    assert result.events_skipped == 1


def test_candidate_builder_reuses_bulk_mapping_without_event_requery(monkeypatch):
    scheduler = SimpleNamespace(
        recently_rescheduled=set(),
        event_repo=SimpleNamespace(
            get_event_by_id=lambda *_args: (_ for _ in ()).throw(
                AssertionError("unchanged events must not cause an N+1 database query")
            )
        ),
    )
    event = {
        "id": 101,
        "slug": "home-away",
        "sport": "Football",
        "start_time_utc": None,
    }
    state = _state(101, "sofascore", "9001", True)

    def _timing_decision(
        event_id,
        minutes,
        start_time,
        *,
        sofascore_event_id,
    ):
        assert (event_id, minutes, start_time, sofascore_event_id) == (
            101,
            30,
            None,
            9001,
        )
        return True, None, False, sofascore_event_id

    monkeypatch.setattr(
        event_candidate_builder,
        "should_extract_odds_for_event",
        _timing_decision,
    )

    plan = event_candidate_builder.build_pre_start_event_candidates(
        scheduler,
        [event],
        {101: 30},
        {101: {"sofascore": state}},
    )

    assert len(plan.candidates) == 1
    assert plan.by_event_id[101] is plan.candidates[0]


def test_orchestrator_loads_odds_state_after_event_filtering(monkeypatch):
    upcoming_events = [
        {"id": 101, "start_time_utc": None},
        {"id": 102, "start_time_utc": None},
    ]
    filtered_events = [upcoming_events[1]]
    loaded_event_ids = []
    scheduler = SimpleNamespace(
        event_repo=SimpleNamespace(),
        recently_rescheduled=set(),
    )

    monkeypatch.setattr(
        pre_start_job_runner,
        "api_client",
        SimpleNamespace(
            challenge_evidence_enabled=False,
            set_challenge_evidence_enabled=lambda _enabled: None,
        ),
    )
    monkeypatch.setattr(pre_start_job_runner, "_tracked_season_ids", lambda: None)
    monkeypatch.setattr(
        pre_start_job_runner,
        "_load_upcoming_events",
        lambda *_args: upcoming_events,
    )
    monkeypatch.setattr(
        pre_start_job_runner,
        "minutes_until_start",
        lambda _start_time: 30,
    )
    monkeypatch.setattr(
        pre_start_job_runner,
        "start_oddsportal_scrape_for_events",
        lambda *_args: SimpleNamespace(
            event_states={},
            event_ids=set(),
            data_cache={},
        ),
    )
    monkeypatch.setattr(
        pre_start_job_runner,
        "_maintain_recently_started_events",
        lambda *_args: filtered_events,
    )
    monkeypatch.setattr(pre_start_job_runner, "run_in_game_checks", lambda: None)
    monkeypatch.setattr(
        pre_start_job_runner,
        "load_pre_start_odds_source_states",
        lambda events: loaded_event_ids.extend(event["id"] for event in events)
        or {},
    )
    monkeypatch.setattr(
        pre_start_job_runner,
        "build_pre_start_event_candidates",
        lambda _scheduler, events, _timings, _states: (
            event_candidate_builder.PreStartEventPlan(
                candidates=[],
                by_event_id={},
            )
            if events == filtered_events
            else (_ for _ in ()).throw(
                AssertionError("candidate planning must use filtered events")
            )
        ),
    )
    monkeypatch.setattr(
        pre_start_job_runner,
        "_ingest_provider_odds",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        pre_start_job_runner,
        "evaluate_pre_start_key_moments",
        lambda *_args, **_kwargs: None,
    )

    pre_start_job_runner.run_pre_start_check_job(scheduler)

    assert loaded_event_ids == [102]


def test_sofascore_404_is_batched_and_empty_response_is_not(monkeypatch):
    calls = []
    responses = iter(
        [
            OddsFetchResult.endpoint_not_found(),
            OddsFetchResult.from_payload(None),
        ]
    )
    fetcher = SimpleNamespace(fetch_odds=lambda *_args, **_kwargs: next(responses))
    monkeypatch.setattr(
        sofascore_odds_processor.EventSourceMappingRepository,
        "mark_odds_unavailable",
        lambda event_ids, source: calls.append((set(event_ids), source)) or len(event_ids),
    )

    sofascore_odds_processor.process_sofascore_pre_start_odds(
        [_event_info(101), _event_info(102)],
        {},
        odds_fetcher=fetcher,
    )

    assert calls == [({101}, "sofascore")]


def test_sofascore_client_separates_strict_and_compatibility_requests(monkeypatch):
    client = object.__new__(SofaScoreAPI)
    not_found = SofaScoreNotFoundException(9001, "/event/9001/odds/1/all")
    monkeypatch.setattr(client, "_make_request", lambda *args, **kwargs: (_ for _ in ()).throw(not_found))

    assert client._request_json("/event/9001/odds/1/all") is None
    with pytest.raises(SofaScoreNotFoundException):
        client.request_json("/event/9001/odds/1/all")


def test_sofascore_fetcher_translates_404_to_expected_result():
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SofaScoreNotFoundException(9001, "/event/9001/odds/1/all")
        )
    )

    result = SofaScoreOddsFetcher(client).fetch_odds(9001, "home-away")

    assert result.status is OddsFetchStatus.ENDPOINT_NOT_FOUND
    assert result.payload is None


def test_oddspapi_fetcher_uses_structured_404():
    client = SimpleNamespace(
        get_odds=lambda **_kwargs: (_ for _ in ()).throw(
            OddsPapiHttpError(404, "/v4/odds")
        )
    )

    result = OddspapiOddsFetcher(client=client).fetch_odds("fixture-1")

    assert result.status is OddsFetchStatus.ENDPOINT_NOT_FOUND
    assert result.payload is None


def test_oddspapi_client_exposes_http_status_for_404():
    response = SimpleNamespace(
        status_code=404,
        text="fixture odds not found",
        headers={},
        json=lambda: {"error": {"code": "FIXTURE_NOT_FOUND"}},
    )
    client = OddsPapiClient(
        api_key="test-key",
        max_retries=1,
        request_delay_seconds=0,
    )
    client.session = SimpleNamespace(get=lambda *_args, **_kwargs: response)

    with pytest.raises(OddsPapiHttpError) as caught:
        client.get_odds("fixture-1")

    assert caught.value.status_code == 404
    assert caught.value.endpoint == "/v4/odds"
    assert caught.value.error_code == "FIXTURE_NOT_FOUND"


def test_oddspapi_false_state_skips_request_and_mapping_index(monkeypatch):
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("HTTP request must be skipped")
            )
        )
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.pre_start_odds.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("mapping index must not be loaded")
        ),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fixture-1",
        minutes_until_start=30,
        has_odds=False,
    )

    summary = processor.process([candidate], bookmakers=["bookmaker"])

    assert summary.requests_attempted == 0
    assert summary.events_skipped == 1
    assert summary.results[0].skip_reason == "oddspapi_odds_unavailable"


def test_oddspapi_missing_api_key_skips_mapping_query(monkeypatch):
    monkeypatch.setattr(
        pre_start_odds_job.Config,
        "ENABLE_ODDSPAPI_PRE_START_ODDS",
        True,
    )
    monkeypatch.setattr(pre_start_odds_job.Config, "ODDSPAPI_KEY", "")
    monkeypatch.setattr(
        pre_start_odds_job.EventSourceMappingRepository,
        "get_odds_source_states",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("missing API credentials must skip the database lookup")
        ),
    )

    summary = pre_start_odds_job.run_oddspapi_pre_start_odds_ingestion(
        [_event_info()]
    )

    assert summary.events_skipped == 1
    assert summary.requests_attempted == 0
    assert summary.results[0].skip_reason == "missing_oddspapi_api_key"


def test_oddspapi_404_is_persisted_once_for_provider(monkeypatch):
    marked = []
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **_kwargs: OddsFetchResult.endpoint_not_found()
        )
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.pre_start_odds.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.pre_start_odds.odds_batch_processor."
        "EventSourceMappingRepository.mark_odds_unavailable",
        lambda event_ids, source: marked.append((set(event_ids), source)) or len(event_ids),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fixture-1",
        minutes_until_start=30,
    )

    summary = processor.process([candidate], bookmakers=["bookmaker"])

    assert summary.requests_attempted == 1
    assert summary.events_skipped == 1
    assert summary.events_failed == 0
    assert summary.results[0].skip_reason == "oddspapi_odds_endpoint_not_found"
    assert marked == [({101}, "oddspapi")]


def test_manual_simulator_uses_production_provider_processors(monkeypatch):
    calls = []
    states = {
        101: {
            "sofascore": _state(101, "sofascore", "9001", True),
            "oddspapi": _state(101, "oddspapi", "fixture-1", True),
        }
    }
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "load_pre_start_odds_source_states",
        lambda _events: states,
    )
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "get_numeric_source_event_id",
        lambda *_args: 9001,
    )
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "should_extract_odds_for_event",
        lambda *_args, **_kwargs: (True, None, False, 9001),
    )
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "process_sofascore_pre_start_odds",
        lambda event_infos, source_states, **_kwargs: calls.append(
            ("sofascore", event_infos, source_states)
        ),
    )
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "run_oddspapi_pre_start_odds_ingestion",
        lambda event_infos, source_states, **_kwargs: (
            calls.append(("oddspapi", event_infos, source_states))
            or SimpleNamespace(
                requests_attempted=0,
                events_ingested=0,
                events_skipped=1,
                events_failed=0,
            )
        ),
    )
    event = SimpleNamespace(
        id=101,
        slug="home-away",
        sport="Football",
        home_team="Home",
        away_team="Away",
        start_time_utc=None,
        season_id=5,
    )

    pre_start_odds_simulation.run_production_odds_phase(
        event,
        30,
        [30],
        debug_mode=True,
        show_persistence_report=False,
        log_persisted_market_odds=lambda *_args: None,
    )

    assert [call[0] for call in calls] == ["sofascore", "oddspapi"]
    assert calls[0][2] is states
    assert calls[1][2] is states
