import json
import logging
from types import SimpleNamespace

import pytest
import modules.oddspapi.client as oddspapi_client_module

from infrastructure.persistence.repositories import EventOddsSourceState
from infrastructure.persistence.repositories.market.market_read_models import (
    ExternalChoiceQuote,
    ExternalMarketQuoteBlock,
)
from modules.alerts.alerts_formatter.odds_alert import (
    _format_external_markets_section,
)
from modules.jobs.pre_start_check_job import event_candidate_builder
from modules.jobs.pre_start_check_job import intraday_result_freshness
from modules.jobs.pre_start_check_job import run_pre_start_check_job as pre_start_job_runner
from modules.jobs.pre_start_check_job.providers.oddspapi import odds_phase as oddspapi_odds_phase
from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
    OddspapiPreStartCandidate,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import (
    OddspapiPreStartOddsBatchProcessor,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_fetcher import (
    OddspapiOddsFetcher,
)
from modules.jobs.pre_start_check_job.providers.sofascore import odds_phase as sofascore_odds_phase
from modules.odds_ingestion.fetch_result import OddsFetchResult, OddsFetchStatus
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiHttpError
from modules.sofascore.client import SofaScoreAPI
from modules.sofascore import event_details
from modules.sofascore.exceptions import SofaScoreNotFoundException
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher
from modules.sofascore.results_parser import (
    extract_results_from_response,
    is_event_status_deletable,
    parse_event_result,
)
from scripts.development import pre_start_odds_simulation
from scripts.development import simulate_pre_start_check


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


def _stub_mainline_cache(monkeypatch, event_ids=None):
    """Treat listed event ids (or every queried id) as having mainline cache."""
    def event_ids_with_cache(ids):
        requested = {int(event_id) for event_id in (ids or []) if event_id is not None}
        if event_ids is None:
            return requested
        return requested.intersection({int(event_id) for event_id in event_ids})

    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "OddspapiMainlineCacheRepository.event_ids_with_cache",
        event_ids_with_cache,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_acquisition_service."
        "OddspapiMainlineCacheRepository.event_ids_with_cache",
        event_ids_with_cache,
    )


def test_sofascore_false_state_skips_entire_odds_flow(monkeypatch):
    fetcher = SimpleNamespace(
        fetch_odds=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("HTTP request must be skipped")
        )
    )
    monkeypatch.setattr(
        sofascore_odds_phase.MarketOddsIngestionService,
        "save_from_sofascore_response",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("ingestion must be skipped")
        ),
    )

    result = sofascore_odds_phase.run_sofascore_pre_start_odds(
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
        **_kwargs,
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


def test_candidate_builder_skips_key_moment_after_timing_api_failure(monkeypatch):
    scheduler = SimpleNamespace(
        recently_rescheduled=set(),
        event_repo=SimpleNamespace(),
    )
    event = {
        "id": 101,
        "slug": "home-away",
        "sport": "Football",
        "start_time_utc": None,
    }
    state = _state(101, "sofascore", "9001", True)

    monkeypatch.setattr(
        event_candidate_builder,
        "Config",
        SimpleNamespace(
            ENABLE_ODDS_EXTRACTION=True,
            PRE_START_ODDS_MOMENTS=[30],
        ),
    )
    monkeypatch.setattr(
        event_candidate_builder,
        "should_extract_odds_for_event",
        lambda *_args, **_kwargs: (False, None, False, 9001),
    )

    plan = event_candidate_builder.build_pre_start_event_candidates(
        scheduler,
        [event],
        {101: 30},
        {101: {"sofascore": state}},
    )

    assert plan.candidates == []
    assert plan.by_event_id == {}


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
    monkeypatch.setattr(
        pre_start_job_runner,
        "_tracked_competition_ids",
        lambda: None,
    )
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
        lambda *_args, **_kwargs: SimpleNamespace(
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
        lambda _scheduler, events, _timings, _states, **_kwargs: (
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
        "modules.odds_ingestion.provider_odds_phase.EventSourceMappingRepository",
        SimpleNamespace(
            mark_odds_unavailable=(
                lambda event_ids, source: calls.append((set(event_ids), source))
                or len(event_ids)
            )
        ),
    )

    sofascore_odds_phase.run_sofascore_pre_start_odds(
        [_event_info(101), _event_info(102)],
        {},
        odds_fetcher=fetcher,
    )

    assert calls == [({101}, "sofascore")]


def test_sofascore_client_separates_strict_and_tolerant_requests(monkeypatch):
    client = object.__new__(SofaScoreAPI)
    not_found = SofaScoreNotFoundException(9001, "/event/9001/odds/1/all")
    monkeypatch.setattr(
        client,
        "request_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(not_found),
    )

    assert client.request_json_or_none("/event/9001/odds/1/all") is None


def test_event_404_does_not_delete_without_batch_collector(monkeypatch):
    delete_calls = []
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SofaScoreNotFoundException(9001, "/event/9001")
        )
    )
    monkeypatch.setattr(
        event_details.EventRepository,
        "batch_delete_events",
        lambda event_ids: delete_calls.append(event_ids),
    )

    result = event_details.get_event_results(
        client,
        9001,
        canonical_event_id=101,
        update_time=True,
        return_snapshot=True,
    )

    assert result == (None, None)
    assert delete_calls == []


def test_event_404_is_queued_when_batch_collector_is_provided():
    deferred_deletion_event_ids = set()
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SofaScoreNotFoundException(9001, "/event/9001")
        )
    )

    result = event_details.get_event_results(
        client,
        9001,
        canonical_event_id=101,
        deferred_deletion_event_ids=deferred_deletion_event_ids,
    )

    assert result is None
    assert deferred_deletion_event_ids == {101}


def test_canceled_event_is_queued_for_batch_deletion():
    deferred_deletion_event_ids = set()
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: {
            "event": {
                "id": 9001,
                "status": {
                    "code": 60,
                    "type": "postponed",
                    "description": "postponed",
                },
            }
        }
    )

    result = event_details.get_event_results(
        client,
        9001,
        canonical_event_id=101,
        deferred_deletion_event_ids=deferred_deletion_event_ids,
    )

    assert result is None
    assert deferred_deletion_event_ids == {101}


def test_canceled_event_does_not_delete_without_batch_collector(monkeypatch):
    delete_calls = []
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: {
            "event": {
                "id": 9001,
                "status": {
                    "code": 60,
                    "type": "canceled",
                    "description": "canceled",
                },
            }
        }
    )
    monkeypatch.setattr(
        event_details.EventRepository,
        "batch_delete_events",
        lambda event_ids: delete_calls.append(event_ids),
    )

    result = event_details.get_event_results(
        client,
        9001,
        canonical_event_id=101,
    )

    assert result is None
    assert delete_calls == []


@pytest.mark.parametrize(
    ("status_type", "status_description"),
    [
        ("finished", "postponed"),
        ("ended", "postponed"),
        ("postponed", "finished"),
        ("postponed", "ended"),
    ],
)
def test_deletable_status_code_is_rejected_for_finished_or_ended_text(
    status_type,
    status_description,
):
    raw_event = {
        "id": 9001,
        "status": {
            "code": 60,
            "type": status_type,
            "description": status_description,
        },
    }

    assert is_event_status_deletable(raw_event) is False
    assert extract_results_from_response({"event": raw_event}) is None


def test_postponed_status_requires_a_deletable_status_code():
    postponed_event = {
        "id": 9001,
        "status": {
            "code": 60,
            "type": "postponed",
            "description": "postponed",
        },
    }
    non_deletable_code_event = {
        "id": 9002,
        "status": {
            "code": 100,
            "type": "postponed",
            "description": "postponed",
        },
    }

    assert is_event_status_deletable(postponed_event) is True
    assert is_event_status_deletable(non_deletable_code_event) is False


def test_walkover_is_deletable_even_when_type_is_finished():
    """Walkover never starts; SofaScore may still send type=finished."""
    raw_event = {
        "id": 16601610,
        "status": {
            "code": 91,
            "type": "finished",
            "description": "Walkover",
        },
    }

    assert is_event_status_deletable(raw_event) is True

    parsed = parse_event_result({"event": raw_event})
    assert parsed.kind == "canceled"
    assert parsed.status_code == 91
    assert parsed.status_description == "walkover"

    legacy = extract_results_from_response({"event": raw_event})
    assert legacy == {
        "_canceled": True,
        "status_code": 91,
        "status_description": "walkover",
    }


def test_walkover_is_queued_for_batch_deletion_with_walkover_reason(monkeypatch):
    deferred_deletion_event_ids = set()
    queued_reasons = []
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: {
            "event": {
                "id": 9001,
                "status": {
                    "code": 91,
                    "type": "finished",
                    "description": "Walkover",
                },
            }
        }
    )

    def _tracking_queue(canonical_event_id, sofascore_event_id, reason, deferred_ids):
        queued_reasons.append(reason)
        deferred_ids.add(canonical_event_id)
        return True

    monkeypatch.setattr(
        event_details,
        "_queue_canonical_event_for_deletion",
        _tracking_queue,
    )

    result = event_details.get_event_results(
        client,
        9001,
        update_event_info=False,
        canonical_event_id=101,
        deferred_deletion_event_ids=deferred_deletion_event_ids,
    )

    assert result is None
    assert deferred_deletion_event_ids == {101}
    assert queued_reasons == ["walkover"]


def test_parse_event_result_classifies_not_started():
    payload = {
        "event": {
            "id": 1,
            "status": {
                "code": 0,
                "type": "notstarted",
                "description": "Not started",
            },
        }
    }
    parsed = parse_event_result(payload)
    assert parsed.kind == "not_started"
    assert extract_results_from_response(payload) is None


def test_get_event_results_can_delete_stale_not_started():
    deferred_deletion_event_ids = set()
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: {
            "event": {
                "id": 9001,
                "status": {
                    "code": 0,
                    "type": "notstarted",
                    "description": "Not started",
                },
            }
        }
    )

    result = event_details.get_event_results(
        client,
        9001,
        update_event_info=False,
        canonical_event_id=101,
        deferred_deletion_event_ids=deferred_deletion_event_ids,
        on_not_started="delete",
    )

    assert result is None
    assert deferred_deletion_event_ids == {101}


def test_get_event_results_ignores_not_started_by_default():
    deferred_deletion_event_ids = set()
    client = SimpleNamespace(
        request_json=lambda *_args, **_kwargs: {
            "event": {
                "id": 9001,
                "status": {
                    "code": 0,
                    "type": "notstarted",
                    "description": "Not started",
                },
            }
        }
    )

    result = event_details.get_event_results(
        client,
        9001,
        update_event_info=False,
        canonical_event_id=101,
        deferred_deletion_event_ids=deferred_deletion_event_ids,
    )

    assert result is None
    assert deferred_deletion_event_ids == set()


def test_intraday_batches_postponed_event_using_shared_status_parser(monkeypatch):
    response = {
        "event": {
            "id": 9001,
            "status": {
                "code": 60,
                "type": "postponed",
                "description": "postponed",
            },
        }
    }
    deleted_batches = []

    monkeypatch.setattr(
        intraday_result_freshness,
        "_should_check_result_now",
        lambda _event: True,
    )
    monkeypatch.setattr(
        intraday_result_freshness,
        "resolve_sofascore_event_id",
        lambda _event_id: 9001,
    )
    monkeypatch.setattr(
        intraday_result_freshness.api_client,
        "request_json",
        lambda *_args, **_kwargs: response,
    )
    monkeypatch.setattr(
        intraday_result_freshness,
        "update_event_information_from_response",
        lambda _response: True,
    )
    monkeypatch.setattr(
        intraday_result_freshness.EventRepository,
        "batch_delete_events",
        lambda event_ids: deleted_batches.append(event_ids) or len(event_ids),
    )

    stats = intraday_result_freshness.process_intraday_result_freshness(
        [{"id": 101, "sport": "Football", "start_time_utc": None}]
    )

    assert stats["queued_for_deletion"] == 1
    assert stats["deleted_events"] == 1
    assert deleted_batches == [[101]]


def test_intraday_batches_event_endpoint_404(monkeypatch):
    deleted_batches = []

    monkeypatch.setattr(
        intraday_result_freshness,
        "_should_check_result_now",
        lambda _event: True,
    )
    monkeypatch.setattr(
        intraday_result_freshness,
        "resolve_sofascore_event_id",
        lambda _event_id: 9001,
    )
    monkeypatch.setattr(
        intraday_result_freshness.api_client,
        "request_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SofaScoreNotFoundException(9001, "/event/9001")
        ),
    )
    monkeypatch.setattr(
        intraday_result_freshness.EventRepository,
        "batch_delete_events",
        lambda event_ids: deleted_batches.append(event_ids) or len(event_ids),
    )

    stats = intraday_result_freshness.process_intraday_result_freshness(
        [{"id": 101, "sport": "Football", "start_time_utc": None}]
    )

    assert stats["queued_for_deletion"] == 1
    assert stats["deleted_events"] == 1
    assert stats["failed"] == 0
    assert deleted_batches == [[101]]


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


def test_oddspapi_fetcher_uses_only_configured_historical_endpoint():
    calls = []
    historical_payload = {
        "fixtureId": "fixture-1",
        "bookmakers": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {
                                "players": {
                                    "0": [
                                        {
                                            "createdAt": "2026-06-19T00:00:00Z",
                                            "price": 1.9,
                                            "active": True,
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        },
    }
    client = SimpleNamespace(
        get_historical_odds=lambda **kwargs: calls.append(("historical", kwargs))
        or historical_payload,
        get_odds=lambda **kwargs: calls.append(("odds", kwargs)) or {},
    )

    result = OddspapiOddsFetcher(client=client).fetch_odds(
        "fixture-1",
        bookmakers=["pinnacle"],
        endpoint="historical-odds",
        source_sport_id="10",
    )

    assert result.status is OddsFetchStatus.SUCCESS
    assert [name for name, _ in calls] == ["historical"]
    assert result.payload["sportId"] == "10"


def test_oddspapi_client_builds_historical_request_and_enforces_bookmaker_limit():
    captured = {}
    response = SimpleNamespace(
        status_code=200,
        text="",
        headers={},
        json=lambda: {"fixtureId": "fixture-1"},
    )
    client = OddsPapiClient(
        base_url="https://example.test",
        api_key="test-key",
        max_retries=1,
        request_delay_seconds=0,
        endpoint_cooldowns={},
    )
    client.session.get = lambda *_args, **kwargs: captured.update(kwargs) or response

    client.get_historical_odds(
        "fixture-1",
        bookmakers=["pinnacle", "bet365"],
        historical_id=12,
        player_id=3,
        outcome_id=101,
        active=True,
    )

    assert captured["params"] == {
        "fixtureId": "fixture-1",
        "bookmakers": "pinnacle,bet365",
        "id": 12,
        "playerId": 3,
        "outcomeId": 101,
        "active": True,
        "apiKey": "test-key",
    }
    with pytest.raises(ValueError, match="at most 3 bookmakers"):
        client.get_historical_odds(
            "fixture-1",
            bookmakers=["one", "two", "three", "four"],
        )
    with pytest.raises(ValueError, match="only bookmaker"):
        client.get_historical_odds(
            "fixture-1",
            bookmakers=["pinnacle", "betfair-ex"],
            outcome_id=101,
        )
    with pytest.raises(ValueError, match="exactly one outcome_id"):
        client.get_historical_odds(
            "fixture-1",
            bookmakers=["betfair-ex"],
        )


def test_oddspapi_historical_endpoint_observes_five_second_cooldown(monkeypatch):
    responses = iter(
        [
            SimpleNamespace(
                status_code=200,
                text="",
                headers={},
                json=lambda: {"fixtureId": "fixture-1"},
            ),
            SimpleNamespace(
                status_code=200,
                text="",
                headers={},
                json=lambda: {"fixtureId": "fixture-2"},
            ),
        ]
    )
    client = OddsPapiClient(
        base_url="https://example.test",
        api_key="test-key",
        max_retries=1,
        request_delay_seconds=0,
        endpoint_cooldowns={"historical-odds": 5.0},
    )
    client.session.get = lambda *_args, **_kwargs: next(responses)
    clock = iter([0.0, 0.0, 2.0, 2.0])
    sleeps = []
    monkeypatch.setattr(
        oddspapi_client_module.time,
        "monotonic",
        lambda: next(clock),
    )
    monkeypatch.setattr(
        oddspapi_client_module.time,
        "sleep",
        lambda seconds: sleeps.append(seconds),
    )

    client.get_historical_odds("fixture-1", bookmakers=["pinnacle"])
    client.get_historical_odds("fixture-2", bookmakers=["pinnacle"])

    assert sleeps == [3.0]


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
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
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


def test_closing_only_skips_non_closing_pre_start_moments(monkeypatch):
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor.Config.ODDSPAPI_PRE_START_CLOSING_ONLY",
        True,
    )
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("HTTP request must be skipped")
            )
        )
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("mapping index must not be loaded")
        ),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fixture-1",
        minutes_until_start=30,
        has_odds=True,
    )

    summary = processor.process([candidate], bookmakers=["pinnacle"])

    assert summary.requests_attempted == 0
    assert summary.events_skipped == 1
    assert summary.results[0].skip_reason == "oddspapi_closing_only"


def test_historical_mode_ignores_current_endpoint_availability_flag(monkeypatch):
    requested = []
    marked = []
    _stub_mainline_cache(monkeypatch)
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **kwargs: requested.append(kwargs)
            or OddsFetchResult.endpoint_not_found()
        )
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "modules.odds_ingestion.provider_odds_phase.EventSourceMappingRepository."
        "mark_odds_unavailable",
        lambda event_ids, source: marked.append((set(event_ids), source)),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fixture-1",
        minutes_until_start=-5,
        has_odds=False,
        source_sport_id="10",
    )

    summary = processor.process(
        [candidate],
        bookmakers=["pinnacle"],
        endpoint="historical-odds",
    )

    assert summary.requests_attempted == 1
    assert requested[0]["endpoint"] == "historical-odds"
    assert marked == []


def test_live_historical_skips_request_when_mainline_cache_empty(monkeypatch):
    requested = []
    _stub_mainline_cache(monkeypatch, event_ids=())
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **kwargs: requested.append(kwargs)
            or (_ for _ in ()).throw(
                AssertionError("historical-odds must not be requested without mainline cache")
            )
        )
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("mapping index must not be loaded without mainline cache")
        ),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=183537,
        fixture_id="id1000211071802252",
        minutes_until_start=-5,
        has_odds=True,
        source_sport_id="10",
        is_live=True,
    )

    summary = processor.process(
        [candidate],
        bookmakers=["pinnacle", "bet365"],
        endpoint="historical-odds",
    )

    assert requested == []
    assert summary.requests_attempted == 0
    assert summary.http_requests_attempted == 0
    assert summary.events_skipped == 1
    assert summary.results[0].requested is False
    assert summary.results[0].skip_reason == "missing_mainline_cache"


def test_oddspapi_current_response_without_bookmaker_odds_marks_unavailable(monkeypatch):
    marked = []
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **_kwargs: OddsFetchResult.from_payload(
                {
                    "fixtureId": "fixture-1",
                    "participant1Id": 419353,
                    "participant2Id": 419351,
                    "sportId": 13,
                    "hasOdds": False,
                }
            )
        )
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "modules.odds_ingestion.provider_odds_phase.EventSourceMappingRepository."
        "mark_odds_unavailable",
        lambda event_ids, source: marked.append((set(event_ids), source)),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fixture-1",
        minutes_until_start=1,
    )

    summary = processor.process([candidate], bookmakers=["pinnacle"])

    assert summary.requests_attempted == 1
    assert summary.events_skipped == 1
    assert summary.results[0].skip_reason == "no_oddspapi_odds"
    assert marked == [({101}, "oddspapi")]


def test_oddspapi_historical_response_without_bookmaker_odds_does_not_mark_unavailable(
    monkeypatch,
):
    marked = []
    _stub_mainline_cache(monkeypatch)
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **_kwargs: OddsFetchResult.from_payload(
                {
                    "fixtureId": "fixture-1",
                    "sportId": "13",
                    "bookmakerOdds": {},
                }
            )
        ),
        ingestion_service=_NoOpOddspapiIngestionService,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "modules.odds_ingestion.provider_odds_phase.EventSourceMappingRepository."
        "mark_odds_unavailable",
        lambda event_ids, source: marked.append((set(event_ids), source)),
    )
    candidate = OddspapiPreStartCandidate(
        event_id=101,
        fixture_id="fixture-1",
        minutes_until_start=-5,
        has_odds=True,
        source_sport_id="13",
        is_live=True,
    )

    summary = processor.process([candidate], bookmakers=["pinnacle"])

    assert summary.requests_attempted == 1
    assert marked == []


def test_oddspapi_missing_api_key_skips_mapping_query(monkeypatch):
    monkeypatch.setattr(
        oddspapi_odds_phase.Config,
        "ENABLE_ODDSPAPI_PRE_START_ODDS",
        True,
    )
    monkeypatch.setattr(oddspapi_odds_phase.Config, "ODDSPAPI_KEY", "")
    monkeypatch.setattr(oddspapi_odds_phase.Config, "ODDSPAPI_KEYS", [])
    monkeypatch.setattr(
        oddspapi_odds_phase.EventSourceMappingRepository,
        "get_odds_source_states",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("missing API credentials must skip the database lookup")
        ),
    )

    summary = oddspapi_odds_phase.run_oddspapi_pre_start_odds(
        [_event_info()]
    )

    assert summary.events_skipped == 1
    assert summary.requests_attempted == 0
    assert summary.results[0].skip_reason == "missing_oddspapi_api_key"


def test_oddspapi_404_is_persisted_once_for_provider(monkeypatch):
    from infrastructure.settings import Config
    monkeypatch.setattr(Config, "ODDSPAPI_PRE_START_CLOSING_ONLY", False)
    marked = []
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=SimpleNamespace(
            fetch_odds=lambda *_args, **_kwargs: OddsFetchResult.endpoint_not_found()
        )
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor."
        "MarketMappingRepository.build_index",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        "modules.odds_ingestion.provider_odds_phase.EventSourceMappingRepository."
        "mark_odds_unavailable",
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


def test_oddspapi_client_random_api_key(monkeypatch):
    monkeypatch.setattr(oddspapi_client_module.Config, "ODDSPAPI_KEYS", ["key1", "key2", "key3"])
    monkeypatch.setattr(oddspapi_client_module.Config, "ODDSPAPI_KEY", "key1")

    keys_used = set()
    for _ in range(100):
        client = OddsPapiClient()
        keys_used.add(client.api_key)
        client.close()

    assert keys_used == {"key1", "key2", "key3"}

    client_explicit = OddsPapiClient(api_key="explicit_key")
    assert client_explicit.api_key == "explicit_key"
    client_explicit.close()


def test_manual_simulator_uses_production_provider_processors(monkeypatch):
    calls = []
    event_info = _event_info()
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
        "build_pre_start_event_candidates",
        lambda scheduler, events, timings, source_states: (
            calls.append(("candidate_builder", events, source_states))
            or SimpleNamespace(
                candidates=[event_info],
                by_event_id={101: event_info},
            )
        ),
    )
    monkeypatch.setattr(
        pre_start_odds_simulation.EventRepository,
        "_build_event_data_with_legacy_fallback",
        lambda _event: event_info["event_data"],
    )
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "run_sofascore_pre_start_odds",
        lambda event_infos, source_states, **_kwargs: calls.append(
            ("sofascore", event_infos, source_states)
        ),
    )
    monkeypatch.setattr(
        pre_start_odds_simulation,
        "run_oddspapi_pre_start_odds",
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

    assert [call[0] for call in calls] == [
        "candidate_builder",
        "sofascore",
        "oddspapi",
    ]
    assert calls[1][2] is states
    assert calls[2][2] is states


def test_untracked_pipeline_gate_explains_ingestion_without_evaluation(
    monkeypatch,
    caplog,
):
    event = SimpleNamespace(competition_id=999999)
    monkeypatch.setattr(
        simulate_pre_start_check.Config,
        "TRACKED_COMPETITIONS_ONLY",
        False,
    )
    monkeypatch.setattr(
        simulate_pre_start_check.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        True,
    )

    should_continue = simulate_pre_start_check._log_pipeline_eligibility(event)

    assert should_continue is True
    assert "ALERT AND PILLAR PIPELINES WILL SKIP" in caplog.text
    assert "Provider odds can still be ingested" in caplog.text


def test_single_event_simulator_uses_production_op_and_evaluation_flow(
    monkeypatch,
):
    event = SimpleNamespace(
        id=101,
        home_team="Home",
        away_team="Away",
        sport="Football",
        season_id=999999,
        competition_id=999999,
        start_time_utc=None,
    )
    scheduler = SimpleNamespace(
        event_repo=SimpleNamespace(get_event_by_id=lambda _event_id: event),
        recently_rescheduled=set(),
        _active_op_thread=None,
    )
    event_data = {
        "id": event.id,
        "home_team": event.home_team,
        "away_team": event.away_team,
        "sport": event.sport,
        "season_id": event.season_id,
        "competition_id": event.competition_id,
        "start_time_utc": event.start_time_utc,
    }
    event_plan = SimpleNamespace(
        candidates=[{"event_id": event.id}],
        by_event_id={event.id: {"event_id": event.id}},
    )
    op_context = SimpleNamespace(
        event_states={},
        event_ids=set(),
        data_cache={},
    )
    calls = []

    monkeypatch.setattr(
        simulate_pre_start_check,
        "_SingleEventSimulationScheduler",
        lambda: scheduler,
    )
    monkeypatch.setattr(
        simulate_pre_start_check,
        "_log_pipeline_eligibility",
        lambda _event: True,
    )
    monkeypatch.setattr(
        simulate_pre_start_check.EventRepository,
        "_build_event_data_with_legacy_fallback",
        lambda _event: event_data,
    )
    monkeypatch.setattr(
        simulate_pre_start_check,
        "start_oddsportal_scrape_for_events",
        lambda actual_scheduler, events, timings, **kwargs: (
            calls.append(
                ("oddsportal", actual_scheduler, events, timings, kwargs)
            )
            or op_context
        ),
    )
    monkeypatch.setattr(
        simulate_pre_start_check,
        "run_production_odds_phase",
        lambda *args, **kwargs: (
            calls.append(("providers", args, kwargs))
            or SimpleNamespace(event_plan=event_plan)
        ),
    )
    monkeypatch.setattr(
        simulate_pre_start_check,
        "evaluate_pre_start_key_moments",
        lambda actual_scheduler, actual_plan, actual_context, **kwargs: (
            calls.append(
                (
                    "evaluation",
                    actual_scheduler,
                    actual_plan,
                    actual_context,
                    kwargs,
                )
            )
        ),
    )

    result = simulate_pre_start_check._run_pre_start_check_simulation(101, 0)

    assert result is True
    assert [call[0] for call in calls] == [
        "oddsportal",
        "providers",
        "evaluation",
    ]
    assert calls[0][3] == {101: 0}
    assert calls[0][4] == {"debug_mode": True}
    assert calls[1][2]["scheduler"] is scheduler
    assert calls[2][2] is event_plan
    assert calls[2][3] is op_context
    assert calls[2][4] == {"debug_mode": True}


def test_oddsportal_initial_only_choices_are_not_rendered_as_fully_missing():
    external_markets = [
        ExternalMarketQuoteBlock(
            market_id=1,
            bookie_id=2,
            bookie_name="Betfair Exchange",
            market_name="Home/Away Full Time Including Overtime",
            market_group="Home/Away",
            market_period="Full Time Including Overtime",
            choice_group=None,
            is_live=False,
            aggregation="exchange",
            source="oddsportal",
            exchange_side="back",
            contributing_sources=("oddsportal",),
            choices=(
                ExternalChoiceQuote(1, "1", 0, 1.89, None, None, None, None),
                ExternalChoiceQuote(2, "2", 0, 1.72, None, None, None, None),
            ),
        )
    ]

    message = _format_external_markets_section(external_markets)

    assert "Betfair Exchange (Back): 1.89→N/A | 1.72→N/A" in message


class _NoOpOddspapiIngestionService:
    @staticmethod
    def save_from_oddspapi_response(*_args, **_kwargs):
        return SimpleNamespace(skipped=False, reason=None)


class _OneBookmakerOddspapiIngestionService:
    @staticmethod
    def save_from_oddspapi_response(*_args, **_kwargs):
        return SimpleNamespace(
            skipped=False,
            reason=None,
            bookies_detected=1,
            bookmaker_slugs_detected=["pinnacle"],
        )


def _raw_oddspapi_historical_payload():
    return {
        "fixtureId": "fixture-raw-1",
        "bookmakers": {
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "101": {
                                "players": {
                                    "0": [
                                        {
                                            "createdAt": "2026-08-05T18:00:00Z",
                                            "price": 1.9,
                                            "active": True,
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        },
    }


def test_debug_mode_saves_raw_oddspapi_response(tmp_path, monkeypatch):
    _stub_mainline_cache(monkeypatch)
    raw_payload = _raw_oddspapi_historical_payload()
    client = SimpleNamespace(
        get_historical_odds=lambda **_kwargs: raw_payload,
        get_odds=lambda **_kwargs: {},
    )
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=OddspapiOddsFetcher(client=client),
        ingestion_service=_NoOpOddspapiIngestionService,
    )
    candidate = OddspapiPreStartCandidate(
        event_id=156608,
        fixture_id="fixture-raw-1",
        minutes_until_start=-5,
        has_odds=True,
        source_sport_id="13",
    )
    monkeypatch.chdir(tmp_path)

    summary = processor.process(
        [candidate],
        bookmakers=["pinnacle", "bet365"],
        endpoint="historical-odds",
        market_mapping_index={},
        debug_mode=True,
    )

    output_path = (
        tmp_path
        / "debug"
        / "oddspapi_odds_responses"
        / "156608_fixture-raw-1_historical_pinnacle_bet365.json"
    )
    stored_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert summary.events_ingested == 1
    assert stored_payload == raw_payload
    # The raw endpoint uses `bookmakers`; normalized ingestion uses
    # `bookmakerOdds`. This prevents saving the wrong representation.
    assert "bookmakers" in stored_payload


def test_fetcher_does_not_retain_raw_response_when_debug_capture_is_disabled():
    raw_payload = _raw_oddspapi_historical_payload()
    client = SimpleNamespace(
        get_historical_odds=lambda **_kwargs: raw_payload,
        get_odds=lambda **_kwargs: {},
    )

    result = OddspapiOddsFetcher(client=client).fetch_odds(
        "fixture-raw-1",
        bookmakers=["pinnacle"],
        endpoint="historical-odds",
        source_sport_id="13",
    )

    assert result.raw_payload is None


def test_oddspapi_warns_when_requested_and_detected_bookie_counts_differ(caplog, monkeypatch):
    _stub_mainline_cache(monkeypatch)
    raw_payload = _raw_oddspapi_historical_payload()
    client = SimpleNamespace(
        get_historical_odds=lambda **_kwargs: raw_payload,
        get_odds=lambda **_kwargs: {},
    )
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=OddspapiOddsFetcher(client=client),
        ingestion_service=_OneBookmakerOddspapiIngestionService,
    )
    candidate = OddspapiPreStartCandidate(
        event_id=156608,
        fixture_id="fixture-raw-1",
        minutes_until_start=-5,
        has_odds=True,
        source_sport_id="13",
        is_live=True,
    )

    with caplog.at_level(
        logging.WARNING,
        logger=(
            "modules.jobs.pre_start_check_job.providers.oddspapi."
            "odds_batch_processor"
        ),
    ):
        summary = processor.process(
            [candidate],
            bookmakers=["pinnacle", "bet365"],
            endpoint="historical-odds",
            market_mapping_index={},
        )

    assert summary.results[0].bookies_requested == 2
    assert summary.results[0].bookies_detected == 1
    assert "Oddspapi bookmaker coverage gap" in caplog.text
    assert "missing_after_normalization=['bet365']" in caplog.text or (
        "missing_after_normalization" in caplog.text
        and "bet365" in caplog.text
    )


def test_oddspapi_does_not_warn_when_requested_and_detected_counts_match(caplog, monkeypatch):
    _stub_mainline_cache(monkeypatch)
    raw_payload = _raw_oddspapi_historical_payload()
    client = SimpleNamespace(
        get_historical_odds=lambda **_kwargs: raw_payload,
        get_odds=lambda **_kwargs: {},
    )
    processor = OddspapiPreStartOddsBatchProcessor(
        fetcher=OddspapiOddsFetcher(client=client),
        ingestion_service=_OneBookmakerOddspapiIngestionService,
    )
    candidate = OddspapiPreStartCandidate(
        event_id=156608,
        fixture_id="fixture-raw-1",
        minutes_until_start=-5,
        has_odds=True,
        source_sport_id="13",
        is_live=True,
    )

    with caplog.at_level(
        logging.WARNING,
        logger=(
            "modules.jobs.pre_start_check_job.providers.oddspapi."
            "odds_batch_processor"
        ),
    ):
        summary = processor.process(
            [candidate],
            bookmakers=["pinnacle"],
            endpoint="historical-odds",
            market_mapping_index={},
        )

    assert summary.results[0].bookies_requested == 1
    assert summary.results[0].bookies_detected == 1
    assert "Oddspapi bookmaker coverage gap" not in caplog.text
    assert "Oddspapi bookmaker count mismatch" not in caplog.text
