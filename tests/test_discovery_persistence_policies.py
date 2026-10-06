from importlib import import_module
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from infrastructure.persistence.repositories.event_batch_writer import EventWriteResult
from infrastructure.settings import discovery as settings
from modules.jobs.discovery import persistence
from modules.jobs.discovery.fetching import OddsFetchSummary
from shared.execution_context import WorkDeferred


def event(source_id):
    return {"id": source_id, "sport": "Football", "startTimestamp": 4_102_444_800}


@pytest.fixture
def writes(monkeypatch):
    discarded = Mock(return_value=set())
    monkeypatch.setattr(persistence.EventRepository, "discarded_source_ids", discarded)
    writer = Mock(
        side_effect=lambda events: EventWriteResult(
            events={str(raw["id"]): SimpleNamespace(id=raw["id"] + 100) for raw in events}
        )
    )
    monkeypatch.setattr(persistence.EventRepository, "batch_upsert_events", writer)
    return discarded, writer


def test_odds_first_filters_once_and_marks_only_confirmed_missing_endpoints(monkeypatch, writes):
    discarded, writer = writes
    monkeypatch.setattr(
        persistence,
        "fetch_event_odds",
        lambda *_args, **_kwargs: OddsFetchSummary(
            odds_by_source_event_id={"1": {"markets": [1]}},
            endpoint_missing_source_event_ids={2},
            failed_source_event_ids={3},
        ),
    )
    mappings = Mock(return_value={"2": 102})
    mark_missing = Mock()
    monkeypatch.setattr(
        persistence.EventSourceMappingRepository, "get_event_ids_by_sofascore_ids", mappings
    )
    monkeypatch.setattr(
        persistence.EventSourceMappingRepository, "mark_odds_unavailable", mark_missing
    )
    save = Mock(return_value=SimpleNamespace(markets_saved=1))
    monkeypatch.setattr(
        persistence.MarketOddsIngestionService, "save_from_sofascore_response", save
    )
    store = SimpleNamespace(record_mapping=Mock())

    assert persistence.fetch_and_persist_events_with_odds(
        [event(1), event(2), event(3)], tracked_competitions=None, run_store=store
    ) == (1, 2)
    discarded.assert_called_once()
    writer.assert_called_once_with([event(1)])
    mappings.assert_called_once_with(["2"])
    mark_missing.assert_called_once()
    assert list(mark_missing.call_args.args[0]) == [102]
    assert mark_missing.call_args.args[1] == "sofascore"
    save.assert_called_once_with(101, {"markets": [1]}, source="sofascore")
    assert set(store.record_mapping.call_args.args[0]) == {"1"}


def test_feed_policy_keeps_events_without_odds(monkeypatch, writes):
    _, writer = writes
    save = Mock(return_value=SimpleNamespace(markets_saved=1))
    monkeypatch.setattr(
        persistence.MarketOddsIngestionService, "save_from_dropping_odds_map_entry", save
    )
    events = [event(1), event(2)]
    assert persistence.persist_events_with_odds(
        events, {"1": {"markets": [1]}}, tracked_competitions=None
    ) == (1, 1)
    writer.assert_called_once_with(events)
    save.assert_called_once()


def test_daily_normalization_propagates_budget_pause(monkeypatch, writes):
    from modules.jobs.daily_discovery.persistence import persist_daily_events

    _, writer = writes
    client = SimpleNamespace(normalize_event_payload=Mock(side_effect=WorkDeferred("budget")))
    with pytest.raises(WorkDeferred, match="budget"):
        persist_daily_events(client, [event(1)])
    writer.assert_not_called()


def test_secondary_pause_closes_temporary_store_and_reaches_worker(monkeypatch):
    job = import_module("modules.jobs.discover_secondary_sources.run_discover_secondary_sources")
    original_store = job.DiscoveryRunStore
    directories = []

    def create_store():
        store = original_store()
        directories.append(store._directory.path)
        return store

    monkeypatch.setattr(job, "DiscoveryRunStore", create_store)
    monkeypatch.setattr(job, "sofascore_discovery_sport_slugs", lambda: ["football"])
    monkeypatch.setattr(job, "load_tracked_source_competitions", lambda _: None)
    monkeypatch.setattr(job, "run_high_value_streaks", Mock(side_effect=WorkDeferred("budget")))
    with pytest.raises(WorkDeferred, match="budget"):
        job.run_discover_secondary_sources()
    assert len(directories) == 1 and not directories[0].exists()


@pytest.mark.parametrize("require_odds", [True, False])
def test_team_streaks_odds_requirement_can_be_disabled(monkeypatch, require_odds):
    job = import_module("modules.jobs.discover_secondary_sources.run_discover_secondary_sources")
    monkeypatch.setattr(settings, "SOFASCORE", replace(settings.SOFASCORE, team_streaks_require_odds=require_odds))
    monkeypatch.setattr(job, "sofascore_discovery_sport_slugs", lambda: ["football"])
    monkeypatch.setattr(job, "load_tracked_source_competitions", lambda _: None)
    monkeypatch.setattr(job, "run_high_value_streaks", lambda _: ([], []))
    monkeypatch.setattr(job, "run_team_streaks", lambda _: [event(1)])
    monkeypatch.setattr(job, "run_top_h2h", lambda _: [])
    monkeypatch.setattr(job, "run_winning_odds", lambda _: ([], {}))
    plain, odds_first = Mock(return_value=(1, 0)), Mock(return_value=(1, 0))
    monkeypatch.setattr(job, "persist_events", plain)
    monkeypatch.setattr(job, "fetch_and_persist_events_with_odds", odds_first)
    monkeypatch.setattr(job, "persist_events_with_odds", Mock())
    monkeypatch.setattr(job, "log_discovery_summary", Mock())
    job.run_discover_secondary_sources()
    if require_odds:
        odds_first.assert_called_once()
        assert all(call.args[1] != "team_streaks" for call in plain.call_args_list)
    else:
        odds_first.assert_not_called()
        assert any(call.args[:2] == ([event(1)], "team_streaks") for call in plain.call_args_list)
