"""Calendar accounting for committed discovery events, without external services."""

import logging
from datetime import datetime, timezone
from importlib import import_module
from types import SimpleNamespace

import pytest

from infrastructure.settings import Config
from infrastructure.persistence.repositories.event_batch_writer import EventWriteResult
from modules.jobs.discovery_persistence_summary import DiscoveryPersistenceSummary
from modules.jobs.daily_discovery import persistence as daily_persistence
from modules.jobs.daily_discovery.extractor import DailyDiscoveryExtractor
from modules.jobs.parallelism import discovery_optimization as parallel


def event(event_id, kickoff="2026-10-02T05:59:00+00:00", sport="Football"):
    return SimpleNamespace(id=event_id, starts_at=datetime.fromisoformat(kickoff), sport=sport)


@pytest.fixture(autouse=True)
def mexico_timezone(monkeypatch, caplog):
    monkeypatch.setattr(Config, "TIMEZONE", "America/Mexico_City")
    caplog.set_level(logging.INFO)


def test_local_midnight_deduplication_and_latest_persisted_calendar(caplog):
    summary = DiscoveryPersistenceSummary()
    summary.record([event(1), event(2, "2026-10-02T06:00:00+00:00", "Tennis"), event(3)])
    summary.record([event(1), event(3, "2026-10-02T06:01:00+00:00", "Basketball"),
                    event(4, "2026-10-02T06:00:00+00:00"), event(5, "2026-10-03T06:00:00+00:00")])
    summary.log(logging.getLogger(__name__), job="daily_discovery",
                requested_date="2026-10-02", run_slot="PM")

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "Discovery persistence summary: job=daily_discovery requested_date=2026-10-02 "
        "slot=PM timezone=America/Mexico_City persisted_unique=5\n"
        "- Basketball:\n  - 2026-10-02: 1\n"
        "- Football:\n  - 2026-10-01: 1\n  - 2026-10-02: 1\n  - 2026-10-03: 1\n"
        "- Tennis:\n  - 2026-10-02: 1\n"
        "- Total by date:\n  - 2026-10-01: 1\n  - 2026-10-02: 3\n  - 2026-10-03: 1"
    )


def test_invalid_kickoff_is_explicit_instead_of_assuming_host_timezone(caplog):
    summary = DiscoveryPersistenceSummary()
    summary.record([SimpleNamespace(id=1, starts_at=datetime(2026, 10, 2), sport=None)])
    summary.log(logging.getLogger(__name__), job="secondary_sources")
    assert "- Unknown:\n  - unknown: 1" in caplog.text


def test_daily_counts_only_committed_events_even_when_odds_fail(monkeypatch, caplog):
    summary = DiscoveryPersistenceSummary()
    monkeypatch.setattr(daily_persistence, "is_supported_sofascore_event", lambda _: True)
    monkeypatch.setattr(daily_persistence, "chunks", lambda events, _: [list(events)])
    monkeypatch.setattr(daily_persistence.EventRepository, "discarded_source_ids", lambda *a: {"2"})
    normalized_ids = []

    def normalize(raw, **kwargs):
        normalized_ids.append(raw["id"])
        return raw

    monkeypatch.setattr(daily_persistence.EventRepository, "batch_upsert_events", lambda _: EventWriteResult(
        events={"1": event(1), "4": event(4, "2026-10-02T06:00:00+00:00", "Tennis")},
        errors={"3": "invalid"}, inserted=1, updated=1,
    ))

    def fail_odds(*args, **kwargs):
        raise RuntimeError("odds write failed")

    monkeypatch.setattr(daily_persistence.MarketOddsIngestionService, "save_from_sofascore_response", fail_odds)
    result = daily_persistence.persist_events_and_optional_odds(
        SimpleNamespace(normalize_event_payload=normalize), [{"id": i} for i in range(1, 5)],
        {"1": {"odds": True}}, tracked_competitions=None, persistence_summary=summary,
    )
    summary.log(logging.getLogger(__name__), job="daily_discovery")
    assert normalized_ids == [1, 3, 4]
    assert (result.persisted, result.discarded, result.failed) == (2, 1, 2)
    assert "- Football:\n  - 2026-10-01: 1" in caplog.text
    assert "- Tennis:\n  - 2026-10-02: 1" in caplog.text
    assert "timezone=America/Mexico_City persisted_unique=2" in caplog.text


def test_daily_extractor_emits_calendar_with_request_context_after_partial_failure(monkeypatch, caplog):
    module = import_module("modules.jobs.daily_discovery.extractor")
    monkeypatch.setattr(module, "sofascore_sport_slugs", lambda _: ["football"])
    monkeypatch.setattr(module, "load_tracked_source_competitions", lambda _: None)
    monkeypatch.setattr(module, "load_source_competitions", lambda _: [])
    monkeypatch.setattr(module, "is_supported_sofascore_event", lambda _: True)
    monkeypatch.setattr(module, "source_competition_filter_reason", lambda *a: None)
    monkeypatch.setattr(module.DailyDiscoveryRepository, "update_sport_status", lambda *a: True)

    def persist(*args, persistence_summary, **kwargs):
        persistence_summary.record([event(42)])
        return daily_persistence.DiscoveryWriteSummary(persisted=1, inserted=1, failed=1)

    monkeypatch.setattr(module, "persist_events_and_optional_odds", persist)
    client = SimpleNamespace(
        get_today_sport_events_response=lambda *a: {
            "scheduled": [{"tournament": {"uniqueTournament": {"id": 7}}}]},
        get_unique_tournament_scheduled_events=lambda *a: {"events": [{"id": 42}]},
        get_today_sport_events_odds_response=lambda *a: None,
    )
    result = DailyDiscoveryExtractor(client).discover_events_for_date("2026-10-02", ["football"], "PM")
    assert result["events_failed"] == 1
    assert "job=daily_discovery requested_date=2026-10-02 slot=PM" in caplog.text
    assert "- Football:\n  - 2026-10-01: 1" in caplog.text


@pytest.fixture
def parallel_committed_event(monkeypatch):
    payload = {"event": {"id": 99, "sport": "Football"}}
    monkeypatch.setattr(parallel, "_eligible_discovery_events", lambda events, *a: list(events))
    monkeypatch.setattr(parallel.EventRepository, "batch_upsert_events", lambda _: EventWriteResult(events={"99": event(99)}))
    monkeypatch.setattr(parallel, "fetch_event_odds_in_parallel", lambda *a, **k: parallel.ParallelOddsFetchSummary(
        odds_by_source_event_id={"99": {"odds": True}},
    ))

    def fail_odds(*args, **kwargs):
        raise RuntimeError("odds write failed")

    monkeypatch.setattr(parallel.MarketOddsIngestionService, "save_from_sofascore_response", fail_odds)
    monkeypatch.setattr(parallel.MarketOddsIngestionService, "save_from_dropping_odds_map_entry", fail_odds)
    return payload


@pytest.mark.parametrize("mode", ["events_only", "odds_first", "parallel"])
def test_secondary_and_dropping_persistence_paths_count_commits(monkeypatch, caplog, parallel_committed_event, mode):
    summary = DiscoveryPersistenceSummary()
    kwargs = dict(tracked_competitions=None, persistence_summary=summary)
    if mode == "events_only":
        processed, _ = parallel.process_events_only([parallel_committed_event], **kwargs)
        assert processed == 1
    elif mode == "odds_first":
        processed, _ = parallel.process_odds_first([parallel_committed_event], **kwargs)
        assert processed == 0
    else:
        processed, _ = parallel.process_with_parallel_db_ops([parallel_committed_event], {"99": {"odds": True}}, **kwargs)
        assert processed == 0
    summary.log(logging.getLogger(__name__), job="test")
    assert "- Football:\n  - 2026-10-01: 1" in caplog.text


def test_dropping_job_logs_committed_events_when_odds_fail(monkeypatch, caplog, parallel_committed_event):
    module = import_module("modules.jobs.discover_dropping_odds.run_discover_dropping_odds")
    monkeypatch.setattr(module, "sofascore_sport_routes", lambda: [("football", "football")])
    monkeypatch.setattr(module, "load_tracked_source_competitions", lambda _: None)
    monkeypatch.setattr(module, "filter_upcoming_events", lambda events: events)
    monkeypatch.setattr(module.api_client, "get_dropping_odds_with_odds_and_events_response", lambda **k: {"ok": True})
    monkeypatch.setattr(module.api_client, "extract_events_and_odds_from_dropping_response",
                        lambda *a, **k: ([parallel_committed_event], {"99": {"odds": True}}))
    module.run_discover_dropping_odds()
    assert "job=dropping_odds requested_date=none slot=none" in caplog.text
    assert "- Football:\n  - 2026-10-01: 1" in caplog.text


def test_secondary_job_deduplicates_across_all_sources(monkeypatch, caplog, parallel_committed_event):
    module = import_module("modules.jobs.discover_secondary_sources.run_discover_secondary_sources")
    monkeypatch.setattr(module, "sofascore_sport_slugs", lambda: ["football"])
    monkeypatch.setattr(module, "load_tracked_source_competitions", lambda _: None)
    events = [parallel_committed_event]
    monkeypatch.setattr(module, "run_high_value_streaks", lambda _: (events, events))
    monkeypatch.setattr(module, "run_team_streaks", lambda _: events)
    monkeypatch.setattr(module, "run_top_h2h", lambda _: events)
    monkeypatch.setattr(module, "run_winning_odds", lambda _: (events, {"99": {"odds": True}}))
    module.run_discover_secondary_sources()
    assert "job=secondary_sources requested_date=none slot=none timezone=America/Mexico_City persisted_unique=1" in caplog.text
