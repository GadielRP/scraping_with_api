from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from importlib import import_module

from infrastructure.settings import Config
from modules.jobs.daily_discovery.extractor import DailyDiscoveryExtractor
from modules.jobs.daily_discovery.persistence import persist_event_and_optional_odds
from modules.jobs.discover_secondary_sources import run_discover_secondary_sources as run_secondary_discovery
from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import (
    OddspapiFixtureBatchResult,
)
from modules.jobs.oddspapi.fixture_discovery.fixture_discovery_job import (
    OddspapiFixtureDiscoveryJob,
)
from modules.jobs.discovery_filters import filter_upcoming_events
from modules.jobs.parallelism.discovery_optimization import process_events_only
from modules.sofascore.discovery_feeds import extract_events_and_odds_from_dropping_response


def _future_event(event_id: int, sport: str) -> dict:
    return {
        "event": {
            "id": event_id,
            "sport": sport,
            "startTimestamp": 4_102_444_800,
        }
    }


def test_shared_discovery_filter_keeps_only_supported_sport_variants(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    events = filter_upcoming_events(
        [
            _future_event(1, "Darts"),
            _future_event(2, "American football"),
            _future_event(3, "Football"),
        ]
    )

    assert [event["event"]["id"] for event in events] == [3]


def test_upcoming_filter_fails_closed_when_filtering_raises(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    def fail_timezone(_timezone):
        raise RuntimeError("invalid timezone")

    monkeypatch.setattr("modules.jobs.discovery_filters.now_in_timezone", fail_timezone)
    assert filter_upcoming_events([_future_event(1, "Football")]) == []


def test_event_only_persistence_rejects_unsupported_sports(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    def unexpected_upsert(_event):
        raise AssertionError("unsupported event reached persistence")

    monkeypatch.setattr(
        "modules.jobs.parallelism.discovery_optimization.EventRepository.upsert_event",
        unexpected_upsert,
    )
    assert process_events_only([_future_event(1, "Darts")], discovery_source="test") == (0, 1)


def test_daily_persistence_rejects_unsupported_sport_after_normalization(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    class ApiClient:
        @staticmethod
        def normalize_event_payload(event, discovery_source):
            assert discovery_source == "daily_discovery"
            return {"event": event}

    def unexpected_upsert(_event):
        raise AssertionError("unsupported event reached persistence")

    monkeypatch.setattr(
        "modules.jobs.daily_discovery.persistence.EventRepository.upsert_event",
        unexpected_upsert,
    )
    assert not persist_event_and_optional_odds(
        ApiClient(),
        {"id": 1, "sport": "Darts"},
    )


def test_secondary_discoveries_run_independently_when_a_feed_is_empty(monkeypatch):
    module = import_module("modules.jobs.discover_secondary_sources.run_discover_secondary_sources")
    calls = []
    monkeypatch.setattr(module, "run_high_value_streaks", lambda: ([], [{"id": 2}]))
    monkeypatch.setattr(module, "run_team_streaks", lambda: [])
    monkeypatch.setattr(module, "run_top_h2h", lambda: [])
    monkeypatch.setattr(module, "run_winning_odds", lambda: ([], {}))
    monkeypatch.setattr(
        module,
        "process_events_only",
        lambda events, discovery_source: calls.append((discovery_source, events)) or (0, len(events)),
    )
    monkeypatch.setattr(module, "process_with_parallel_db_ops", lambda *args, **kwargs: (0, 0))

    run_secondary_discovery()

    assert calls == [
        ("high_value_streaks", []),
        ("high_value_streaks_h2h", [{"id": 2}]),
        ("h2h", []),
    ]


def test_sofascore_feed_filters_before_event_normalization(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    response = {
        "events": [
            {
                "id": 1,
                "slug": "darts-match",
                "startTimestamp": 4_102_444_800,
                "sport": "Darts",
                "competition": "Darts",
                "homeTeam": {"id": 1, "name": "Home"},
                "awayTeam": {"id": 2, "name": "Away"},
            },
            {
                "id": 2,
                "slug": "football-match",
                "startTimestamp": 4_102_444_800,
                "sport": "Football",
                "competition": "Football",
                "homeTeam": {"id": 3, "name": "Home"},
                "awayTeam": {"id": 4, "name": "Away"},
            },
        ],
        "oddsMap": {"1": {"markets": []}, "2": {"markets": []}},
    }

    events, odds_map = extract_events_and_odds_from_dropping_response(response, odds_extraction=True)

    assert [event["event"]["id"] for event in events] == [2]
    assert list(odds_map) == ["2"]


def test_daily_discovery_skips_unsupported_sport_scope_before_api_calls(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    class FailingApiClient:
        def __getattr__(self, name):
            raise AssertionError(f"unsupported sport reached API: {name}")

    result = DailyDiscoveryExtractor(api_client=FailingApiClient()).discover_events_for_date(
        "2026-09-19",
        sports=["table-tennis"],
    )

    assert result == {"events_processed": 0, "events_inserted": 0, "odds_inserted": 0}


def test_oddspapi_discovery_filters_scope_and_fixture_payloads(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    processed_payloads = []

    class Client:
        def get_fixtures(self, **kwargs):
            return [
                {"fixtureId": "darts-1", "sportName": "Darts"},
                {"fixtureId": "football-1", "sportName": "Soccer"},
            ]

    class Processor:
        def process_batch(self, fixture_payloads, **kwargs):
            processed_payloads.extend(fixture_payloads)
            return OddspapiFixtureBatchResult()

    @contextmanager
    def session_context():
        yield object()

    from modules.jobs.oddspapi.fixture_discovery import fixture_discovery_job as job_module

    monkeypatch.setattr(job_module.db_manager, "get_session", session_context)
    job = OddspapiFixtureDiscoveryJob(
        client=Client(),
        sports={"darts": 99, "soccer": 10},
        create_mappings=False,
        batch_processor=Processor(),
    )

    summary = job.run(
        datetime(2026, 9, 19, tzinfo=timezone.utc),
        datetime(2026, 9, 20, tzinfo=timezone.utc),
    )

    assert list(job.sports) == ["soccer"]
    assert [payload["fixtureId"] for payload in processed_payloads] == ["football-1"]
    assert summary.total_fixtures_fetched == 1
