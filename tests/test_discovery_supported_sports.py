from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from importlib import import_module

import pytest

from infrastructure.settings import Config
from modules.competition.discovery_scope import SourceCompetitionIds
from modules.jobs.daily_discovery.extractor import DailyDiscoveryExtractor
from modules.jobs.daily_discovery.persistence import persist_event_and_optional_odds, DiscoveryWriteSummary
from modules.jobs.discover_secondary_sources import run_discover_secondary_sources as run_secondary_discovery
from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import (
    OddspapiFixtureBatchResult,
)
from modules.jobs.oddspapi.fixture_discovery.fixture_discovery_job import (
    OddspapiFixtureDiscoveryJob,
)
from modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery import _resolve_sports
from modules.jobs.discovery_filters import filter_upcoming_events
from modules.sports.catalog import (
    oddspapi_sport_ids,
    sofascore_sport_slugs,
    sofascore_sport_routes,
)
from modules.jobs.parallelism.discovery_optimization import (
    parallel_team_event_fetching,
    process_events_only,
)
from modules.sofascore.discovery_feeds import extract_events_and_odds_from_dropping_response
from modules.sofascore.discovery_feeds import get_dropping_odds_with_odds_and_events_response


def _future_event(event_id: int, sport: str) -> dict:
    return {
        "event": {
            "id": event_id,
            "sport": sport,
            "startTimestamp": 4_102_444_800,
        }
    }


def test_shared_discovery_filter_logs_reason_for_each_rejection(monkeypatch, caplog):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    caplog.set_level("INFO", logger="modules.jobs.discovery_filters")

    events = filter_upcoming_events(
        [
            _future_event(1, "Darts"),
            _future_event(2, "American football"),
            _future_event(3, "Football"),
            {"event": {"id": 4, "sport": "Football"}},
            {"event": {"id": 5, "sport": "Football", "startTimestamp": 1}},
        ]
    )

    assert [event["event"]["id"] for event in events] == [3]
    assert (
        "input=5 kept=1 rejected_unsupported_sport=2 "
        "rejected_missing_start_timestamp=1 rejected_invalid_start_timestamp=0 "
        "rejected_start_too_soon_or_started=1"
    ) in caplog.text


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


def test_event_only_persistence_rejects_untracked_normalized_event(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football"])

    def unexpected_upsert(_event):
        raise AssertionError("untracked event reached persistence")

    monkeypatch.setattr(
        "modules.jobs.parallelism.discovery_optimization.EventRepository.upsert_event",
        unexpected_upsert,
    )
    event = {
        "event": {"id": 1, "sport": "Football"},
        "competition_ref": {"source_unique_tournament_id": 888},
    }

    assert process_events_only(
        [event],
        discovery_source="test",
        tracked_competitions={SourceCompetitionIds(None, 777)},
    ) == (0, 1)


def test_team_streak_rejects_unsupported_raw_event_before_normalization(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    monkeypatch.setattr(
        "modules.jobs.parallelism.discovery_optimization.load_tracked_source_competitions",
        lambda _source: frozenset({SourceCompetitionIds(None, 777)}),
    )
    monkeypatch.setattr(
        "modules.jobs.parallelism.discovery_optimization.api_client.get_nearest_event_for_team",
        lambda _team_id: {"id": 1, "sport": "Darts"},
    )

    def unexpected_normalization(*_args, **_kwargs):
        raise AssertionError("unsupported response reached event normalization")

    monkeypatch.setattr(
        "modules.jobs.parallelism.discovery_optimization.api_client.normalize_event_payload",
        unexpected_normalization,
    )

    assert parallel_team_event_fetching([42], max_workers=1) == []


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
    monkeypatch.setattr(module, "load_tracked_source_competitions", lambda _source: {SourceCompetitionIds(None, 777)})
    monkeypatch.setattr(module, "run_high_value_streaks", lambda *_args: ([], [{"id": 2}]))
    monkeypatch.setattr(module, "run_team_streaks", lambda *_args: [])
    monkeypatch.setattr(module, "run_top_h2h", lambda *_args: [])
    monkeypatch.setattr(module, "run_winning_odds", lambda *_args: ([], {}))
    monkeypatch.setattr(
        module,
        "process_events_only",
        lambda events, discovery_source, **_kwargs: calls.append((discovery_source, events)) or (0, len(events)),
    )
    monkeypatch.setattr(module, "process_with_parallel_db_ops", lambda *args, **kwargs: (0, 0))

    run_secondary_discovery()

    assert calls == [
        ("high_value_streaks", []),
        ("high_value_streaks_h2h", [{"id": 2}]),
        ("h2h", []),
    ]


def test_sofascore_feed_filters_before_event_normalization(monkeypatch, caplog):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    caplog.set_level("DEBUG", logger="modules.sofascore.discovery_feeds")
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
                "tournament": {"uniqueTournament": {"id": 777}},
            },
            {
                "id": 2,
                "slug": "football-match",
                "startTimestamp": 4_102_444_800,
                "sport": "Football",
                "competition": "Football",
                "homeTeam": {"id": 3, "name": "Home"},
                "awayTeam": {"id": 4, "name": "Away"},
                "tournament": {"uniqueTournament": {"id": 777}},
            },
            {
                "id": 3,
                "slug": "untracked-match",
                "startTimestamp": 4_102_444_800,
                "sport": "Football",
                "competition": "Football",
                "homeTeam": {"id": 5, "name": "Home"},
                "awayTeam": {"id": 6, "name": "Away"},
                "tournament": {"uniqueTournament": {"id": 888}},
            },
        ],
        "oddsMap": {"1": {"markets": []}, "2": {"markets": []}, "3": {"markets": []}},
    }

    events, odds_map = extract_events_and_odds_from_dropping_response(
        response,
        odds_extraction=True,
        tracked_competitions={SourceCompetitionIds(None, 777)},
    )

    assert [event["event"]["id"] for event in events] == [2]
    assert list(odds_map) == ["2"]
    assert "events=3 rejected_unsupported_sport=1 rejected_untracked_competition=1" in caplog.text
    assert "eligible_events=1 response_odds=3 odds_without_eligible_event=2 eligible_odds=1" in caplog.text
    assert "reason=unsupported_sport" in caplog.text
    assert "reason=untracked_competition" in caplog.text


def test_daily_discovery_scopes_tournament_calls_before_fetching_events(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football"])
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.load_tracked_source_competitions",
        lambda _source: frozenset({SourceCompetitionIds(None, 100)}),
    )
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.DailyDiscoveryRepository.update_sport_status",
        lambda *_args: True,
    )
    persisted = []
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.persist_events_and_optional_odds",
        lambda _client, events, _odds, **kwargs: persisted.extend((event, kwargs) for event in events) or DiscoveryWriteSummary(persisted=len(events), inserted=len(events)),
    )
    requested_tournaments = []

    class ApiClient:
        def get_today_sport_events_response(self, _date, _sport, _page):
            return {
                "scheduled": [
                    {"timezoneEventCount": {"UTC": 1}, "tournament": {"uniqueTournament": {"id": 100}}},
                    {"timezoneEventCount": {"UTC": 1}, "tournament": {"uniqueTournament": {"id": 200}}},
                ],
                "hasNextPage": False,
            }

        def get_unique_tournament_scheduled_events(self, tournament_id, _date):
            requested_tournaments.append(tournament_id)
            return {
                "events": [
                    {
                        "id": 1,
                        "sport": "Football",
                        "startTimestamp": 4_102_444_800,
                        "tournament": {"uniqueTournament": {"id": tournament_id}},
                    }
                ]
            }

        def get_today_sport_events_odds_response(self, _date, _sport):
            return {"odds": {}}

    from modules.jobs.daily_discovery.extractor import DailyDiscoveryExtractor

    result = DailyDiscoveryExtractor(ApiClient()).discover_events_for_date(
        "2100-01-01",
        sports=["football"],
        run_slot="AM",
    )

    assert requested_tournaments == [100]
    assert result["events_inserted"] == 1
    assert persisted[0][1]["tracked_competitions"] == frozenset({SourceCompetitionIds(None, 100)})


def test_daily_discovery_toggle_off_logs_shadow_rejections_without_filtering(monkeypatch, caplog):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football"])
    monkeypatch.setattr(Config, "DISCOVERY_TRACKED_COMPETITIONS_ONLY", False)
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.load_tracked_source_competitions",
        lambda _source: None,
    )
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.load_source_competitions",
        lambda _source: frozenset({SourceCompetitionIds(None, 100)}),
    )
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.DailyDiscoveryRepository.update_sport_status",
        lambda *_args: True,
    )
    persisted = []
    monkeypatch.setattr(
        "modules.jobs.daily_discovery.extractor.persist_events_and_optional_odds",
        lambda _client, events, _odds, **_kwargs: persisted.extend(events) or DiscoveryWriteSummary(persisted=len(events), inserted=len(events)),
    )
    requested_tournaments = []

    class ApiClient:
        def get_today_sport_events_response(self, _date, _sport, _page):
            return {
                "scheduled": [
                    {"timezoneEventCount": {"UTC": 1}, "tournament": {"uniqueTournament": {"id": 100}}},
                    {"timezoneEventCount": {"UTC": 1}, "tournament": {"uniqueTournament": {"id": 200}}},
                ],
                "hasNextPage": False,
            }

        def get_unique_tournament_scheduled_events(self, tournament_id, _date):
            requested_tournaments.append(tournament_id)
            return {
                "events": [
                    {
                        "id": tournament_id,
                        "sport": "Football",
                        "startTimestamp": 4_102_444_800,
                        "tournament": {"uniqueTournament": {"id": tournament_id}},
                    }
                ]
            }

        def get_today_sport_events_odds_response(self, _date, _sport):
            return {"odds": {}}

    caplog.set_level("INFO", logger="modules.jobs.daily_discovery.extractor")
    result = DailyDiscoveryExtractor(ApiClient()).discover_events_for_date(
        "2100-01-01",
        sports=["football"],
        run_slot="AM",
    )

    assert requested_tournaments == [100, 200]
    assert len(persisted) == 2
    assert result["events_inserted"] == 2
    assert "tracked_competition_filter=disabled_observation_only provider_competition_pairs=1" in caplog.text
    assert "mode=observe_only" in caplog.text
    assert "would_reject_untracked=1" in caplog.text
    assert "rejected_untracked=0" in caplog.text
    assert (
        "Daily event filter sport=football mode=observe_only response_events=2 "
        "rejected_unsupported_sport=0 rejected_untracked=0 rejected_missing_ids=0 "
        "would_reject_untracked=1"
    ) in caplog.text


def test_daily_persistence_rejects_untracked_event_before_normalizing(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football"])

    class ApiClient:
        @staticmethod
        def normalize_event_payload(*_args, **_kwargs):
            raise AssertionError("untracked event reached normalization")

    event = {
        "id": 10,
        "sport": "Football",
        "tournament": {"uniqueTournament": {"id": 200}},
    }
    assert not persist_event_and_optional_odds(
        ApiClient(),
        event,
        tracked_competitions={SourceCompetitionIds(None, 100)},
    )


def test_canonical_sports_resolve_to_each_providers_exact_scope(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["american_football", "handball", "tennis_doubles"])

    assert sofascore_sport_slugs() == ["american-football", "tennis", "handball"]
    assert sofascore_sport_routes() == [
        ("american_football", "american-football"),
        ("tennis_doubles", "tennis"),
        ("handball", "handball"),
    ]
    assert {
        slug: oddspapi_sport_ids()[slug]
        for slug in ("american-football", "tennis", "handball")
    } == {
        "american-football": 14,
        "tennis": 12,
        "handball": 22,
    }


def test_display_sport_labels_normalize_and_dropping_adapter_uses_provider_slug(monkeypatch, caplog):
    monkeypatch.setattr(
        Config,
        "SUPPORTED_SPORTS",
        ["Football", "American football", "Ice hockey", "Tennis doubles"],
    )
    requests = []
    caplog.set_level("INFO", logger="modules.sofascore.discovery_feeds")

    class Client:
        def request_json_or_none(self, endpoint):
            requests.append(endpoint)
            return {"events": []}

    get_dropping_odds_with_odds_and_events_response(Client(), "American football")

    assert sofascore_sport_slugs() == ["football", "american-football", "tennis", "ice-hockey"]
    assert requests == ["/odds/1/dropping/american-football"]
    assert "✈️ Fetching SofaScore dropping feed" in caplog.text
    assert "requested_sport=American football canonical_sport=american_football " in caplog.text
    assert "endpoint=/odds/1/dropping/american-football" in caplog.text
    with pytest.raises(ValueError, match="disabled SofaScore sport"):
        get_dropping_odds_with_odds_and_events_response(Client(), "baseball")


def test_oddspapi_cli_canonical_sports_translate_to_provider_slugs(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football", "american_football", "handball"])

    assert _resolve_sports("football,american_football,handball") == {
        "soccer": 10,
        "american-football": 14,
        "handball": 22,
    }


def test_oddspapi_job_keeps_shared_tennis_route_for_doubles_only(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["tennis_doubles"])

    job = OddspapiFixtureDiscoveryJob(client=object())

    assert job.sports == {"tennis": 12}


def test_daily_discovery_skips_unsupported_sport_scope_before_api_calls(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    class FailingApiClient:
        def __getattr__(self, name):
            raise AssertionError(f"unsupported sport reached API: {name}")

    result = DailyDiscoveryExtractor(api_client=FailingApiClient()).discover_events_for_date(
        "2026-09-19",
        sports=["table-tennis"],
    )

    assert result == dict.fromkeys(("events_processed", "events_persisted", "events_inserted",
                                    "events_updated", "events_discarded", "events_failed", "odds_inserted"), 0)


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
