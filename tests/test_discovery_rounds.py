"""SofaScore discovery admits only configured classified or raw rounds."""

from dataclasses import replace
from unittest.mock import Mock
from types import SimpleNamespace

import pytest

from infrastructure.settings import Config, discovery as settings
from modules.jobs.discovery import fetching, filters, persistence
from modules.sofascore import discovery_feeds, event_metadata, event_normalizer
from modules.sofascore.event_metadata import classify_event_round
from modules.sofascore.event_normalizer import normalize_event_payload


@pytest.fixture(autouse=True)
def discovery_policy(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football"])
    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE,
        round_filter_enabled=True,
        allowed_rounds=frozenset({"regular-season"}),
        filters=replace(settings.SOFASCORE.filters, future_only=False),
    ))


def event(round_slug=None, competition="League"):
    raw = dict(
        id=4001, slug="home-away", startTimestamp=1791108000,
        homeTeam=dict(id=1, name="Home", gender="M"),
        awayTeam=dict(id=2, name="Away", gender="M"),
        tournament=dict(id=50, name=competition,
                        uniqueTournament=dict(id=5, name=competition),
                        category=dict(name="Country", sport=dict(name="Football"))),
    )
    if round_slug is not None:
        raw["roundInfo"] = dict(slug=round_slug)
    return raw


def test_default_round_policy_is_enabled_and_regular_season_only():
    defaults = settings.SofascoreDiscoverySettings()
    assert defaults.round_filter_enabled
    assert defaults.allowed_rounds == frozenset({"regular-season"})


@pytest.mark.parametrize("slug,competition,expected,accepted", [
    (None, "League", "regular_season", True),
    ("regular-season", "League", "regular-season", True),
    ("regular_season", "League", "regular_season", True),
    ("quarterfinal", "League", "knockouts/playoffs", False),
    ("wild-card-round", "League", "knockouts/playoffs", False),
    ("placement-match", "League", "knockouts/playoffs", False),
    ("match-for-5th-place", "League", "knockouts/playoffs", False),
    ("round-of-256", "League", "knockouts/playoffs", False),
    (None, "Friendly Games", "friendly", False),
    (None, "League Preseason", "pre_season", False),
    ("preseason", "League", "pre_season", False),
    (None, "League Qualification", "qualification", False),
    (None, "League Promotion", "relegation/promotion", False),
    ("preliminary", "League", "qualification", False),
    (None, "Preliminary Round", "qualification", False),
    ("custom-stage", "League", "custom-stage", False),
])
def test_discovery_and_full_normalization_use_same_round(slug, competition, expected, accepted):
    raw = event(slug, competition)
    assert classify_event_round(raw) == expected
    normalized = normalize_event_payload(raw)
    assert normalized["event"]["round"] == expected
    for payload in (raw, {"event": raw}, normalized):
        assert filters.admit_sofascore_event(payload).rejection_reason == (None if accepted else "round_not_allowed")


def test_allowed_rounds_are_configurable_and_accept_internal_aliases(monkeypatch):
    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE,
        allowed_rounds=frozenset({" Regular_Season ", "knockouts/playoffs", "custom-stage"}),
    ))
    assert settings.SOFASCORE.allowed_rounds == frozenset({"regular-season", "knockouts/playoffs", "custom-stage"})
    for raw in (event(), event("quarterfinal"), event("custom-stage")):
        assert filters.admit_sofascore_event(raw).rejection_reason is None
    assert filters.admit_sofascore_event(event(competition="Friendly Games")).rejection_reason == "round_not_allowed"


def test_round_filter_can_be_disabled(monkeypatch):
    monkeypatch.setattr(settings, "SOFASCORE", replace(settings.SOFASCORE, round_filter_enabled=False))
    assert filters.admit_sofascore_event(event("quarterfinal")).rejection_reason is None


def test_explicit_unknown_normalized_round_is_not_assumed_regular_season():
    raw = normalize_event_payload(event())
    raw["event"]["round"] = None
    assert filters.admit_sofascore_event(raw).rejection_reason == "round_not_allowed"


def test_nba_cup_persistence_override_is_applied_before_admission():
    raw = event("regular-season", "NBA Cup")
    raw["season"] = dict(id=84238, name="NBA Cup 2025/2026")
    assert classify_event_round(raw) == "knockouts/playoffs"
    assert normalize_event_payload(raw)["event"]["round"] == "knockouts/playoffs"
    assert filters.admit_sofascore_event(raw).rejection_reason == "round_not_allowed"


def test_round_policy_does_not_apply_to_oddspapi():
    fixture = dict(fixtureId="fixture-1", sportName="Soccer", round="quarterfinal")
    policy = replace(settings.ODDSPAPI.filters, future_only=False)
    assert filters.oddspapi_fixture_filter_reason(fixture, policy=policy) is None


def test_feeds_and_nearest_events_reject_round_before_normalization(monkeypatch, caplog):
    raw = event("quarterfinal")
    normalize = Mock()
    monkeypatch.setattr(discovery_feeds, "normalize_event_payload", normalize)
    monkeypatch.setattr(fetching.api_client, "normalize_event_payload", normalize)
    monkeypatch.setattr(fetching.api_client, "get_nearest_event_for_team", lambda _: {"event": raw})
    with caplog.at_level("INFO", logger=discovery_feeds.__name__):
        assert discovery_feeds.extract_events_and_odds_from_dropping_response(
            {"events": [raw], "oddsMap": {"4001": {"markets": [1]}}}, tracked_competitions=None,
        ) == ([], {})
    assert "round_not_allowed" in caplog.text
    assert discovery_feeds.extract_events_from_high_value_streaks(
        {"general": [{"event": raw}], "head2head": [{"event": raw}]}, tracked_competitions=None,
    ) == ([], [])
    assert fetching.fetch_nearest_team_events([1], max_workers=1, tracked_competitions=None) == []
    normalize.assert_not_called()


@pytest.mark.parametrize("mode", ["events", "feed_odds", "fetch_odds"])
def test_shared_persistence_rejects_normalized_round_before_fetch_or_write(monkeypatch, mode):
    raw = normalize_event_payload(event("quarterfinal"))
    writer, fetcher = Mock(), Mock()
    monkeypatch.setattr(persistence.EventRepository, "batch_upsert_events", writer)
    monkeypatch.setattr(persistence, "fetch_event_odds", fetcher)
    if mode == "events":
        result = persistence.persist_events([raw], tracked_competitions=None)
    elif mode == "feed_odds":
        result = persistence.persist_events_with_odds([raw], {}, tracked_competitions=None)
    else:
        result = persistence.fetch_and_persist_events_with_odds([raw], tracked_competitions=None)
    assert result == (0, 1)
    writer.assert_not_called()
    fetcher.assert_not_called()


@pytest.mark.parametrize("route", ["dropping", "high_value_streaks", "nearest_team"])
def test_accepted_feeds_reuse_round_through_normalization_and_persistence(monkeypatch, route):
    raw = event("regular-season")
    classifier = Mock(wraps=event_metadata.classify_event_round)
    monkeypatch.setattr(event_metadata, "classify_event_round", classifier)
    monkeypatch.setattr(event_normalizer, "classify_event_round", Mock(
        side_effect=AssertionError("Round must not be classified again during normalization"),
    ))
    if route == "dropping":
        events, _ = discovery_feeds.extract_events_and_odds_from_dropping_response(
            {"events": [None, {"event": None}, raw]}, tracked_competitions=None,
        )
    elif route == "high_value_streaks":
        events, h2h = discovery_feeds.extract_events_from_high_value_streaks(
            {"general": [{"event": raw}]}, tracked_competitions=None,
        )
        assert h2h == []
    else:
        monkeypatch.setattr(fetching.api_client, "get_nearest_event_for_team", lambda _: {"event": raw})
        events = fetching.fetch_nearest_team_events([1], max_workers=1, tracked_competitions=None)
    assert len(events) == 1
    assert events[0]["event"]["round"] == "regular-season"
    assert "round" not in raw
    discarded = Mock(return_value=set())
    writer = Mock(return_value=SimpleNamespace(events={"4001": SimpleNamespace(id=1)}))
    monkeypatch.setattr(persistence.EventRepository, "discarded_source_ids", discarded)
    monkeypatch.setattr(persistence.EventRepository, "batch_upsert_events", writer)
    assert persistence.persist_events(events, tracked_competitions=None) == (1, 0)
    assert classifier.call_count == 1
    discarded.assert_called_once_with("sofascore", ["4001"])
    writer.assert_called_once_with(events)


def test_round_preparation_and_reuse_need_no_database_access_or_payload_copy(monkeypatch):
    from infrastructure.persistence.database import db_manager

    monkeypatch.setattr(db_manager, "get_session", Mock(
        side_effect=AssertionError("Admission and normalization must not query the database"),
    ))
    raw = event("regular-season")
    admission = filters.admit_sofascore_event(raw)
    assert admission.rejection_reason is None
    assert admission.raw_event is raw
    normalized = normalize_event_payload(raw, resolved_round=admission.round_name)
    assert normalized["event"]["round"] == "regular-season"
    unknown = normalize_event_payload(raw, resolved_round=None)
    assert unknown["event"]["round"] is None
    assert "round" not in raw


def test_time_rejection_skips_round_classification(monkeypatch):
    monkeypatch.setattr(event_metadata, "classify_event_round", Mock(
        side_effect=AssertionError("Started events must be rejected before classifying their round"),
    ))
    policy = replace(settings.SOFASCORE.filters, future_only=True)
    raw = event()
    raw["startTimestamp"] = 1
    assert filters.admit_sofascore_event(raw, policy=policy).rejection_reason == "start_too_soon_or_started"
