"""Tennis ranking admission uses provider metadata before normalization."""

from dataclasses import replace
from unittest.mock import Mock

import pytest

from infrastructure.settings import Config
from infrastructure.settings import discovery as settings
from modules.jobs.discovery import fetching, filters
from modules.sofascore import discovery_feeds


@pytest.fixture(autouse=True)
def discovery_policy(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["tennis", "tennis_doubles", "football"])
    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE,
        tennis_ranking_filter_enabled=False,
        tennis_ranking_cutoff=120,
        filters=replace(settings.SOFASCORE.filters, future_only=False),
    ))


def enable_ranking_filter(monkeypatch, cutoff=120):
    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE,
        tennis_ranking_filter_enabled=True,
        tennis_ranking_cutoff=cutoff,
    ))


def event(home=None, away=None, sport="tennis"):
    return {
        "id": 1,
        "sport": sport,
        "homeTeam": {"name": "Home", **(home or {})},
        "awayTeam": {"name": "Away", **(away or {})},
    }


def test_ranking_filter_is_disabled_by_default():
    assert not settings.SofascoreDiscoverySettings().tennis_ranking_filter_enabled
    assert not settings.SOFASCORE.tennis_ranking_filter_enabled
    assert settings.SofascoreDiscoverySettings().tennis_ranking_cutoff == 120
    assert filters.sofascore_event_filter_reason(
        event(home={"playerTeamInfo": {"currentRanking": 1788}})
    ) is None


@pytest.mark.parametrize("side,participant", [
    ("homeTeam", {"ranking": 120}),
    ("awayTeam", {"ranking": 120}),
    ("homeTeam", {"playerTeamInfo": {"currentRanking": 1788}}),
])
def test_one_known_rank_at_or_above_cutoff_rejects_wrapped_event(monkeypatch, side, participant):
    enable_ranking_filter(monkeypatch)
    raw = event()
    raw[side].update(participant)
    assert filters.sofascore_event_filter_reason({"event": raw}) == "tennis_ranking_excluded"


@pytest.mark.parametrize("home,away,reason", [
    ({"ranking": 119}, {"ranking": 12}, None),
    ({}, {}, None),
    ({"ranking": 120, "playerTeamInfo": {"currentRanking": 12}}, {}, None),
    ({"ranking": 12, "playerTeamInfo": {"currentRanking": 120}}, {}, "tennis_ranking_excluded"),
    ({"ranking": 120, "playerTeamInfo": {"currentRanking": 0}}, {}, "tennis_ranking_excluded"),
])
def test_current_rank_precedence_fallback_and_missing_ranks(monkeypatch, home, away, reason):
    enable_ranking_filter(monkeypatch)
    assert filters.sofascore_event_filter_reason(event(home, away)) == reason


def test_cutoff_is_configurable_and_independent_of_future_filter(monkeypatch):
    enable_ranking_filter(monkeypatch, cutoff=500)
    assert filters.sofascore_event_filter_reason(event(home={"ranking": 500})) == "tennis_ranking_excluded"
    assert filters.sofascore_event_filter_reason(event(home={"ranking": 499})) is None


@pytest.mark.parametrize("sport,reason", [
    ("football", None),
    ("tennis_doubles", "tennis_ranking_excluded"),
])
def test_ranking_scope_is_tennis_only(monkeypatch, sport, reason):
    enable_ranking_filter(monkeypatch)
    assert filters.sofascore_event_filter_reason(event(home={"ranking": 120}, sport=sport)) == reason


def test_cutoff_must_be_positive():
    with pytest.raises(ValueError, match="tennis_ranking_cutoff must be positive"):
        settings.SofascoreDiscoverySettings(tennis_ranking_cutoff=0)


def test_dropping_and_secondary_feeds_reject_before_normalization(monkeypatch, caplog):
    enable_ranking_filter(monkeypatch)
    normalize = Mock()
    monkeypatch.setattr(discovery_feeds, "normalize_event_payload", normalize)
    raw = event(home={"playerTeamInfo": {"currentRanking": 1788}})
    with caplog.at_level("INFO", logger=discovery_feeds.__name__):
        events, odds = discovery_feeds.extract_events_and_odds_from_dropping_response(
            {"events": [raw], "oddsMap": {"1": {"market": "odds"}}}, tracked_competitions=None,
        )
    assert events == [] and odds == {}
    assert "tennis_ranking_excluded" in caplog.text
    assert discovery_feeds.extract_events_from_high_value_streaks(
        {"general": [{"event": raw}], "head2head": [{"event": raw}]}, tracked_competitions=None,
    ) == ([], [])
    normalize.assert_not_called()


def test_nearest_team_events_reject_before_normalization(monkeypatch):
    enable_ranking_filter(monkeypatch)
    raw = event(away={"ranking": 120})
    monkeypatch.setattr(fetching.api_client, "get_nearest_event_for_team", lambda _: {"event": raw})
    normalize = Mock()
    monkeypatch.setattr(fetching.api_client, "normalize_event_payload", normalize)
    assert fetching.fetch_nearest_team_events([123], max_workers=1, tracked_competitions=None) == []
    normalize.assert_not_called()
