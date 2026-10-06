"""Discovery policies at raw, normalized and canonical persistence boundaries."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Base, Competition, Event, EventSourceMapping
from infrastructure.persistence.repositories.discovery_repository import DiscoveryRepository, OddspapiCandidatePool
from infrastructure.settings import Config, discovery as settings
from infrastructure.settings.discovery import DiscoveryFilters
from modules.jobs.discovery.filters import (
    SourceCompetitionIds, filter_sofascore_events, oddspapi_fixture_filter_reason,
    sofascore_event_filter_reason, sofascore_tournament_filter_reason,
    oddspapi_discovery_sport_ids, sofascore_discovery_sport_slugs,
)
from modules.jobs.discovery import persistence
from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import OddspapiFixtureBatchProcessor
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity


NOW = datetime(2026, 10, 6, 16, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clock_and_sports(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["football", "baseball", "handball"])
    monkeypatch.setattr("modules.jobs.discovery.filters.utc_now", lambda: NOW)
    monkeypatch.setattr("infrastructure.persistence.repositories.discovery_repository.utc_now", lambda: NOW)
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor."
        "OddspapiFixtureResponseDebugWriter.save_if_incomplete", lambda _: None,
    )


def event(*, normalized=False, start=None, category="Romania Amateur", unique_id=27118):
    payload = dict(id=4001, sport="Football", startTimestamp=int((start or NOW+timedelta(hours=1)).timestamp()),
                   country="Romania Amateur", has_odds=False)
    ref = dict(source_tournament_id=123, source_unique_tournament_id=unique_id, category_name=category)
    if normalized:
        return {"event": payload, "competition_ref": ref}
    payload["tournament"] = dict(id=123, uniqueTournament=dict(id=unique_id), category=dict(name=category))
    return payload


@pytest.mark.parametrize("normalized", [False, True])
@pytest.mark.parametrize("toggle,reason", [
    ("future_only", "start_too_soon_or_started"),
    ("tracked_competitions_only", "untracked_competition"),
    ("exclude_sports", "excluded_sport"),
    ("exclude_categories", "excluded_category"),
    ("exclude_competitions", "excluded_competition"),
])
def test_each_toggle_rejects_and_can_be_disabled(normalized, toggle, reason):
    policy = DiscoveryFilters(future_only=False, exclude_categories=False, exclude_competitions=False,
                              excluded_sports=frozenset({"football"}))
    raw = event(normalized=normalized, start=NOW-timedelta(hours=1))
    enabled = replace(policy, **{toggle: True})
    scope = {SourceCompetitionIds(999, 888)} if enabled.tracked_competitions_only else None
    assert sofascore_event_filter_reason(raw, scope, policy=enabled) == reason
    assert sofascore_event_filter_reason(raw, policy=replace(enabled, **{toggle: False})) is None


def test_time_margin_and_unknown_availability_are_independent():
    policy = DiscoveryFilters(exclude_categories=False, exclude_competitions=False)
    soon = event(start=NOW+timedelta(minutes=5))
    assert filter_sofascore_events([soon], policy=policy) == []
    assert filter_sofascore_events([soon], policy=replace(policy, minimum_lead_minutes=0)) == [soon]
    assert filter_sofascore_events([event(start=NOW)], policy=replace(policy, minimum_lead_minutes=0)) == []
    for availability in (False, None, True):
        raw = event()
        raw["has_odds"] = availability
        assert filter_sofascore_events([raw], policy=policy) == [raw]


def test_category_uses_source_metadata_and_tournaments_are_rejected_before_event_fetch():
    raw = event(category="England", unique_id=17)
    assert sofascore_event_filter_reason(raw) is None  # Venue country is not the category.
    assert sofascore_tournament_filter_reason(event(), None, "football") == "excluded_category"


def test_sport_exclusion_removes_request_routes_and_remains_provider_specific(monkeypatch):
    policy = replace(settings.SOFASCORE.filters, exclude_sports=True, excluded_sports=frozenset({"football"}))
    monkeypatch.setattr(settings, "SOFASCORE", replace(settings.SOFASCORE, filters=policy))
    assert "football" not in sofascore_discovery_sport_slugs()
    assert oddspapi_discovery_sport_ids()["soccer"] == 10


def test_excluding_tennis_singles_keeps_the_shared_calendar_for_doubles(monkeypatch):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["tennis", "tennis_doubles"])
    policy = replace(settings.SOFASCORE.filters, exclude_sports=True, excluded_sports=frozenset({"tennis"}))
    monkeypatch.setattr(settings, "SOFASCORE", replace(settings.SOFASCORE, filters=policy))
    raw = event(category="ATP", unique_id=999)
    raw["sport"] = "Tennis"
    raw["homeTeam"], raw["awayTeam"] = {"name": "A/B"}, {"name": "C/D"}
    assert sofascore_discovery_sport_slugs() == ["tennis"]
    assert sofascore_tournament_filter_reason(raw, None, "tennis") is None
    assert sofascore_event_filter_reason(raw) is None
    raw["homeTeam"], raw["awayTeam"] = {"name": "A"}, {"name": "C"}
    assert sofascore_event_filter_reason(raw) == "excluded_sport"


def test_oddspapi_rejects_old_fixtures_and_disabled_time_filter_allows_backfill():
    raw = dict(fixtureId="fixture-1", sportName="Soccer", startTime="2026-01-01T12:00:00Z")
    assert oddspapi_fixture_filter_reason(raw) == "start_too_soon_or_started"
    assert oddspapi_fixture_filter_reason(raw, policy=replace(settings.ODDSPAPI.filters, future_only=False)) is None
    raw["startTime"] = "invalid"
    assert oddspapi_fixture_filter_reason(raw) == "invalid_start_timestamp"


@pytest.mark.parametrize("mode", ["events", "feed_odds", "fetch_odds"])
def test_all_shared_persistence_modes_stop_rejected_events_before_fetch_or_write(monkeypatch, mode):
    writer, fetcher = Mock(), Mock()
    monkeypatch.setattr(persistence.EventRepository, "batch_upsert_events", writer)
    monkeypatch.setattr(persistence, "fetch_event_odds", fetcher)
    raw = event(category="England", unique_id=17, start=NOW-timedelta(hours=1))
    if mode == "events":
        result = persistence.persist_events([raw], tracked_competitions=None)
    elif mode == "feed_odds":
        result = persistence.persist_events_with_odds([raw], {"4001": {"markets": [1]}}, tracked_competitions=None)
    else:
        result = persistence.fetch_and_persist_events_with_odds([raw], tracked_competitions=None)
    assert result == (0, 1)
    writer.assert_not_called()
    fetcher.assert_not_called()


@pytest.fixture
def canonical_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all([
            Competition(competition_id=1, source="sofascore", source_tournament_id=11,
                        source_unique_tournament_id=12, canonical_name="Amateur", display_name="Amateur", category_name="Romania Amateur"),
            Competition(competition_id=2, source="sofascore", source_tournament_id=21,
                        source_unique_tournament_id=20468, canonical_name="Pioneer League", display_name="Pioneer League"),
            Competition(competition_id=3, source="sofascore", source_tournament_id=31,
                        source_unique_tournament_id=32, canonical_name="Incomplete", display_name="Incomplete", category_name=None),
        ])
        for identifier, sport, competition, start in [
            (1, "Football", 1, NOW+timedelta(hours=1)),
            (2, "Baseball", 2, NOW+timedelta(hours=1)),
            (3, "Football", 3, NOW+timedelta(hours=1)),
            (4, "Football", None, NOW+timedelta(hours=1)),
            (5, "Football", 3, NOW-timedelta(days=1)),
        ]:
            session.add(Event(id=identifier, slug=f"event-{identifier}", starts_at=start, sport=sport,
                              competition="League", competition_id=competition, home_team="Home", away_team="Away"))
            session.add(EventSourceMapping(event_id=identifier, source="sofascore",
                                           source_event_id=str(4000+identifier), has_odds=False))
        session.commit()
        yield session
    engine.dispose()


def test_canonical_sql_exclusions_allow_missing_metadata_and_disable_independently(canonical_session):
    ids = {1, 2, 3, 4, 5}
    assert DiscoveryRepository.admitted_event_ids(canonical_session, ids) == {3, 4}
    assert DiscoveryRepository.admitted_event_ids(canonical_session, ids, competition_ids=(3,)) == {3}
    policy = replace(settings.ODDSPAPI.filters, future_only=False, exclude_categories=False, exclude_competitions=False)
    assert DiscoveryRepository.admitted_event_ids(canonical_session, ids, policy=policy) == ids


def test_candidate_pool_applies_canonical_policy_before_fuzzy_matching(canonical_session):
    fixture = OddspapiFixtureIdentity.from_payload(dict(
        fixtureId="candidate", sportName="Soccer", startTime=(NOW+timedelta(hours=1)).isoformat(),
    ))
    pool = OddspapiCandidatePool.load([fixture], canonical_session)
    assert {candidate.id for candidate in pool.get_candidates_for(fixture)} == {3, 4}


@pytest.mark.parametrize("sport,raw_sport,home,away", [
    ("Tennis doubles", "Tennis", "A/B", "C/D"),
    ("Hockey", "Hockey", "Home", "Away"),
])
def test_candidate_pool_reuses_catalog_for_doubles_and_historical_aliases(
    canonical_session, monkeypatch, sport, raw_sport, home, away,
):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["tennis", "tennis_doubles", "ice_hockey"])
    policy = replace(settings.ODDSPAPI.filters, exclude_sports=True, excluded_sports=frozenset({"tennis"}))
    monkeypatch.setattr(settings, "ODDSPAPI", replace(settings.ODDSPAPI, filters=policy))
    canonical_session.add(Event(id=6, slug="event-6", starts_at=NOW+timedelta(hours=1), sport=sport,
                                competition="League", home_team=home, away_team=away))
    canonical_session.commit()
    payload = dict(fixtureId="sport-alias", sportName=raw_sport, participant1Name=home, participant2Name=away,
                   startTime=(NOW+timedelta(hours=1)).isoformat())
    assert oddspapi_fixture_filter_reason(payload) is None
    fixture = OddspapiFixtureIdentity.from_payload(payload)
    pool = OddspapiCandidatePool.load([fixture], canonical_session)
    assert [candidate.id for candidate in pool.get_candidates_for(fixture)] == [6]


@pytest.mark.parametrize("source", ["sofascore", "oddspapi"])
def test_existing_links_to_excluded_or_started_canonical_events_do_not_create_mappings(canonical_session, source):
    if source == "oddspapi":
        for identifier in (1, 2, 5):
            canonical_session.add(EventSourceMapping(event_id=identifier, source=source, source_event_id=f"fixture-{identifier}"))
        canonical_session.commit()
    writer = Mock(return_value={})
    fixtures = [dict(fixtureId=f"fixture-{identifier}", sportName="Soccer",
                     startTime=(NOW+timedelta(hours=1)).isoformat(),
                     externalProviders={"sofascoreId": str(4000+identifier)}) for identifier in (1, 2, 5)]
    result = OddspapiFixtureBatchProcessor(persistence_writer=writer).process_batch(
        fixtures, create_mappings=True, persist_queue=True, session=canonical_session,
    )
    assert result.fixtures_skipped_policy == 3
    assert result.rejected_by_reason == {"ineligible_canonical_event": 3}
    assert result.mappings_created == result.queue_rows_written == 0
    writer.assert_not_called()


def test_negative_sofascore_availability_can_still_resolve_oddspapi_without_writing_in_dry_run(canonical_session):
    writer = Mock()
    fixture = dict(fixtureId="accepted", sportName="Soccer", hasOdds=True,
                   startTime=(NOW+timedelta(hours=1)).isoformat(), externalProviders={"sofascoreId": "4003"})
    before = canonical_session.query(EventSourceMapping).count()
    result = OddspapiFixtureBatchProcessor(persistence_writer=writer).process_batch(
        [fixture], create_mappings=False, persist_queue=True, session=canonical_session,
    )
    assert result.resolved_external_sofascore == 1
    assert result.fixtures_skipped_policy == 0
    assert canonical_session.query(EventSourceMapping).count() == before
    writer.assert_not_called()
