from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence import database as database_module
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Base,
    Bookie,
    Event,
    EventObservation,
    EventSourceMapping,
    Market,
    PredictionLog,
    Result,
)
from infrastructure.persistence.repositories import EventRepository, EventSourceMappingRepository


@pytest.fixture()
def sqlite_db(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'identity_migration.db'}")
    session_local = sessionmaker(autocommit=False, autoflush=False, expire_on_commit=False, bind=engine)

    monkeypatch.setattr(database_module.db_manager, "engine", engine)
    monkeypatch.setattr(database_module.db_manager, "SessionLocal", session_local)

    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


def _seed_event(session, event_id: int, home_team: str, away_team: str) -> Event:
    event = Event(
        id=event_id,
        slug=f"{home_team.lower()}-{away_team.lower()}",
        starts_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        sport="Football",
        competition="League",
        country="USA",
        home_team=home_team,
        away_team=away_team,
        gender="unknown",
        discovery_source="test",
    )
    session.add(event)
    return event


def test_event_source_mapping_repository_upsert_and_lookup(sqlite_db):
    with db_manager.get_session() as session:
        event = _seed_event(session, 1, "Home", "Away")
        session.flush()
        session.commit()

    mapping = EventSourceMappingRepository.upsert_mapping(
        event_id=1,
        source=" SofaScore ",
        source_event_id=12345,
        source_tournament_id="t-1",
        confidence=1.0,
    )

    assert mapping.event_id == 1
    assert mapping.source == "sofascore"
    assert mapping.source_event_id == "12345"

    mapping_repeat = EventSourceMappingRepository.upsert_mapping(
        event_id=1,
        source="sofascore",
        source_event_id="12345",
        source_tournament_id="t-2",
        source_season_id="s-3",
        match_method="direct",
        confidence=0.875,
        raw_external_providers={"provider": "sofascore"},
    )

    assert mapping_repeat.mapping_id == mapping.mapping_id
    assert mapping_repeat.source_tournament_id == "t-2"
    assert mapping_repeat.source_season_id == "s-3"
    assert float(mapping_repeat.confidence) == pytest.approx(0.875)

    assert EventSourceMappingRepository.get_event_id_by_source("sofascore", "12345") == 1
    assert EventSourceMappingRepository.get_source_event_id(1, "sofascore") == "12345"
    assert EventSourceMappingRepository.resolve_required_source_event_id(1, "sofascore") == "12345"
    assert len(EventSourceMappingRepository.get_mappings_for_event(1)) == 1

    with pytest.raises(ValueError, match="Missing source mapping for event_id=999, source=sofascore"):
        EventSourceMappingRepository.resolve_required_source_event_id(999, "sofascore")


def test_event_repository_upsert_event_creates_canonical_event_and_mapping(sqlite_db):
    payload = {
        "event": {
            "id": 12345,
            "startTimestamp": 1700000000,
            "slug": "home-away",
            "sport": "Football",
            "competition": "League",
            "country": "USA",
            "homeTeam": "Home",
            "awayTeam": "Away",
        }
    }

    first = EventRepository.upsert_event(payload)
    second = EventRepository.upsert_event(payload)

    assert first is not None
    assert second is not None
    assert first.id == second.id
    assert first.id != 12345

    with db_manager.get_session() as session:
        assert session.query(Event).count() == 1
        assert session.query(EventSourceMapping).count() == 1

    assert EventSourceMappingRepository.get_event_id_by_source("sofascore", "12345") == first.id
    assert EventSourceMappingRepository.get_source_event_id(first.id, "sofascore") == "12345"


def test_event_repository_upsert_event_no_season_id_and_discovery_source_update(sqlite_db):
    payload = {
        "event": {
            "id": 99999,
            "startTimestamp": 1700000000,
            "slug": "test-no-season",
            "sport": "Football",
            "competition": "League",
            "country": "USA",
            "homeTeam": "Home Team",
            "awayTeam": "Away Team",
            "discovery_source": "some_source",
        },
        "home_participant": {
            "source": "sofascore",
            "source_participant_id": 111,
            "name": "Home Team",
        },
        "away_participant": {
            "source": "sofascore",
            "source_participant_id": 222,
            "name": "Away Team",
        },
        "competition_ref": {
            "source": "sofascore",
            "source_tournament_id": 333,
            "source_unique_tournament_id": 444,
            "name": "League",
        }
    }

    # First insert: event should be created, and home/away participants and competition should be processed
    # even though season_id is not in the event payload.
    event_obj = EventRepository.upsert_event(payload)
    assert event_obj is not None
    assert event_obj.home_participant_id is not None
    assert event_obj.away_participant_id is not None
    assert event_obj.competition_id is not None
    assert event_obj.discovery_source == "some_source"

    # Second upsert (update branch):
    # Change discovery_source to dropping_odds to hit the logging line
    payload["event"]["discovery_source"] = "dropping_odds"
    updated_event = EventRepository.upsert_event(payload)
    assert updated_event is not None
    assert updated_event.id == event_obj.id
    assert updated_event.discovery_source == "dropping_odds"


def test_event_identity_migration_rewrites_fks_and_creates_mappings(sqlite_db):
    with db_manager.get_session() as session:
        bookie = Bookie(name="SofaScore", slug="sofascore")
        session.add(bookie)
        session.flush()

        _seed_event(session, 1001, "Alpha", "Beta")
        _seed_event(session, 1002, "Gamma", "Delta")
        session.add_all(
            [
                Result(event_id=1001, home_score=1, away_score=0, winner="1"),
                Market(
                    event_id=1001,
                    bookie_id=bookie.bookie_id,
                    market_name="Full time",
                    market_group="1X2",
                    market_period="Match",
                    choice_group=None,
                    is_live=False,
                ),
                EventObservation(
                    event_id=1001,
                    observation_type="ground_type",
                    observation_value="grass",
                    sport="Football",
                ),
                PredictionLog(
                    event_id=1001,
                    sport="Football",
                    participants="Alpha vs Beta",
                    competition="League",
                    prediction_type="process1",
                    status="pending",
                ),
            ]
        )

    db_manager._migrate_events_to_canonical_identity()

    with db_manager.get_session() as session:
        event_ids = [row[0] for row in session.query(Event.id).order_by(Event.id).all()]
        assert event_ids != [1001, 1002]
        assert len(event_ids) == 2

        migrated_event_id = EventSourceMappingRepository.get_event_id_by_source("sofascore", "1001")
        assert migrated_event_id in event_ids

        assert session.query(Result).filter(Result.event_id == migrated_event_id).count() == 1
        assert session.query(Market).filter(Market.event_id == migrated_event_id).count() == 1
        assert session.query(EventObservation).filter(EventObservation.event_id == migrated_event_id).count() == 1
        assert session.query(PredictionLog).filter(PredictionLog.event_id == migrated_event_id).count() == 1
        assert session.query(EventSourceMapping).filter(EventSourceMapping.event_id == migrated_event_id).count() >= 1

        orphan_results = (
            session.query(func.count())
            .select_from(Result)
            .outerjoin(Event, Result.event_id == Event.id)
            .filter(Event.id.is_(None))
            .scalar()
        )
        assert orphan_results == 0


def test_event_identity_migration_cleans_orphan_event_source_mappings(sqlite_db):
    with db_manager.get_session() as session:
        _seed_event(session, 2001, "Omega", "Sigma")
        session.add(
            EventSourceMapping(
                event_id=999999,
                source="legacy_source",
                source_event_id="orphan-row",
                match_method="direct",
            )
        )

    db_manager._migrate_events_to_canonical_identity()

    with db_manager.get_session() as session:
        orphan_count = (
            session.query(func.count())
            .select_from(EventSourceMapping)
            .outerjoin(Event, EventSourceMapping.event_id == Event.id)
            .filter(Event.id.is_(None))
            .scalar()
        )
        assert orphan_count == 0

        mapping_rows = session.execute(
            text("SELECT source, source_event_id FROM event_source_mappings ORDER BY source, source_event_id")
        ).fetchall()
        assert ("sofascore", "2001") in mapping_rows
        assert ("legacy_source", "orphan-row") not in mapping_rows


def test_public_cleanup_orphan_event_source_mappings(sqlite_db):
    with db_manager.get_session() as session:
        # Seed a valid event and its mapping
        _seed_event(session, 3001, "Team A", "Team B")
        session.add(
            EventSourceMapping(
                event_id=3001,
                source="some_source",
                source_event_id="valid-row",
                match_method="direct",
            )
        )
        # Seed an orphan mapping (event_id 9999 does not exist)
        session.add(
            EventSourceMapping(
                event_id=9999,
                source="legacy_source",
                source_event_id="orphan-row",
                match_method="direct",
            )
        )

    # Call the new public cleanup method
    removed = db_manager.cleanup_orphan_event_source_mappings()
    assert removed == 1

    with db_manager.get_session() as session:
        # Verify orphan is deleted and valid mapping is kept
        orphan_count = (
            session.query(func.count())
            .select_from(EventSourceMapping)
            .outerjoin(Event, EventSourceMapping.event_id == Event.id)
            .filter(Event.id.is_(None))
            .scalar()
        )
        assert orphan_count == 0

        mapping_rows = session.execute(
            text("SELECT source, source_event_id FROM event_source_mappings ORDER BY source, source_event_id")
        ).fetchall()
        assert ("some_source", "valid-row") in mapping_rows
        assert ("legacy_source", "orphan-row") not in mapping_rows
