from datetime import datetime
from types import SimpleNamespace

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Event, EventSourceMapping, Participant
from infrastructure.persistence.repositories import event_repository as event_repo_module
from infrastructure.persistence.repositories import event_source_mapping_repository as mapping_repo_module
from infrastructure.persistence.repositories.event_repository import EventRepository


def _normalized_event_payload(*, source_event_id=123456, season_id=80229, tournament_id=132):
    return {
        "event": {
            "id": source_event_id,
            "customId": "lal-bos",
            "slug": "lakers-celtics",
            "startTimestamp": int(datetime(2025, 10, 1, 0, 0).timestamp()),
            "sport": "Basketball",
            "competition": "USA, NBA, NBA",
            "country": "USA",
            "homeTeam": "Los Angeles Lakers",
            "awayTeam": "Boston Celtics",
            "gender": "M",
            "discovery_source": "scraping_on_command",
            "season_id": season_id,
            "season_name": "NBA 25/26",
            "season_year": 2025,
            "round": "regular_season",
        },
        "home_participant": {
            "source": "sofascore",
            "source_participant_id": 3427,
            "name": "Los Angeles Lakers",
            "slug": "los-angeles-lakers",
            "short_name": "Lakers",
            "code_name": "LAL",
        },
        "away_participant": {
            "source": "sofascore",
            "source_participant_id": 3428,
            "name": "Boston Celtics",
            "slug": "boston-celtics",
            "short_name": "Celtics",
            "code_name": "BOS",
        },
        "competition_ref": {
            "source": "sofascore",
            "source_tournament_id": tournament_id,
            "source_unique_tournament_id": tournament_id,
            "canonical_name": "NBA",
            "display_name": "NBA",
            "slug": "nba",
            "unique_slug": "nba",
            "category_id": 15,
            "category_name": "USA",
        },
    }


def test_source_mapping_fields_include_tournament_season_and_participants():
    fields = EventRepository._source_mapping_fields(
        event_id=10,
        source="sofascore",
        source_event_id="123456",
        match_method="direct",
        confidence=1.0,
        event_payload={"season_id": 80229},
        home_participant=SimpleNamespace(participant_id=21),
        away_participant=SimpleNamespace(participant_id=22),
        competition=SimpleNamespace(source_tournament_id=132),
    )

    assert fields == {
        "event_id": 10,
        "source": "sofascore",
        "source_event_id": "123456",
        "source_tournament_id": "132",
        "source_season_id": "80229",
        "source_participant_home_id": 21,
        "source_participant_away_id": 22,
        "match_method": "direct",
        "confidence": 1.0,
    }


def test_upsert_event_persists_sofascore_mapping_metadata(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'event_mapping.db'}")
    manager.create_tables()
    monkeypatch.setattr(event_repo_module, "db_manager", manager)
    monkeypatch.setattr(mapping_repo_module, "db_manager", manager)

    event = EventRepository.upsert_event(_normalized_event_payload())
    assert event is not None
    assert event.id != 123456
    assert event.home_participant_id is not None
    assert event.away_participant_id is not None
    assert event.competition_id is not None
    assert event.season_id == 80229

    with manager.get_session() as session:
        mapping = (
            session.query(EventSourceMapping)
            .filter(
                EventSourceMapping.source == "sofascore",
                EventSourceMapping.source_event_id == "123456",
            )
            .one()
        )
        home = session.query(Participant).filter(Participant.participant_id == event.home_participant_id).one()
        away = session.query(Participant).filter(Participant.participant_id == event.away_participant_id).one()
        persisted_event = session.query(Event).filter(Event.id == event.id).one()

        assert mapping.event_id == event.id
        assert mapping.source_tournament_id == "132"
        assert mapping.source_season_id == "80229"
        assert mapping.source_participant_home_id == event.home_participant_id
        assert mapping.source_participant_away_id == event.away_participant_id
        assert home.source == "sofascore"
        assert home.source_participant_id == 3427
        assert away.source_participant_id == 3428
        assert persisted_event.competition_id == event.competition_id
