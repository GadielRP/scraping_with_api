import pytest

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.repositories import (
    EventRepository,
    EventSourceMappingRepository,
)
import infrastructure.persistence.repositories.event_repository as event_repository_module
import infrastructure.persistence.repositories.event_source_mapping_repository as event_source_mapping_repository_module


@pytest.fixture()
def isolated_sqlite_db(tmp_path, monkeypatch):
    db_path = tmp_path / "event_repository_tests.db"
    database_url = f"sqlite:///{db_path.as_posix()}"
    test_db_manager = DatabaseManager(database_url)
    test_db_manager.create_tables()

    monkeypatch.setattr(event_repository_module, "db_manager", test_db_manager)
    monkeypatch.setattr(event_source_mapping_repository_module, "db_manager", test_db_manager)

    return test_db_manager


def test_upsert_event_defaults_to_sofascore_source_mapping(isolated_sqlite_db):
    event_data = {
        "id": 123456,
        "startTimestamp": 1710000000,
    }

    event = EventRepository.upsert_event(event_data)

    assert event is not None
    assert event.id is not None
    assert EventSourceMappingRepository.get_event_id_by_source("sofascore", "123456") == event.id

    mappings = EventSourceMappingRepository.get_mappings_for_event(event.id)
    assert len(mappings) == 1

    mapping = mappings[0]
    assert mapping.source == "sofascore"
    assert mapping.source_event_id == "123456"
    assert mapping.match_method == "direct"
    assert float(mapping.confidence) == pytest.approx(1.0)


def test_upsert_event_honors_explicit_source_and_keeps_sources_isolated(isolated_sqlite_db):
    event_data = {
        "id": 789012,
        "startTimestamp": 1710000000,
    }

    sofascore_event = EventRepository.upsert_event(event_data)
    oddspapi_event = EventRepository.upsert_event(
        event_data,
        source="oddspapi",
        match_method="external_provider_sofascore_id",
        confidence=1.0,
    )

    assert sofascore_event is not None
    assert oddspapi_event is not None
    assert oddspapi_event.id != sofascore_event.id

    assert EventSourceMappingRepository.get_event_id_by_source("sofascore", "789012") == sofascore_event.id
    assert EventSourceMappingRepository.get_event_id_by_source("oddspapi", "789012") == oddspapi_event.id

    sofascore_mappings = EventSourceMappingRepository.get_mappings_for_event(sofascore_event.id)
    oddspapi_mappings = EventSourceMappingRepository.get_mappings_for_event(oddspapi_event.id)

    assert len(sofascore_mappings) == 1
    assert len(oddspapi_mappings) == 1
    assert sofascore_mappings[0].source == "sofascore"
    assert oddspapi_mappings[0].source == "oddspapi"
    assert oddspapi_mappings[0].match_method == "external_provider_sofascore_id"
    assert float(oddspapi_mappings[0].confidence) == pytest.approx(1.0)
