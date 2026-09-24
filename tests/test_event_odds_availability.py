from types import SimpleNamespace

from sqlalchemy import create_engine, event as sqlalchemy_event, text
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence.models import Base, EventSourceMapping
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.odds_ingestion.provider_odds_phase import run_provider_odds_phase


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine)()


def test_has_odds_defaults_to_unknown_for_new_source_mapping():
    _, session = _session()
    mapping = EventSourceMapping(
        event_id=101,
        source="sofascore",
        source_event_id="9001",
    )
    session.add(mapping)
    session.flush()

    assert mapping.has_odds is None


def test_has_odds_direct_insert_defaults_to_unknown():
    _, session = _session()
    session.execute(
        text(
            "INSERT INTO event_source_mappings "
            "(event_id, source, source_event_id, match_method) "
            "VALUES (101, 'sofascore', '9001', 'direct')"
        )
    )

    assert session.execute(
        text(
            "SELECT has_odds FROM event_source_mappings "
            "WHERE source = 'sofascore' AND source_event_id = '9001'"
        )
    ).scalar_one() is None


def test_bulk_load_returns_provider_specific_states_in_one_select():
    engine, session = _session()
    session.add_all(
        [
            EventSourceMapping(
                event_id=101,
                source="sofascore",
                source_event_id="9001",
                has_odds=False,
            ),
            EventSourceMapping(
                event_id=101,
                source="oddspapi",
                source_event_id="fixture-1",
                has_odds=True,
                source_sport_id="10",
            ),
            EventSourceMapping(
                event_id=101,
                source="oddsportal",
                source_event_id="fixture-2",
            ),
        ]
    )
    session.flush()
    selects = []

    def _record_select(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            selects.append(statement)

    sqlalchemy_event.listen(engine, "before_cursor_execute", _record_select)
    states = EventSourceMappingRepository.get_odds_source_states(
        [101],
        ["sofascore", "oddspapi", "oddsportal"],
        session=session,
    )
    sqlalchemy_event.remove(engine, "before_cursor_execute", _record_select)

    assert len(selects) == 1
    assert states[101]["sofascore"].source_event_id == "9001"
    assert states[101]["sofascore"].has_odds is False
    assert states[101]["oddspapi"].source_event_id == "fixture-1"
    assert states[101]["oddspapi"].has_odds is True
    assert states[101]["oddspapi"].source_sport_id == "10"
    assert states[101]["oddsportal"].has_odds is None


def test_mark_odds_unavailable_updates_only_requested_provider():
    _, session = _session()
    session.add_all(
        [
            EventSourceMapping(
                event_id=101,
                source="sofascore",
                source_event_id="9001",
            ),
            EventSourceMapping(
                event_id=101,
                source="oddspapi",
                source_event_id="fixture-1",
            ),
        ]
    )
    session.flush()

    updated = EventSourceMappingRepository.mark_odds_unavailable(
        [101],
        "sofascore",
        session=session,
    )
    session.expire_all()
    states = EventSourceMappingRepository.get_odds_source_states(
        [101],
        ["sofascore", "oddspapi"],
        session=session,
    )

    assert updated == 1
    assert states[101]["sofascore"].has_odds is False
    assert states[101]["oddspapi"].has_odds is None
    assert (
        EventSourceMappingRepository.mark_odds_unavailable(
            [101],
            "sofascore",
            session=session,
        )
        == 0
    )


def test_mark_odds_available_updates_unknown_mapping():
    _, session = _session()
    session.add(
        EventSourceMapping(
            event_id=101,
            source="sofascore",
            source_event_id="9001",
        )
    )
    session.flush()

    updated = EventSourceMappingRepository.mark_odds_available(
        [101],
        "sofascore",
        session=session,
    )
    session.expire_all()
    states = EventSourceMappingRepository.get_odds_source_states(
        [101],
        ["sofascore"],
        session=session,
    )

    assert updated == 1
    assert states[101]["sofascore"].has_odds is True


def test_provider_response_marks_available_even_when_normalization_saves_nothing(
    monkeypatch,
):
    updates = []
    monkeypatch.setattr(
        EventSourceMappingRepository,
        "mark_odds_available",
        lambda event_ids, source: updates.append((set(event_ids), source)),
    )

    summary = run_provider_odds_phase(
        [{"event_id": 101, "should_extract_odds": True}],
        {},
        source="sofascore",
        fetch=lambda _candidate: OddsFetchResult.from_payload(
            {"valid_provider_odds": True}
        ),
        ingest=lambda _candidate, _payload: SimpleNamespace(
            markets_saved=0,
            dual_process_market_available=False,
            reason="filtered by local market configuration",
        ),
    )

    assert summary.events_ingested == 0
    assert updates == [({101}, "sofascore")]
