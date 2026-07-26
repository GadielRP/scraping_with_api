from sqlalchemy import create_engine, event as sqlalchemy_event, text
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence.models import Base, EventSourceMapping
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine)()


def test_has_odds_defaults_true_for_new_source_mapping():
    _, session = _session()
    mapping = EventSourceMapping(
        event_id=101,
        source="sofascore",
        source_event_id="9001",
    )
    session.add(mapping)
    session.flush()

    assert mapping.has_odds is True


def test_has_odds_database_default_is_true_for_direct_insert():
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
    ).scalar_one() == 1


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
        ["sofascore", "oddspapi"],
        session=session,
    )
    sqlalchemy_event.remove(engine, "before_cursor_execute", _record_select)

    assert len(selects) == 1
    assert states[101]["sofascore"].source_event_id == "9001"
    assert states[101]["sofascore"].has_odds is False
    assert states[101]["oddspapi"].source_event_id == "fixture-1"
    assert states[101]["oddspapi"].has_odds is True


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
    assert states[101]["oddspapi"].has_odds is True
    assert (
        EventSourceMappingRepository.mark_odds_unavailable(
            [101],
            "sofascore",
            session=session,
        )
        == 0
    )
