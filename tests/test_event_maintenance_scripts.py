"""Bounded maintenance writes, selection semantics and deletion revalidation."""
from dataclasses import replace
from datetime import datetime, timezone

import pytest
from sqlalchemy import event as sql_event

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie, CanonicalMarketType, Competition, Event, EventSourceMapping, Market, Result,
)
from infrastructure.settings import discovery
from scripts.maintenance import prune_events as prune
from scripts.maintenance import reset_oddspapi_has_odds as reset

CUTOFF = datetime(2026, 9, 24, tzinfo=timezone.utc)
OLD = datetime(2026, 9, 1, tzinfo=timezone.utc)


@pytest.fixture
def database(tmp_path):
    manager = DatabaseManager(f'sqlite:///{tmp_path / "maintenance.db"}')

    @sql_event.listens_for(manager.engine, 'connect')
    def foreign_keys(connection, _):
        connection.execute('PRAGMA foreign_keys=ON')

    manager.create_tables()
    with manager.get_session() as session:
        session.add_all([
            Competition(competition_id=1, source='sofascore', source_tournament_id=11,
                source_unique_tournament_id=101, canonical_name='A', display_name='A'),
            Competition(competition_id=2, source='sofascore', source_tournament_id=22,
                source_unique_tournament_id=202, canonical_name='B', display_name='B'),
            Competition(competition_id=3, source='other', source_tournament_id=33,
                source_unique_tournament_id=101, canonical_name='C', display_name='C'),
            Bookie(bookie_id=1, name='Test', slug='test'),
            CanonicalMarketType(market_type_id=1, canonical_market_key='1x2_full_time',
                canonical_market_name='1X2', canonical_market_group='1X2',
                canonical_market_period='Full Time', market_family='side_3way'),
        ])
    yield manager
    manager.engine.dispose()


def add_event(manager, eid, *, competition=1, states=(('sofascore', False),),
              created_at=OLD, market=False):
    with manager.get_session() as session:
        session.add(Event(id=eid, slug=f'event-{eid}', starts_at=OLD, sport='Football',
            competition='Test', competition_id=competition, home_team='Home', away_team='Away'))
        session.flush()
        for index, (source, state) in enumerate(states):
            session.add(EventSourceMapping(event_id=eid, source=source,
                source_event_id=f'{eid}-{index}', has_odds=state, created_at=created_at))
        if market:
            session.add(Market(event_id=eid, bookie_id=1, market_type_id=1))


def test_reset_previews_then_commits_batches_and_is_idempotent(database):
    # Every SofaScore state, including no mapping, is within the requested scope.
    add_event(database, 1, states=(('oddspapi', True), ('sofascore', False)))
    add_event(database, 2, states=(('oddspapi', True), ('sofascore', True)))
    add_event(database, 3, states=(('oddspapi', True),))
    add_event(database, 4, states=(('oddspapi', True),), created_at=CUTOFF)
    add_event(database, 5, states=(('oddspapi', True),), market=True)
    add_event(database, 6, states=(('oddspapi', False),))
    add_event(database, 7, states=(('oddspapi', None),))
    add_event(database, 8, states=(('other', True),))
    preview = reset.run(database, created_before=CUTOFF, batch_size=2)
    assert preview['selected'] == 3 and preview['updated'] == 0
    assert preview['scanned'] == 10 and preview['batches'] == 5
    first = reset.run(database, created_before=CUTOFF, batch_size=2, limit=1, apply=True)
    assert first['updated'] == 1
    rest = reset.run(database, created_before=CUTOFF, batch_size=1,
        after_id=first['last_mapping_id'], apply=True)
    assert rest['updated'] == 2
    assert reset.run(database, created_before=CUTOFF, apply=True)['selected'] == 0
    with database.get_session() as session:
        values = dict(session.query(EventSourceMapping.event_id, EventSourceMapping.has_odds).filter(
            EventSourceMapping.source == 'oddspapi').all())
        assert values == {1: None, 2: None, 3: None, 4: True, 5: True, 6: False, 7: None}
        assert session.query(Event).count() == 8


def test_prune_filters_union_and_deletes_dependencies_in_batches(database):
    for eid in (1, 2, 3):
        add_event(database, eid, competition=1 if eid < 3 else 2)
    add_event(database, 4, competition=3)  # Same number, different provider namespace.
    add_event(database, 5, states=(('sofascore', False), ('oddspapi', None)))
    add_event(database, 6, market=True)
    add_event(database, 7, states=(('sofascore', False), ('sofascore', None)))
    with database.get_session() as session:
        session.add(Result(event_id=1, home_score=1, away_score=0))
    args = {'states': (('sofascore', 'false'), ('oddspapi', 'missing')), 'markets': 'absent',
        'competition_ids': {2}, 'tournament_ids': {101}, 'batch_size': 1}
    preview = prune.run(database, **args)
    assert preview['selected'] == 3 and preview['deleted'] == 0
    first = prune.run(database, **args, limit=2, apply=True)
    assert first['deleted'] == 2 and first['batches'] == 2
    assert prune.run(database, **args, after_id=first['last_event_id'], apply=True)['deleted'] == 1
    with database.get_session() as session:
        assert [eid for eid, in session.query(Event.id).order_by(Event.id)] == [4, 5, 6, 7]
        assert session.query(Result).count() == 0
        assert session.query(EventSourceMapping).filter(EventSourceMapping.event_id.in_([1, 2, 3])).count() == 0


def test_prune_rechecks_state_and_markets_after_page_selection(database, monkeypatch):
    add_event(database, 1)
    add_event(database, 2)
    original = prune.delete_events

    def changed(manager, ids, **kwargs):
        with manager.get_session() as session:
            session.query(EventSourceMapping).filter_by(event_id=1).update({'has_odds': True})
            session.add(Market(event_id=2, bookie_id=1, market_type_id=1))
        return original(manager, ids, **kwargs)

    monkeypatch.setattr(prune, 'delete_events', changed)
    assert prune.run(database, states=(('sofascore', 'false'),), markets='absent',
        competition_ids={1}, apply=True)['deleted'] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 2
        assert session.query(EventSourceMapping).count() == 2


def test_prune_source_selection_is_configurable(database):
    add_event(database, 1, states=(('sofascore', False), ('oddspapi', None)))
    add_event(database, 2, states=(('sofascore', False), ('oddspapi', True)))
    add_event(database, 3)
    summary = prune.run(database, states=(('oddspapi', 'unknown'),), all_competitions=True)
    assert summary['selected'] == 1
    with pytest.raises(ValueError, match='No competition selectors'):
        prune.run(database)


def test_cli_uses_current_discovery_lists_and_user_additions(database, monkeypatch):
    monkeypatch.setattr(prune, 'db_manager', database)
    for provider, ids in [('SOFASCORE', {101}), ('ODDSPAPI', {202})]:
        settings = getattr(discovery, provider)
        monkeypatch.setattr(discovery, provider, replace(settings, filters=replace(
            settings.filters, excluded_sofascore_unique_tournament_ids=frozenset(ids))))
    calls = []
    monkeypatch.setattr(prune, 'run', lambda manager, **kwargs: calls.append(kwargs))
    prune.main(['--discovery-exclusions', '--competition-ids', '9', '10',
        '--source-unique-tournament-id', '303'])
    assert calls[-1]['competition_ids'] == {9, 10}
    assert calls[-1]['tournament_ids'] == {101, 202, 303}
    assert calls[-1]['states'] == ()
    assert calls[-1]['markets'] == 'any'
    prune.main(['--source-state', 'oddspapi=unknown', '--no-discovery-exclusions',
        '--competition-id', '1'])
    assert calls[-1]['states'] == [('oddspapi', 'unknown')]
    assert calls[-1]['tournament_ids'] == set()
    assert calls[-1]['competition_ids'] == {1}
    prune.main(['--competition-id', '1'])
    assert calls[-1]['states'] == ()
    assert calls[-1]['tournament_ids'] == set()
    with pytest.raises(SystemExit):
        prune.main(['--source-state', 'sofascore=false', '--source-state', 'sofascore=true'])
    with pytest.raises(SystemExit):
        reset.main(['--batch-size', '0'])


def test_prune_has_no_default_population_and_markets_are_explicit(database, monkeypatch):
    monkeypatch.setattr(prune, 'db_manager', database)
    with pytest.raises(SystemExit):
        prune.main([])
    add_event(database, 1, states=(('sofascore', True),), market=True)
    add_event(database, 2, states=(('oddspapi', None),))
    assert prune.run(database, competition_ids={1})['selected'] == 2
    assert prune.run(database, competition_ids={1}, markets='present')['selected'] == 1
    assert prune.run(database, competition_ids={1}, markets='absent')['selected'] == 1


@pytest.mark.parametrize('script', ['reset', 'prune'])
def test_sparse_populations_are_filtered_inside_bounded_id_windows(database, capsys, script):
    import json

    for eid, available in [(1, False), (2, True), (3, False), (4, True), (5, True)]:
        add_event(database, eid, states=(('oddspapi', available),))
    statements = []

    def record(_conn, _cursor, sql, parameters, _context, _many):
        statements.append((sql, parameters))

    sql_event.listen(database.engine, 'before_cursor_execute', record)
    try:
        if script == 'reset':
            result = reset.run(database, created_before=CUTOFF, batch_size=2, limit=2)
        else:
            result = prune.run(database, states=(('oddspapi', 'true'),),
                all_competitions=True, batch_size=2, limit=2)
    finally:
        sql_event.remove(database.engine, 'before_cursor_execute', record)
    assert result['selected'] == 2
    assert result['scanned'] == 4
    assert result['last_mapping_id' if script == 'reset' else 'last_event_id'] == 4
    batches = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert all(row['scanned'] <= 2 and row['selected'] <= row['scanned']
        for row in batches if 'batch' in row)
    selects = [(sql.lower(), params) for sql, params in statements if sql.lstrip().startswith('SELECT')]
    assert selects
    for sql, params in selects:
        assert 'count(' not in sql and 'max(' not in sql
        assert 'limit' in sql  # Both the upper bound and each ID/filtered page.
        if ' in (' in sql:
            placeholders = sql.split(' in (', 1)[1].split(')', 1)[0].count('?')
            assert placeholders <= 2
    if script == 'reset':
        assert reset.run(database, created_before=CUTOFF, batch_size=2, after_id=4)['selected'] == 1
    else:
        assert prune.run(database, states=(('oddspapi', 'true'),),
            all_competitions=True, batch_size=2, after_id=4)['selected'] == 1
