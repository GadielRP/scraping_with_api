"""Bounded event writes with shared preloads and a single commit per chunk.

Single-event callers use exactly the same path. This module owns no HTTP work.
"""
from dataclasses import dataclass, field
from itertools import islice
import logging
from time import monotonic
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, DataError, OperationalError
from infrastructure.persistence.models import Event, EventSourceMapping, Competition, Season
from infrastructure.persistence.event_identity_lock import lock_event_identities
from modules.events.discards.settings import DiscardSettings
from modules.events.round_policy import resolve_event_round
from shared.temporal import from_unix_timestamp, utc_now
from .event_discard_repository import EventDiscardRepository
from .participant_repository import ParticipantRepository
from .season_repository import SeasonRepository
from .event_source_mapping_repository import EventSourceMappingRepository

logger = logging.getLogger(__name__)


@dataclass
class EventWriteResult:
    events: dict[str, Event] = field(default_factory=dict)
    discarded: set[str] = field(default_factory=set)
    errors: dict[str, str] = field(default_factory=dict)
    inserted: int = 0
    updated: int = 0


def chunks(values, size):
    iterator = iter(values)
    while batch := list(islice(iterator, size)):
        yield batch


def _upsert_references(session, model, rows, keys):
    """Upsert unique reference identities once, preserving sparse fields."""
    if not rows:
        return {}
    if session.get_bind().dialect.name == 'postgresql':
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    rows = sorted(rows.values(), key=lambda row: tuple(row[k] for k in keys))
    statement = insert(model).values(rows)
    fields = set(rows[0]) - set(keys)
    updates = {}
    for key in fields:
        incoming = getattr(statement.excluded, key)
        if key in {'canonical_name', 'display_name'}:
            incoming = func.nullif(incoming, 'Unknown')
        updates[key] = func.coalesce(incoming, getattr(model, key))
    statement = statement.on_conflict_do_update(
        index_elements=keys,
        set_=updates,
    ).returning(model)
    saved = session.scalars(statement, execution_options={'populate_existing': True}).all()
    return {tuple(getattr(row, k) for k in keys): row for row in saved}


def _references(session, payloads):
    participants = []
    competitions = {}
    seasons = {}
    competition_fields = ('slug', 'unique_slug', 'category_id', 'category_name', 'number_of_teams',
                          'total_regular_season_games', 'standings_grouping', 'league_config_source')
    for data in payloads.values():
        p = data.get('event', data)
        for role in ('home_participant', 'away_participant'):
            if data.get(role):
                participants.append(data[role])
        c = data.get('competition_ref') or {}
        if c.get('source_tournament_id') is not None and c.get('source_unique_tournament_id') is not None:
            row = {k: c.get(k) for k in competition_fields}
            row.update(source=c.get('source') or 'sofascore',
                       source_tournament_id=int(c['source_tournament_id']),
                       source_unique_tournament_id=int(c['source_unique_tournament_id']),
                       canonical_name=(c.get('canonical_name') or c.get('display_name') or 'Unknown').strip(),
                       display_name=(c.get('display_name') or c.get('canonical_name') or 'Unknown').strip(),
                       updated_at=utc_now())
            key = (row['source'], row['source_tournament_id'])
            if key in competitions:
                competitions[key].update({k: v for k, v in row.items() if v is not None and v != 'Unknown'})
            else:
                competitions[key] = row
        if p.get('season_id') and p.get('season_name') and p.get('sport'):
            year = p.get('season_year')
            year = SeasonRepository._parse_year(year) if isinstance(year, str) else year
            year = year or SeasonRepository._parse_year(p['season_name'])
            if year:
                seasons[(int(p['season_id']),)] = dict(id=int(p['season_id']), name=p['season_name'],
                                                       year=year, sport=p['sport'])
    # Deterministic order reduces conflicts when unrelated event batches share teams.
    participants.sort(key=lambda p: (str(p.get('source', 'sofascore')), str(p.get('source_participant_id'))))
    teams = ParticipantRepository.upsert_participants(session, participants)
    leagues = _upsert_references(session, Competition, competitions, ['source', 'source_tournament_id'])
    _upsert_references(session, Season, seasons, ['id'])
    return teams, leagues


def source_mapping_fields(
    *,
    event_id: int,
    source: str,
    source_event_id: str,
    match_method: str,
    confidence: float,
    event_payload: dict,
    home_participant,
    away_participant,
    competition,
) -> dict:
    """Build EventSourceMapping fields from the normalized SofaScore payload.

    Event.id is canonical. Provider IDs belong on the mapping row, including
    tournament/season and the source-scoped participant FKs used by later
    cross-source matching.
    """
    source_season_id = event_payload.get("season_id")
    source_tournament_id = None
    if competition is not None and competition.source_tournament_id is not None:
        source_tournament_id = str(competition.source_tournament_id)

    return {
        "event_id": event_id,
        "source": source,
        "source_event_id": source_event_id,
        "source_tournament_id": source_tournament_id,
        "source_season_id": str(source_season_id) if source_season_id is not None else None,
        "source_participant_home_id": home_participant.participant_id if home_participant else None,
        "source_participant_away_id": away_participant.participant_id if away_participant else None,
        "match_method": match_method,
        "confidence": confidence,
    }


def _write_chunk(session, data_by_id, source, match_method, confidence, settings):
    lock_event_identities(session, [(source, sid) for sid in data_by_id])
    blocked = EventDiscardRepository.blocked_ids(session, source, list(data_by_id), settings)
    eligible = {sid: data for sid, data in data_by_id.items() if sid not in blocked}
    result = EventWriteResult(discarded=blocked)
    if not eligible:
        return result
    mappings = dict(session.query(EventSourceMapping.source_event_id, EventSourceMapping.event_id).filter(
        EventSourceMapping.source == source, EventSourceMapping.source_event_id.in_(eligible)).all())
    existing = {e.id: e for e in session.query(Event).filter(Event.id.in_(mappings.values())).order_by(Event.id).with_for_update().all()}
    if set(mappings.values()) - set(existing):
        raise ValueError('Source mapping refers to an absent canonical event')
    teams, leagues = _references(session, eligible)
    mapping_rows = []
    for sid, data in eligible.items():
        p = data.get('event', data)
        obj = existing.get(mappings.get(sid))
        new = obj is None
        if new:
            obj = Event(discovery_source=p.get('discovery_source', 'dropping_odds'))
            session.add(obj)
            result.inserted += 1
        else:
            result.updated += 1
        obj.custom_id = p.get('customId')
        obj.slug = p.get('slug') or obj.slug or sid
        obj.starts_at = from_unix_timestamp(p['startTimestamp'])
        for field_name, incoming in (('sport', 'sport'), ('competition', 'competition'),
                                     ('home_team', 'homeTeam'), ('away_team', 'awayTeam')):
            setattr(obj, field_name, p.get(incoming) or getattr(obj, field_name) or 'Unknown')
        obj.country = p.get('country')
        obj.gender = str(p.get('gender') or 'unknown')[:10]
        home = away = None
        for role in ('home', 'away'):
            ref = data.get(f'{role}_participant') or {}
            normalized = ParticipantRepository._normalize_participant_data(ref)
            participant = teams.get(normalized[0]) if normalized else None
            if participant:
                setattr(obj, f'{role}_participant_id', participant.participant_id)
            if role == 'home':
                home = participant
            else:
                away = participant
        c = data.get('competition_ref') or {}
        league = leagues.get((c.get('source') or 'sofascore', c.get('source_tournament_id')))
        if league:
            obj.competition_id = league.competition_id
        if p.get('discovery_source') == 'dropping_odds':
            obj.discovery_source = 'dropping_odds'
        if p.get('season_id'):
            obj.season_id = int(p['season_id'])
        obj.round = resolve_event_round(p.get('season_id'), p.get('competition'), p.get('round'), obj.round)
        obj.updated_at = utc_now()
        result.events[sid] = obj
        mapping_rows.append((sid, obj, p, home, away, league))
    session.flush()  # All new event IDs; not one flush per event.
    EventSourceMappingRepository.upsert_mappings(session, [source_mapping_fields(
        event_id=obj.id, source=source, source_event_id=sid, match_method=match_method,
        confidence=confidence, event_payload=p, home_participant=home, away_participant=away,
        competition=league) for sid, obj, p, home, away, league in mapping_rows])
    return result


def write_events(db_manager, events, *, source='sofascore', match_method='direct', confidence=1.0):
    settings = DiscardSettings.current()
    source = str(source or 'sofascore').strip().lower()
    result = EventWriteResult()

    def persist(batch, attempt=0):
        try:
            started = monotonic()
            with db_manager.get_session() as session:
                saved = _write_chunk(session, batch, source, match_method, confidence, settings)
            result.events.update(saved.events)
            result.discarded.update(saved.discarded)
            result.inserted += saved.inserted
            result.updated += saved.updated
            log = logger.info if len(batch) > 1 or saved.discarded else logger.debug
            log('Event batch committed: source=%s inserted=%s updated=%s discarded=%s '
                'duration_s=%.3f discarded_source_event_ids=%s',
                source, saved.inserted, saved.updated, len(saved.discarded),
                monotonic() - started, sorted(saved.discarded))
        except (IntegrityError, DataError) as exc:
            if len(batch) > 1:
                items = list(batch.items())
                midpoint = len(items) // 2
                persist(dict(items[:midpoint]))
                persist(dict(items[midpoint:]))
            else:
                result.errors.update({sid: type(exc).__name__ for sid in batch})
                logger.warning('Invalid event batch source=%s ids=%s error=%s', source, list(batch), type(exc).__name__)
        except OperationalError as exc:
            code = getattr(exc.orig, 'sqlstate', None) or getattr(exc.orig, 'pgcode', None)
            if code in {'40001', '40P01'} and attempt < 2:
                persist(batch, attempt + 1)
            else:
                raise  # Infrastructure failure is not an empty mapping or an intentional skip.

    for chunk in chunks(events, settings.batch_size):
        valid = {}
        for data in chunk:
            sid = ''
            try:
                p = data.get('event', data) if isinstance(data, dict) else {}
                if not isinstance(p, dict):
                    raise ValueError('Invalid event payload')
                sid = str(p.get('id') or '').strip()
                if not sid:
                    raise ValueError('Missing source event ID')
                from_unix_timestamp(p['startTimestamp'])
                p = dict(p)
                if p.get('season_id'):
                    p['season_id'] = int(p['season_id'])
                normalized = {**data, 'event': p}
                c = dict(data.get('competition_ref') or {})
                for key in ('source_tournament_id', 'source_unique_tournament_id'):
                    if c.get(key) is not None:
                        c[key] = int(c[key])
                c['source'] = str(c.get('source') or 'sofascore').strip().lower()
                normalized['competition_ref'] = c
                valid[sid] = normalized
            except (TypeError, ValueError, KeyError, OverflowError, OSError) as exc:
                result.errors[sid or f'invalid:{len(result.errors)}'] = str(exc)
        if valid:
            persist(valid)
    return result
