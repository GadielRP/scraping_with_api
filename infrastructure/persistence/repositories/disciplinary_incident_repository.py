"""Scalar keyset reads, atomic multiset reconciliation and SQL team totals."""
from collections import Counter

from sqlalchemy import and_, delete, func, insert, or_, select

from infrastructure.persistence.event_identity_lock import lock_event_identities
from infrastructure.persistence.models import DisciplinaryIncident as Incident, Event, EventSourceMapping as Mapping, Result
from modules.discipline.contracts import (
    DisciplineEventContext, IncidentType, Origin, Outcome, Side, TeamSeasonTotals,
)
from shared.execution_context import check_execution_budget
from shared.temporal import utc_now


class DisciplinaryIncidentRepository:
    def __init__(self, database):
        self.database = database

    @staticmethod
    def _context(row):
        return DisciplineEventContext(*row)

    @staticmethod
    def _columns():
        return (Event.id, Mapping.mapping_id, Mapping.source_event_id, Mapping.source,
                Event.sport, Event.competition_id, Event.season_id,
                Mapping.source_participant_home_id, Mapping.source_participant_away_id)

    def iter_candidates(self, selection, page_size):
        if page_size < 1:
            raise ValueError("page_size must be positive")
        filters = [Event.sport.in_(("Football", "Basketball")),
                   select(Result.event_id).where(Result.event_id == Event.id,
                       Result.home_score.isnot(None), Result.away_score.isnot(None)).exists()]
        if selection.event_id is not None:
            filters.append(Event.id == selection.event_id)
        elif selection.event_ids:
            filters.append(Event.id.in_(selection.event_ids))
        elif selection.start is not None:
            filters.extend((Event.starts_at >= selection.start, Event.starts_at < selection.end))
        else:
            filters.extend((Event.competition_id == selection.competition_id, Event.season_id == selection.season_id))
        if selection.event_ids:
            high_water = max(selection.event_ids)
        else:
            with self.database.get_session() as session:
                high_water = session.scalar(select(func.max(Event.id)).where(*filters))
        after = selection.after_event_id
        while high_water is not None and after < high_water:
            check_execution_budget()
            with self.database.get_session() as session:
                # Indexed lookups per candidate avoid grouping the whole mapping history per page.
                mapping_filters = (Mapping.event_id == Event.id, Mapping.source == "sofascore")
                single_mapping = (select(func.min(Mapping.mapping_id)).where(*mapping_filters)
                                  .having(func.count() == 1).correlate(Event).scalar_subquery())
                mapping_count = (select(func.count()).where(*mapping_filters)
                                 .correlate(Event).scalar_subquery())
                rows = session.execute(select(*self._columns(), mapping_count)
                    .select_from(Event).outerjoin(Mapping, Mapping.mapping_id == single_mapping)
                    .where(*filters, Event.id > after, Event.id <= high_water)
                    .order_by(Event.id).limit(page_size)).all()
                batch = [self._context(tuple(row[:3]) + (row[3] or "sofascore",) + tuple(row[4:])) for row in rows]
            if not batch:
                break
            after = batch[-1].event_id
            yield batch
            if selection.event_ids and len(batch) < page_size:
                break

    def replace_snapshot(self, context, incidents, managed_types, *, dry_run=False):
        managed = frozenset(IncidentType(value) for value in managed_types)
        if not managed:
            raise ValueError("A snapshot must declare its managed types")
        values = []
        for incident in incidents:
            kind, origin, side = IncidentType(incident.type), Origin(incident.origin), Side(incident.side)
            player_id, name = incident.source_player_id, incident.player_name
            if kind not in managed:
                raise ValueError("Incident is outside the managed scope")
            if any(v is not None and (not isinstance(v, str) or not v.strip()) for v in (player_id, name)):
                raise ValueError("Player identifiers and names must be nonempty strings or None")
            values.append((kind.value, origin.value, player_id, name, side.value))
        check_execution_budget()
        with self.database.get_session() as session:
            lock_event_identities(session, [(context.source, context.source_event_id)])
            parent = session.scalar(select(Event.id).where(Event.id == context.event_id).with_for_update())
            if parent is None:
                return Outcome.MAPPING_CONFLICT
            # Lock every source mapping to detect ambiguity introduced since selection.
            mappings = session.execute(select(Mapping.mapping_id).where(
                Mapping.event_id == context.event_id, Mapping.source == context.source)
                .order_by(Mapping.mapping_id).with_for_update()).all()
            if mappings != [(context.mapping_id,)]:
                return Outcome.MAPPING_CONFLICT
            row = session.execute(select(*self._columns()).join(Mapping, Mapping.event_id == Event.id).where(
                Event.id == context.event_id, Mapping.mapping_id == context.mapping_id,
                select(Result.event_id).where(Result.event_id == Event.id,
                    Result.home_score.isnot(None), Result.away_score.isnot(None)).exists())).one_or_none()
            if row is None or self._context(row) != context:
                return Outcome.MAPPING_CONFLICT
            scope = (Incident.event_source_mapping_id == context.mapping_id, Incident.type.in_([t.value for t in managed]))
            existing = session.execute(select(Incident.type, Incident.origin, Incident.source_player_id,
                Incident.player_name, Incident.side).where(*scope)).all()
            if Counter(tuple(row) for row in existing) == Counter(values):
                return Outcome.UNCHANGED
            if dry_run:
                return Outcome.UPDATED
            check_execution_budget()
            session.execute(delete(Incident).where(*scope))
            if values:
                now = utc_now()
                session.execute(insert(Incident), [dict(event_source_mapping_id=context.mapping_id,
                    type=kind, origin=origin, source_player_id=player_id, player_name=name, side=side,
                    created_at=now, updated_at=now) for kind, origin, player_id, name, side in values])
        return Outcome.UPDATED

    def get_team_season_totals(self, source, participant_id, competition_id, season_id):
        if any(value is None for value in (source, participant_id, competition_id, season_id)):
            raise ValueError("Source, participant, competition and season are required")
        with self.database.get_session() as session:
            counts = session.execute(select(*[
                func.count().filter(Incident.type == kind.value) for kind in IncidentType
            ]).select_from(Incident).join(Mapping, Incident.event_source_mapping_id == Mapping.mapping_id)
                .join(Event, Event.id == Mapping.event_id).where(
                    Mapping.source == source, Event.competition_id == competition_id, Event.season_id == season_id,
                    or_(and_(Incident.side == Side.HOME.value, Mapping.source_participant_home_id == participant_id),
                        and_(Incident.side == Side.AWAY.value, Mapping.source_participant_away_id == participant_id)))).one()
        return TeamSeasonTotals(*counts)
