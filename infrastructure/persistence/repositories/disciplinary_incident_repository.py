"""Scalar keyset reads, atomic multiset reconciliation and SQL team totals."""
from collections import Counter

from sqlalchemy import and_, delete, func, insert, literal, or_, select, tuple_, union_all

from infrastructure.persistence.event_identity_lock import lock_event_identities
from infrastructure.persistence.models import DisciplinaryIncident as Incident, DisciplinaryIncidentTotal as Total, Event, EventSourceMapping as Mapping, Result
from modules.discipline.contracts import (
    DisciplineEndpoint, DisciplineEventContext, DisciplineTotalDraft, IncidentType, Origin, Outcome,
    SECOND_YELLOW_TOTAL_TYPES, Side,
    TeamSeasonTotals,
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
        filters = [select(Result.event_id).where(Result.event_id == Event.id,
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

    def replace_acquisition(self, context, acquisition, *, dry_run=False):
        if acquisition.outcome != Outcome.READY:
            raise ValueError("Only a ready acquisition can be persisted")
        managed = frozenset(IncidentType(value) for value in acquisition.managed_types)
        total_values = {(item.type.value, item.side.value): (item.count, item.endpoint.value)
                        for item in acquisition.totals}
        if len(total_values) != len(acquisition.totals):
            raise ValueError("Duplicate aggregate scope")
        if not managed and not total_values:
            raise ValueError("An acquisition must declare a writable scope")
        values = []
        for incident in acquisition.incidents:
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
                Incident.player_name, Incident.side).where(*scope)).all() if managed else []
            detail_changed = Counter(tuple(row) for row in existing) != Counter(values)
            current_totals = {}
            if total_values:
                rows = session.execute(select(Total.type, Total.side, Total.count, Total.endpoint).where(
                    Total.event_source_mapping_id == context.mapping_id,
                    tuple_(Total.type, Total.side).in_(tuple(total_values)))).all()
                current_totals = {(kind, side): (count, endpoint) for kind, side, count, endpoint in rows}
            changes = {key: value for key, value in total_values.items() if current_totals.get(key) != value}
            if not detail_changed and not changes:
                return Outcome.UNCHANGED
            if dry_run:
                return Outcome.UPDATED
            check_execution_budget()
            now = utc_now()
            if detail_changed:
                session.execute(delete(Incident).where(*scope))
                if values:
                    session.execute(insert(Incident), [dict(event_source_mapping_id=context.mapping_id,
                        type=kind, origin=origin, source_player_id=player_id, player_name=name, side=side,
                        created_at=now, updated_at=now) for kind, origin, player_id, name, side in values])
            if changes:
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                statement = (pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert)(Total).values([
                    dict(event_source_mapping_id=context.mapping_id, type=kind, side=side, count=count,
                         endpoint=endpoint, created_at=now, updated_at=now)
                    for (kind, side), (count, endpoint) in changes.items()])
                session.execute(statement.on_conflict_do_update(
                    index_elements=["event_source_mapping_id", "type", "side"],
                    set_={"count": statement.excluded.count, "endpoint": statement.excluded.endpoint,
                          "updated_at": statement.excluded.updated_at}))
        return Outcome.UPDATED

    @staticmethod
    def _effective_totals(mapping_ids):
        detail = (
            select(Incident.event_source_mapping_id, Incident.type, Incident.side, func.count().label("count"))
            .where(Incident.event_source_mapping_id.in_(mapping_ids))
            .group_by(Incident.event_source_mapping_id, Incident.type, Incident.side)
            .cte()
        )
        projected = union_all(select(detail), *(
            select(detail.c.event_source_mapping_id, literal(kind.value).label("type"), detail.c.side, detail.c.count)
            .where(detail.c.type == IncidentType.RED_CARD_SECOND_YELLOW.value)
            for kind in SECOND_YELLOW_TOTAL_TYPES
        )).subquery()
        # Compare the projected scope so an acquired total (including zero) overrides all detail for it.
        legacy = select(projected).where(~select(Total.type).where(
            Total.event_source_mapping_id == projected.c.event_source_mapping_id,
            Total.type == projected.c.type, Total.side == projected.c.side).exists())
        acquired = select(Total.event_source_mapping_id, Total.type, Total.side, Total.count).where(
            Total.event_source_mapping_id.in_(mapping_ids))
        return union_all(acquired, legacy).subquery()

    def get_event_totals(self, source, event_id):
        """Acquired scopes with provenance; missing rows do not prove zero coverage."""
        with self.database.get_session() as session:
            rows = session.execute(select(Total.type, Total.side, Total.count, Total.endpoint).join(
                Mapping, Mapping.mapping_id == Total.event_source_mapping_id).where(
                    Mapping.source == source, Mapping.event_id == event_id).order_by(
                        Total.event_source_mapping_id, Total.type, Total.side)).all()
        return tuple(DisciplineTotalDraft(*row) for row in rows)

    def get_event_card_totals(self, source, event_ids, *, include_second_yellow=False):
        """Read acquired card totals in one batch; absent scopes remain unknown.

        Only an unambiguous provider mapping can supply a canonical event's
        totals. Do not combine duplicate mappings or fall back to legacy detail.
        """
        if not event_ids:
            return {}
        mappings = select(Mapping.event_id, func.min(Mapping.mapping_id).label("mapping_id")).where(
            Mapping.source == source, Mapping.event_id.in_(event_ids),
        ).group_by(Mapping.event_id).having(func.count() == 1).subquery()
        yellow, red = (kind.value for kind in SECOND_YELLOW_TOTAL_TYPES)
        second_yellow = IncidentType.RED_CARD_SECOND_YELLOW.value
        card_types = (yellow, red, second_yellow) if include_second_yellow else (yellow, red)
        query = (select(mappings.c.event_id, Total.side, Total.type, Total.count)
                 .join(Total, Total.event_source_mapping_id == mappings.c.mapping_id)
                 .where(Total.type.in_(card_types)))
        if include_second_yellow:
            query = query.add_columns(Total.endpoint).where(
                or_(Total.type != second_yellow, Total.endpoint == DisciplineEndpoint.COMMENTS.value)
            )
        with self.database.get_session() as session:
            rows = session.execute(query).all()
        totals = {(event_id, side, kind): count for event_id, side, kind, count, *_ in rows}
        if include_second_yellow:
            for event_id, side, kind, count, endpoint in rows:
                # Legacy comments may predate yellowRedCard support. Only an
                # explicit zero from match statistics can fill a missing split.
                if (kind == red and count == 0 and endpoint == DisciplineEndpoint.STATISTICS.value
                        and (event_id, side, yellow) in totals):
                    totals.setdefault((event_id, side, second_yellow), 0)
        return totals

    def get_team_season_totals(self, source, participant_id, competition_id, season_id):
        if any(value is None for value in (source, participant_id, competition_id, season_id)):
            raise ValueError("Source, participant, competition and season are required")
        with self.database.get_session() as session:
            # Restrict mappings before grouping legacy detail, rather than
            # aggregating the full history for a single team's season query.
            mappings = select(Mapping.mapping_id).join(Event, Event.id == Mapping.event_id).where(
                Mapping.source == source, Event.competition_id == competition_id, Event.season_id == season_id,
                or_(Mapping.source_participant_home_id == participant_id,
                    Mapping.source_participant_away_id == participant_id)).cte("discipline_team_mappings")
            effective = self._effective_totals(select(mappings.c.mapping_id))
            counts = session.execute(select(*[
                func.coalesce(func.sum(effective.c.count).filter(effective.c.type == kind.value), 0)
                for kind in (IncidentType.YELLOW_CARD, IncidentType.RED_CARD, IncidentType.EJECTION, IncidentType.FOUL_OUT)
            ]).select_from(effective).join(Mapping, effective.c.event_source_mapping_id == Mapping.mapping_id)
                .where(
                    or_(and_(effective.c.side == Side.HOME.value, Mapping.source_participant_home_id == participant_id),
                        and_(effective.c.side == Side.AWAY.value, Mapping.source_participant_away_id == participant_id)))).one()
        return TeamSeasonTotals(*counts)
