import logging
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta
from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload

from infrastructure.persistence.models import Competition, Event, Result
from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config
from shared.temporal import (
    as_utc,
    local_day_bounds_utc,
    utc_now,
)
from .event_source_mapping_repository import EventSourceMappingRepository

logger = logging.getLogger(__name__)


class EventRepository:
    """Repository for event-related database operations"""

    @staticmethod
    def _build_event_data_with_legacy_fallback(event_obj: Event) -> Dict:
        # LEGACY(EXEC-001): retain historical text fallback until entity backfill is verified.
        """Build event payloads for runtime use, falling back to legacy display fields when needed.

        This is a temporary compatibility bridge while historical rows are backfilled.
        Legacy-derived payloads are marked explicitly so they can be removed later.
        """
        home_participant = event_obj.__dict__.get("home_participant")
        away_participant = event_obj.__dict__.get("away_participant")
        competition_ref = event_obj.__dict__.get("competition_ref")

        home_team = home_participant.name if home_participant else (event_obj.home_team or None)
        away_team = away_participant.name if away_participant else (event_obj.away_team or None)
        competition_name = (
            competition_ref.display_name if competition_ref else (event_obj.competition or None)
        )

        if not home_team or not away_team or not competition_name:
            raise ValueError(
                f"Missing normalized participants/competition for event_id={event_obj.id}"
            )

        legacy_compat_used = not (home_participant and away_participant and competition_ref)

        return {
            "id": event_obj.id,
            "home_team": home_team,
            "away_team": away_team,
            "competition": competition_name,
            "starts_at": event_obj.starts_at,
            "sport": event_obj.sport,
            "country": event_obj.country,
            "slug": event_obj.slug,
            "custom_id": event_obj.custom_id,
            "season_id": event_obj.season_id,
            "home_participant_id": event_obj.home_participant_id,
            "away_participant_id": event_obj.away_participant_id,
            "competition_id": event_obj.competition_id,
            "competition_slug": competition_ref.slug if competition_ref else None,
            "home_source_participant_id": (
                home_participant.source_participant_id if home_participant else None
            ),
            "away_source_participant_id": (
                away_participant.source_participant_id if away_participant else None
            ),
            "competition_source_tournament_id": (
                competition_ref.source_tournament_id if competition_ref else None
            ),
            "competition_source_unique_tournament_id": (
                competition_ref.source_unique_tournament_id if competition_ref else None
            ),
            "context_status": "legacy_compat" if legacy_compat_used else "normalized",
            "legacy_compat_used": legacy_compat_used,
        }

    @staticmethod
    def discarded_source_ids(source, source_event_ids):
        """Bulk optimization only; writes recheck under the identity lock."""
        from .event_discard_repository import EventDiscardRepository
        from shared.batching import chunks
        from modules.events.discards.settings import DiscardSettings

        settings = DiscardSettings.current()
        if not settings.enabled or not settings.kinds:
            return set()
        source = str(source or "sofascore").strip().lower()
        blocked = set()
        for ids in chunks((str(value) for value in source_event_ids), settings.batch_size):
            with db_manager.get_session() as session:
                blocked.update(EventDiscardRepository.blocked_ids(session, source, ids, settings))
        return blocked

    @staticmethod
    def batch_upsert_events(
        events,
        source="sofascore",
        match_method="direct",
        confidence=1.0,
        *,
        expected_event_ids=None,
    ):
        """Bounded transactions with batch preloads and explicit skipped/error outcomes."""
        from .event_batch_writer import write_events

        return write_events(
            db_manager,
            events,
            source=source,
            match_method=match_method,
            confidence=confidence,
            expected_event_ids=expected_event_ids,
        )

    @staticmethod
    def upsert_event(event_data, source="sofascore", match_method="direct", confidence=1.0):
        # LEGACY(EXEC-004): migrate remaining single-response consumers before retirement.
        """Write one provider response using the shared identity/transaction rules."""
        result = EventRepository.batch_upsert_events([event_data], source, match_method, confidence)
        return next(iter(result.events.values()), None)

    @staticmethod
    def get_event_by_id(event_id: int) -> Optional[Event]:
        """Get event by ID with display relationships loaded."""
        with db_manager.get_session() as session:
            return (
                session.query(Event)
                .options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                    joinedload(Event.season),
                )
                .filter(Event.id == event_id)
                .first()
            )

    @staticmethod
    def get_events_started_between_minutes_ago(
        sport: str,
        competition: Optional[str] = None,
        min_minutes_ago: int = 105,
        max_minutes_ago: int = 140,
        alert_sent: Optional[bool] = None,
    ) -> List[Dict]:
        """
        Get events that started within a specific minute range for a given sport/competition.

        This is a modular function that can be used for any sport and competition.
        Similar to get_events_started_recently but with sport/competition filtering.

        Args:
            sport: Sport name (e.g., 'Basketball', 'Hockey', 'Football')
            competition: Optional competition filter (e.g., 'NBA', 'NHL'). If None, returns all events for sport.
            min_minutes_ago: Minimum minutes since event started (e.g., 80)
            max_minutes_ago: Maximum minutes since event started (e.g., 100)
            alert_sent: Optional filter for alert_sent flag. If True, only returns events with alert_sent=True.
                       If False, only returns events with alert_sent=False. If None, returns all events.

        Returns:
            List of event dictionaries matching the criteria

        Example:
            # Get NBA games that started 105-140 minutes ago and haven't sent alert yet
            events = get_events_started_between_minutes_ago('Basketball', 'NBA', 105, 140, alert_sent=False)
        """
        with db_manager.get_session() as session:
            now = utc_now()

            # Calculate time window
            window_start = now - timedelta(minutes=max_minutes_ago)
            window_end = now - timedelta(minutes=min_minutes_ago)

            logger.info(
                f"Searching for {sport} events (competition: {competition or 'all'}) "
                f"that started between {max_minutes_ago} and {min_minutes_ago} minutes ago"
            )
            logger.debug(f"Time window: {window_start} to {window_end}")

            # Build query with sport filter
            filters = [
                Event.sport == sport,
                Event.starts_at >= window_start,
                Event.starts_at <= window_end,
            ]

            # Add alert_sent filter if specified
            if alert_sent is not None:
                filters.append(Event.alert_sent == alert_sent)

            query = (
                session.query(Event)
                .options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                )
                .filter(and_(*filters))
            )

            # Add competition filter if specified
            if competition:
                query = query.join(Event.competition_ref).filter(
                    or_(
                        Competition.display_name.ilike(f"%{competition}%"),
                        Competition.canonical_name.ilike(f"%{competition}%"),
                        Competition.slug.ilike(f"%{competition}%"),
                        Competition.unique_slug.ilike(f"%{competition}%"),
                    )
                )

            events = query.all()

            # Convert to list of dictionaries
            result = []
            for event in events:
                try:
                    result.append(EventRepository._build_event_data_with_legacy_fallback(event))
                except ValueError as exc:
                    logger.warning("Skipping event %s in minutes-range query: %s", event.id, exc)

            if result:
                logger.info(
                    f"Found {len(result)} {sport} events (competition: {competition or 'all'}) "
                    f"in {max_minutes_ago}-{min_minutes_ago} minute window"
                )
            else:
                logger.debug(
                    f"No {sport} events (competition: {competition or 'all'}) found in time window"
                )

            return result

    @staticmethod
    def batch_update_starting_times(corrections: List[Tuple[int, datetime]]) -> int:
        """Batch update starting times of events in a single transaction/session."""
        if not corrections:
            return 0
        event_id_to_time = {
            event_id: as_utc(
                new_time,
                field_name=f"start time correction for event {event_id}",
            )
            for event_id, new_time in corrections
        }
        with db_manager.get_session() as session:
            events = session.query(Event).filter(Event.id.in_(event_id_to_time)).all()
            updated_at = utc_now()
            for event in events:
                event.starts_at = event_id_to_time[event.id]
                event.updated_at = updated_at
        logger.info("Batch updated starting times for %s event(s)", len(events))
        return len(events)

    @staticmethod
    def batch_delete_events(event_ids) -> int:
        """Preserve DeletionBatch evidence; commit memory and deletion together."""
        from .event_deletion_writer import delete_events

        return delete_events(db_manager, event_ids)

    @staticmethod
    def get_events_starting_between(
        window_start: datetime,
        window_end: datetime,
        competition_ids: Optional[List[int]] = None,
    ) -> List[Dict]:
        """Load canonical events in an indexed half-open start-time window."""
        window_start = as_utc(window_start, field_name="window_start")
        window_end = as_utc(window_end, field_name="window_end")
        if window_end <= window_start:
            raise ValueError("window_end must be later than window_start")
        with db_manager.get_session() as session:
            query = (
                session.query(Event)
                .options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                )
                .filter(
                    and_(
                        Event.starts_at >= window_start,
                        Event.starts_at < window_end,
                    )
                )
            )
            if competition_ids:
                query = query.filter(Event.competition_id.in_(competition_ids))

            result = []
            for event_obj in query.all():
                try:
                    result.append(EventRepository._build_event_data_with_legacy_fallback(event_obj))
                except ValueError as exc:
                    logger.warning(
                        "Skipping event %s in start-time query: %s",
                        event_obj.id,
                        exc,
                    )
            return result

    @staticmethod
    def get_events_starting_soon(
        window_minutes: int = 30,
        competition_ids: Optional[List[int]] = None,
    ) -> List[Dict]:
        """Get events starting soon.

        Modern pre_start_check_job flow should use this method as it returns
        the event payloads without querying latest odds.

        When ``competition_ids`` is provided, filtering is pushed into the
        database so unrelated events are never materialized in Python.
        """
        now = utc_now()
        return EventRepository.get_events_starting_between(
            now.replace(second=0, microsecond=0) - timedelta(minutes=5),
            now + timedelta(minutes=window_minutes, microseconds=1),
            competition_ids=competition_ids,
        )

    @staticmethod
    def get_events_started_recently(
        window_minutes: int = 15,
        competition_ids: Optional[List[int]] = None,
    ) -> List[Dict]:
        """Get recently started events without results in selected competitions."""
        with db_manager.get_session() as session:
            now = utc_now()
            window_start = now - timedelta(minutes=window_minutes, seconds=10)
            window_start = window_start.replace(microsecond=0)

            query = (
                session.query(Event)
                .outerjoin(Result, Result.event_id == Event.id)
                .options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                )
                .filter(
                    and_(
                        Event.starts_at >= window_start,
                        Event.starts_at < now,
                        Result.event_id.is_(None),
                    )
                )
            )

            if competition_ids:
                query = query.filter(Event.competition_id.in_(competition_ids))

            events_started_recently = query.all()
            result = []
            for event_obj in events_started_recently:
                try:
                    result.append(EventRepository._build_event_data_with_legacy_fallback(event_obj))
                except ValueError as exc:
                    logger.warning(
                        "Skipping event %s in recently-started query: %s", event_obj.id, exc
                    )
            return result

    @staticmethod
    def get_events_by_date(target_date) -> List[Event]:
        """Get all events for a specific date"""
        with db_manager.get_session() as session:
            if hasattr(target_date, "date"):
                target_date = target_date.date()
            day_start, day_end = local_day_bounds_utc(
                target_date,
                Config.TIMEZONE,
            )
            return (
                session.query(Event)
                .options(
                    joinedload(Event.home_participant),
                    joinedload(Event.away_participant),
                    joinedload(Event.competition_ref),
                )
                .filter(and_(Event.starts_at >= day_start, Event.starts_at < day_end))
                .all()
            )

    @staticmethod
    def mark_event_as_alerted(event_id: int) -> bool:
        """Mark the alert flag; return False if the event is absent or the write fails."""
        try:
            with db_manager.get_session() as session:
                event = session.query(Event).filter(Event.id == event_id).first()
                if event is None:
                    logger.warning("Event %s not found when marking as alerted", event_id)
                    return False
                event.alert_sent = True
            logger.info("Marked event %s as alert_sent=True", event_id)
            return True
        except Exception:
            logger.exception("Error marking event %s as alerted", event_id)
            return False
