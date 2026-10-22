"""Append and read sporting evidence independently from pillar evaluations."""

from datetime import datetime
from uuid import uuid4
from sqlalchemy import insert, select, tuple_
from infrastructure.persistence.models import EventOutcomeObservation
from modules.pillars.mining.serialization import to_json_value
from shared.temporal import utc_now


def append_outcome_observation(session, event_id, result):
    observation = result.get("_outcome_observation")
    if observation is None:
        observation = {
            "source": "internal",
            "received_at": utc_now(),
            "payload": result,
        }
    observation_id = str(uuid4())
    session.execute(
        insert(EventOutcomeObservation).values(
            observation_id=observation_id,
            event_id=event_id,
            source=observation["source"],
            source_event_id=observation.get("source_event_id"),
            source_observed_at=observation.get("source_observed_at"),
            received_at=observation["received_at"],
            payload=to_json_value(observation["payload"]),
        )
    )
    return observation_id


def _observation_payload(row):
    return to_json_value(
        {column.name: getattr(row, column.name)
         for column in EventOutcomeObservation.__table__.columns}
    )


def _event_observations(event_id):
    return select(EventOutcomeObservation).where(
        EventOutcomeObservation.event_id == event_id
    ).order_by(
        EventOutcomeObservation.received_at.desc(),
        EventOutcomeObservation.observation_id.desc(),
    )


def get_outcome_observation(session, event_id, *, observation_id=None):
    """Read a pinned observation, or the latest evidence for exploration.

    A requested identity must belong to this event. Never silently replace a
    missing or mismatched observation with newer evidence.
    """
    query = _event_observations(event_id)
    if observation_id is not None:
        query = query.where(EventOutcomeObservation.observation_id == observation_id)
    row = session.scalars(query.limit(1)).first()
    if row is None:
        if observation_id is not None:
            raise LookupError("outcome observation not found for event")
        return None
    return _observation_payload(row)


def get_outcomes_page(session, event_id, *, cursor=None, page_size=100):
    """Read a bounded page in stable reverse chronological order."""
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be between 1 and 1000")
    query = _event_observations(event_id)
    if cursor:
        received_at, observation_id = cursor
        query = query.where(
            tuple_(
                EventOutcomeObservation.received_at,
                EventOutcomeObservation.observation_id,
            ) < tuple_(datetime.fromisoformat(received_at), observation_id)
        )
    rows = session.scalars(query.limit(page_size + 1)).all()
    more, rows = len(rows) > page_size, rows[:page_size]
    return {
        "items": [_observation_payload(row) for row in rows],
        "next_cursor": (
            (rows[-1].received_at.isoformat(), rows[-1].observation_id)
            if more else None
        ),
    }
