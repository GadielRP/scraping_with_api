"""Append sporting evidence independently from immutable evaluations."""

from uuid import uuid4
from sqlalchemy import insert
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
    session.execute(
        insert(EventOutcomeObservation).values(
            observation_id=str(uuid4()),
            event_id=event_id,
            source=observation["source"],
            source_event_id=observation.get("source_event_id"),
            source_observed_at=observation.get("source_observed_at"),
            received_at=observation["received_at"],
            payload=to_json_value(observation["payload"]),
        )
    )
