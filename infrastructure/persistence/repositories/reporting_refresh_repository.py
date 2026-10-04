"""Invalidation is committed with source data; completion acknowledges a generation."""

from sqlalchemy import or_
from infrastructure.persistence.reporting_models import ReportingRefreshState as State
from infrastructure.persistence.database import db_manager
from shared.temporal import utc_now

REPORTING_VIEWS = ("mv_alert_events", "mv_p5_price_memory")


def invalidate_reporting(session):
    if session.get_bind().dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    statement = insert(State).values(
        [
            dict(view_name=name, requested_generation=1, completed_generation=0, attempts=0)
            for name in REPORTING_VIEWS
        ]
    )
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["view_name"],
            set_={"requested_generation": State.requested_generation + 1},
        )
    )


class ReportingRefreshRepository:
    @staticmethod
    def pending(*, force=False):
        with db_manager.get_session() as session:
            query = session.query(
                State.view_name, State.requested_generation, State.attempts
            ).filter(State.requested_generation > State.completed_generation)
            if not force:
                query = query.filter(
                    or_(State.next_attempt_at.is_(None), State.next_attempt_at <= utc_now())
                )
            return query.order_by(State.view_name).all()

    @staticmethod
    def due_generation(session, name, *, force=False):
        query = session.query(State.requested_generation).filter(
            State.view_name == name, State.requested_generation > State.completed_generation
        )
        if not force:
            query = query.filter(
                or_(State.next_attempt_at.is_(None), State.next_attempt_at <= utc_now())
            )
        return query.scalar()

    @staticmethod
    def complete(session, name, generation, next_attempt=None):
        session.query(State).filter(State.view_name == name).update(
            dict(
                completed_generation=generation,
                attempts=0,
                next_attempt_at=next_attempt,
                last_error=None,
            )
        )

    @staticmethod
    def failed(name, attempts, error, next_attempt):
        with db_manager.get_session() as session:
            session.query(State).filter(State.view_name == name).update(
                dict(
                    attempts=attempts + 1,
                    last_error=str(error)[:1024],
                    next_attempt_at=next_attempt,
                )
            )
