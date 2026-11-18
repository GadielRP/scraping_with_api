"""When a stored result counts as known at a cutoff.

An event that kicked off before a cutoff may still be in play at it, yet its final score is already
stored once the database is read afterwards. A result is available only from the instant it was final:
the recorded ``finished_at`` or, when that is unknown, a conservative per-sport estimate. The estimate
errs late, so an unfinished event is never counted; a finished one may be left out for a few minutes.
"""

from datetime import datetime, timedelta

from sqlalchemy import and_, or_

# Longest ordinary running time of one event, by sport, measured on provider data. Used only when
# ``finished_at`` is missing, which includes every result stored before that column existed.
SETTLEMENT_WINDOWS = {
    "Football": timedelta(hours=2.5),
    "Futsal": timedelta(hours=2.5),
    "Handball": timedelta(hours=2.5),
    "Basketball": timedelta(hours=3.5),
    "Volleyball": timedelta(hours=3.5),
    "Hockey": timedelta(hours=4),
    "Ice hockey": timedelta(hours=4),
    "Tennis Doubles": timedelta(hours=4),
    "American football": timedelta(hours=4.5),
    "Baseball": timedelta(hours=5),
    "Tennis": timedelta(hours=5),
}
DEFAULT_SETTLEMENT_WINDOW = timedelta(hours=4)


def settlement_window(sport: str | None) -> timedelta:
    return SETTLEMENT_WINDOWS.get(sport, DEFAULT_SETTLEMENT_WINDOW)


def settled_at(starts_at: datetime, sport: str | None, last_changed_at: datetime | None = None) -> datetime:
    """Final instant to record when the provider did not say when the event ended.

    The provider's latest update time and the sport's window after kickoff are both upper bounds of
    the real end, so the earlier of the two is the tighter one.
    """
    estimate = starts_at + settlement_window(sport)
    return estimate if last_changed_at is None else min(last_changed_at, estimate)


def available_at(starts_at: datetime, finished_at: datetime | None, sport: str | None) -> datetime:
    """Instant from which the result may be used; never earlier than the kickoff.

    A result final before kickoff (a walkover) counts once the event has started, so a cutoff at or
    before the kickoff never sees it.
    """
    settled = finished_at if finished_at is not None else settled_at(starts_at, sport)
    return max(starts_at, settled)


def available_before(cutoff: datetime, *, starts_at, finished_at, sport):
    """SQL condition equivalent to ``available_at(...) < cutoff`` over the given columns.

    Written without date arithmetic on columns so it runs unchanged on PostgreSQL and SQLite: the
    estimate is compared against kickoffs moved back by the sport's window instead.
    """
    estimated = or_(
        *(and_(sport == name, starts_at < cutoff - window) for name, window in SETTLEMENT_WINDOWS.items()),
        and_(sport.notin_(tuple(SETTLEMENT_WINDOWS)), starts_at < cutoff - DEFAULT_SETTLEMENT_WINDOW),
    )
    return and_(starts_at < cutoff, or_(finished_at < cutoff, and_(finished_at.is_(None), estimated)))
