"""Transaction-scoped identity locks shared by creation and deletion."""
import hashlib
from sqlalchemy import text


def lock_event_identities(session, keys):
    if session.get_bind().dialect.name != 'postgresql':
        return  # SQLite serializes writers; PostgreSQL is the production backend.
    lock_ids = sorted({int.from_bytes(hashlib.blake2b(
        f'event:{source}:{external_id}'.encode(), digest_size=8
    ).digest(), 'big', signed=True) for source, external_id in keys})
    if lock_ids:
        # Ordered subquery, one round trip, same stable ordering for all writers.
        session.execute(text('SELECT pg_advisory_xact_lock(k) FROM '
                             '(SELECT unnest(CAST(:keys AS bigint[])) AS k ORDER BY k) ordered'),
                        {'keys': lock_ids})
