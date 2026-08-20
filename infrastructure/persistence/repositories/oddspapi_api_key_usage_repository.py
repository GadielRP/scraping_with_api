"""Persistence adapter for OddsPapi quota estimates.

The table intentionally contains no raw key and no secondary indexes: the
configured pool is tiny and every lookup is by its primary-key fingerprint.
"""

from __future__ import annotations

from datetime import datetime
import threading

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import OddspapiApiKeyUsage
from modules.oddspapi.account_usage import AccountUsageSnapshot
from modules.oddspapi.api_key_scheduler import PersistedApiKeyUsage
from shared.timezone_utils import get_local_now


def _now() -> datetime:
    return get_local_now()


class OddspapiApiKeyUsageRepository:
    # The SQL increment is atomic for existing PostgreSQL rows. This lock also
    # serializes the rare first insert inside one process, before a refresh has
    # created the normal four-row inventory.
    _insert_lock = threading.Lock()

    @staticmethod
    def load(fingerprints: list[str]) -> list[PersistedApiKeyUsage]:
        if not fingerprints:
            return []
        with db_manager.get_session() as session:
            rows = (
                session.query(OddspapiApiKeyUsage)
                .filter(OddspapiApiKeyUsage.key_fingerprint.in_(fingerprints))
                .all()
            )
            return [
                PersistedApiKeyUsage(
                    key_fingerprint=row.key_fingerprint,
                    subscription_id=row.subscription_id,
                    subscription_valid_from=row.subscription_valid_from,
                    subscription_valid_until=row.subscription_valid_until,
                    request_limit=row.request_limit,
                    reported_request_count=row.reported_request_count,
                    estimated_request_count=row.estimated_request_count,
                    status=row.status,
                    account_refreshed_at=row.account_refreshed_at,
                    last_error_code=row.last_error_code,
                    last_error_at=row.last_error_at,
                )
                for row in rows
            ]

    @staticmethod
    def _get_or_create(session, fingerprint: str) -> OddspapiApiKeyUsage:
        row = session.get(OddspapiApiKeyUsage, fingerprint)
        if row is None:
            row = OddspapiApiKeyUsage(
                key_fingerprint=fingerprint,
                estimated_request_count=0,
                status='unknown',
                updated_at=_now(),
            )
            session.add(row)
        return row

    @staticmethod
    def apply_account_snapshot(snapshot: AccountUsageSnapshot) -> None:
        with db_manager.get_session() as session:
            row = OddspapiApiKeyUsageRepository._get_or_create(
                session,
                snapshot.key_fingerprint,
            )
            row.subscription_id = snapshot.subscription_id
            row.subscription_valid_from = snapshot.subscription_valid_from
            row.subscription_valid_until = snapshot.subscription_valid_until
            row.request_limit = snapshot.request_limit
            row.reported_request_count = snapshot.request_count
            row.estimated_request_count = max(0, int(snapshot.request_count or 0))
            row.status = snapshot.status
            row.account_refreshed_at = snapshot.refreshed_at
            row.last_error_code = None
            row.last_error_at = None
            row.updated_at = _now()

    @staticmethod
    def increment_estimated_usage(fingerprint: str) -> None:
        # PostgreSQL can make both the initial insert and every subsequent
        # increment one atomic statement, so multiple application instances
        # cannot lose updates for the same key.
        with db_manager.get_session() as session:
            if session.bind.dialect.name == "postgresql":
                table = OddspapiApiKeyUsage.__table__
                statement = postgresql_insert(table).values(
                    key_fingerprint=fingerprint,
                    estimated_request_count=1,
                    status="unknown",
                    updated_at=_now(),
                )
                statement = statement.on_conflict_do_update(
                    index_elements=[table.c.key_fingerprint],
                    set_={
                        "estimated_request_count": (
                            func.coalesce(table.c.estimated_request_count, 0) + 1
                        ),
                        "updated_at": _now(),
                    },
                )
                session.execute(statement)
                return

        def increment_existing(session) -> int:
            return (
                session.query(OddspapiApiKeyUsage)
                .filter(OddspapiApiKeyUsage.key_fingerprint == fingerprint)
                .update(
                    {
                        OddspapiApiKeyUsage.estimated_request_count: (
                            func.coalesce(
                                OddspapiApiKeyUsage.estimated_request_count,
                                0,
                            )
                            + 1
                        ),
                        OddspapiApiKeyUsage.updated_at: _now(),
                    },
                    synchronize_session=False,
                )
            )

        # The common path is a single atomic UPDATE and requires no Python
        # serialization. Only the first-ever use of a fingerprint takes the
        # creation lock and rechecks before INSERT.
        with db_manager.get_session() as session:
            updated = increment_existing(session)
        if updated:
            return

        with OddspapiApiKeyUsageRepository._insert_lock:
            with db_manager.get_session() as session:
                if increment_existing(session) == 0:
                    session.add(
                        OddspapiApiKeyUsage(
                            key_fingerprint=fingerprint,
                            estimated_request_count=1,
                            status="unknown",
                            updated_at=_now(),
                        )
                    )

    @staticmethod
    def update_status(
        fingerprint: str,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None:
        with db_manager.get_session() as session:
            row = OddspapiApiKeyUsageRepository._get_or_create(session, fingerprint)
            row.status = status
            row.last_error_code = error_code
            row.last_error_at = _now() if error_code else row.last_error_at
            row.updated_at = _now()

    @staticmethod
    def record_refresh_failure(fingerprint: str, error_code: str) -> None:
        with db_manager.get_session() as session:
            row = OddspapiApiKeyUsageRepository._get_or_create(session, fingerprint)
            row.last_error_code = str(error_code)[:100]
            row.last_error_at = _now()
            row.updated_at = _now()


__all__ = ["OddspapiApiKeyUsageRepository"]
