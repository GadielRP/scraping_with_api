from datetime import datetime, timezone

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Base, OddspapiApiKeyUsage
import infrastructure.persistence.repositories.oddspapi_api_key_usage_repository as repository_module
from infrastructure.persistence.repositories.oddspapi_api_key_usage_repository import (
    OddspapiApiKeyUsageRepository,
)
from modules.oddspapi.account_usage import AccountUsageSnapshot
from modules.oddspapi.api_key_inventory import api_key_fingerprint
from shared.timezone_utils import convert_utc_to_local, get_local_now


def test_usage_repository_persists_only_fingerprint_and_atomic_estimate(monkeypatch):
    manager = DatabaseManager("sqlite:///:memory:")
    Base.metadata.create_all(manager.engine)
    monkeypatch.setattr(repository_module, "db_manager", manager)
    fingerprint = api_key_fingerprint("never-persist-this-secret")
    snapshot = AccountUsageSnapshot(
        key_fingerprint=fingerprint,
        subscription_id="sub-1",
        subscription_valid_from=convert_utc_to_local(
            datetime(2026, 8, 1, tzinfo=timezone.utc)
        ),
        subscription_valid_until=None,
        request_limit=250,
        request_count=80,
        status="active",
        refreshed_at=get_local_now(),
    )

    OddspapiApiKeyUsageRepository.apply_account_snapshot(snapshot)
    OddspapiApiKeyUsageRepository.increment_estimated_usage(fingerprint)

    loaded = OddspapiApiKeyUsageRepository.load([fingerprint])
    assert len(loaded) == 1
    assert loaded[0].estimated_request_count == 81
    assert loaded[0].account_refreshed_at.tzinfo is None
    assert "api_key" not in OddspapiApiKeyUsage.__table__.columns
    assert "never-persist-this-secret" not in repr(loaded[0])
