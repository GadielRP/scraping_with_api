"""Lazy composition root for the shared OddsPapi key scheduler."""

from __future__ import annotations

import threading

from infrastructure.settings import Config
from modules.oddspapi.account_usage import OddspapiAccountUsageService
from modules.oddspapi.api_key_inventory import ApiKeyInventory
from modules.oddspapi.api_key_scheduler import OddsPapiApiKeyScheduler

_runtime_lock = threading.Lock()
_scheduler: OddsPapiApiKeyScheduler | None = None


def get_oddspapi_key_scheduler() -> OddsPapiApiKeyScheduler:
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    with _runtime_lock:
        if _scheduler is None:
            # Imports are intentionally delayed so importing the HTTP client
            # does not eagerly connect to PostgreSQL.
            from infrastructure.persistence.repositories.oddspapi_api_key_usage_repository import (
                OddspapiApiKeyUsageRepository,
            )

            refresh_enabled = getattr(
                Config,
                "ENABLE_ODDSPAPI_ACCOUNT_USAGE_REFRESH",
                True,
            )
            _scheduler = OddsPapiApiKeyScheduler(
                inventory=ApiKeyInventory(),
                store=(OddspapiApiKeyUsageRepository() if refresh_enabled else None),
                account_usage_service=(
                    OddspapiAccountUsageService() if refresh_enabled else None
                ),
                refresh_hours=getattr(
                    Config,
                    "ODDSPAPI_ACCOUNT_USAGE_REFRESH_HOURS",
                    24,
                ),
                refresh_retry_minutes=getattr(
                    Config,
                    "ODDSPAPI_ACCOUNT_USAGE_REFRESH_RETRY_MINUTES",
                    60,
                ),
                endpoint_cooldowns=getattr(
                    Config,
                    "ODDSPAPI_ENDPOINT_COOLDOWNS",
                    {},
                ),
            )
    return _scheduler


def refresh_oddspapi_account_usage_if_due(*, force: bool = False) -> bool:
    return get_oddspapi_key_scheduler().refresh_if_due(force=force)


def reset_oddspapi_runtime_for_tests() -> None:
    """Reset the lazy singleton; intended only for isolated unit tests."""
    global _scheduler
    with _runtime_lock:
        _scheduler = None


__all__ = [
    "get_oddspapi_key_scheduler",
    "refresh_oddspapi_account_usage_if_due",
]
