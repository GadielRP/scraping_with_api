"""Read and validate account quota snapshots from ``/v4/account``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from modules.oddspapi.api_key_inventory import api_key_fingerprint
from shared.timezone_utils import convert_utc_to_local, get_local_now


def _parse_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return convert_utc_to_local(parsed)


@dataclass(frozen=True)
class AccountUsageSnapshot:
    key_fingerprint: str
    subscription_id: str | None
    subscription_valid_from: datetime | None
    subscription_valid_until: datetime | None
    request_limit: int | None
    request_count: int | None
    status: str
    refreshed_at: datetime


class OddspapiAccountUsageService:
    """Fetch account payloads with an explicitly keyed client."""

    def __init__(self, client_factory: Callable | None = None) -> None:
        self._client_factory = client_factory

    def fetch(self, api_key: str) -> AccountUsageSnapshot:
        if self._client_factory is None:
            # Local import keeps the explicit-key account path independent of
            # the default scheduler construction used by OddsPapiClient().
            from modules.oddspapi.client import OddsPapiClient

            client = OddsPapiClient(api_key=api_key)
        else:
            client = self._client_factory(api_key=api_key)
        try:
            return self.parse(client.get_account(), api_key=api_key)
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    @staticmethod
    def parse(payload: dict, *, api_key: str) -> AccountUsageSnapshot:
        if not isinstance(payload, dict):
            raise ValueError("OddsPapi /account response must be an object")

        echoed_key = str(payload.get("api_key") or "").strip()
        if echoed_key and echoed_key != api_key:
            raise ValueError("OddsPapi /account returned a different API key")

        subscriptions = payload.get("subscriptions")
        subscriptions = subscriptions if isinstance(subscriptions, list) else []
        current_id = str(payload.get("current_subscription_id") or "").strip()
        selected = next(
            (
                item
                for item in subscriptions
                if isinstance(item, dict)
                and current_id
                and str(item.get("subscription_id") or "").strip() == current_id
            ),
            None,
        )
        if selected is None:
            active = [
                item
                for item in subscriptions
                if isinstance(item, dict) and item.get("is_active") is True
            ]
            if len(active) == 1:
                selected = active[0]

        now = get_local_now()
        fingerprint = api_key_fingerprint(api_key)
        if selected is None:
            return AccountUsageSnapshot(
                key_fingerprint=fingerprint,
                subscription_id=None,
                subscription_valid_from=None,
                subscription_valid_until=None,
                request_limit=None,
                request_count=None,
                status="no_active_subscription",
                refreshed_at=now,
            )

        request_limit = int(selected["request_limit"])
        request_count = int(selected["request_count"])
        if request_limit <= 0 or request_count < 0:
            raise ValueError("OddsPapi /account returned invalid quota counters")
        status = "exhausted" if request_count >= request_limit else "active"
        return AccountUsageSnapshot(
            key_fingerprint=fingerprint,
            subscription_id=str(selected.get("subscription_id") or "").strip() or None,
            subscription_valid_from=_parse_datetime(selected.get("valid_from")),
            subscription_valid_until=_parse_datetime(selected.get("valid_until")),
            request_limit=request_limit,
            request_count=request_count,
            status=status,
            refreshed_at=now,
        )


__all__ = ["AccountUsageSnapshot", "OddspapiAccountUsageService"]
