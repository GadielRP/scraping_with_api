"""OddsPapi API key pools by endpoint.

``ODDSPAPI_PAID_KEY`` empty: every configured key may call any v4 endpoint.
``ODDSPAPI_PAID_KEY`` set: that key owns ``/odds``; free keys own fixtures
and ``/historical-odds``. Callers construct one ``OddsPapiClient`` per key;
this module only answers which keys belong on which endpoint.
"""

from __future__ import annotations

from infrastructure.settings import Config


def paid_odds_api_key() -> str:
    """Dedicated /odds key. Empty when the paid plan is not configured."""
    return str(getattr(Config, "ODDSPAPI_PAID_KEY", "") or "").strip()


def configured_api_keys() -> list[str]:
    """Paid (if any) then free, de-duplicated.

    If the dedicated env vars are empty, falls back to ``ODDSPAPI_KEY``.
    """
    paid = paid_odds_api_key()
    free = unique_api_keys(getattr(Config, "ODDSPAPI_FREE_KEYS", None) or [])
    if paid or free:
        return unique_api_keys([paid, *free] if paid else free)
    return unique_api_keys(getattr(Config, "ODDSPAPI_KEYS", None) or [])


def odds_endpoint_api_keys() -> list[str]:
    """Keys allowed to call ``/odds``."""
    paid = paid_odds_api_key()
    if paid:
        return [paid]
    return configured_api_keys()


def free_endpoint_api_keys() -> list[str]:
    """Keys for fixtures and ``/historical-odds``.

    When a paid key is set these are the free keys only. If nothing else is
    configured, the paid key is returned so a one-key install still works.
    """
    paid = paid_odds_api_key()
    free = unique_api_keys(
        key
        for key in (getattr(Config, "ODDSPAPI_FREE_KEYS", None) or [])
        if str(key).strip() != paid
    )
    if paid:
        return free or [paid]
    if free:
        return free
    return configured_api_keys()


def api_key_for_slot(slot: int, api_keys: list[str] | None = None) -> str:
    """Rotate through a pool so added keys are used without code changes.

    Slot ``n`` maps to ``keys[n % len(keys)]``. Empty string when none exist.
    """
    pool = unique_api_keys(api_keys) if api_keys is not None else configured_api_keys()
    if not pool:
        return ""
    return pool[int(slot) % len(pool)]


def parallel_worker_count(
    *,
    max_workers: int,
    api_key_count: int,
    item_count: int,
) -> int:
    """One worker per key, never more workers than keys or items."""
    if api_key_count <= 0 or item_count <= 0:
        return 1
    return min(max(1, int(max_workers or 1)), api_key_count, item_count)


def unique_api_keys(keys) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for key in keys or []:
        cleaned = str(key or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            unique.append(cleaned)
    return unique
