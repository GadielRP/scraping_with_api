"""Thread-safe, quota-aware API key scheduling for OddsPapi."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import threading
import time
from typing import Protocol

from modules.oddspapi.account_usage import (
    AccountUsageSnapshot,
    OddspapiAccountUsageService,
)
from modules.oddspapi.api_key_inventory import ApiKeyCredential, ApiKeyInventory
from modules.oddspapi.endpoint_policy import (
    EndpointPolicyRegistry,
    EndpointQuotaPolicy,
    normalize_endpoint,
)
from modules.oddspapi.exceptions import OddsPapiQuotaExhaustedError
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = {"unknown", "active"}
DISABLED_STATUSES = {"exhausted", "invalid", "no_active_subscription"}
QUOTA_EXHAUSTED_CODE = "REQUEST_LIMIT_EXCEEDED"
INVALID_KEY_CODES = {"INVALID_API_KEY", "INVALID_KEY", "UNAUTHORIZED"}


@dataclass(frozen=True)
class PersistedApiKeyUsage:
    key_fingerprint: str
    subscription_id: str | None = None
    subscription_valid_from: datetime | None = None
    subscription_valid_until: datetime | None = None
    request_limit: int | None = None
    reported_request_count: int | None = None
    estimated_request_count: int = 0
    status: str = "unknown"
    account_refreshed_at: datetime | None = None
    last_error_code: str | None = None
    last_error_at: datetime | None = None


class ApiKeyUsageStore(Protocol):
    def load(self, fingerprints: list[str]) -> list[PersistedApiKeyUsage]: ...

    def apply_account_snapshot(self, snapshot: AccountUsageSnapshot) -> None: ...

    def increment_estimated_usage(self, fingerprint: str) -> None: ...

    def update_status(
        self,
        fingerprint: str,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None: ...

    def record_refresh_failure(self, fingerprint: str, error_code: str) -> None: ...


@dataclass(frozen=True)
class RequestOutcome:
    status_code: int | None = None
    error_code: str | None = None
    response_received: bool = False
    network_error: bool = False
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class ApiKeyLease:
    api_key: str
    key_fingerprint: str
    endpoint: str
    quota_policy: EndpointQuotaPolicy
    sequence: int
    wait_seconds: float = 0.0

    @property
    def key_id(self) -> str:
        return self.key_fingerprint[:10]


@dataclass
class _RuntimeKeyState:
    fingerprint: str
    subscription_id: str | None = None
    subscription_valid_from: datetime | None = None
    subscription_valid_until: datetime | None = None
    request_limit: int | None = None
    reported_request_count: int | None = None
    estimated_request_count: int = 0
    status: str = "unknown"
    account_refreshed_at: datetime | None = None
    last_error_code: str | None = None
    last_error_at: datetime | None = None
    last_assigned_sequence: int = -1
    in_flight_by_endpoint: Counter = field(default_factory=Counter)
    metered_in_flight: int = 0
    ready_at_by_endpoint: dict[str, float] = field(default_factory=dict)
    blocked_until_by_endpoint: dict[str, float] = field(default_factory=dict)
    refreshing: bool = False

    @property
    def total_in_flight(self) -> int:
        return sum(self.in_flight_by_endpoint.values())


class OddsPapiApiKeyScheduler:
    """Allocate credentials independently from HTTP workers and endpoint parsing."""

    def __init__(
        self,
        *,
        inventory: ApiKeyInventory | None = None,
        policy_registry: EndpointPolicyRegistry | None = None,
        store: ApiKeyUsageStore | None = None,
        account_usage_service: OddspapiAccountUsageService | None = None,
        refresh_hours: int = 24,
        refresh_retry_minutes: int = 60,
        endpoint_cooldowns: dict[str, float] | None = None,
        monotonic=time.monotonic,
    ) -> None:
        self.inventory = inventory or ApiKeyInventory()
        self.policy_registry = policy_registry or EndpointPolicyRegistry()
        self.store = store
        self.account_usage_service = account_usage_service
        self.refresh_hours = max(1, int(refresh_hours or 24))
        self.refresh_retry_minutes = max(
            1,
            int(refresh_retry_minutes or 60),
        )
        self.endpoint_cooldowns = {
            normalize_endpoint(endpoint): max(0.0, float(seconds))
            for endpoint, seconds in (endpoint_cooldowns or {}).items()
        }
        self._monotonic = monotonic
        self._condition = threading.Condition(threading.RLock())
        self._refresh_lock = threading.Lock()
        self._states: dict[str, _RuntimeKeyState] = {}
        self._sequence = 0
        self._assignment_counts: Counter[str] = Counter()
        self._diagnostic_counts: Counter[str] = Counter()
        self._load_persisted_state()

    def _load_persisted_state(self) -> None:
        credentials = self.inventory.all_credentials()
        with self._condition:
            for credential in credentials:
                self._states.setdefault(
                    credential.fingerprint,
                    _RuntimeKeyState(credential.fingerprint),
                )
        if self.store is None or not credentials:
            return
        try:
            rows = self.store.load([item.fingerprint for item in credentials])
        except Exception:
            logger.exception("Could not load persisted OddsPapi API key usage")
            return
        with self._condition:
            for row in rows:
                self._states[row.key_fingerprint] = _RuntimeKeyState(
                    fingerprint=row.key_fingerprint,
                    subscription_id=row.subscription_id,
                    subscription_valid_from=row.subscription_valid_from,
                    subscription_valid_until=row.subscription_valid_until,
                    request_limit=row.request_limit,
                    reported_request_count=row.reported_request_count,
                    estimated_request_count=max(0, int(row.estimated_request_count or 0)),
                    status=row.status if row.status in ACTIVE_STATUSES | DISABLED_STATUSES else "unknown",
                    account_refreshed_at=row.account_refreshed_at,
                    last_error_code=row.last_error_code,
                    last_error_at=row.last_error_at,
                )

    def _state_for(self, credential: ApiKeyCredential) -> _RuntimeKeyState:
        return self._states.setdefault(
            credential.fingerprint,
            _RuntimeKeyState(credential.fingerprint),
        )

    def available_key_count(self, endpoint: str) -> int:
        policy = self.policy_registry.policy_for(endpoint)
        with self._condition:
            return sum(
                1
                for credential in self.inventory.credentials_for_endpoint(endpoint)
                if self._is_eligible(self._state_for(credential), policy)
            )

    @staticmethod
    def _is_eligible(
        state: _RuntimeKeyState,
        policy: EndpointQuotaPolicy,
    ) -> bool:
        if state.refreshing:
            return False
        if policy is EndpointQuotaPolicy.UNMETERED:
            return True
        return state.status in ACTIVE_STATUSES

    def acquire(self, endpoint: str) -> ApiKeyLease:
        normalized = normalize_endpoint(endpoint)
        if not normalized:
            raise ValueError("OddsPapi endpoint is required")
        policy = self.policy_registry.policy_for(normalized)
        credentials = self.inventory.credentials_for_endpoint(normalized)
        if not credentials:
            raise ValueError("ODDSPAPI_KEY is required to make an OddsPapi request")

        with self._condition:
            while True:
                pool = [
                    (credential, self._state_for(credential))
                    for credential in credentials
                ]
                candidates = [
                    item
                    for item in pool
                    if self._is_eligible(item[1], policy)
                ]
                if candidates:
                    # A temporary 429 makes only this key/endpoint pair
                    # unavailable. Prefer an unblocked credential and, when
                    # every credential is blocked, wait only for the earliest.
                    now = self._monotonic()
                    unblocked = [
                        item
                        for item in candidates
                        if item[1].blocked_until_by_endpoint.get(normalized, 0.0)
                        <= now
                    ]
                    if unblocked:
                        candidates = unblocked
                    else:
                        earliest = min(
                            item[1].blocked_until_by_endpoint.get(normalized, 0.0)
                            for item in candidates
                        )
                        candidates = [
                            item
                            for item in candidates
                            if item[1].blocked_until_by_endpoint.get(normalized, 0.0)
                            == earliest
                        ]
                    break
                # Refresh temporarily removes a credential from allocation.
                # Wait instead of misreporting quota exhaustion, especially
                # when /odds has one dedicated paid key.
                if any(state.refreshing for _credential, state in pool):
                    self._condition.wait(timeout=1.0)
                    continue
                raise OddsPapiQuotaExhaustedError(f"/v4/{normalized}")

            now = self._monotonic()
            known_limits = [
                state.request_limit
                for _credential, state in candidates
                if state.request_limit is not None and state.request_limit > 0
            ]
            inferred_request_limit = max(known_limits, default=1)
            credential, state = min(
                candidates,
                key=lambda item: self._selection_score(
                    item[1],
                    normalized,
                    policy,
                    now,
                    inferred_request_limit,
                ),
            )
            self._sequence += 1
            sequence = self._sequence
            state.last_assigned_sequence = sequence
            state.in_flight_by_endpoint[normalized] += 1
            if policy is EndpointQuotaPolicy.METERED:
                state.metered_in_flight += 1
            self._assignment_counts[credential.log_id] += 1
            ready_at = max(
                state.ready_at_by_endpoint.get(normalized, 0.0),
                state.blocked_until_by_endpoint.get(normalized, 0.0),
            )
            return ApiKeyLease(
                api_key=credential.api_key,
                key_fingerprint=credential.fingerprint,
                endpoint=normalized,
                quota_policy=policy,
                sequence=sequence,
                wait_seconds=max(0.0, ready_at - now),
            )

    @staticmethod
    def _selection_score(
        state: _RuntimeKeyState,
        endpoint: str,
        policy: EndpointQuotaPolicy,
        now: float,
        inferred_request_limit: int,
    ) -> tuple:
        endpoint_in_flight = state.in_flight_by_endpoint.get(endpoint, 0)
        delay = max(
            state.ready_at_by_endpoint.get(endpoint, 0.0),
            state.blocked_until_by_endpoint.get(endpoint, 0.0),
        ) - now
        delay = max(0.0, delay)
        if policy is EndpointQuotaPolicy.METERED:
            if state.request_limit and state.request_limit > 0:
                utilization = (
                    state.estimated_request_count + state.metered_in_flight
                ) / state.request_limit
            else:
                # A new/unknown account starts at zero utilization, but its
                # locally estimated requests must subsequently increase its
                # score. Infer a denominator from the current pool so a failed
                # /account refresh cannot give one key permanent priority.
                utilization = (
                    state.estimated_request_count + state.metered_in_flight
                ) / max(1, inferred_request_limit)
            return utilization, endpoint_in_flight > 0, delay, state.last_assigned_sequence
        return endpoint_in_flight > 0, endpoint_in_flight, delay, state.last_assigned_sequence

    def complete(self, lease: ApiKeyLease, outcome: RequestOutcome) -> None:
        normalized_error = str(outcome.error_code or "").strip().upper() or None
        persist_increment = False
        persist_status: tuple[str, str | None] | None = None
        with self._condition:
            state = self._states.get(lease.key_fingerprint)
            if state is None:
                return
            if state.in_flight_by_endpoint.get(lease.endpoint, 0) > 0:
                state.in_flight_by_endpoint[lease.endpoint] -= 1
                if state.in_flight_by_endpoint[lease.endpoint] <= 0:
                    del state.in_flight_by_endpoint[lease.endpoint]
            if (
                lease.quota_policy is EndpointQuotaPolicy.METERED
                and state.metered_in_flight > 0
            ):
                state.metered_in_flight -= 1

            now_mono = self._monotonic()
            cooldown = self.endpoint_cooldowns.get(lease.endpoint, 0.0)
            state.ready_at_by_endpoint[lease.endpoint] = now_mono + cooldown

            quota_exhausted = normalized_error == QUOTA_EXHAUSTED_CODE
            invalid_key = (
                outcome.status_code == 401 or normalized_error in INVALID_KEY_CODES
            )
            if quota_exhausted:
                self._diagnostic_counts[
                    f"{lease.key_id}:quota_exhausted"
                ] += 1
                state.status = "exhausted"
                state.last_error_code = normalized_error
                state.last_error_at = get_local_now()
                persist_status = ("exhausted", normalized_error)
            elif invalid_key:
                self._diagnostic_counts[f"{lease.key_id}:invalid"] += 1
                state.status = "invalid"
                state.last_error_code = normalized_error or "HTTP_401"
                state.last_error_at = get_local_now()
                persist_status = ("invalid", state.last_error_code)
            elif outcome.status_code == 429:
                self._diagnostic_counts[f"{lease.key_id}:rate_limited"] += 1
                delay = max(1.0, float(outcome.retry_after_seconds or 0.0))
                state.blocked_until_by_endpoint[lease.endpoint] = now_mono + delay

            if lease.quota_policy is EndpointQuotaPolicy.METERED:
                rejected_before_processing = quota_exhausted or invalid_key
                if not rejected_before_processing and (
                    outcome.response_received or outcome.network_error
                ):
                    state.estimated_request_count += 1
                    persist_increment = True
                    if (
                        state.request_limit is not None
                        and state.estimated_request_count >= state.request_limit
                    ):
                        state.status = "exhausted"
                        persist_status = ("exhausted", "ESTIMATED_LIMIT_REACHED")
            self._condition.notify_all()

        if persist_increment:
            self._persist(
                "increment estimated usage",
                lambda: self.store.increment_estimated_usage(lease.key_fingerprint),
            )
        if persist_status is not None:
            status, error_code = persist_status
            self._persist(
                "update key status",
                lambda: self.store.update_status(
                    lease.key_fingerprint,
                    status,
                    error_code=error_code,
                ),
            )

    def refresh_if_due(self, *, force: bool = False) -> bool:
        if self.account_usage_service is None:
            return False
        if not self._refresh_lock.acquire(blocking=False):
            return False
        refreshed_any = False
        try:
            now = get_local_now()
            for credential in self.inventory.all_credentials():
                with self._condition:
                    state = self._state_for(credential)
                    refreshed_at = state.account_refreshed_at
                    due = force or refreshed_at is None or (
                        now - refreshed_at >= timedelta(hours=self.refresh_hours)
                    )
                    last_error_at = state.last_error_at
                    if (
                        due
                        and not force
                        and last_error_at is not None
                        and str(state.last_error_code or "").startswith(
                            "ACCOUNT_REFRESH_"
                        )
                        and (refreshed_at is None or last_error_at >= refreshed_at)
                        and now - last_error_at
                        < timedelta(minutes=self.refresh_retry_minutes)
                    ):
                        due = False
                    if not due:
                        continue
                    state.refreshing = True
                    while state.total_in_flight > 0:
                        self._condition.wait(timeout=1.0)
                try:
                    snapshot = self.account_usage_service.fetch(credential.api_key)
                except Exception as exc:  # stale state is intentionally fail-open
                    error_code = f"ACCOUNT_REFRESH_{type(exc).__name__}"
                    with self._condition:
                        failed_state = self._state_for(credential)
                        failed_state.last_error_code = error_code
                        failed_state.last_error_at = get_local_now()
                    logger.warning(
                        "OddsPapi account usage refresh failed key_id=%s error=%s",
                        credential.log_id,
                        error_code,
                    )
                    self._persist(
                        "record account refresh failure",
                        lambda: self.store.record_refresh_failure(
                            credential.fingerprint,
                            error_code,
                        ),
                    )
                else:
                    self._apply_snapshot(snapshot)
                    refreshed_any = True
                    remaining = (
                        max((snapshot.request_limit or 0) - (snapshot.request_count or 0), 0)
                        if snapshot.request_limit is not None
                        and snapshot.request_count is not None
                        else None
                    )
                    logger.info(
                        "OddsPapi account usage refreshed key_id=%s reported=%s "
                        "estimated=%s limit=%s remaining=%s status=%s",
                        credential.log_id,
                        snapshot.request_count,
                        snapshot.request_count,
                        snapshot.request_limit,
                        remaining,
                        snapshot.status,
                    )
                finally:
                    with self._condition:
                        self._state_for(credential).refreshing = False
                        self._condition.notify_all()
        finally:
            self._refresh_lock.release()
        return refreshed_any

    def _apply_snapshot(self, snapshot: AccountUsageSnapshot) -> None:
        with self._condition:
            state = self._states.setdefault(
                snapshot.key_fingerprint,
                _RuntimeKeyState(snapshot.key_fingerprint),
            )
            state.subscription_id = snapshot.subscription_id
            state.subscription_valid_from = snapshot.subscription_valid_from
            state.subscription_valid_until = snapshot.subscription_valid_until
            state.request_limit = snapshot.request_limit
            state.reported_request_count = snapshot.request_count
            state.estimated_request_count = max(0, int(snapshot.request_count or 0))
            state.status = snapshot.status
            state.account_refreshed_at = snapshot.refreshed_at
            state.last_error_code = None
            state.last_error_at = None
        self._persist(
            "persist account usage snapshot",
            lambda: self.store.apply_account_snapshot(snapshot),
        )

    def assignment_counts(self) -> dict[str, int]:
        with self._condition:
            return dict(self._assignment_counts)

    def diagnostic_counts(self) -> dict[str, int]:
        """Return non-secret counters for quota, auth, and rate-limit events."""
        with self._condition:
            return dict(self._diagnostic_counts)

    def usage_snapshot(self) -> dict[str, PersistedApiKeyUsage]:
        with self._condition:
            return {
                fingerprint: PersistedApiKeyUsage(
                    key_fingerprint=fingerprint,
                    subscription_id=state.subscription_id,
                    subscription_valid_from=state.subscription_valid_from,
                    subscription_valid_until=state.subscription_valid_until,
                    request_limit=state.request_limit,
                    reported_request_count=state.reported_request_count,
                    estimated_request_count=state.estimated_request_count,
                    status=state.status,
                    account_refreshed_at=state.account_refreshed_at,
                    last_error_code=state.last_error_code,
                    last_error_at=state.last_error_at,
                )
                for fingerprint, state in self._states.items()
            }

    def _persist(self, operation: str, callback) -> None:
        if self.store is None:
            return
        try:
            callback()
        except Exception:
            logger.exception("Could not %s for OddsPapi key state", operation)


__all__ = [
    "ApiKeyLease",
    "ApiKeyUsageStore",
    "OddsPapiApiKeyScheduler",
    "PersistedApiKeyUsage",
    "RequestOutcome",
]
