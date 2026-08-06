"""Provider-neutral result of requesting an odds endpoint."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class OddsFetchStatus(str, Enum):
    SUCCESS = "success"
    EMPTY = "empty"
    ENDPOINT_NOT_FOUND = "endpoint_not_found"


@dataclass(frozen=True)
class OddsFetchResult:
    """Expected odds-fetch outcomes; unexpected transport failures still raise."""

    status: OddsFetchStatus
    payload: dict | None = None
    # Provider adapters may expose the unmodified transport payload for an
    # explicitly enabled debug capture. Keeping it separate prevents debug
    # files from accidentally containing the normalized ingestion contract.
    raw_payload: dict | None = None

    @classmethod
    def from_payload(
        cls,
        payload: object,
        *,
        raw_payload: object = None,
    ) -> "OddsFetchResult":
        if isinstance(payload, dict) and payload:
            return cls(
                OddsFetchStatus.SUCCESS,
                payload,
                raw_payload if isinstance(raw_payload, dict) else None,
            )
        return cls(OddsFetchStatus.EMPTY)

    @classmethod
    def endpoint_not_found(cls) -> "OddsFetchResult":
        return cls(OddsFetchStatus.ENDPOINT_NOT_FOUND)

    @property
    def endpoint_missing(self) -> bool:
        return self.status is OddsFetchStatus.ENDPOINT_NOT_FOUND
