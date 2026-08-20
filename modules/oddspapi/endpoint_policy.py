"""Quota policy for OddsPapi endpoints.

Keeping quota semantics outside the HTTP client prevents request formatting,
credential selection, and accounting rules from becoming one responsibility.
Unknown endpoints are deliberately metered: under-counting is more dangerous
than conservatively accounting a newly added endpoint.
"""

from __future__ import annotations

from enum import Enum


class EndpointQuotaPolicy(str, Enum):
    METERED = "metered"
    FREE_QUOTA_GATED = "free_quota_gated"
    UNMETERED = "unmetered"


def normalize_endpoint(endpoint: str) -> str:
    normalized = str(endpoint or "").strip().lstrip("/").lower()
    if normalized.startswith("v4/"):
        normalized = normalized[3:]
    return normalized.rstrip("/")


class EndpointPolicyRegistry:
    """Classify endpoints by quota behavior."""

    FREE_QUOTA_GATED_ENDPOINTS = frozenset({"historical-odds"})
    UNMETERED_ENDPOINTS = frozenset({"account"})

    def policy_for(self, endpoint: str) -> EndpointQuotaPolicy:
        normalized = normalize_endpoint(endpoint)
        if normalized in self.UNMETERED_ENDPOINTS:
            return EndpointQuotaPolicy.UNMETERED
        if normalized in self.FREE_QUOTA_GATED_ENDPOINTS:
            return EndpointQuotaPolicy.FREE_QUOTA_GATED
        return EndpointQuotaPolicy.METERED


__all__ = [
    "EndpointPolicyRegistry",
    "EndpointQuotaPolicy",
    "normalize_endpoint",
]
