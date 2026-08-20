"""Configured OddsPapi credentials and endpoint pool membership."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

from modules.oddspapi.api_keys import (
    configured_api_keys,
    free_endpoint_api_keys,
    odds_endpoint_api_keys,
    unique_api_keys,
)
from modules.oddspapi.endpoint_policy import normalize_endpoint


def api_key_fingerprint(api_key: str) -> str:
    """Return a stable identifier without retaining the secret in persistence."""
    return hashlib.sha256(str(api_key).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ApiKeyCredential:
    api_key: str
    fingerprint: str

    @property
    def log_id(self) -> str:
        return self.fingerprint[:10]


class ApiKeyInventory:
    """Read credentials and preserve the existing paid/free pool policy."""

    def all_credentials(self) -> tuple[ApiKeyCredential, ...]:
        return self._credentials(configured_api_keys())

    def credentials_for_endpoint(
        self,
        endpoint: str,
    ) -> tuple[ApiKeyCredential, ...]:
        normalized = normalize_endpoint(endpoint)
        if normalized == "account":
            keys = configured_api_keys()
        elif normalized == "odds":
            keys = odds_endpoint_api_keys()
        else:
            keys = free_endpoint_api_keys()
        return self._credentials(keys)

    @staticmethod
    def _credentials(keys) -> tuple[ApiKeyCredential, ...]:
        return tuple(
            ApiKeyCredential(key, api_key_fingerprint(key))
            for key in unique_api_keys(keys)
        )


class StaticApiKeyInventory(ApiKeyInventory):
    """Small injectable inventory for tests and explicit bounded workflows."""

    def __init__(self, api_keys) -> None:
        self._api_keys = unique_api_keys(api_keys)

    def all_credentials(self) -> tuple[ApiKeyCredential, ...]:
        return self._credentials(self._api_keys)

    def credentials_for_endpoint(self, endpoint: str) -> tuple[ApiKeyCredential, ...]:
        return self.all_credentials()


__all__ = [
    "ApiKeyCredential",
    "ApiKeyInventory",
    "StaticApiKeyInventory",
    "api_key_fingerprint",
]
