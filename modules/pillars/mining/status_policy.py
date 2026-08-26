"""Normalize producer-specific statuses without losing their raw vocabulary."""

from __future__ import annotations


_STATUS_MAP = {
    "ACTIVE": "SUCCESS",
    "OK": "SUCCESS",
    "PARTIAL": "PARTIAL",
    "INSUFFICIENT_DATA": "INSUFFICIENT",
    "ERROR": "ERROR",
    "IGNORE": "SKIPPED",
    "SKIPPED": "SKIPPED",
}


def normalize_status(status: object) -> tuple[str, str]:
    producer_status = str(status or "ERROR").strip().upper()
    try:
        return producer_status, _STATUS_MAP[producer_status]
    except KeyError as exc:
        raise ValueError(f"unsupported pillar status: {producer_status!r}") from exc
