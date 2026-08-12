"""Configuration-backed field priority for quote-aware reads.

Write identity always keeps providers separate.  This policy only composes the
single number expected by existing non-exchange presentation consumers while
retaining per-field provenance in the read model.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional, Tuple


def _normalize_sources(values, *, context: str) -> Tuple[str, ...]:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{context} must be a non-empty JSON array")
    normalized = tuple(str(item or "").strip().lower() for item in values)
    if any(not item for item in normalized):
        raise ValueError(f"{context} cannot contain empty sources")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{context} cannot contain duplicate sources")
    return normalized


@dataclass(frozen=True, slots=True)
class QuoteFieldPriority:
    initial: Tuple[str, ...]
    current: Tuple[str, ...]


@dataclass(frozen=True, slots=True)
class QuoteReadPriorityOverride:
    priority: QuoteFieldPriority
    sport: Optional[str] = None
    bookie_id: Optional[int] = None

    @property
    def specificity(self) -> int:
        return int(self.sport is not None) + int(self.bookie_id is not None)

    def matches(self, *, sport: Optional[str], bookie_id: int) -> bool:
        if self.sport is not None and self.sport.casefold() != (sport or "").casefold():
            return False
        return self.bookie_id is None or self.bookie_id == bookie_id


@dataclass(frozen=True, slots=True)
class QuoteReadPriorityPolicy:
    version: int
    default: QuoteFieldPriority
    overrides: Tuple[QuoteReadPriorityOverride, ...] = ()

    def resolve(self, *, sport: Optional[str], bookie_id: int) -> QuoteFieldPriority:
        candidates = [
            item
            for item in self.overrides
            if item.matches(sport=sport, bookie_id=bookie_id)
        ]
        if not candidates:
            return self.default
        candidates.sort(
            key=lambda item: (
                -item.specificity,
                0 if item.bookie_id is not None else 1,
                (item.sport or "").casefold(),
            )
        )
        return candidates[0].priority


def _parse_priority(payload, *, context: str) -> QuoteFieldPriority:
    if not isinstance(payload, dict):
        raise ValueError(f"{context} must be an object")
    return QuoteFieldPriority(
        initial=_normalize_sources(payload.get("initial"), context=f"{context}.initial"),
        current=_normalize_sources(payload.get("current"), context=f"{context}.current"),
    )


@lru_cache(maxsize=16)
def load_quote_read_priority_policy(path_value: str) -> QuoteReadPriorityPolicy:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[4] / path
    path = path.resolve()
    if not path.is_file():
        raise ValueError(f"Odds read priority config does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid odds read priority config {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError("Odds read priority config requires version=1")

    overrides = []
    seen_scopes: set[tuple[Optional[str], Optional[int]]] = set()
    raw_overrides = payload.get("overrides", [])
    if not isinstance(raw_overrides, list):
        raise ValueError("overrides must be a JSON array")
    for index, raw in enumerate(raw_overrides):
        if not isinstance(raw, dict):
            raise ValueError(f"overrides[{index}] must be an object")
        sport = str(raw.get("sport") or "").strip() or None
        bookie_raw = raw.get("bookie_id")
        bookie_id = int(bookie_raw) if bookie_raw is not None else None
        if sport is None and bookie_id is None:
            raise ValueError(f"overrides[{index}] must define sport or bookie_id")
        scope = (sport.casefold() if sport else None, bookie_id)
        if scope in seen_scopes:
            raise ValueError(f"Duplicate odds read priority scope: {scope}")
        seen_scopes.add(scope)
        overrides.append(
            QuoteReadPriorityOverride(
                sport=sport,
                bookie_id=bookie_id,
                priority=_parse_priority(raw, context=f"overrides[{index}]"),
            )
        )
    return QuoteReadPriorityPolicy(
        version=1,
        default=_parse_priority(payload.get("default"), context="default"),
        overrides=tuple(overrides),
    )


def order_available_sources(
    available: Iterable[str], preferred: Iterable[str]
) -> tuple[Tuple[str, ...], Tuple[str, ...]]:
    available_set = {str(item).strip().lower() for item in available if str(item).strip()}
    ordered = [item for item in preferred if item in available_set]
    unconfigured = tuple(sorted(available_set.difference(ordered)))
    return tuple(ordered) + unconfigured, unconfigured


__all__ = [
    "QuoteFieldPriority",
    "QuoteReadPriorityOverride",
    "QuoteReadPriorityPolicy",
    "load_quote_read_priority_policy",
    "order_available_sources",
]
