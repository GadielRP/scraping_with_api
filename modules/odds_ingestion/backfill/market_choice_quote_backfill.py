"""Phase 4b MarketChoiceQuote backfill: contracts, classifier, orchestrator.

Dry-run and commit share the same classification and merge policy. Commit mode
calls ``MarketChoiceQuoteWriter`` once per quote identity bucket and bulk-links
snapshots; dry-run evaluates the same policy without DML.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from infrastructure.persistence.repositories.market.market_choice_quote_backfill_repository import (
    ChoiceStateCandidateRow,
    MarketChoiceQuoteBackfillRepository,
    SnapshotCandidateRow,
)
from infrastructure.persistence.repositories.market.market_choice_quote_merge_policy import (
    QuoteCandidateState,
    QuoteExistingState,
    QuoteMergeMode,
    decide_quote_merge,
    existing_state_from_quote,
)
from infrastructure.persistence.repositories.market.market_choice_quote_writer import (
    MarketChoiceQuoteWriter,
)
from infrastructure.persistence.catalogs.canonical_market_types import (
    CANONICAL_MARKET_TYPE_SEEDS,
)
from infrastructure.persistence.market_write_policy import market_write_policy_for_source

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "4b.7"
# Cooperative cancel for Ctrl+C / SIGTERM: finish the current batch, persist
# checkpoint, then stop (campaign loop also honors this between chunks).
_STOP_REQUESTED = False
_STOP_FORCE = False


def request_stop(*, force: bool = False) -> None:
    global _STOP_REQUESTED, _STOP_FORCE
    _STOP_REQUESTED = True
    if force:
        _STOP_FORCE = True


def clear_stop() -> None:
    global _STOP_REQUESTED, _STOP_FORCE
    _STOP_REQUESTED = False
    _STOP_FORCE = False


def stop_requested() -> bool:
    return _STOP_REQUESTED


def stop_forced() -> bool:
    return _STOP_FORCE

# Provider sources (canonical) + historical SofaScore *pipeline channel* labels
# that were written into snapshot.source. Channels are accepted as input but
# always rewritten to quote.source = "sofascore" so ticks from daily_discovery /
# dropping_odds / sofascore share one quote identity (current = latest tick).
PROVIDER_SOURCES = frozenset({"sofascore", "oddspapi", "oddsportal"})
CHANNEL_SOURCES = frozenset(
    {
        "daily_discovery",
        "dropping_odds",
        "winning_odds",
        "secondary_discovery",
        "parallel_odds_checking",
        "sofascore_daily_discovery",
        "sofascore_dropping_odds",
    }
)
KNOWN_SOURCES = PROVIDER_SOURCES | CHANNEL_SOURCES
# Sources whose ingested markets are always main-line (Oddspapi is the only
# provider that distinguishes main vs alternate lines in payload).
ALWAYS_MAIN_LINE_SOURCES = frozenset({"sofascore", "oddsportal"}) | CHANNEL_SOURCES
LEGACY_BACK_LAY_RE = re.compile(r"(?i)^(Back|Lay)(?:\s+(.+))?$")
SOFA_SCORE_BOOKIE_ID = 1
# Bookies that historically also mapped to OddsPortal. Snapless ``choice_state``
# mirrors on these bookies must not be unique-mapped to oddspapi: pre-policy
# OddsPortal wrote initial/current onto MarketChoice without snapshots, and
# today's opening-only policy cannot rewrite that history.
ODDSPORTAL_ERA_BOOKIE_IDS = frozenset({3, 4})  # bet365, Betfair Exchange
# Only these bookies are kept by the backfill; every other market (and its
# choices / snapshots / quotes) is deleted automatically on each run.
BACKFILL_ALLOWED_BOOKIE_IDS = frozenset({1, 3, 4, 302})  # SofaScore, bet365, Betfair, Pinnacle
DEFAULT_BATCH_SIZE = 200
MAX_BATCH_SIZE = 1000
MAX_EVENTS_HARD_CAP = 500
MAX_ROWS_HARD_CAP = 100_000
ADVISORY_LOCK_KEY = 4_020_240_801


class ClassificationStatus(str, Enum):
    RESOLVED = "resolved"
    AMBIGUOUS = "ambiguous"
    CONFLICT = "conflict"
    INVALID = "invalid"


@dataclass(frozen=True)
class QuoteIdentity:
    choice_id: int
    source: str
    exchange_side: Optional[str]
    exchange_level: int


@dataclass(frozen=True)
class BackfillCandidate:
    """Immutable legacy evidence for one snapshot or choice-state row."""

    kind: str  # "snapshot" | "choice_state"
    snapshot_id: Optional[int]
    choice_id: int
    market_id: int
    event_id: int
    bookie_id: Optional[int]
    market_name: str
    market_period: str
    choice_group: Optional[str]
    is_live: bool
    choice_name: str
    raw_source: Optional[str]
    exchange_side: Optional[str]
    exchange_level: Optional[int]
    odds_value: Any = None
    collected_at: Optional[datetime] = None
    source_collected_at: Optional[datetime] = None
    source_market_id: Optional[str] = None
    source_outcome_id: Optional[str] = None
    bookmaker_outcome_id: Optional[str] = None
    main_line: Optional[bool] = None
    source_limit: Any = None
    choice_initial_odds: Any = None
    choice_current_odds: Any = None
    already_linked_quote_id: Optional[int] = None


@dataclass(frozen=True)
class ClassificationDecision:
    status: ClassificationStatus
    reason_code: str
    candidate: BackfillCandidate
    identity: Optional[QuoteIdentity] = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    plan_create_canonical: bool = False
    canonical_market_key: Optional[
        tuple[int, int, str, str, Optional[str], bool]
    ] = None


@dataclass(frozen=True)
class QuoteStateCandidate:
    identity: QuoteIdentity
    initial_price: Any = None
    initial_captured_at: Optional[datetime] = None
    current_price: Any = None
    current_captured_at: Optional[datetime] = None
    main_line: Optional[bool] = None
    source_market_id: Optional[str] = None
    source_outcome_id: Optional[str] = None
    bookmaker_outcome_id: Optional[str] = None
    source_limit: Any = None
    snapshot_ids: tuple[int, ...] = ()
    ownership_from_choice_state: bool = False


@dataclass
class BatchReport:
    pass_name: str
    last_snapshot_id: Optional[int] = None
    last_choice_id: Optional[int] = None
    rows_scanned: int = 0
    resolved: int = 0
    ambiguous: int = 0
    conflicts: int = 0
    invalid: int = 0
    quote_buckets: int = 0
    quotes_inserted: int = 0
    quotes_updated: int = 0
    quotes_unchanged: int = 0
    snapshots_linked: int = 0
    stale_candidates_ignored: int = 0
    metadata_conflicts: int = 0
    canonical_markets_created: int = 0
    canonical_choices_created: int = 0
    rejections: list[dict[str, Any]] = field(default_factory=list)
    notes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RunConfig:
    dry_run: bool = True
    event_id: Optional[int] = None
    event_id_min: Optional[int] = None
    event_id_max: Optional[int] = None
    after_event_id: Optional[int] = None
    after_snapshot_id: Optional[int] = None
    source: Optional[str] = None
    pass_name: str = "all"  # snapshots | choice-states | all
    batch_size: int = DEFAULT_BATCH_SIZE
    max_events: Optional[int] = None
    max_rows: Optional[int] = None
    resolution_file: Optional[Path] = None
    checkpoint_file: Optional[Path] = None
    resume_from: Optional[Path] = None
    output_json: Optional[Path] = None
    output_rejections: Optional[Path] = None
    append_rejections: bool = False
    confirm_ingestion_paused: bool = False
    purge_oddspapi_null_mainline_lines: bool = False
    purge_legacy_back_lay: bool = False
    purge_ambiguous_choice_states: bool = False
    confirm_purge: bool = False


@dataclass
class Checkpoint:
    algorithm_version: str
    run_id: str
    pass_name: str
    event_scope: list[int]
    last_snapshot_id: Optional[int]
    last_choice_id: Optional[int]
    after_event_id: Optional[int]
    filters: dict[str, Any]
    resolution_sha256: Optional[str]
    rows_consumed_total: int
    events_completed: bool
    updated_at: str


def parse_legacy_back_lay_choice_group(
    choice_group: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Return (side, line) from legacy OddsPortal ``Back`` / ``Lay 2.5`` groups."""
    if choice_group is None:
        return None, None
    match = LEGACY_BACK_LAY_RE.match(str(choice_group).strip())
    if not match:
        return None, None
    side = match.group(1).strip().lower()
    line = match.group(2).strip() if match.group(2) else None
    return side, (line or None)


def lookup_catalog_seed_for_market(
    market_name: str, market_period: str
) -> Optional[dict[str, Any]]:
    """Match a persisted market name/period to ``CANONICAL_MARKET_TYPE_SEEDS``.

    Used when seeding the destination market for legacy Back/Lay rematerialization
    so ``market_group`` / ``requires_choice_group`` come from the catalog.
    """
    name = str(market_name or "").strip()
    period = str(market_period or "Full Time").strip() or "Full Time"
    if not name:
        return None
    exact: list[dict[str, Any]] = []
    by_name: list[dict[str, Any]] = []
    for seed in CANONICAL_MARKET_TYPE_SEEDS.values():
        seed_name = str(seed.get("canonical_market_name") or "").strip()
        seed_period = str(seed.get("canonical_market_period") or "").strip()
        if seed_name != name:
            continue
        by_name.append(seed)
        if seed_period == period:
            exact.append(seed)
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        enabled = [s for s in exact if s.get("enabled_for_ingestion")]
        return (enabled or exact)[0]
    if len(by_name) == 1:
        return by_name[0]
    return None


def _normalize_source(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def canonicalize_quote_source(source: Optional[str]) -> Optional[str]:
    """Map historical SofaScore pipeline channels onto provider ``sofascore``.

    Snapshot rows may still carry ``daily_discovery`` / ``dropping_odds`` / etc.
    Quote identity must use the canonical provider so all SofaScore ticks for a
    choice share one quote and ``current`` is the latest across channels.
    """
    normalized = _normalize_source(source)
    if normalized is None:
        return None
    if normalized in CHANNEL_SOURCES:
        return "sofascore"
    return normalized


def _normalize_side(value: Optional[str]) -> Optional[str]:
    if value is None or str(value).strip() == "":
        return None
    side = str(value).strip().lower()
    if side == "single":
        return None
    return side


def _normalize_level(value: Any) -> tuple[Optional[int], Optional[str]]:
    if value is None or value == "":
        return 0, None
    try:
        level = int(value)
    except (TypeError, ValueError):
        return None, "invalid_side_or_level"
    if level < 0:
        return None, "invalid_side_or_level"
    return level, None


def _feasible_sources_for_candidate(
    mapped: set[str], candidate: "BackfillCandidate"
) -> set[str]:
    """Narrow ``mapped`` bookie sources using hard ``MarketWritePolicy`` facts.

    Only applied to ``snapshot`` candidates. OddsPortal's opening-only policy
    forbids writing snapshots, so a multi-mapped bookie (e.g. historical
    bet365 → oddspapi+oddsportal) can drop OddsPortal when the row is a
    snapshot. ``choice_state`` rows are NOT narrowed here: opening-only is a
    *recent* policy, and pre-policy OddsPortal wrote ``initial``/``current``
    onto ``MarketChoice`` without snapshots — using today's policy to
    attribute those mirrors to Oddspapi would be historically wrong.
    """
    if len(mapped) <= 1:
        return mapped
    if candidate.kind == "snapshot":
        feasible = {
            s
            for s in mapped
            if (
                market_write_policy_for_source(s).persist_opening_snapshots
                or market_write_policy_for_source(s).persist_current_snapshots
            )
        }
        return feasible or mapped
    return mapped


def classify_candidate(
    candidate: BackfillCandidate,
    *,
    bookie_sources: Mapping[int, set[str]],
    exchange_choice_ids: set[int],
    canonical_markets: Mapping[
        tuple[int, int, str, str, Optional[str], bool], Any
    ],
    choices_by_market_name: Mapping[tuple[int, str], Any],
    resolutions: Mapping[int, Mapping[str, Any]] | None = None,
) -> ClassificationDecision:
    """Pure classifier — no Session, no mutation."""
    if candidate.already_linked_quote_id is not None:
        return ClassificationDecision(
            status=ClassificationStatus.RESOLVED,
            reason_code="already_linked",
            candidate=candidate,
            evidence={"quote_id": candidate.already_linked_quote_id},
        )

    resolutions = resolutions or {}
    if candidate.snapshot_id is not None and candidate.snapshot_id in resolutions:
        res = resolutions[candidate.snapshot_id]
        try:
            identity = QuoteIdentity(
                choice_id=int(res["canonical_choice_id"]),
                source=canonicalize_quote_source(res["source"]) or "",
                exchange_side=_normalize_side(res.get("exchange_side")),
                exchange_level=int(res.get("exchange_level") or 0),
            )
        except (KeyError, TypeError, ValueError) as exc:
            return ClassificationDecision(
                status=ClassificationStatus.INVALID,
                reason_code="invalid_resolution",
                candidate=candidate,
                evidence={"error": str(exc), "resolution": dict(res)},
            )
        if identity.source not in PROVIDER_SOURCES:
            return ClassificationDecision(
                status=ClassificationStatus.INVALID,
                reason_code="invalid_resolution_source",
                candidate=candidate,
                evidence={"source": identity.source},
            )
        if identity.exchange_side not in {None, "back", "lay"}:
            return ClassificationDecision(
                status=ClassificationStatus.INVALID,
                reason_code="invalid_side_or_level",
                candidate=candidate,
                evidence={"exchange_side": identity.exchange_side},
            )
        return ClassificationDecision(
            status=ClassificationStatus.RESOLVED,
            reason_code="resolution_file",
            candidate=candidate,
            identity=identity,
            evidence={"resolution": dict(res)},
        )

    legacy_side, legacy_line = parse_legacy_back_lay_choice_group(candidate.choice_group)
    evidence: dict[str, Any] = {
        "raw_source": candidate.raw_source,
        "bookie_id": candidate.bookie_id,
        "legacy_side": legacy_side,
        "legacy_line": legacy_line,
    }

    # OddsPortal-era Back/Lay markets are abandoned: do not rematerialize them
    # into ``source=oddsportal`` quotes. Use ``--purge-legacy-back-lay`` to
    # delete the legacy markets/choices/snapshots. This decision is a note
    # (non-blocking) so campaigns can finish while purge catches up.
    if legacy_side is not None:
        return ClassificationDecision(
            status=ClassificationStatus.INVALID,
            reason_code="legacy_back_lay_abandoned",
            candidate=candidate,
            evidence={
                **evidence,
                "note": (
                    "OddsPortal Back/Lay rematerialization disabled; "
                    "purge with --purge-legacy-back-lay"
                ),
            },
        )

    raw_source = _normalize_source(candidate.raw_source)
    source: Optional[str] = None

    if raw_source:
        if raw_source not in KNOWN_SOURCES:
            return ClassificationDecision(
                status=ClassificationStatus.INVALID,
                reason_code="unknown_source",
                candidate=candidate,
                evidence=evidence,
            )
        # Historical SofaScore pipeline channels → canonical provider source.
        source = canonicalize_quote_source(raw_source)
        if raw_source in CHANNEL_SOURCES:
            evidence["canonicalized_from"] = raw_source
    else:
        # Infer only when snapshot.source / choice_state source is NULL.
        source_candidates: list[str] = []
        if candidate.bookie_id == SOFA_SCORE_BOOKIE_ID:
            source_candidates.append("sofascore")

        mapped = set()
        if candidate.bookie_id is not None:
            mapped = set(bookie_sources.get(int(candidate.bookie_id), set()))
            evidence["mapped_sources"] = sorted(mapped)
            # Unique bookie→provider mapping is trustworthy for *snapshots*.
            # For choice_state on OddsPortal-era bookies it is not: early
            # OddsPortal wrote initial/current without snapshots, and after
            # oddsportal was removed from mappings a lone oddspapi row would
            # silently mis-attribute those mirrors. Other bookies (oddspapi-
            # only) still use the unique mapping.
            if len(mapped) == 1 and not (
                candidate.kind == "choice_state"
                and candidate.bookie_id is not None
                and int(candidate.bookie_id) in ODDSPORTAL_ERA_BOOKIE_IDS
            ):
                source_candidates.append(next(iter(mapped)))

        unique_sources = {s for s in source_candidates if s}
        if len(unique_sources) > 1:
            return ClassificationDecision(
                status=ClassificationStatus.CONFLICT,
                reason_code="contradictory_evidence",
                candidate=candidate,
                evidence={**evidence, "sources": sorted(unique_sources)},
            )
        if len(unique_sources) == 0:
            if len(mapped) > 1 and candidate.kind == "snapshot":
                feasible = _feasible_sources_for_candidate(mapped, candidate)
                if len(feasible) == 1:
                    source = next(iter(feasible))
                    evidence["resolved_by"] = "write_policy_elimination"
                    evidence["eliminated_sources"] = sorted(mapped - feasible)
                else:
                    return ClassificationDecision(
                        status=ClassificationStatus.AMBIGUOUS,
                        reason_code="ambiguous_source",
                        candidate=candidate,
                        evidence=evidence,
                    )
            else:
                reason = (
                    "ambiguous_choice_state"
                    if candidate.kind == "choice_state"
                    else "ambiguous_source"
                )
                if (
                    candidate.kind == "choice_state"
                    and candidate.bookie_id is not None
                    and int(candidate.bookie_id) in ODDSPORTAL_ERA_BOOKIE_IDS
                ):
                    evidence["note"] = (
                        "OddsPortal-era bookie snapless choice_state; "
                        "not attributed via unique oddspapi mapping"
                    )
                return ClassificationDecision(
                    status=ClassificationStatus.AMBIGUOUS,
                    reason_code=reason,
                    candidate=candidate,
                    evidence=evidence,
                )
        else:
            source = next(iter(unique_sources))

    assert source is not None

    side = _normalize_side(candidate.exchange_side)
    if side is not None and side not in {"back", "lay"}:
        return ClassificationDecision(
            status=ClassificationStatus.INVALID,
            reason_code="invalid_side_or_level",
            candidate=candidate,
            evidence={**evidence, "exchange_side": side},
        )

    # Oddspapi side-agnostic historical ticks: map to top-back only with proof.
    if (
        side is None
        and source == "oddspapi"
        and candidate.kind == "snapshot"
        and candidate.choice_id in exchange_choice_ids
    ):
        side = "back"

    level, level_error = _normalize_level(candidate.exchange_level)
    if level_error:
        return ClassificationDecision(
            status=ClassificationStatus.INVALID,
            reason_code=level_error,
            candidate=candidate,
            evidence=evidence,
        )
    assert level is not None

    if side is None:
        level = 0

    identity = QuoteIdentity(
        choice_id=int(candidate.choice_id),
        source=source,
        exchange_side=side,
        exchange_level=level,
    )
    return ClassificationDecision(
        status=ClassificationStatus.RESOLVED,
        reason_code="classified",
        candidate=candidate,
        identity=identity,
        evidence=evidence,
    )


def candidate_from_snapshot(row: SnapshotCandidateRow) -> BackfillCandidate:
    return BackfillCandidate(
        kind="snapshot",
        snapshot_id=row.snapshot_id,
        choice_id=row.choice_id,
        market_id=row.market_id,
        event_id=row.event_id,
        bookie_id=row.bookie_id,
        market_name=row.market_name,
        market_period=row.market_period,
        choice_group=row.choice_group,
        is_live=row.is_live,
        choice_name=row.choice_name,
        raw_source=row.source,
        exchange_side=row.exchange_side,
        exchange_level=row.exchange_level,
        odds_value=row.odds_value,
        collected_at=row.collected_at,
        source_collected_at=row.source_collected_at,
        source_market_id=row.source_market_id,
        source_outcome_id=row.source_outcome_id,
        bookmaker_outcome_id=row.bookmaker_outcome_id,
        main_line=row.main_line,
        source_limit=row.source_limit,
        choice_initial_odds=row.choice_initial_odds,
        choice_current_odds=row.choice_current_odds,
        already_linked_quote_id=row.quote_id,
    )


def candidate_from_choice_state(row: ChoiceStateCandidateRow) -> BackfillCandidate:
    return BackfillCandidate(
        kind="choice_state",
        snapshot_id=None,
        choice_id=row.choice_id,
        market_id=row.market_id,
        event_id=row.event_id,
        bookie_id=row.bookie_id,
        market_name=row.market_name,
        market_period=row.market_period,
        choice_group=row.choice_group,
        is_live=row.is_live,
        choice_name=row.choice_name,
        raw_source=None,
        exchange_side=None,
        exchange_level=None,
        choice_initial_odds=row.initial_odds,
        choice_current_odds=row.current_odds,
    )


def _tick_sort_key(candidate: BackfillCandidate) -> tuple:
    source_ts = candidate.source_collected_at or candidate.collected_at
    return (
        source_ts or datetime.min,
        candidate.collected_at or datetime.min,
        candidate.snapshot_id or 0,
    )


def default_main_line_for_source(source: str, existing_main_line: Optional[bool] = None) -> Optional[bool]:
    """SofaScore (+ channel variants) and OddsPortal ingest only main lines."""
    if existing_main_line is not None:
        return existing_main_line
    if source in ALWAYS_MAIN_LINE_SOURCES:
        return True
    return None


def build_quote_state_candidates(
    decisions: Sequence[ClassificationDecision],
) -> list[QuoteStateCandidate]:
    """Group resolved snapshot decisions into one state candidate per identity."""
    buckets: dict[QuoteIdentity, list[ClassificationDecision]] = defaultdict(list)
    for decision in decisions:
        if decision.status is not ClassificationStatus.RESOLVED:
            continue
        if decision.reason_code == "already_linked":
            continue
        if decision.identity is None:
            continue
        buckets[decision.identity].append(decision)

    states: list[QuoteStateCandidate] = []
    for identity, group in buckets.items():
        snapshots = [d for d in group if d.candidate.kind == "snapshot"]
        choice_states = [d for d in group if d.candidate.kind == "choice_state"]

        current_price = None
        current_captured_at = None
        source_limit = None
        main_line = None
        source_market_id = None
        source_outcome_id = None
        bookmaker_outcome_id = None
        snapshot_ids: list[int] = []

        if snapshots:
            ordered = sorted(snapshots, key=lambda d: _tick_sort_key(d.candidate))
            latest = ordered[-1].candidate
            current_price = latest.odds_value
            current_captured_at = latest.source_collected_at or latest.collected_at
            source_limit = latest.source_limit
            for item in ordered:
                cand = item.candidate
                if cand.snapshot_id is not None:
                    snapshot_ids.append(cand.snapshot_id)
                if main_line is None and cand.main_line is not None:
                    main_line = cand.main_line
                if source_market_id is None and cand.source_market_id:
                    source_market_id = cand.source_market_id
                if source_outcome_id is None and cand.source_outcome_id:
                    source_outcome_id = cand.source_outcome_id
                if bookmaker_outcome_id is None and cand.bookmaker_outcome_id:
                    bookmaker_outcome_id = cand.bookmaker_outcome_id

        initial_price = None
        initial_captured_at = None
        ownership = False
        if choice_states:
            # Ownership already proven by classifier for these rows.
            ownership = True
            first = choice_states[0].candidate
            initial_price = first.choice_initial_odds
            if not snapshots:
                current_price = first.choice_current_odds
        elif snapshots:
            # Prefer choice-level initial only when source ownership is unique
            # (already true for resolved decisions). Do not invent timestamps.
            first = snapshots[0].candidate
            initial_price = first.choice_initial_odds
            ownership = initial_price is not None

        main_line = default_main_line_for_source(identity.source, main_line)

        states.append(
            QuoteStateCandidate(
                identity=identity,
                initial_price=initial_price if ownership else None,
                initial_captured_at=initial_captured_at,
                current_price=current_price,
                current_captured_at=current_captured_at,
                main_line=main_line,
                source_market_id=source_market_id,
                source_outcome_id=source_outcome_id,
                bookmaker_outcome_id=bookmaker_outcome_id,
                source_limit=source_limit,
                snapshot_ids=tuple(snapshot_ids),
                ownership_from_choice_state=bool(choice_states),
            )
        )
    return states


def load_resolution_file(path: Path) -> tuple[dict[int, dict[str, Any]], str]:
    raw = path.read_text(encoding="utf-8")
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    payload = json.loads(raw)
    if int(payload.get("version", 0)) != 1:
        raise ValueError("resolution file version must be 1")
    decisions = payload.get("decisions")
    if not isinstance(decisions, list):
        raise ValueError("resolution file decisions must be a list")
    by_snapshot: dict[int, dict[str, Any]] = {}
    for item in decisions:
        if not isinstance(item, dict):
            raise ValueError("resolution decision must be an object")
        snapshot_id = int(item["snapshot_id"])
        if snapshot_id in by_snapshot and by_snapshot[snapshot_id] != item:
            raise ValueError(f"duplicate conflicting resolution for snapshot_id={snapshot_id}")
        by_snapshot[snapshot_id] = item
    return by_snapshot, digest


def load_checkpoint(path: Path) -> Checkpoint:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return Checkpoint(
        algorithm_version=str(payload["algorithm_version"]),
        run_id=str(payload["run_id"]),
        pass_name=str(payload["pass_name"]),
        event_scope=[int(x) for x in payload["event_scope"]],
        last_snapshot_id=payload.get("last_snapshot_id"),
        last_choice_id=payload.get("last_choice_id"),
        after_event_id=payload.get("after_event_id"),
        filters=dict(payload.get("filters") or {}),
        resolution_sha256=payload.get("resolution_sha256"),
        rows_consumed_total=int(payload.get("rows_consumed_total") or 0),
        events_completed=bool(payload.get("events_completed")),
        updated_at=str(payload.get("updated_at") or ""),
    )


def write_checkpoint_atomic(path: Path, checkpoint: Checkpoint) -> None:
    """Write checkpoint via temp file + replace, with Windows-friendly retries.

    Editors (Cursor/VS Code) often keep ``checkpoint.json`` open and briefly
    deny ``os.replace`` on Windows (WinError 5). Retry and fall back to
    unlink+replace before giving up.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(asdict(checkpoint), indent=2, default=str)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    last_error: Optional[BaseException] = None
    try:
        for attempt in range(8):
            try:
                os.replace(tmp, path)
                return
            except PermissionError as exc:
                last_error = exc
                time.sleep(0.05 * (2**attempt))
                try:
                    if path.exists():
                        path.unlink()
                    os.replace(tmp, path)
                    return
                except OSError as exc2:
                    last_error = exc2
                    time.sleep(0.05 * (2**attempt))
        assert last_error is not None
        raise last_error
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _rejection_row(decision: ClassificationDecision) -> dict[str, Any]:
    cand = decision.candidate
    return {
        "status": decision.status.value,
        "reason_code": decision.reason_code,
        "event_id": cand.event_id,
        "market_id": cand.market_id,
        "choice_id": cand.choice_id,
        "snapshot_id": cand.snapshot_id,
        "evidence": dict(decision.evidence),
    }


def _initial_unavailable_note(state: QuoteStateCandidate) -> dict[str, Any]:
    return {
        "status": "note",
        "reason_code": "initial_odds_unavailable",
        "event_id": None,
        "market_id": None,
        "choice_id": state.identity.choice_id,
        "snapshot_id": state.snapshot_ids[0] if state.snapshot_ids else None,
        "evidence": {
            "source": state.identity.source,
            "exchange_side": state.identity.exchange_side,
            "exchange_level": state.identity.exchange_level,
            "current_odds": state.current_price,
            "current_captured_at": state.current_captured_at,
            "snapshot_ids": list(state.snapshot_ids),
            "ownership_from_choice_state": state.ownership_from_choice_state,
            "detail": (
                "quote planned/upserted with current from latest tick but "
                "initial_odds left NULL (no safe MarketChoice ownership)"
            ),
        },
    }


def _append_ndjson(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), default=str) + "\n")


def _write_rejections_run_header(
    path: Path,
    *,
    run_id: str,
    config: RunConfig,
    append: bool,
) -> None:
    """Append a parseable run boundary (and a blank line when continuing a file)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    leading_blank = append and path.exists() and path.stat().st_size > 0
    header = {
        "status": "run_header",
        "reason_code": "run_boundary",
        "run_id": run_id,
        "event_id": config.event_id,
        "market_id": None,
        "choice_id": None,
        "snapshot_id": None,
        "evidence": {
            "algorithm_version": ALGORITHM_VERSION,
            "dry_run": config.dry_run,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "append": append,
            "filters": {
                "event_id": config.event_id,
                "event_id_min": config.event_id_min,
                "event_id_max": config.event_id_max,
                "after_event_id": config.after_event_id,
                "source": config.source,
                "pass": config.pass_name,
                "max_events": config.max_events,
                "max_rows": config.max_rows,
            },
        },
    }
    with path.open("a", encoding="utf-8") as handle:
        if leading_blank:
            handle.write("\n")
        handle.write(json.dumps(header, default=str) + "\n")


class MarketChoiceQuoteBackfillService:
    """Orchestrates preflight, keyset batches, classification, apply, reports."""

    def __init__(self, session_factory):
        self._session_factory = session_factory

    def run(self, config: RunConfig) -> tuple[int, dict[str, Any]]:
        """Execute backfill. Returns (exit_code, summary_dict)."""
        started = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        resolutions: dict[int, dict[str, Any]] = {}
        resolution_sha: Optional[str] = None
        if config.resolution_file is not None:
            resolutions, resolution_sha = load_resolution_file(config.resolution_file)

        checkpoint: Optional[Checkpoint] = None
        if config.resume_from is not None:
            checkpoint = load_checkpoint(config.resume_from)
            if checkpoint.algorithm_version != ALGORITHM_VERSION:
                return 3, {
                    "error": "incompatible checkpoint algorithm_version",
                    "expected": ALGORITHM_VERSION,
                    "found": checkpoint.algorithm_version,
                }
            if (
                resolution_sha
                and checkpoint.resolution_sha256
                and resolution_sha != checkpoint.resolution_sha256
            ):
                return 3, {"error": "resolution file checksum mismatch vs checkpoint"}

        with self._session_factory() as session:
            preflight_errors = MarketChoiceQuoteBackfillRepository.schema_preflight(session)
            if preflight_errors:
                return 3, {"error": "preflight_failed", "details": preflight_errors}

        event_scope: list[int]
        after_snapshot_id = config.after_snapshot_id
        after_choice_id: Optional[int] = None
        active_pass = (
            "snapshots"
            if config.pass_name == "all"
            else config.pass_name.replace("-", "_")
            if config.pass_name == "choice-states"
            else config.pass_name
        )
        if config.pass_name == "choice-states":
            active_pass = "choice_states"
        elif config.pass_name == "snapshots":
            active_pass = "snapshots"
        else:
            active_pass = "snapshots"

        if checkpoint is not None:
            event_scope = list(checkpoint.event_scope)
            after_snapshot_id = checkpoint.last_snapshot_id
            after_choice_id = checkpoint.last_choice_id
            active_pass = checkpoint.pass_name
            if checkpoint.events_completed:
                # Select next scope after last completed event.
                with self._session_factory() as session:
                    event_scope = MarketChoiceQuoteBackfillRepository.select_event_scope(
                        session,
                        event_id=config.event_id,
                        event_id_min=config.event_id_min,
                        event_id_max=config.event_id_max,
                        after_event_id=checkpoint.event_scope[-1]
                        if checkpoint.event_scope
                        else checkpoint.after_event_id,
                        max_events=config.max_events,
                    )
                after_snapshot_id = None
                after_choice_id = None
                active_pass = (
                    "snapshots"
                    if config.pass_name in {"all", "snapshots"}
                    else "choice_states"
                )
        else:
            with self._session_factory() as session:
                event_scope = MarketChoiceQuoteBackfillRepository.select_event_scope(
                    session,
                    event_id=config.event_id,
                    event_id_min=config.event_id_min,
                    event_id_max=config.event_id_max,
                    after_event_id=config.after_event_id,
                    max_events=config.max_events
                    if config.event_id is None
                    else None,
                )
                if config.purge_oddspapi_null_mainline_lines and config.event_id is None:
                    purge_scope = (
                        MarketChoiceQuoteBackfillRepository.select_purge_event_scope(
                            session,
                            event_id=config.event_id,
                            event_id_min=config.event_id_min,
                            event_id_max=config.event_id_max,
                            after_event_id=config.after_event_id,
                            max_events=config.max_events,
                        )
                    )
                    event_scope = sorted(set(event_scope) | set(purge_scope))
                    if config.max_events is not None:
                        event_scope = event_scope[: int(config.max_events)]
                if config.purge_legacy_back_lay and config.event_id is None:
                    back_lay_scope = (
                        MarketChoiceQuoteBackfillRepository.select_legacy_back_lay_event_scope(
                            session,
                            event_id=config.event_id,
                            event_id_min=config.event_id_min,
                            event_id_max=config.event_id_max,
                            after_event_id=config.after_event_id,
                            max_events=config.max_events,
                        )
                    )
                    event_scope = sorted(set(event_scope) | set(back_lay_scope))
                    if config.max_events is not None:
                        event_scope = event_scope[: int(config.max_events)]
                if config.purge_ambiguous_choice_states and config.event_id is None:
                    choice_state_scope = (
                        MarketChoiceQuoteBackfillRepository.select_ambiguous_choice_state_event_scope(
                            session,
                            bookie_ids=sorted(ODDSPORTAL_ERA_BOOKIE_IDS),
                            event_id=config.event_id,
                            event_id_min=config.event_id_min,
                            event_id_max=config.event_id_max,
                            after_event_id=config.after_event_id,
                            max_events=config.max_events,
                        )
                    )
                    event_scope = sorted(set(event_scope) | set(choice_state_scope))
                    if config.max_events is not None:
                        event_scope = event_scope[: int(config.max_events)]

        summary: dict[str, Any] = {
            "run_id": run_id,
            "algorithm_version": ALGORITHM_VERSION,
            "dry_run": config.dry_run,
            "filters": {
                "event_id": config.event_id,
                "event_id_min": config.event_id_min,
                "event_id_max": config.event_id_max,
                "source": config.source,
                "pass": config.pass_name,
                "purge_oddspapi_null_mainline_lines": (
                    config.purge_oddspapi_null_mainline_lines
                ),
                "purge_legacy_back_lay": config.purge_legacy_back_lay,
                "purge_ambiguous_choice_states": config.purge_ambiguous_choice_states,
            },
            "configured_batch_size": config.batch_size,
            "configured_max_events": config.max_events,
            "configured_max_rows": config.max_rows,
            "events_selected": len(event_scope),
            "event_scope": event_scope,
            "resolution_sha256": resolution_sha,
            "snapshots_scanned": 0,
            "snapshots_linkable": 0,
            "snapshots_linked": 0,
            "choice_states_scanned": 0,
            "quote_buckets_planned": 0,
            "quotes_inserted": 0,
            "quotes_updated": 0,
            "quotes_unchanged": 0,
            "stale_candidates_ignored": 0,
            "metadata_conflicts": 0,
            "canonical_markets_created": 0,
            "canonical_choices_created": 0,
            "ambiguous_source": 0,
            "ambiguous_target": 0,
            "ambiguous_choice_state": 0,
            "contradictory_evidence": 0,
            "invalid_side_or_level": 0,
            "legacy_back_lay_abandoned": 0,
            "initial_odds_unavailable": 0,
            "purge_snapshots_matched": 0,
            "purge_snapshots_deleted": 0,
            "purge_choices_deleted": 0,
            "purge_markets_deleted": 0,
            "purge_legacy_back_lay_markets_deleted": 0,
            "purge_legacy_back_lay_choices_deleted": 0,
            "purge_legacy_back_lay_snapshots_deleted": 0,
            "purge_oddsportal_quotes_deleted": 0,
            "purge_ambiguous_choice_states_deleted": 0,
            "purge_ambiguous_choice_state_markets_deleted": 0,
            "purge_disallowed_bookie_markets_deleted": 0,
            "purge_disallowed_bookie_choices_deleted": 0,
            "purge_disallowed_bookie_snapshots_deleted": 0,
            "purge_disallowed_bookie_quotes_deleted": 0,
            "rows_consumed": 0,
            "stop_reason": "completed_scope",
            "blocking_decisions": 0,
            "notes_written": 0,
        }

        rows_remaining = (
            config.max_rows if config.max_rows is not None else MAX_ROWS_HARD_CAP
        )
        hit_row_limit = False
        hit_interrupt = False
        rejection_path = config.output_rejections
        if rejection_path is not None:
            rejection_path.parent.mkdir(parents=True, exist_ok=True)
            if config.append_rejections and rejection_path.exists():
                # Keep prior campaign / prior-run audit rows.
                _write_rejections_run_header(
                    rejection_path,
                    run_id=run_id,
                    config=config,
                    append=True,
                )
            else:
                rejection_path.write_text("", encoding="utf-8")
                _write_rejections_run_header(
                    rejection_path,
                    run_id=run_id,
                    config=config,
                    append=False,
                )

        # Always keep only SofaScore / bet365 / Betfair / Pinnacle markets.
        # No CLI flag: this is part of the backfill contract (algorithm ≥4b.7).
        with self._session_factory() as session:
            if not config.dry_run:
                self._acquire_lock(session)
            disallowed_purge = (
                MarketChoiceQuoteBackfillRepository.purge_markets_outside_allowed_bookies(
                    session,
                    allowed_bookie_ids=sorted(BACKFILL_ALLOWED_BOOKIE_IDS),
                    dry_run=config.dry_run,
                )
            )
            if not config.dry_run:
                session.commit()
            else:
                session.rollback()
        summary["purge_disallowed_bookie_markets_deleted"] = int(
            disallowed_purge.get("markets_deleted") or 0
        )
        summary["purge_disallowed_bookie_choices_deleted"] = int(
            disallowed_purge.get("choices_deleted") or 0
        )
        summary["purge_disallowed_bookie_snapshots_deleted"] = int(
            disallowed_purge.get("snapshots_deleted") or 0
        )
        summary["purge_disallowed_bookie_quotes_deleted"] = int(
            disallowed_purge.get("quotes_deleted") or 0
        )
        if rejection_path is not None and disallowed_purge.get("markets_matched"):
            _append_ndjson(
                rejection_path,
                [
                    {
                        "status": "note" if config.dry_run else "purged",
                        "reason_code": "purge_disallowed_bookie_markets",
                        "event_id": None,
                        "market_id": None,
                        "choice_id": None,
                        "snapshot_id": None,
                        "evidence": {
                            "dry_run": config.dry_run,
                            "criterion": (
                                "markets.bookie_id NOT IN "
                                f"{sorted(BACKFILL_ALLOWED_BOOKIE_IDS)} "
                                "(or NULL); cascade choices/snapshots/quotes"
                            ),
                            **disallowed_purge,
                        },
                    }
                ],
            )
            summary["notes_written"] += 1

        if config.purge_oddspapi_null_mainline_lines and event_scope:
            with self._session_factory() as session:
                if not config.dry_run:
                    self._acquire_lock(session)
                purge_result = (
                    MarketChoiceQuoteBackfillRepository.purge_oddspapi_null_mainline_line_markets(
                        session,
                        event_ids=event_scope,
                        dry_run=config.dry_run,
                    )
                )
                if not config.dry_run:
                    session.commit()
                else:
                    session.rollback()
            summary["purge_snapshots_matched"] = int(
                purge_result.get("snapshots_matched") or 0
            )
            summary["purge_snapshots_deleted"] = int(
                purge_result.get("snapshots_deleted") or 0
            )
            summary["purge_choices_deleted"] = int(
                purge_result.get("choices_deleted") or 0
            )
            summary["purge_markets_deleted"] = int(
                purge_result.get("markets_deleted") or 0
            )
            if rejection_path is not None and summary["purge_snapshots_matched"]:
                _append_ndjson(
                    rejection_path,
                    [
                        {
                            "status": "note" if config.dry_run else "purged",
                            "reason_code": "purge_oddspapi_null_mainline_line",
                            "event_id": None,
                            "market_id": None,
                            "choice_id": None,
                            "snapshot_id": None,
                            "evidence": {
                                "dry_run": config.dry_run,
                                "criterion": (
                                    "source=oddspapi (or NULL uniquely mapped) "
                                    "+ main_line IS NULL + choice_group IS NOT NULL"
                                ),
                                **purge_result,
                            },
                        }
                    ],
                )
                summary["notes_written"] += 1

        if config.purge_legacy_back_lay and event_scope:
            with self._session_factory() as session:
                if not config.dry_run:
                    self._acquire_lock(session)
                back_lay_purge = (
                    MarketChoiceQuoteBackfillRepository.purge_legacy_back_lay_markets(
                        session,
                        event_ids=event_scope,
                        dry_run=config.dry_run,
                    )
                )
                if not config.dry_run:
                    session.commit()
                else:
                    session.rollback()
            summary["purge_legacy_back_lay_markets_deleted"] = int(
                back_lay_purge.get("markets_deleted") or 0
            )
            summary["purge_legacy_back_lay_choices_deleted"] = int(
                back_lay_purge.get("choices_deleted") or 0
            )
            summary["purge_legacy_back_lay_snapshots_deleted"] = int(
                back_lay_purge.get("snapshots_deleted") or 0
            )
            summary["purge_oddsportal_quotes_deleted"] = int(
                back_lay_purge.get("oddsportal_quotes_deleted") or 0
            )
            if rejection_path is not None and (
                back_lay_purge.get("markets_matched")
                or back_lay_purge.get("oddsportal_quotes_deleted")
            ):
                _append_ndjson(
                    rejection_path,
                    [
                        {
                            "status": "note" if config.dry_run else "purged",
                            "reason_code": "purge_legacy_back_lay",
                            "event_id": None,
                            "market_id": None,
                            "choice_id": None,
                            "snapshot_id": None,
                            "evidence": {
                                "dry_run": config.dry_run,
                                "criterion": (
                                    "markets.choice_group matches Back/Lay "
                                    "(OddsPortal-era) + any source=oddsportal "
                                    "quotes in the same event scope"
                                ),
                                **back_lay_purge,
                            },
                        }
                    ],
                )
                summary["notes_written"] += 1

        if config.purge_ambiguous_choice_states and event_scope:
            with self._session_factory() as session:
                if not config.dry_run:
                    self._acquire_lock(session)
                choice_state_purge = (
                    MarketChoiceQuoteBackfillRepository.purge_ambiguous_choice_states(
                        session,
                        event_ids=event_scope,
                        bookie_ids=sorted(ODDSPORTAL_ERA_BOOKIE_IDS),
                        dry_run=config.dry_run,
                    )
                )
                if not config.dry_run:
                    session.commit()
                else:
                    session.rollback()
            summary["purge_ambiguous_choice_states_deleted"] = int(
                choice_state_purge.get("choices_deleted") or 0
            )
            summary["purge_ambiguous_choice_state_markets_deleted"] = int(
                choice_state_purge.get("markets_deleted") or 0
            )
            if rejection_path is not None and choice_state_purge.get("choices_matched"):
                _append_ndjson(
                    rejection_path,
                    [
                        {
                            "status": "note" if config.dry_run else "purged",
                            "reason_code": "purge_ambiguous_choice_states",
                            "event_id": None,
                            "market_id": None,
                            "choice_id": None,
                            "snapshot_id": None,
                            "evidence": {
                                "dry_run": config.dry_run,
                                "criterion": (
                                    "snapless MarketChoice odds mirrors on "
                                    f"OddsPortal-era bookies {sorted(ODDSPORTAL_ERA_BOOKIE_IDS)} "
                                    "(ambiguous_choice_state cohort)"
                                ),
                                **choice_state_purge,
                            },
                        }
                    ],
                )
                summary["notes_written"] += 1

        if not event_scope:
            summary["stop_reason"] = "completed_scope"
            summary["duration_seconds"] = (
                datetime.now(timezone.utc) - started
            ).total_seconds()
            self._write_outputs(config, summary, [])
            return 0, summary

        passes: list[str]
        if config.pass_name == "snapshots":
            passes = ["snapshots"]
        elif config.pass_name == "choice-states":
            passes = ["choice_states"]
        else:
            passes = ["snapshots", "choice_states"]
            # Resume may already be in choice_states.
            if active_pass == "choice_states":
                passes = ["choice_states"]
            elif active_pass == "snapshots":
                passes = ["snapshots", "choice_states"]

        blocking = 0
        checkpoint_pass_name = passes[0] if passes else "snapshots"
        try:
            for pass_name in passes:
                checkpoint_pass_name = pass_name
                if rows_remaining <= 0:
                    summary["stop_reason"] = "max_rows"
                    hit_row_limit = True
                    break
                if pass_name == "snapshots" and config.pass_name == "choice-states":
                    continue
                if pass_name == "choice_states" and config.pass_name == "snapshots":
                    continue

                cursor_snapshot = after_snapshot_id if pass_name == "snapshots" else None
                cursor_choice = after_choice_id if pass_name == "choice_states" else None

                while rows_remaining > 0:
                    if stop_requested():
                        hit_interrupt = True
                        summary["stop_reason"] = "interrupted"
                        logger.info(
                            "stop requested: skipping further batches "
                            "(last committed batch already checkpointed)"
                        )
                        break
                    batch_limit = min(config.batch_size, rows_remaining)
                    report = self._process_batch(
                        config=config,
                        event_scope=event_scope,
                        pass_name=pass_name,
                        after_snapshot_id=cursor_snapshot,
                        after_choice_id=cursor_choice,
                        limit=batch_limit,
                        resolutions=resolutions,
                        source_filter=None,  # filter after resolved source
                    )
                    if report.rows_scanned == 0:
                        break

                    rows_remaining -= report.rows_scanned
                    if config.max_rows is not None and rows_remaining <= 0:
                        hit_row_limit = True
                    summary["rows_consumed"] += report.rows_scanned
                    if pass_name == "snapshots":
                        summary["snapshots_scanned"] += report.rows_scanned
                        summary["snapshots_linkable"] += report.resolved
                        summary["snapshots_linked"] += report.snapshots_linked
                        cursor_snapshot = report.last_snapshot_id
                    else:
                        summary["choice_states_scanned"] += report.rows_scanned
                        cursor_choice = report.last_choice_id

                    summary["quote_buckets_planned"] += report.quote_buckets
                    summary["quotes_inserted"] += report.quotes_inserted
                    summary["quotes_updated"] += report.quotes_updated
                    summary["quotes_unchanged"] += report.quotes_unchanged
                    summary["stale_candidates_ignored"] += report.stale_candidates_ignored
                    summary["metadata_conflicts"] += report.metadata_conflicts
                    summary["canonical_markets_created"] += report.canonical_markets_created
                    summary["canonical_choices_created"] += report.canonical_choices_created

                    for rejection in report.rejections:
                        reason = rejection.get("reason_code")
                        if reason in summary:
                            summary[reason] = int(summary[reason]) + 1
                        # Abandoned Back/Lay is expected until purge removes the
                        # rows; do not fail the campaign on it.
                        if reason in {
                            "legacy_back_lay_abandoned",
                            "ambiguous_choice_state",
                        }:
                            summary["notes_written"] = int(summary["notes_written"]) + 1
                            if rejection_path is not None:
                                note = dict(rejection)
                                note["status"] = "note"
                                _append_ndjson(rejection_path, [note])
                        else:
                            blocking += 1
                            if rejection_path is not None:
                                _append_ndjson(rejection_path, [rejection])

                    for note in report.notes:
                        reason = note.get("reason_code")
                        if reason in summary:
                            summary[reason] = int(summary[reason]) + 1
                        summary["notes_written"] = int(summary["notes_written"]) + 1
                        if rejection_path is not None:
                            _append_ndjson(rejection_path, [note])

                    if config.checkpoint_file is not None and not config.dry_run:
                        write_checkpoint_atomic(
                            config.checkpoint_file,
                            Checkpoint(
                                algorithm_version=ALGORITHM_VERSION,
                                run_id=run_id,
                                pass_name=pass_name,
                                event_scope=event_scope,
                                last_snapshot_id=cursor_snapshot,
                                last_choice_id=cursor_choice,
                                after_event_id=config.after_event_id,
                                filters=summary["filters"],
                                resolution_sha256=resolution_sha,
                                rows_consumed_total=summary["rows_consumed"],
                                events_completed=False,
                                updated_at=datetime.now(timezone.utc).isoformat(),
                            ),
                        )

                    if report.rows_scanned < batch_limit:
                        break
                    if stop_requested():
                        hit_interrupt = True
                        summary["stop_reason"] = "interrupted"
                        logger.info(
                            "stop requested after batch: checkpoint persisted, exiting chunk"
                        )
                        break

                after_snapshot_id = cursor_snapshot
                after_choice_id = cursor_choice
                if hit_interrupt:
                    break

            if hit_interrupt:
                summary["stop_reason"] = "interrupted"
                # Mid-batch checkpoints already wrote events_completed=False.
                # Re-write once more so updated_at / cursors reflect the stop.
                if config.checkpoint_file is not None and not config.dry_run:
                    write_checkpoint_atomic(
                        config.checkpoint_file,
                        Checkpoint(
                            algorithm_version=ALGORITHM_VERSION,
                            run_id=run_id,
                            pass_name=checkpoint_pass_name,
                            event_scope=event_scope,
                            last_snapshot_id=after_snapshot_id,
                            last_choice_id=after_choice_id,
                            after_event_id=config.after_event_id,
                            filters=summary["filters"],
                            resolution_sha256=resolution_sha,
                            rows_consumed_total=summary["rows_consumed"],
                            events_completed=False,
                            updated_at=datetime.now(timezone.utc).isoformat(),
                        ),
                    )
            elif hit_row_limit:
                summary["stop_reason"] = "max_rows"

            if (
                summary["stop_reason"] == "completed_scope"
                and config.checkpoint_file
                and not config.dry_run
            ):
                write_checkpoint_atomic(
                    config.checkpoint_file,
                    Checkpoint(
                        algorithm_version=ALGORITHM_VERSION,
                        run_id=run_id,
                        pass_name=passes[-1],
                        event_scope=event_scope,
                        last_snapshot_id=after_snapshot_id,
                        last_choice_id=after_choice_id,
                        after_event_id=config.after_event_id,
                        filters=summary["filters"],
                        resolution_sha256=resolution_sha,
                        rows_consumed_total=summary["rows_consumed"],
                        events_completed=True,
                        updated_at=datetime.now(timezone.utc).isoformat(),
                    ),
                )
        except Exception as exc:
            logger.exception("backfill failed")
            summary["stop_reason"] = "error"
            summary["error"] = str(exc)
            summary["duration_seconds"] = (
                datetime.now(timezone.utc) - started
            ).total_seconds()
            self._write_outputs(config, summary, [])
            return 4, summary

        summary["blocking_decisions"] = blocking
        summary["duration_seconds"] = (
            datetime.now(timezone.utc) - started
        ).total_seconds()
        self._write_outputs(config, summary, [])
        if summary.get("stop_reason") == "interrupted":
            return 130, summary
        if blocking:
            return 2, summary
        return 0, summary

    def _seed_missing_canonical_targets(
        self,
        session: Session,
        *,
        decisions: Sequence[ClassificationDecision],
        canonical_markets: dict[tuple[int, int, str, str, Optional[str], bool], Any],
        choices_by_market_name: dict[tuple[int, str], Any],
    ) -> dict[str, int]:
        """Create catalog-aligned markets/choices for legacy Back/Lay remaps.

        Only seeds when the classifier already proved a deterministic identity
        (``plan_create_canonical``) and the market name/period matches
        ``CANONICAL_MARKET_TYPE_SEEDS``. Line markets that require a choice_group
        but have no parsed line stay ambiguous.
        """
        markets_created = 0
        choices_created = 0
        seed_jobs: dict[
            tuple[int, int, str, str, Optional[str], bool], set[str]
        ] = defaultdict(set)

        for decision in decisions:
            if decision.status is not ClassificationStatus.AMBIGUOUS:
                continue
            if not decision.plan_create_canonical:
                continue
            if decision.canonical_market_key is None:
                continue
            key = decision.canonical_market_key
            seed_jobs[key].add(str(decision.candidate.choice_name).strip())

        for key, batch_names in seed_jobs.items():
            event_id, bookie_id, market_name, market_period, line, is_live = key
            catalog = lookup_catalog_seed_for_market(market_name, market_period)
            if catalog is None:
                logger.warning(
                    "skip canonical seed: market not in catalog name=%s period=%s",
                    market_name,
                    market_period,
                )
                continue
            if catalog.get("requires_choice_group") and line is None:
                logger.warning(
                    "skip canonical seed: requires_choice_group but line missing "
                    "name=%s period=%s event_id=%s",
                    market_name,
                    market_period,
                    event_id,
                )
                continue

            sibling_names = (
                MarketChoiceQuoteBackfillRepository.find_legacy_back_lay_choice_names(
                    session,
                    event_id=event_id,
                    bookie_id=bookie_id,
                    market_name=market_name,
                    market_period=market_period,
                    line=line,
                    is_live=is_live,
                )
            )
            choice_names = sorted(
                {n for n in batch_names if n} | {n for n in sibling_names if n}
            )
            if not choice_names:
                continue

            market_group = str(catalog.get("canonical_market_group") or "").strip()
            market, choices, created_market, created_choice_count = (
                MarketChoiceQuoteBackfillRepository.ensure_canonical_market_with_choices(
                    session,
                    event_id=event_id,
                    bookie_id=bookie_id,
                    market_name=market_name,
                    market_group=market_group,
                    market_period=market_period,
                    choice_group=line,
                    is_live=is_live,
                    choice_names=choice_names,
                )
            )
            if created_market:
                markets_created += 1
                logger.info(
                    "seeded canonical market event_id=%s bookie_id=%s name=%s "
                    "period=%s choice_group=%s market_id=%s",
                    event_id,
                    bookie_id,
                    market_name,
                    market_period,
                    line,
                    market.market_id,
                )
            choices_created += created_choice_count
            canonical_markets[key] = market
            for choice in choices:
                choices_by_market_name[
                    (int(market.market_id), str(choice.choice_name).strip().lower())
                ] = choice

        return {
            "markets_created": markets_created,
            "choices_created": choices_created,
        }

    def _process_batch(
        self,
        *,
        config: RunConfig,
        event_scope: Sequence[int],
        pass_name: str,
        after_snapshot_id: Optional[int],
        after_choice_id: Optional[int],
        limit: int,
        resolutions: Mapping[int, Mapping[str, Any]],
        source_filter: Optional[str],
    ) -> BatchReport:
        report = BatchReport(pass_name=pass_name)
        with self._session_factory() as session:
            if not config.dry_run:
                self._acquire_lock(session)

            if pass_name == "snapshots":
                rows = MarketChoiceQuoteBackfillRepository.fetch_pending_snapshots(
                    session,
                    event_ids=event_scope,
                    after_snapshot_id=after_snapshot_id,
                    limit=limit,
                )
                candidates = [candidate_from_snapshot(row) for row in rows]
                if rows:
                    report.last_snapshot_id = rows[-1].snapshot_id
            else:
                rows = MarketChoiceQuoteBackfillRepository.fetch_choice_states_without_snapshots(
                    session,
                    event_ids=event_scope,
                    after_choice_id=after_choice_id,
                    limit=limit,
                )
                candidates = [candidate_from_choice_state(row) for row in rows]
                if rows:
                    report.last_choice_id = rows[-1].choice_id

            report.rows_scanned = len(candidates)
            if not candidates:
                if not config.dry_run:
                    session.rollback()
                return report

            bookie_ids = {c.bookie_id for c in candidates if c.bookie_id is not None}
            bookie_sources = MarketChoiceQuoteBackfillRepository.preload_bookie_sources(
                session, bookie_ids
            )
            choice_ids = {c.choice_id for c in candidates}
            exchange_choice_ids = (
                MarketChoiceQuoteBackfillRepository.preload_exchange_quote_evidence(
                    session, choice_ids
                )
            )

            legacy_lookups = []
            for cand in candidates:
                side, line = parse_legacy_back_lay_choice_group(cand.choice_group)
                if side is not None and cand.bookie_id is not None:
                    legacy_lookups.append(
                        (
                            cand.event_id,
                            cand.bookie_id,
                            str(cand.market_name).strip(),
                            str(cand.market_period or "Full Time").strip() or "Full Time",
                            line,
                            bool(cand.is_live),
                        )
                    )
            canonical_markets = MarketChoiceQuoteBackfillRepository.preload_canonical_markets(
                session, lookups=legacy_lookups
            )
            market_ids = {m.market_id for m in canonical_markets.values()}
            market_ids.update(c.market_id for c in candidates)
            choices_by_market_name = (
                MarketChoiceQuoteBackfillRepository.preload_choices_for_markets(
                    session, market_ids
                )
            )

            decisions: list[ClassificationDecision] = []
            for candidate in candidates:
                decision = classify_candidate(
                    candidate,
                    bookie_sources=bookie_sources,
                    exchange_choice_ids=exchange_choice_ids,
                    canonical_markets=canonical_markets,
                    choices_by_market_name=choices_by_market_name,
                    resolutions=resolutions,
                )
                # Filter by resolved source when requested (scanned but skipped).
                if (
                    config.source
                    and decision.identity is not None
                    and decision.identity.source != config.source.strip().lower()
                ):
                    continue
                if (
                    config.source
                    and decision.status is ClassificationStatus.RESOLVED
                    and decision.identity is None
                ):
                    continue
                decisions.append(decision)

            if not config.dry_run:
                seeded = self._seed_missing_canonical_targets(
                    session,
                    decisions=decisions,
                    canonical_markets=canonical_markets,
                    choices_by_market_name=choices_by_market_name,
                )
                report.canonical_markets_created += seeded["markets_created"]
                report.canonical_choices_created += seeded["choices_created"]
                if seeded["markets_created"] or seeded["choices_created"]:
                    # Re-classify after seed so Back/Lay ticks resolve to the new
                    # canonical choice_ids in this same batch.
                    decisions = [
                        classify_candidate(
                            decision.candidate,
                            bookie_sources=bookie_sources,
                            exchange_choice_ids=exchange_choice_ids,
                            canonical_markets=canonical_markets,
                            choices_by_market_name=choices_by_market_name,
                            resolutions=resolutions,
                        )
                        for decision in decisions
                    ]

            for decision in decisions:
                if decision.status is ClassificationStatus.RESOLVED:
                    if decision.reason_code != "already_linked":
                        report.resolved += 1
                elif decision.status is ClassificationStatus.AMBIGUOUS:
                    report.ambiguous += 1
                    report.rejections.append(_rejection_row(decision))
                elif decision.status is ClassificationStatus.CONFLICT:
                    report.conflicts += 1
                    report.rejections.append(_rejection_row(decision))
                else:
                    report.invalid += 1
                    report.rejections.append(_rejection_row(decision))

            states = build_quote_state_candidates(decisions)
            report.quote_buckets = len(states)
            for state in states:
                if state.initial_price is None and state.current_price is not None:
                    report.notes.append(_initial_unavailable_note(state))

            identities = [
                (
                    s.identity.choice_id,
                    s.identity.source,
                    s.identity.exchange_side,
                    s.identity.exchange_level,
                )
                for s in states
            ]
            quote_index = MarketChoiceQuoteBackfillRepository.preload_quotes(
                session, identities=identities
            )

            link_pairs: list[tuple[int, int]] = []
            for state in states:
                existing = quote_index.get(
                    (
                        state.identity.choice_id,
                        state.identity.source,
                        state.identity.exchange_side,
                        state.identity.exchange_level,
                    )
                )
                candidate_state = QuoteCandidateState(
                    initial_price=state.initial_price,
                    initial_captured_at=state.initial_captured_at,
                    current_price=state.current_price,
                    current_captured_at=state.current_captured_at,
                    main_line=state.main_line,
                    source_market_id=state.source_market_id,
                    source_outcome_id=state.source_outcome_id,
                    bookmaker_outcome_id=state.bookmaker_outcome_id,
                    source_limit=state.source_limit,
                )
                decision = decide_quote_merge(
                    existing=existing_state_from_quote(existing),
                    candidate=candidate_state,
                    mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
                )
                report.stale_candidates_ignored += len(decision.stale_fields)
                report.metadata_conflicts += sum(
                    1 for c in decision.conflicts if c.startswith("metadata_")
                )

                if config.dry_run:
                    if existing is None and (
                        state.initial_price is not None or state.current_price is not None
                    ):
                        report.quotes_inserted += 1
                    elif decision.has_mutations:
                        report.quotes_updated += 1
                    else:
                        report.quotes_unchanged += 1
                    report.snapshots_linked += len(state.snapshot_ids)
                    continue

                upsert_result = MarketChoiceQuoteWriter.upsert(
                    session,
                    quote_index=quote_index,
                    choice_id=state.identity.choice_id,
                    source=state.identity.source,
                    exchange_side=state.identity.exchange_side,
                    exchange_level=state.identity.exchange_level,
                    initial_price=state.initial_price,
                    initial_captured_at=state.initial_captured_at,
                    current_price=state.current_price,
                    current_captured_at=state.current_captured_at,
                    main_line=state.main_line,
                    source_market_id=state.source_market_id,
                    source_outcome_id=state.source_outcome_id,
                    bookmaker_outcome_id=state.bookmaker_outcome_id,
                    source_limit=state.source_limit,
                    mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
                )
                if upsert_result is None:
                    report.quotes_unchanged += 1
                    continue
                if upsert_result.decision.is_create:
                    report.quotes_inserted += 1
                elif upsert_result.decision.has_mutations:
                    report.quotes_updated += 1
                else:
                    report.quotes_unchanged += 1

            if not config.dry_run:
                session.flush()
                for state in states:
                    key = (
                        state.identity.choice_id,
                        state.identity.source,
                        state.identity.exchange_side,
                        state.identity.exchange_level,
                    )
                    quote = quote_index.get(key)
                    if quote is None or quote.quote_id is None:
                        continue
                    for snapshot_id in state.snapshot_ids:
                        link_pairs.append((snapshot_id, int(quote.quote_id)))
                linked = MarketChoiceQuoteBackfillRepository.bulk_link_snapshots(
                    session, link_pairs
                )
                report.snapshots_linked = linked
                session.commit()
            else:
                session.rollback()

        return report

    @staticmethod
    def _acquire_lock(session: Session) -> None:
        bind = session.get_bind()
        if bind is not None and bind.dialect.name == "postgresql":
            locked = session.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": ADVISORY_LOCK_KEY},
            ).scalar()
            if not locked:
                raise RuntimeError("could not acquire backfill advisory lock")

    @staticmethod
    def _write_outputs(
        config: RunConfig, summary: dict[str, Any], _unused: list
    ) -> None:
        if config.output_json is not None:
            config.output_json.parent.mkdir(parents=True, exist_ok=True)
            config.output_json.write_text(
                json.dumps(summary, indent=2, default=str), encoding="utf-8"
            )


__all__ = [
    "ALGORITHM_VERSION",
    "BackfillCandidate",
    "BatchReport",
    "Checkpoint",
    "ClassificationDecision",
    "ClassificationStatus",
    "MarketChoiceQuoteBackfillService",
    "QuoteIdentity",
    "QuoteStateCandidate",
    "RunConfig",
    "canonicalize_quote_source",
    "classify_candidate",
    "clear_stop",
    "load_checkpoint",
    "load_resolution_file",
    "lookup_catalog_seed_for_market",
    "parse_legacy_back_lay_choice_group",
    "request_stop",
    "stop_requested",
    "write_checkpoint_atomic",
]
