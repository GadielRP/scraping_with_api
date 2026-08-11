"""Phase 4b MarketChoiceQuote backfill: contracts, classifier, orchestrator.

Dry-run and commit share the same classification and merge policy. Commit mode
calls ``MarketChoiceQuoteWriter`` once per quote identity bucket and bulk-links
snapshots; dry-run evaluates the same policy without DML.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
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

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "4b.1"
# Provider sources (canonical) + historical SofaScore *pipeline channel* labels
# that were written into snapshot.source. Channel labels are kept verbatim on
# quotes — never rewritten to "sofascore".
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


def _normalize_source(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


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
                source=_normalize_source(res["source"]) or "",
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
        if identity.source not in KNOWN_SOURCES:
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
        # Keep channel labels verbatim (daily_discovery, dropping_odds, …).
        # Do not rewrite them to "sofascore".
        source = raw_source
        # Back/Lay legacy markets are OddsPortal; an explicit non-oddsportal
        # *provider* label on those rows is contradictory. Channel labels are
        # left as-is even if odd (no silent rewrite).
        if (
            legacy_side is not None
            and raw_source in PROVIDER_SOURCES
            and raw_source != "oddsportal"
        ):
            return ClassificationDecision(
                status=ClassificationStatus.CONFLICT,
                reason_code="contradictory_evidence",
                candidate=candidate,
                evidence={
                    **evidence,
                    "sources": sorted({raw_source, "oddsportal"}),
                },
            )
    else:
        # Infer only when snapshot.source is NULL.
        source_candidates: list[str] = []
        if legacy_side is not None:
            source_candidates.append("oddsportal")
        if candidate.bookie_id == SOFA_SCORE_BOOKIE_ID:
            source_candidates.append("sofascore")

        mapped = set()
        if candidate.bookie_id is not None:
            mapped = set(bookie_sources.get(int(candidate.bookie_id), set()))
            evidence["mapped_sources"] = sorted(mapped)
            if len(mapped) == 1:
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
            if len(mapped) > 1:
                return ClassificationDecision(
                    status=ClassificationStatus.AMBIGUOUS,
                    reason_code="ambiguous_source",
                    candidate=candidate,
                    evidence=evidence,
                )
            reason = (
                "ambiguous_choice_state"
                if candidate.kind == "choice_state"
                else "ambiguous_source"
            )
            return ClassificationDecision(
                status=ClassificationStatus.AMBIGUOUS,
                reason_code=reason,
                candidate=candidate,
                evidence=evidence,
            )
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

    if side is None and legacy_side is not None:
        side = legacy_side
    elif side is not None and legacy_side is not None and side != legacy_side:
        return ClassificationDecision(
            status=ClassificationStatus.CONFLICT,
            reason_code="contradictory_evidence",
            candidate=candidate,
            evidence={
                **evidence,
                "snapshot_side": side,
                "legacy_side": legacy_side,
            },
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

    canonical_choice_id = candidate.choice_id
    plan_create = False
    canonical_market_key = None

    if legacy_side is not None:
        if candidate.bookie_id is None:
            return ClassificationDecision(
                status=ClassificationStatus.AMBIGUOUS,
                reason_code="ambiguous_target",
                candidate=candidate,
                evidence=evidence,
            )
        market_period = str(candidate.market_period or "Full Time").strip() or "Full Time"
        canonical_market_key = (
            int(candidate.event_id),
            int(candidate.bookie_id),
            str(candidate.market_name).strip(),
            market_period,
            legacy_line,
            bool(candidate.is_live),
        )
        target_market = canonical_markets.get(canonical_market_key)
        if target_market is None:
            # Deterministic identity pieces present — may plan creation in commit.
            plan_create = True
            return ClassificationDecision(
                status=ClassificationStatus.AMBIGUOUS,
                reason_code="ambiguous_target",
                candidate=candidate,
                evidence={
                    **evidence,
                    "canonical_market_key": list(canonical_market_key),
                    "note": "canonical market missing; creation requires resolution or prior seed",
                },
                plan_create_canonical=plan_create,
                canonical_market_key=canonical_market_key,
            )
        target_market_id = int(target_market.market_id)
        choice_key = (target_market_id, str(candidate.choice_name).strip().lower())
        target_choice = choices_by_market_name.get(choice_key)
        if target_choice is None:
            return ClassificationDecision(
                status=ClassificationStatus.AMBIGUOUS,
                reason_code="ambiguous_target",
                candidate=candidate,
                evidence={
                    **evidence,
                    "target_market_id": target_market_id,
                    "choice_name": candidate.choice_name,
                },
                canonical_market_key=canonical_market_key,
            )
        canonical_choice_id = int(target_choice.choice_id)

    identity = QuoteIdentity(
        choice_id=canonical_choice_id,
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
        plan_create_canonical=plan_create,
        canonical_market_key=canonical_market_key,
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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(asdict(checkpoint), indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


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
            "ambiguous_source": 0,
            "ambiguous_target": 0,
            "ambiguous_choice_state": 0,
            "contradictory_evidence": 0,
            "invalid_side_or_level": 0,
            "initial_odds_unavailable": 0,
            "purge_snapshots_matched": 0,
            "purge_snapshots_deleted": 0,
            "purge_choices_deleted": 0,
            "purge_markets_deleted": 0,
            "rows_consumed": 0,
            "stop_reason": "completed_scope",
            "blocking_decisions": 0,
            "notes_written": 0,
        }

        rows_remaining = (
            config.max_rows if config.max_rows is not None else MAX_ROWS_HARD_CAP
        )
        hit_row_limit = False
        rejection_path = config.output_rejections
        if rejection_path is not None:
            rejection_path.parent.mkdir(parents=True, exist_ok=True)
            if config.append_rejections and rejection_path.exists():
                # Keep prior campaign audit rows (multi-invocation / resume).
                pass
            else:
                rejection_path.write_text("", encoding="utf-8")

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
        try:
            for pass_name in passes:
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

                    for rejection in report.rejections:
                        reason = rejection.get("reason_code")
                        if reason in summary:
                            summary[reason] = int(summary[reason]) + 1
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

                after_snapshot_id = cursor_snapshot
                after_choice_id = cursor_choice

            if hit_row_limit:
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
        if blocking:
            return 2, summary
        return 0, summary

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
    "classify_candidate",
    "load_checkpoint",
    "load_resolution_file",
    "parse_legacy_back_lay_choice_group",
    "write_checkpoint_atomic",
]
