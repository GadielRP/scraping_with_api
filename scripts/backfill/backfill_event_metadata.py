#!/usr/bin/env python3
"""
Backfill Event Metadata Script

Progressive backfill of normalized metadata for historical events:
  - participants (home/away)
  - competitions
  - seasons
  - round / gender / country
  - optionally: results

Uses the current architecture:
  SofaScore /event/{id}
  -> api_client.get_event_information()
  -> EventRepository.upsert_event()
  -> ResultRepository.upsert_result()  (optional)
  -> checkpoint

Usage examples:
    # Dry-run with 10 events
    python scripts/backfill/backfill_event_metadata.py --test --limit 10

    # Missing metadata only (safe default)
    python scripts/backfill/backfill_event_metadata.py --missing-only --batch-size 100

    # Missing metadata + missing results
    python scripts/backfill/backfill_event_metadata.py --missing-only --refresh-results --missing-results-only --batch-size 100

    # Date range, missing only
    python scripts/backfill/backfill_event_metadata.py --from-date 2025-10-01 --to-date 2025-12-31 --missing-only

    # Force all + results
    python scripts/backfill/backfill_event_metadata.py --force --refresh-results --sleep 1
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import and_, or_, cast, Date

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories import EventRepository, ResultRepository
from modules.sofascore import api_client
from modules.sofascore.exceptions import (
    SofaScoreNotFoundException,
    SofaScoreRateLimitException,
)
from scripts.sport_seasons_processing import season_to_process

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "backfill_event_metadata.log")

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("backfill_event_metadata")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MIN_EVENT_ID = 269
MAX_CONSECUTIVE_EMPTY = 10
IGNORED_SEASON_IDS = [s["season_id"] for s in season_to_process if "season_id" in s]

DEFAULT_STATE_FILE = os.path.join("data", "backfill_event_metadata_state.json")
DEFAULT_BATCH_SIZE = 100
DEFAULT_SLEEP = 1.0
MAX_CONSECUTIVE_EMPTY = 3


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class BackfillEventCandidate:
    """Lightweight representation of an event row — no ORM lazy-load risk."""
    id: int
    slug: str
    start_time_utc: datetime
    sport: str
    home_participant_id: Optional[int]
    away_participant_id: Optional[int]
    competition_id: Optional[int]
    season_id: Optional[int]
    round: Optional[str]


@dataclass
class BackfillEventResult:
    """Outcome of processing a single event."""
    status: str  # "updated" | "skipped" | "not_found" | "failed" | "empty_response" | "dry_run"
    metadata_updated: bool = False
    result_updated: bool = False
    detail: str = ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill normalised metadata for historical events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Process only events with incomplete normalisation (default if neither --missing-only nor --force).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Process all events regardless of normalisation state.",
    )
    parser.add_argument(
        "--refresh-results",
        action="store_true",
        help="Also upsert results from the same /event/{id} response.",
    )
    parser.add_argument(
        "--missing-results-only",
        action="store_true",
        help="With --refresh-results: skip events that already have a complete result.",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        help="Filter events from this date (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--to-date",
        type=str,
        default=None,
        help="Filter events until this date (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap total events processed in this run.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Events per DB fetch chunk (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Dry-run — fetch from API but do NOT write to DB.",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default=DEFAULT_STATE_FILE,
        help=f"Path to the progress state file (default: {DEFAULT_STATE_FILE}).",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignore / delete previous progress and start from the beginning.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help=f"Pause (seconds) between API calls (default: {DEFAULT_SLEEP}).",
    )

    args = parser.parse_args()

    if args.missing_results_only and not args.refresh_results:
        parser.error("--missing-results-only requires --refresh-results")

    if args.missing_only and args.force:
        parser.error("--missing-only and --force are mutually exclusive")

    # Safe default: if neither flag is set -> missing-only
    if not args.missing_only and not args.force:
        args.missing_only = True

    # Parse date strings
    if args.from_date:
        try:
            args.from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"Invalid --from-date format: {args.from_date}. Use YYYY-MM-DD.")
    if args.to_date:
        try:
            args.to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()
        except ValueError:
            parser.error(f"Invalid --to-date format: {args.to_date}. Use YYYY-MM-DD.")

    return args


# ---------------------------------------------------------------------------
# State / checkpoint
# ---------------------------------------------------------------------------
def current_mode(args: argparse.Namespace) -> Dict:
    return {
        "missing_only": args.missing_only,
        "force": args.force,
        "refresh_results": args.refresh_results,
        "missing_results_only": args.missing_results_only,
        "from_date": str(args.from_date) if args.from_date else None,
        "to_date": str(args.to_date) if args.to_date else None,
    }


def validate_state_mode_or_exit(state: Dict, args: argparse.Namespace) -> None:
    if args.test or args.reset_state or not state:
        return
    state_mode = state.get("mode")
    if state_mode and state_mode != current_mode(args):
        logger.error("❌ State file was created with different arguments.")
        logger.error("State mode: %s", state_mode)
        logger.error("Current mode: %s", current_mode(args))
        logger.error("Use --reset-state to start fresh, or specify a different --state-file.")
        sys.exit(1)


def load_state(state_file: str) -> Dict:
    """Load progress state from JSON file."""
    try:
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as fh:
                state = json.load(fh)
                logger.info("📂 Loaded state: date=%s, event_id=%s", state.get("last_date"), state.get("last_event_id"))
                return state
    except Exception as exc:
        logger.warning("⚠️  Error loading state file: %s", exc)
    return {}


def save_state(
    state_file: str,
    processed_date: date,
    last_event_id: int,
    last_start_time_utc: datetime,
    args: argparse.Namespace,
) -> None:
    """Persist checkpoint after each successful event."""
    try:
        os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)
        state = {
            "last_date": processed_date.strftime("%Y-%m-%d"),
            "last_event_id": last_event_id,
            "last_start_time_utc": last_start_time_utc.isoformat(),
            "updated_at": datetime.now().isoformat(),
            "mode": current_mode(args),
        }
        with open(state_file, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2)
        logger.debug("💾 Checkpoint saved: date=%s, event_id=%s", state["last_date"], last_event_id)
    except Exception as exc:
        logger.error("❌ Error saving state: %s", exc)


def reset_state(state_file: str) -> None:
    """Delete the state file to start fresh."""
    try:
        if os.path.exists(state_file):
            os.remove(state_file)
            logger.info("🗑️  State file deleted: %s", state_file)
    except Exception as exc:
        logger.warning("⚠️  Could not delete state file: %s", exc)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def build_missing_filter():
    """Return SQLAlchemy OR filter for events with incomplete normalisation."""
    return or_(
        Event.home_participant_id.is_(None),
        Event.away_participant_id.is_(None),
        Event.competition_id.is_(None),
        Event.season_id.is_(None),
        Event.round.is_(None),
    )


def get_candidate_dates(
    args: argparse.Namespace,
    resume_date: Optional[date] = None,
) -> List[date]:
    """Return distinct event dates matching the current filters, ordered ASC."""
    try:
        with db_manager.get_session() as session:
            query = session.query(
                cast(Event.start_time_utc, Date).label("event_date"),
            ).filter(Event.id > MIN_EVENT_ID)

            # Date bounds
            if args.from_date:
                query = query.filter(Event.start_time_utc >= datetime.combine(args.from_date, datetime.min.time()))
            if args.to_date:
                query = query.filter(Event.start_time_utc < datetime.combine(args.to_date + timedelta(days=1), datetime.min.time()))

            # Missing-only filter
            if args.missing_only:
                query = query.filter(build_missing_filter())

            # Ignore specified seasons
            if IGNORED_SEASON_IDS:
                query = query.filter(
                    or_(Event.season_id.is_(None), Event.season_id.notin_(IGNORED_SEASON_IDS))
                )

            # Resume
            if resume_date:
                query = query.filter(cast(Event.start_time_utc, Date) >= resume_date)

            dates = query.distinct().order_by("event_date").all()
            return [row.event_date for row in dates]
    except Exception as exc:
        logger.error("❌ Error fetching candidate dates: %s", exc)
        return []


def get_events_for_date(
    target_date: date,
    args: argparse.Namespace,
    resume_after: Optional[Tuple[datetime, int]] = None,
    limit_remaining: Optional[int] = None,
) -> List[BackfillEventCandidate]:
    """Load BackfillEventCandidate rows for a single date."""
    try:
        with db_manager.get_session() as session:
            day_start = datetime.combine(target_date, datetime.min.time())
            day_end = day_start + timedelta(days=1)

            query = session.query(
                Event.id,
                Event.slug,
                Event.start_time_utc,
                Event.sport,
                Event.home_participant_id,
                Event.away_participant_id,
                Event.competition_id,
                Event.season_id,
                Event.round,
            ).filter(
                and_(
                    Event.id > MIN_EVENT_ID,
                    Event.start_time_utc >= day_start,
                    Event.start_time_utc < day_end,
                )
            )

            if args.missing_only:
                query = query.filter(build_missing_filter())

            # Ignore specified seasons
            if IGNORED_SEASON_IDS:
                query = query.filter(
                    or_(Event.season_id.is_(None), Event.season_id.notin_(IGNORED_SEASON_IDS))
                )

            # Fine-grained resume within the same date
            if resume_after:
                last_ts, last_eid = resume_after
                query = query.filter(
                    or_(
                        Event.start_time_utc > last_ts,
                        and_(Event.start_time_utc == last_ts, Event.id > last_eid),
                    )
                )

            query = query.order_by(Event.start_time_utc, Event.id)

            if limit_remaining is not None:
                query = query.limit(limit_remaining)

            rows = query.all()
            return [
                BackfillEventCandidate(
                    id=r.id,
                    slug=r.slug,
                    start_time_utc=r.start_time_utc,
                    sport=r.sport,
                    home_participant_id=r.home_participant_id,
                    away_participant_id=r.away_participant_id,
                    competition_id=r.competition_id,
                    season_id=r.season_id,
                    round=r.round,
                )
                for r in rows
            ]
    except Exception as exc:
        logger.error("❌ Error fetching events for date %s: %s", target_date, exc)
        return []


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def fetch_event_response_strict(event_id: int) -> Optional[Dict]:
    """
    Fetch /event/{event_id} using _make_request so that
    SofaScoreRateLimitException and SofaScoreNotFoundException propagate.
    """
    try:
        return api_client._make_request(f"/event/{event_id}")
    except (SofaScoreRateLimitException, SofaScoreNotFoundException):
        raise  # caller must handle
    except Exception as exc:
        logger.error("Unexpected error fetching /event/%s: %s", event_id, exc)
        return None


def extract_result_from_response(response: Dict) -> Optional[Dict]:
    """Extract result data from a /event/{id} response without an extra HTTP call."""
    try:
        return api_client.extract_results_from_response(response)
    except Exception as exc:
        logger.warning("Error extracting result from response: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Per-event processing
# ---------------------------------------------------------------------------
def process_event(
    candidate: BackfillEventCandidate,
    args: argparse.Namespace,
    dry_run: bool,
) -> BackfillEventResult:
    """
    Fetch, normalise and persist metadata (and optionally results) for one event.

    Raises:
        SofaScoreRateLimitException — caller must stop and save checkpoint.
        SofaScoreNotFoundException — caller treats as 404 skip.
    """
    # 1. Fetch /event/{id}
    response = fetch_event_response_strict(candidate.id)
    if not response:
        return BackfillEventResult(status="empty_response", detail="No response from API")

    # 2. Validate presence of event payload
    if "event" not in response:
        return BackfillEventResult(status="empty_response", detail="Response missing 'event' key")

    # 3. Normalise
    event_data = api_client.get_event_information(
        response["event"],
        discovery_source="backfill_event_metadata",
    )
    if not event_data:
        return BackfillEventResult(status="failed", detail="Normaliser returned empty data")

    event_payload = event_data.get("event", event_data)
    if not event_payload or not event_payload.get("id"):
        return BackfillEventResult(status="failed", detail="Normalised payload missing id")

    if int(event_payload["id"]) != candidate.id:
        return BackfillEventResult(
            status="failed",
            detail=f"ID mismatch: expected {candidate.id}, got {event_payload['id']}",
        )

    # 4. Build before/after summary for logging
    home_p = event_data.get("home_participant", {})
    away_p = event_data.get("away_participant", {})
    comp_ref = event_data.get("competition_ref", {})
    summary_lines = [
        f"  home: id={home_p.get('source_participant_id')} name={home_p.get('name')}",
        f"  away: id={away_p.get('source_participant_id')} name={away_p.get('name')}",
        f"  competition: tid={comp_ref.get('source_tournament_id')} display={comp_ref.get('display_name')} canonical={comp_ref.get('canonical_name')}",
        f"  season_id={event_payload.get('season_id')} round={event_payload.get('round')}",
        f"  gender={event_payload.get('gender')} country={event_payload.get('country')}",
    ]

    # 5. Dry-run
    if dry_run:
        logger.info(
            "🔍 [DRY-RUN] Event %s — would update metadata:\n%s",
            candidate.id,
            "\n".join(summary_lines),
        )

        would_update_result = False
        # Also preview result if requested
        if args.refresh_results:
            result_data = extract_result_from_response(response)
            if result_data and not result_data.get("_canceled"):
                should_upsert = True
                if args.missing_results_only:
                    existing = ResultRepository.get_result_by_event_id(candidate.id)
                    if existing and existing.home_score is not None and existing.away_score is not None:
                        should_upsert = False
                
                if should_upsert:
                    would_update_result = True
                    logger.info(
                        "🔍 [DRY-RUN] Event %s — would update result: %s-%s winner=%s",
                        candidate.id,
                        result_data.get("home_score"),
                        result_data.get("away_score"),
                        result_data.get("winner"),
                    )
            elif result_data and result_data.get("_canceled"):
                logger.info("🔍 [DRY-RUN] Event %s — event is canceled, result skipped", candidate.id)
            else:
                logger.info("🔍 [DRY-RUN] Event %s — no result available (event may not be finished)", candidate.id)

        return BackfillEventResult(status="dry_run", metadata_updated=True, result_updated=would_update_result, detail="Dry-run")

    # 6. Persist metadata
    upserted = EventRepository.upsert_event(event_data)
    if not upserted:
        return BackfillEventResult(status="failed", detail="EventRepository.upsert_event returned None")

    result_obj = BackfillEventResult(status="updated", metadata_updated=True)
    logger.info("✅ Event %s metadata updated", candidate.id)

    # 7. Results (optional)
    if args.refresh_results:
        result_data = extract_result_from_response(response)
        if result_data and result_data.get("_canceled"):
            logger.info("⏭️  Event %s is canceled — skipping result (not deleting)", candidate.id)
        elif result_data:
            should_upsert_result = True
            if args.missing_results_only:
                existing = ResultRepository.get_result_by_event_id(candidate.id)
                if existing and existing.home_score is not None and existing.away_score is not None:
                    should_upsert_result = False
                    logger.debug("⏭️  Event %s already has complete result — skipped", candidate.id)

            if should_upsert_result:
                res = ResultRepository.upsert_result(candidate.id, result_data)
                if res:
                    result_obj.result_updated = True
                    logger.info(
                        "✅ Event %s result updated: %s-%s winner=%s",
                        candidate.id,
                        result_data.get("home_score"),
                        result_data.get("away_score"),
                        result_data.get("winner"),
                    )
                else:
                    logger.warning("⚠️  Event %s result upsert failed", candidate.id)
        else:
            logger.debug("Event %s — no result data (event may not be finished)", candidate.id)

    return result_obj


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run_backfill(args: argparse.Namespace) -> None:
    """Orchestrate the full backfill run."""
    dry_run = args.test
    state_file = args.state_file

    # Stats
    stats: Dict[str, int] = {
        "events_seen": 0,
        "metadata_updated": 0,
        "results_updated": 0,
        "skipped_already_complete": 0,
        "skipped_not_found": 0,
        "failed": 0,
        "empty_response": 0,
        "rate_limited": 0,
        "dry_run_metadata_would_update": 0,
        "dry_run_results_would_update": 0,
    }

    # ── Banner ────────────────────────────────────────────────────────────
    logger.info("=" * 80)
    logger.info("BACKFILL EVENT METADATA")
    if dry_run:
        logger.info("🔍 TEST MODE — no changes will be saved to database")
    logger.info("=" * 80)
    logger.info("  missing-only : %s", args.missing_only)
    logger.info("  force        : %s", args.force)
    logger.info("  refresh-results      : %s", args.refresh_results)
    logger.info("  missing-results-only : %s", args.missing_results_only)
    logger.info("  from-date    : %s", args.from_date or "(none)")
    logger.info("  to-date      : %s", args.to_date or "(none)")
    logger.info("  limit        : %s", args.limit or "(none)")
    logger.info("  batch-size   : %s", args.batch_size)
    logger.info("  sleep        : %ss", args.sleep)
    logger.info("  state-file   : %s", state_file)
    logger.info("  MIN_EVENT_ID : %s", MIN_EVENT_ID)
    logger.info("=" * 80)

    # ── DB connection ─────────────────────────────────────────────────────
    if not db_manager.test_connection():
        logger.error("❌ Database connection failed. Exiting.")
        sys.exit(1)
    logger.info("✅ Database connection OK")

    # ── State ─────────────────────────────────────────────────────────────
    if args.reset_state:
        reset_state(state_file)

    state = {} if args.reset_state else load_state(state_file)
    validate_state_mode_or_exit(state, args)

    resume_date: Optional[date] = None
    resume_after: Optional[Tuple[datetime, int]] = None

    if state and not dry_run:
        last_date_str = state.get("last_date")
        last_event_id = state.get("last_event_id")
        last_ts_str = state.get("last_start_time_utc")
        if last_date_str and last_event_id:
            resume_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            if last_ts_str:
                try:
                    last_ts = datetime.fromisoformat(last_ts_str)
                except ValueError:
                    last_ts = datetime.combine(resume_date, datetime.min.time())
            else:
                last_ts = datetime.combine(resume_date, datetime.min.time())
            resume_after = (last_ts, last_event_id)
            logger.info(
                "📂 Resuming from date=%s, after event_id=%s (start_time=%s)",
                resume_date,
                last_event_id,
                last_ts.isoformat(),
            )

    # ── Candidate dates ───────────────────────────────────────────────────
    logger.info("🔍 Querying candidate dates…")
    candidate_dates = get_candidate_dates(args, resume_date=resume_date)
    if not candidate_dates:
        logger.info("✅ No dates found to process. Nothing to do.")
        sys.exit(0)
    logger.info("📅 Found %d candidate date(s): %s → %s", len(candidate_dates), candidate_dates[0], candidate_dates[-1])

    # ── Process ───────────────────────────────────────────────────────────
    total_limit = args.limit
    events_remaining = total_limit
    consecutive_empty = 0
    last_successful_checkpoint: Optional[Tuple[date, int, datetime]] = None

    for date_idx, current_date in enumerate(candidate_dates, 1):
        # Determine resume_after for the first date only
        date_resume_after = None
        if date_idx == 1 and resume_after and resume_date == current_date:
            date_resume_after = resume_after

        date_stats: Dict[str, int] = {k: 0 for k in stats}
        first_chunk = True

        while True:
            limit_for_chunk = args.batch_size
            if events_remaining is not None:
                limit_for_chunk = min(limit_for_chunk, events_remaining)
                if limit_for_chunk <= 0:
                    break

            # Fetch events
            events = get_events_for_date(
                current_date,
                args,
                resume_after=date_resume_after,
                limit_remaining=limit_for_chunk,
            )

            if not events:
                if first_chunk:
                    logger.info("📅 Date %d/%d: %s — no events to process", date_idx, len(candidate_dates), current_date)
                break
            
            first_chunk = False
            logger.info(
                "📅 Date %d/%d: %s — processing chunk of %d event(s)",
                date_idx,
                len(candidate_dates),
                current_date,
                len(events),
            )

            for ev_idx, candidate in enumerate(events, 1):
                stats["events_seen"] += 1
                date_stats["events_seen"] += 1
                date_resume_after = (candidate.start_time_utc, candidate.id)

                # Progress log
                if ev_idx == 1 or ev_idx % 10 == 0 or ev_idx == len(events):
                    logger.info(
                        "  [%s] Event %d/%d (id=%d, %s)",
                        current_date,
                        ev_idx,
                        len(events),
                        candidate.id,
                        candidate.sport,
                    )

                try:
                    result = process_event(candidate, args, dry_run)
                except SofaScoreRateLimitException as exc:
                    # Save checkpoint of *last successful* event
                    stats["rate_limited"] += 1
                    logger.error(
                        "\n❌ RATE LIMIT on event %d: %s\n"
                        "   Last successful checkpoint saved. Re-run the same command to resume.",
                        candidate.id,
                        exc,
                    )
                    if last_successful_checkpoint and not dry_run:
                        cp_date, cp_eid, cp_ts = last_successful_checkpoint
                        save_state(state_file, cp_date, cp_eid, cp_ts, args)
                    _print_stats(stats, "INTERRUPTED — Rate Limited")
                    sys.exit(1)
                except SofaScoreNotFoundException:
                    # 404 — skip but save checkpoint so we don't block progress
                    stats["skipped_not_found"] += 1
                    date_stats["skipped_not_found"] += 1
                    logger.warning("⚠️  Event %d returned 404 — skipped (not deleting)", candidate.id)
                    if not dry_run:
                        save_state(state_file, current_date, candidate.id, candidate.start_time_utc, args)
                        last_successful_checkpoint = (current_date, candidate.id, candidate.start_time_utc)
                    consecutive_empty = 0
                    time.sleep(args.sleep)
                    continue
                except KeyboardInterrupt:
                    logger.warning("\n⚠️  Interrupted by user (Ctrl+C)")
                    if last_successful_checkpoint and not dry_run:
                        cp_date, cp_eid, cp_ts = last_successful_checkpoint
                        save_state(state_file, cp_date, cp_eid, cp_ts, args)
                        logger.info("💾 Checkpoint saved. Re-run to resume.")
                    _print_stats(stats, "INTERRUPTED — User")
                    sys.exit(130)

                # Aggregate stats
                if result.status == "updated":
                    consecutive_empty = 0
                    if result.metadata_updated:
                        stats["metadata_updated"] += 1
                        date_stats["metadata_updated"] += 1
                    if result.result_updated:
                        stats["results_updated"] += 1
                        date_stats["results_updated"] += 1
                    # Checkpoint
                    if not dry_run:
                        save_state(state_file, current_date, candidate.id, candidate.start_time_utc, args)
                        last_successful_checkpoint = (current_date, candidate.id, candidate.start_time_utc)
                
                elif result.status == "dry_run":
                    consecutive_empty = 0
                    if result.metadata_updated:
                        stats["dry_run_metadata_would_update"] += 1
                        date_stats["dry_run_metadata_would_update"] += 1
                    if result.result_updated:
                        stats["dry_run_results_would_update"] += 1
                        date_stats["dry_run_results_would_update"] += 1

                elif result.status == "empty_response":
                    stats["empty_response"] += 1
                    date_stats["empty_response"] += 1
                    consecutive_empty += 1
                    logger.warning("⚠️  Event %d — empty response (%d consecutive)", candidate.id, consecutive_empty)

                    if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                        logger.error(
                            "❌ %d consecutive empty responses — possible soft rate limit. Stopping.",
                            MAX_CONSECUTIVE_EMPTY,
                        )
                        if last_successful_checkpoint and not dry_run:
                            cp_date, cp_eid, cp_ts = last_successful_checkpoint
                            save_state(state_file, cp_date, cp_eid, cp_ts, args)
                        _print_stats(stats, "STOPPED — Consecutive empty responses")
                        sys.exit(1)

                elif result.status == "failed":
                    stats["failed"] += 1
                    date_stats["failed"] += 1
                    consecutive_empty = 0
                    logger.warning("⚠️  Event %d — failed: %s", candidate.id, result.detail)

                elif result.status == "skipped":
                    stats["skipped_already_complete"] += 1
                    date_stats["skipped_already_complete"] += 1
                    consecutive_empty = 0

                # Decrement remaining
                if events_remaining is not None:
                    events_remaining -= 1
                    if events_remaining <= 0:
                        break

                # Sleep between API calls
                time.sleep(args.sleep)
            
            if len(events) < limit_for_chunk:
                break

        # Per-date summary
        _print_date_stats(current_date, date_stats)

        # Break outer loop if limit reached
        if events_remaining is not None and events_remaining <= 0:
            logger.info("🛑 Global --limit reached. Stopping.")
            break

    # ── Final summary ─────────────────────────────────────────────────────
    _print_stats(stats, "DRY-RUN COMPLETED" if dry_run else "BACKFILL COMPLETED")

    if dry_run:
        logger.info("✅ Dry-run finished. Run without --test to persist changes.")
    else:
        logger.info("✅ Backfill finished successfully.")


# ---------------------------------------------------------------------------
# Stats printing
# ---------------------------------------------------------------------------
def _print_date_stats(target_date: date, stats: Dict[str, int]) -> None:
    logger.info(
        "  📊 %s summary: seen=%d updated=%d results=%d skipped=%d 404=%d failed=%d empty=%d (dry update: meta=%d res=%d)",
        target_date,
        stats["events_seen"],
        stats["metadata_updated"],
        stats["results_updated"],
        stats["skipped_already_complete"],
        stats["skipped_not_found"],
        stats["failed"],
        stats["empty_response"],
        stats.get("dry_run_metadata_would_update", 0),
        stats.get("dry_run_results_would_update", 0),
    )


def _print_stats(stats: Dict[str, int], title: str) -> None:
    logger.info("\n" + "=" * 80)
    logger.info(title)
    logger.info("=" * 80)
    logger.info("  Events seen            : %d", stats["events_seen"])
    logger.info("  Metadata updated       : %d", stats["metadata_updated"])
    logger.info("  Results updated        : %d", stats["results_updated"])
    if stats.get("dry_run_metadata_would_update") or stats.get("dry_run_results_would_update"):
        logger.info("  [DRY] Would update Meta: %d", stats["dry_run_metadata_would_update"])
        logger.info("  [DRY] Would update Res : %d", stats["dry_run_results_would_update"])
    logger.info("  Skipped (complete)     : %d", stats["skipped_already_complete"])
    logger.info("  Skipped (404)          : %d", stats["skipped_not_found"])
    logger.info("  Failed                 : %d", stats["failed"])
    logger.info("  Empty response         : %d", stats["empty_response"])
    logger.info("  Rate limited           : %d", stats["rate_limited"])
    logger.info("=" * 80)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    try:
        run_backfill(args)
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        logger.error("💥 Unhandled error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
