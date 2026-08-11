"""CLI for Phase 4b MarketChoiceQuote historical backfill.

Dry-run is the default. ``--commit`` requires ``--confirm-ingestion-paused``
and a bounded scope (``--event-id`` and/or ``--max-events`` / ``--max-rows``).

Default artifacts (shared for single-event and multi-event runs; no per-event
files) live under ``logs/debug/market_choice_quote_backfill/`` so Docker volume
``./logs:/app/logs`` (compose.yaml) persists them:

- ``checkpoint.json`` — resume cursor (root of the artifact folder)
- ``output.json`` — run summary / report
- ``rejections.ndjson`` — ambiguous/conflict/invalid/notes (append on resume)

See docs/refactors/db-schema-odds-refactor-phase-4b.md.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from app.logging_setup import setup_logging  # noqa: E402
from infrastructure.persistence.database import db_manager  # noqa: E402
from modules.odds_ingestion.backfill.market_choice_quote_backfill import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    KNOWN_SOURCES,
    MAX_BATCH_SIZE,
    MAX_EVENTS_HARD_CAP,
    MAX_ROWS_HARD_CAP,
    MarketChoiceQuoteBackfillService,
    RunConfig,
)

logger = logging.getLogger(__name__)

DEFAULT_ARTIFACT_DIR = REPOSITORY_ROOT / "logs" / "debug" / "market_choice_quote_backfill"
DEFAULT_CHECKPOINT_FILE = DEFAULT_ARTIFACT_DIR / "checkpoint.json"
DEFAULT_OUTPUT_JSON = DEFAULT_ARTIFACT_DIR / "output.json"
DEFAULT_OUTPUT_REJECTIONS = DEFAULT_ARTIFACT_DIR / "rejections.ndjson"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill MarketChoiceQuote rows and link historical snapshots."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Read-only classification and merge simulation (default).",
    )
    mode.add_argument(
        "--commit",
        action="store_true",
        help="Persist quote upserts and snapshot.quote_id links.",
    )
    parser.add_argument("--event-id", type=int)
    parser.add_argument("--event-id-min", type=int)
    parser.add_argument("--event-id-max", type=int)
    parser.add_argument("--source", type=str)
    parser.add_argument(
        "--pass",
        dest="pass_name",
        choices=("snapshots", "choice-states", "all"),
        default="all",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--after-event-id", type=int)
    parser.add_argument("--after-snapshot-id", type=int)
    parser.add_argument(
        "--resume-from",
        type=Path,
        nargs="?",
        const=DEFAULT_CHECKPOINT_FILE,
        default=None,
        help=(
            "Resume from checkpoint. Pass flag alone to use the shared "
            f"{DEFAULT_CHECKPOINT_FILE.name}; or pass an explicit path."
        ),
    )
    parser.add_argument("--resolution-file", type=Path)
    parser.add_argument(
        "--checkpoint-file",
        type=Path,
        help=f"Checkpoint path (default: {DEFAULT_CHECKPOINT_FILE}).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help=f"Summary report path (default: {DEFAULT_OUTPUT_JSON}).",
    )
    parser.add_argument(
        "--output-rejections",
        type=Path,
        help=f"Shared NDJSON audit path (default: {DEFAULT_OUTPUT_REJECTIONS}).",
    )
    parser.add_argument(
        "--fresh-artifacts",
        action="store_true",
        help=(
            "Truncate rejections.ndjson and do not auto-resume from the shared "
            "checkpoint (start a new campaign)."
        ),
    )
    parser.add_argument("--progress-every", type=int, default=1)
    parser.add_argument(
        "--confirm-ingestion-paused",
        action="store_true",
        help="Required for --commit; affirms live ingestion is paused.",
    )
    parser.add_argument(
        "--purge-oddspapi-null-mainline-lines",
        action="store_true",
        help=(
            "Before backfill, delete oddspapi snapshots with main_line IS NULL "
            "on markets where choice_group IS NOT NULL; then orphan-clean "
            "choices/markets left without ticks. Dry-run counts only."
        ),
    )
    parser.add_argument(
        "--confirm-purge",
        action="store_true",
        help="Required with --commit when --purge-oddspapi-null-mainline-lines is set.",
    )
    return parser


def apply_default_artifact_paths(args: argparse.Namespace) -> None:
    """Fill shared artifact paths; never invent per-event files."""
    if args.checkpoint_file is None:
        args.checkpoint_file = DEFAULT_CHECKPOINT_FILE
    if args.output_json is None:
        args.output_json = DEFAULT_OUTPUT_JSON
    if args.output_rejections is None:
        args.output_rejections = DEFAULT_OUTPUT_REJECTIONS

    if args.commit and args.checkpoint_file is None and args.resume_from is not None:
        args.checkpoint_file = args.resume_from

    # Resume appends to the shared rejections file; fresh runs truncate.
    args.append_rejections = bool(args.resume_from) and not args.fresh_artifacts


def validate_args(args: argparse.Namespace) -> Optional[str]:
    args.dry_run = not args.commit

    if args.event_id is not None and (
        args.event_id_min is not None or args.event_id_max is not None
    ):
        return "--event-id is mutually exclusive with --event-id-min/--event-id-max"

    if args.source is not None:
        normalized = args.source.strip().lower()
        if normalized not in KNOWN_SOURCES:
            return f"--source must be one of {sorted(KNOWN_SOURCES)}"
        args.source = normalized

    if args.batch_size <= 0:
        return "--batch-size must be > 0"
    if args.batch_size > MAX_BATCH_SIZE:
        return f"--batch-size hard cap is {MAX_BATCH_SIZE}"

    if args.max_events is not None:
        if args.max_events <= 0:
            return "--max-events must be > 0"
        if args.max_events > MAX_EVENTS_HARD_CAP:
            return f"--max-events hard cap is {MAX_EVENTS_HARD_CAP}"

    if args.max_rows is not None:
        if args.max_rows <= 0:
            return "--max-rows must be > 0"
        if args.max_rows > MAX_ROWS_HARD_CAP:
            return f"--max-rows hard cap is {MAX_ROWS_HARD_CAP}"

    bounded = (
        args.event_id is not None
        or args.max_events is not None
        or args.max_rows is not None
        or args.resume_from is not None
    )
    if not bounded:
        return (
            "unbounded scope rejected: provide --event-id and/or "
            "--max-events/--max-rows (or --resume-from)"
        )

    if args.commit:
        if not args.confirm_ingestion_paused:
            return "--commit requires --confirm-ingestion-paused"
        if args.purge_oddspapi_null_mainline_lines and not args.confirm_purge:
            return (
                "--commit with --purge-oddspapi-null-mainline-lines "
                "requires --confirm-purge"
            )

    if args.resolution_file is not None and not args.resolution_file.exists():
        return f"resolution file not found: {args.resolution_file}"
    if args.resume_from is not None and not args.resume_from.exists():
        return f"checkpoint not found: {args.resume_from}"

    return None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    apply_default_artifact_paths(args)
    error = validate_args(args)
    if error:
        parser.error(error)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    setup_logging()
    args = parse_args(argv)
    config = RunConfig(
        dry_run=args.dry_run,
        event_id=args.event_id,
        event_id_min=args.event_id_min,
        event_id_max=args.event_id_max,
        after_event_id=args.after_event_id,
        after_snapshot_id=args.after_snapshot_id,
        source=args.source,
        pass_name=args.pass_name,
        batch_size=args.batch_size,
        max_events=args.max_events,
        max_rows=args.max_rows,
        resolution_file=args.resolution_file,
        checkpoint_file=args.checkpoint_file,
        resume_from=args.resume_from,
        output_json=args.output_json,
        output_rejections=args.output_rejections,
        append_rejections=args.append_rejections,
        confirm_ingestion_paused=args.confirm_ingestion_paused,
        purge_oddspapi_null_mainline_lines=args.purge_oddspapi_null_mainline_lines,
        confirm_purge=args.confirm_purge,
    )
    service = MarketChoiceQuoteBackfillService(db_manager.get_session)
    exit_code, summary = service.run(config)
    logger.info(
        "backfill finished exit=%s stop_reason=%s rows=%s events=%s artifacts=%s",
        exit_code,
        summary.get("stop_reason"),
        summary.get("rows_consumed"),
        summary.get("events_selected"),
        {
            "output_json": str(args.output_json),
            "output_rejections": str(args.output_rejections),
            "checkpoint_file": str(args.checkpoint_file),
            "append_rejections": args.append_rejections,
        },
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
