"""CLI for the persisted canonical-period backfill.

The CLI selects a strategy and delegates execution mechanics to the shared
``BackfillRunner``. It never participates in ingestion and never changes the
PostgreSQL schema.

Examples::

    python -m scripts.maintenance.backfill_canonical_periods audit \
        --strategy canonical_period_backfill_by_fixed_competition_ids_v1 \
        --competition-id 5153 --competition-id 14861 \
        --page-size 250 --limit 1000 \
        --manifest data/backfills/canonical_periods/nfl-v2/manifest.json
    python -m scripts.maintenance.backfill_canonical_periods audit \
        --strategy canonical_period_backfill_by_configured_non_draw_sports_v1 \
        --sport basketball --sport tennis --sport "tennis doubles" --sport volleyball \
        --page-size 250 --manifest data/backfills/canonical_periods/non-draw-sports-v1/manifest.json
    # Repeat the same audit command without --force until it writes manifest.json.
    python -m scripts.maintenance.backfill_canonical_periods apply \
        --confirm-write --expected-db-host db.local --expected-db-name sofascore_local \
        --limit 200 --manifest data/backfills/canonical_periods/nfl-v2/manifest.json
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from infrastructure.persistence.backfill.binary_choice_structure_backfill import (
    DEFAULT_BINARY_CHOICE_SPORTS,
    STRATEGY_NAME as BINARY_CHOICE_STRATEGY,
)
from infrastructure.persistence.backfill.canonical_period_backfill import (
    DEFAULT_COMPETITION_IDS,
    BackfillScope,
)
from infrastructure.persistence.backfill.configured_non_draw_sports_backfill import (
    DEFAULT_NON_DRAW_SPORTS,
    STRATEGY_NAME as NON_DRAW_SPORTS_STRATEGY,
)
from infrastructure.persistence.backfill.checkpoint import read_json, validate_manifest
from infrastructure.persistence.backfill.registry import create_strategy
from infrastructure.persistence.backfill.runner import BackfillRunner
from infrastructure.persistence.database import db_manager

logger = logging.getLogger("canonical_period_backfill")


def _scope_from_args(args: argparse.Namespace) -> BackfillScope:
    if args.strategy == NON_DRAW_SPORTS_STRATEGY:
        competition_ids = (
            frozenset(args.competition_id) if args.competition_id else None
        )
        sports = frozenset(
            str(sport).strip().lower()
            for sport in (args.sport or DEFAULT_NON_DRAW_SPORTS)
            if str(sport).strip()
        )
        round_name = args.round
    elif args.strategy == BINARY_CHOICE_STRATEGY:
        competition_ids = (
            frozenset(args.competition_id) if args.competition_id else None
        )
        sports = (
            frozenset(
                str(sport).strip().lower()
                for sport in (args.sport or DEFAULT_BINARY_CHOICE_SPORTS)
                if str(sport).strip()
            )
            if not args.competition_id or args.sport
            else None
        )
        round_name = args.round
    else:
        competition_ids = frozenset(args.competition_id or DEFAULT_COMPETITION_IDS)
        sports = None
        round_name = args.round if args.round is not None else "regular_season"

    bookie_ids = frozenset(args.bookie_id) if args.bookie_id else None
    return BackfillScope(
        competition_ids=competition_ids,
        sport_names=sports,
        round_name=round_name,
        bookie_ids=bookie_ids,
        is_live=False,
    )



def _scope_from_manifest(manifest: dict) -> BackfillScope:
    scope_data = manifest["scope"]
    return BackfillScope(
        competition_ids=(
            frozenset(scope_data["competition_ids"])
            if scope_data.get("competition_ids") is not None
            else None
        ),
        sport_names=(
            frozenset(scope_data["sports"])
            if scope_data.get("sports") is not None
            else None
        ),
        round_name=scope_data.get("round"),
        bookie_ids=(
            frozenset(scope_data["bookie_ids"])
            if scope_data.get("bookie_ids") is not None
            else None
        ),
        is_live=bool(scope_data.get("is_live", False)),
    )


def _add_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--competition-id",
        type=int,
        action="append",
        help="Competition id; repeatable. Fixed strategy defaults to the initial approved cohort; non-draw strategy treats it as an optional intersection.",
    )
    parser.add_argument(
        "--sport",
        action="append",
        help="Sport name; repeatable for the configured non-draw-sports strategy.",
    )
    parser.add_argument("--bookie-id", type=int, action="append")
    parser.add_argument(
        "--round",
        help="Optional exact event round. Fixed-competition strategy defaults to regular_season; non-draw sports defaults to all rounds.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser(
        "audit", help="Build a read-only immutable manifest (Ctrl+C pauses safely)"
    )
    audit.add_argument("--manifest", type=Path, required=True)
    audit.add_argument(
        "--strategy",
        default="canonical_period_backfill_by_fixed_competition_ids_v1",
        help="Registered backfill strategy.",
    )
    audit.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing manifest explicitly.",
    )
    audit.add_argument(
        "--page-size",
        type=int,
        default=250,
        help="Maximum events read from PostgreSQL per audit page.",
    )
    audit.add_argument(
        "--limit",
        type=int,
        help="Maximum events this audit invocation appends; rerun to resume.",
    )
    _add_scope_args(audit)

    apply = subparsers.add_parser(
        "apply", help="Apply one manifest (Ctrl+C pauses after the current event)"
    )
    apply.add_argument("--manifest", type=Path, required=True)
    apply.add_argument("--checkpoint", type=Path)
    apply.add_argument("--results", type=Path)
    apply.add_argument("--limit", type=int)
    apply.add_argument(
        "--confirm-write",
        action="store_true",
        required=True,
        help="Explicitly acknowledge that apply mutates persisted odds data.",
    )
    apply.add_argument(
        "--expected-db-host",
        required=True,
        help="Expected SQLAlchemy database host; use '(local)' for a hostless local database.",
    )
    apply.add_argument(
        "--expected-db-name",
        required=True,
        help="Expected database name from the active DATABASE_URL.",
    )
    return parser


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _audit(args: argparse.Namespace) -> int:
    scope = _scope_from_args(args)
    strategy = create_strategy(args.strategy, scope=scope)
    runner = BackfillRunner(strategy=strategy, manifest_path=args.manifest)
    runner.audit(force=args.force, page_size=args.page_size, limit=args.limit)
    return 0


def _apply(args: argparse.Namespace) -> int:
    url = db_manager.engine.url
    actual_host = url.host or "(local)"
    actual_name = url.database or ""
    if actual_host.casefold() != args.expected_db_host.casefold():
        raise ValueError(
            f"database host mismatch: expected={args.expected_db_host!r} "
            f"actual={actual_host!r}; refusing to apply"
        )
    if actual_name != args.expected_db_name:
        raise ValueError(
            f"database name mismatch: expected={args.expected_db_name!r} "
            f"actual={actual_name!r}; refusing to apply"
        )
    logger.warning(
        "Explicit write confirmation accepted for database host=%s name=%s",
        actual_host,
        actual_name,
    )
    manifest = read_json(args.manifest)
    validate_manifest(manifest, base_path=args.manifest.parent)
    scope = _scope_from_manifest(manifest)
    strategy = create_strategy(
        manifest["strategy"],
        scope=scope,
        parameters=manifest.get("parameters")
        or {"period_pairs": manifest.get("period_pairs", {})},
    )
    runner = BackfillRunner(
        strategy=strategy,
        manifest_path=args.manifest,
        checkpoint_path=args.checkpoint,
        results_path=args.results,
    )
    return runner.apply(limit=args.limit)


def main() -> int:
    args = _build_parser().parse_args()
    _configure_logging(args.verbose)
    if args.command == "audit":
        return _audit(args)
    if args.command == "apply":
        return _apply(args)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
