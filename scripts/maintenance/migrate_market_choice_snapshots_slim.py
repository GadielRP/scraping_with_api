"""Audit or apply the one-way Phase 6 snapshot slim migration.

Dry-run is the default. ``--commit`` is explicit and PostgreSQL-only. The CLI
rebuilds only odds-read views before the DDL and never uses CASCADE.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.migrations.market_choice_snapshot_slim import (
    MarketChoiceSnapshotSlimMigrator,
)
from infrastructure.persistence.migrations.market_choice_snapshot_slim_postflight import (
    MarketChoiceSnapshotSlimPostflight,
)
from infrastructure.persistence.models import (
    create_or_replace_odds_read_views,
    refresh_materialized_views,
)
from infrastructure.settings import Config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument(
        "--confirm-destructive",
        action="store_true",
        help="Required with --commit; confirms permanent column removal.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Run VACUUM FULL after a successful/already-applied migration.",
    )
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    parser.add_argument("--statement-timeout-ms", type=int, default=120_000)
    parser.add_argument(
        "--reference-event-id",
        action="append",
        type=int,
        default=[],
        help="Repeatable postflight event ID; defaults to 158955 and 169158.",
    )
    parser.add_argument("--output-json", type=Path)
    return parser


def _write_payload(payload: dict, output_path: Optional[Path]) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str)
    print(rendered)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.compact and not args.commit:
        _write_payload(
            {"ok": False, "configuration_error": "--compact requires --commit"},
            args.output_json,
        )
        return 3
    if args.commit and not args.confirm_destructive:
        _write_payload(
            {
                "ok": False,
                "configuration_error": (
                    "--commit requires --confirm-destructive because Phase 6 "
                    "permanently removes snapshot identity columns"
                ),
            },
            args.output_json,
        )
        return 3
    try:
        Config.validate_odds_read_settings()
        before = MarketChoiceSnapshotSlimMigrator.audit(db_manager.engine)
        payload = {"ok": not before.blockers, "mode": "dry-run", "before": before.to_dict()}
        if not args.commit:
            _write_payload(payload, args.output_json)
            return 0 if before.ready_to_migrate or before.is_slim else 2

        create_or_replace_odds_read_views(db_manager.engine)
        before, after = MarketChoiceSnapshotSlimMigrator.apply_postgresql(
            db_manager.engine,
            lock_timeout_ms=args.lock_timeout_ms,
            statement_timeout_ms=args.statement_timeout_ms,
        )
        compacted = False
        if args.compact:
            pre_compact_payload = (
                after.metrics.row_count,
                after.metrics.min_snapshot_id,
                after.metrics.max_snapshot_id,
                after.metrics.payload_checksum,
            )
            MarketChoiceSnapshotSlimMigrator.compact_postgresql(db_manager.engine)
            compacted = True
            after = MarketChoiceSnapshotSlimMigrator.audit(db_manager.engine)
            post_compact_payload = (
                after.metrics.row_count,
                after.metrics.min_snapshot_id,
                after.metrics.max_snapshot_id,
                after.metrics.payload_checksum,
            )
            if pre_compact_payload != post_compact_payload:
                raise RuntimeError("Snapshot payload changed during compact")
        refresh_materialized_views(db_manager.engine)
        payload = {
            "ok": after.is_slim,
            "mode": "commit",
            "compacted": compacted,
            "materialized_views_refreshed": True,
            "before": before.to_dict(),
            "after": after.to_dict(),
        }
        reader_postflight = MarketChoiceSnapshotSlimPostflight.run(
            db_manager.engine,
            event_ids=args.reference_event_id or (158955, 169158),
        )
        payload["reader_postflight"] = reader_postflight.to_dict()
        payload["ok"] = after.is_slim and reader_postflight.ok
        _write_payload(payload, args.output_json)
        return 0 if payload["ok"] else 2
    except ValueError as exc:
        _write_payload(
            {"ok": False, "configuration_error": str(exc)}, args.output_json
        )
        return 3
    except Exception as exc:
        _write_payload({"ok": False, "migration_error": str(exc)}, args.output_json)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
