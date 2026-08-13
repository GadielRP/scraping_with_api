"""Audit or apply the one-way Phase 7 ``market_choices`` migration.

Dry-run is the default. ``--commit`` is explicit and PostgreSQL-only. The
migration never uses CASCADE and preserves a checksum of choice identity.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from infrastructure.persistence.migrations.market_choice_price_state_drop import (
    MarketChoicePriceStateMigrator,
)
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
                    "--commit requires --confirm-destructive because Phase 7 "
                    "permanently removes the MarketChoice price mirror"
                ),
            },
            args.output_json,
        )
        return 3

    try:
        # Keep argument parsing/help independent from the configured DB driver.
        # The engine is needed only once an audit or commit actually starts.
        from infrastructure.persistence.database import db_manager

        Config.validate_odds_read_settings()
        snapshot_schema = MarketChoiceSnapshotSlimMigrator.audit(
            db_manager.engine
        )
        before = MarketChoicePriceStateMigrator.audit(db_manager.engine)
        if not args.commit:
            payload = {
                "ok": (
                    snapshot_schema.is_slim
                    and (before.ready_to_migrate or before.is_slim)
                ),
                "mode": "dry-run",
                "phase6_prerequisite": snapshot_schema.to_dict(),
                "before": before.to_dict(),
            }
            _write_payload(payload, args.output_json)
            return 0 if payload["ok"] else 2

        if not snapshot_schema.is_slim:
            raise RuntimeError(
                "Phase 7 requires the completed Phase 6 slim snapshot schema: "
                + ", ".join(
                    snapshot_schema.blockers
                    or (snapshot_schema.schema_state,)
                )
            )

        create_or_replace_odds_read_views(db_manager.engine)
        before, after = MarketChoicePriceStateMigrator.apply_postgresql(
            db_manager.engine,
            lock_timeout_ms=args.lock_timeout_ms,
            statement_timeout_ms=args.statement_timeout_ms,
        )
        compacted = False
        if args.compact:
            pre_compact_identity = (
                after.metrics.row_count,
                after.metrics.min_choice_id,
                after.metrics.max_choice_id,
                after.metrics.identity_checksum,
            )
            MarketChoicePriceStateMigrator.compact_postgresql(
                db_manager.engine
            )
            compacted = True
            after = MarketChoicePriceStateMigrator.audit(db_manager.engine)
            post_compact_identity = (
                after.metrics.row_count,
                after.metrics.min_choice_id,
                after.metrics.max_choice_id,
                after.metrics.identity_checksum,
            )
            if not after.is_slim:
                raise RuntimeError(
                    "MarketChoice schema failed validation after compact: "
                    + ", ".join(after.blockers or (after.schema_state,))
                )
            if pre_compact_identity != post_compact_identity:
                raise RuntimeError(
                    "MarketChoice identity changed during compact"
                )
        refresh_materialized_views(db_manager.engine)
        reader_postflight = MarketChoiceSnapshotSlimPostflight.run(
            db_manager.engine,
            event_ids=(158955, 169158),
        )
        payload = {
            "ok": after.is_slim and reader_postflight.ok,
            "mode": "commit",
            "compacted": compacted,
            "phase6_prerequisite": snapshot_schema.to_dict(),
            "materialized_views_refreshed": True,
            "reader_postflight": reader_postflight.to_dict(),
            "before": before.to_dict(),
            "after": after.to_dict(),
        }
        _write_payload(payload, args.output_json)
        return 0 if payload["ok"] else 2
    except ValueError as exc:
        _write_payload(
            {"ok": False, "configuration_error": str(exc)},
            args.output_json,
        )
        return 3
    except Exception as exc:
        _write_payload(
            {"ok": False, "migration_error": str(exc)},
            args.output_json,
        )
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
