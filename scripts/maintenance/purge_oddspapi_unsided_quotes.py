"""Audit or purge redundant Oddspapi NULL-side exchange quotes.

Dry-run is the default. A write requires both ``--commit`` and
``--confirm-destructive``. The cleanup aborts atomically if any candidate has
snapshots, lacks an equivalent top back quote, or has different prices.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.market.oddspapi_unsided_quote_cleanup import (
    OddspapiUnsidedQuoteCleanup,
    OddspapiUnsidedQuoteCleanupBlocked,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--confirm-destructive", action="store_true")
    parser.add_argument("--output-json", type=Path)
    return parser


def _render(payload: dict, output_json: Optional[Path]) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    print(rendered)
    if output_json:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(rendered + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.confirm_destructive and not args.commit:
        _render(
            {
                "ok": False,
                "configuration_error": (
                    "--confirm-destructive is only valid together with --commit"
                ),
            },
            args.output_json,
        )
        return 2
    if args.commit and not args.confirm_destructive:
        _render(
            {
                "ok": False,
                "configuration_error": "--commit requires --confirm-destructive",
            },
            args.output_json,
        )
        return 2

    try:
        with db_manager.get_session() as session:
            before = OddspapiUnsidedQuoteCleanup.audit(session)
            deleted_quotes = 0
            after = None
            if args.commit:
                deleted_quotes, after = OddspapiUnsidedQuoteCleanup.purge(session)
    except OddspapiUnsidedQuoteCleanupBlocked as exc:
        _render(
            {
                "ok": False,
                "mode": "commit",
                "before": before.to_dict(),
                "cleanup_error": str(exc),
            },
            args.output_json,
        )
        return 3
    except Exception as exc:
        _render({"ok": False, "query_error": str(exc)}, args.output_json)
        return 4

    payload = {
        "ok": before.ready_to_purge,
        "mode": "commit" if args.commit else "dry-run",
        "before": before.to_dict(),
        "deleted_quotes": deleted_quotes,
    }
    if after is not None:
        payload["after"] = after.to_dict()
    _render(payload, args.output_json)
    return 0 if before.ready_to_purge else 3


if __name__ == "__main__":
    raise SystemExit(main())
