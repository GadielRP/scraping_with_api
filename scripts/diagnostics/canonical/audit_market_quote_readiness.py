"""Read-only Phase 5 readiness audit.

Exit 0 means the requested scope can be read exclusively through quotes.  This
command never runs migrations and never writes database state.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.market.market_quote_readiness import (
    MarketQuoteReadinessAuditor,
)
from infrastructure.settings import Config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-id", action="append", type=int, default=[])
    parser.add_argument("--event-id-from", type=int)
    parser.add_argument("--event-id-to", type=int)
    parser.add_argument("--output-json", type=Path)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        Config.validate_odds_read_settings()
        with db_manager.get_session() as session:
            if (args.event_id_from is None) != (args.event_id_to is None):
                raise ValueError(
                    "--event-id-from and --event-id-to must be provided together"
                )
            event_ids = list(args.event_id)
            if args.event_id_from is not None:
                if args.event_id_from > args.event_id_to:
                    raise ValueError("--event-id-from cannot exceed --event-id-to")
                range_rows = session.execute(
                    text(
                        """
                        SELECT DISTINCT event_id
                        FROM markets
                        WHERE event_id BETWEEN :event_id_from AND :event_id_to
                        ORDER BY event_id
                        """
                    ),
                    {
                        "event_id_from": args.event_id_from,
                        "event_id_to": args.event_id_to,
                    },
                ).scalars().all()
                event_ids.extend(int(item) for item in range_rows)
            report = MarketQuoteReadinessAuditor.audit(
                session, event_ids=event_ids
            )
    except ValueError as exc:
        print(json.dumps({"ready": False, "configuration_error": str(exc)}))
        return 3
    except Exception as exc:
        print(json.dumps({"ready": False, "query_error": str(exc)}))
        return 4

    payload = report.to_dict()
    rendered = json.dumps(payload, indent=2, default=str, sort_keys=True)
    print(rendered)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered + "\n", encoding="utf-8")
    if report.schema_errors:
        return 3
    return 0 if report.ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
