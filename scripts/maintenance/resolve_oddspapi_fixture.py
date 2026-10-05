"""Fetch and resolve one OddsPapi fixture against canonical events."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from app.logging_setup import setup_logging  # noqa: E402
from infrastructure.persistence.database import db_manager  # noqa: E402
from modules.sports.catalog import configured_sport_ids, oddspapi_sport_id_for_fixture  # noqa: E402
from modules.oddspapi.client import OddsPapiClient  # noqa: E402
from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import (  # noqa: E402
    OddspapiFixtureBatchProcessor,
)

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture_id", help="OddsPapi fixtureId to fetch and resolve")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the fixture without writing mappings or review-queue rows",
    )
    parser.add_argument(
        "--persist-queue",
        action="store_true",
        help="Persist an unresolved fixture for review (ambiguous matches are always queued when writing)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    setup_logging()

    client = OddsPapiClient()
    try:
        payload = client.get_fixture(args.fixture_id)
    finally:
        client.close()

    if not isinstance(payload, dict):
        logger.error("Expected a single fixture object from OddsPapi, got %s", type(payload).__name__)
        return 1

    response_fixture_id = str(payload.get("fixtureId") or "").strip()
    if not response_fixture_id:
        logger.error("OddsPapi fixture response is missing fixtureId")
        return 1
    if response_fixture_id != args.fixture_id:
        logger.error(
            "OddsPapi returned fixtureId=%s for requested fixtureId=%s",
            response_fixture_id,
            args.fixture_id,
        )
        return 1

    sport_id = oddspapi_sport_id_for_fixture(payload)
    if sport_id not in configured_sport_ids():
        logger.error(
            "Skipping OddsPapi fixture %s: sport=%s is not configured as supported",
            args.fixture_id,
            sport_id or "unknown",
        )
        return 2

    processor = OddspapiFixtureBatchProcessor(keep_resolutions=True)
    with db_manager.get_session() as session:
        result = processor.process_batch(
            fixture_payloads=[payload],
            create_mappings=not args.dry_run,
            persist_queue=args.persist_queue,
            session=session,
        )

    resolution = (result.resolutions or [None])[0]
    report = {
        "fixture_id": args.fixture_id,
        "dry_run": args.dry_run,
        "resolved": bool(resolution and resolution.resolved),
        "canonical_event_id": resolution.canonical_event_id if resolution else None,
        "match_method": resolution.match_method if resolution else None,
        "confidence": resolution.confidence if resolution else None,
        "created_mappings": resolution.created_mappings if resolution else [],
        "skipped_reason": resolution.skipped_reason if resolution else "invalid_fixture_payload",
        "needs_review": resolution.needs_review if resolution else False,
        "mappings_created": result.mappings_created,
        "queue_rows_written": result.queue_rows_written,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    return 0 if report["resolved"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
