"""Offline-only CLI for validating a saved OddsPapi odds payload."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from modules.odds_ingestion import OddspapiMarketAdapter  # noqa: E402
from modules.odds_ingestion.market_odds_ingestion_service import (  # noqa: E402
    MarketOddsIngestionService,
)


def _load_json(path: str | None):
    if not path:
        return None
    with Path(path).open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or persist a local OddsPapi odds response without HTTP calls."
    )
    parser.add_argument("--file", required=True, help="Local OddsPapi odds JSON file")
    parser.add_argument("--markets-catalog", help="Optional local /v4/markets JSON file")
    parser.add_argument("--bookmakers-catalog", help="Optional local /v4/bookmakers JSON file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Validate without database writes (default)")
    mode.add_argument("--commit", action="store_true", help="Persist mappings, bookies, and markets")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    odds_response = _load_json(args.file)
    market_catalog = _load_json(args.markets_catalog)
    bookmaker_catalog = _load_json(args.bookmakers_catalog)
    dry_run = not args.commit

    adapted = OddspapiMarketAdapter.from_odds_response(
        odds_response,
        market_catalog=market_catalog,
        bookmaker_catalog=bookmaker_catalog,
    )
    result = MarketOddsIngestionService.save_from_oddspapi_response(
        odds_response,
        market_catalog=market_catalog,
        bookmaker_catalog=bookmaker_catalog,
        dry_run=dry_run,
    )

    external_providers = odds_response.get("externalProviders") or {}
    bookmakers = adapted.get("bookmakers", [])
    markets_detected = sum(len(bookmaker.get("markets", [])) for bookmaker in bookmakers)

    print(f"fixtureId: {odds_response.get('fixtureId')}")
    print(f"externalProviders.sofascoreId: {external_providers.get('sofascoreId')}")
    print(f"canonical_event_id: {result.event_id}")
    print(f"resolved: {result.event_id is not None}")
    print(f"skipped: {result.skipped}")
    print(f"skipped_reason: {result.reason}")
    print(f"mappings_created: {result.mappings_created}")
    print(f"bookmakers_detected: {', '.join(b['slug'] for b in bookmakers) or 'none'}")
    print(f"markets_detected: {markets_detected}")
    print(f"markets_saved: {0 if dry_run else result.markets_saved}")
    print(f"mode: {'dry-run' if dry_run else 'commit'}")
    return 0 if result.event_id is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
