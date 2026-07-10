"""Import OddsPapi market mappings from local JSON catalogs into normalized DB tables."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from infrastructure.persistence.database import db_manager  # noqa: E402
from infrastructure.persistence.models import BookieSourceMapping, SourceCatalogSync  # noqa: E402
from infrastructure.persistence.repositories.canonical_market_type_repository import (  # noqa: E402
    CanonicalMarketTypeRepository,
)
from modules.odds_ingestion.canonical_market_resolver import resolve_oddspapi_key  # noqa: E402
from modules.oddspapi.catalog_mapping_service import (  # noqa: E402
    canonical_choice_from_outcome,
    upsert_market_source_mapping_from_catalog_item,
)
from modules.oddspapi.format_utils import (  # noqa: E402
    format_line,
    normalize_source,
    normalize_source_id,
)


def _load_json_file(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_market_items(payload) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("markets"), list):
        return [item for item in payload["markets"] if isinstance(item, dict)]
    raise ValueError("markets JSON must be a list or an object with a 'markets' list")


def _payload_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _summary_payload(summary: dict) -> str:
    return json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import OddsPapi canonical market mappings from a local markets catalog JSON."
    )
    parser.add_argument("--markets-file", required=True, help="Local markets catalog JSON")
    parser.add_argument("--bookmakers-file", help="Optional local bookmakers catalog JSON")
    parser.add_argument("--language", default="en", help="Catalog language metadata")
    parser.add_argument("--source", default="oddspapi", help="Source identifier to persist")
    parser.add_argument(
        "--include-unsupported",
        action="store_true",
        help="Report unsupported rows in more detail. Unsupported rows are still not persisted.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Validate only (default)")
    mode.add_argument("--commit", action="store_true", help="Persist supported mappings")
    return parser


def _count_supported_outcomes(item: dict, canonical_market_key: str | None) -> int:
    if not canonical_market_key:
        return 0
    count = 0
    for outcome in item.get("outcomes", []):
        if not isinstance(outcome, dict):
            continue
        canonical_choice = canonical_choice_from_outcome(
            canonical_market_key,
            outcome.get("outcomeName"),
        )
        if canonical_choice is not None:
            count += 1
    return count


def _bookmaker_mapping_summary(bookmakers_payload, source: str) -> dict:
    normalized_source = normalize_source(source)
    if bookmakers_payload is None:
        return {}

    if isinstance(bookmakers_payload, dict) and isinstance(bookmakers_payload.get("bookmakers"), list):
        bookmakers = bookmakers_payload.get("bookmakers") or []
    elif isinstance(bookmakers_payload, list):
        bookmakers = bookmakers_payload
    else:
        return {
            "provided": True,
            "error": "invalid_bookmakers_payload",
        }

    bookmaker_slugs = {
        str(item.get("slug") or "").strip().lower()
        for item in bookmakers
        if isinstance(item, dict) and str(item.get("slug") or "").strip()
    }
    if not bookmaker_slugs:
        return {
            "provided": True,
            "catalog_count": 0,
            "existing_bookie_mappings": 0,
            "missing_bookie_mappings": [],
        }

    with db_manager.get_session() as session:
        existing_slugs = {
            row[0]
            for row in (
                session.query(BookieSourceMapping.source_bookie_slug)
                .filter(
                    BookieSourceMapping.source == normalized_source,
                    BookieSourceMapping.source_bookie_slug.in_(bookmaker_slugs),
                )
                .all()
            )
        }

    return {
        "provided": True,
        "catalog_count": len(bookmaker_slugs),
        "existing_bookie_mappings": len(existing_slugs),
        "missing_bookie_mappings": sorted(bookmaker_slugs - existing_slugs),
    }


def main() -> int:
    args = build_parser().parse_args()
    dry_run = not args.commit
    markets_path = Path(args.markets_file)
    bookmakers_path = Path(args.bookmakers_file) if args.bookmakers_file else None
    normalized_source = normalize_source(args.source)

    markets_payload = _load_json_file(markets_path)
    market_items = _extract_market_items(markets_payload)
    bookmakers_payload = _load_json_file(bookmakers_path) if bookmakers_path else None
    markets_hash = _payload_hash(markets_path)

    if not db_manager.check_and_migrate_schema():
        print("schema_ready=false")
        return 1

    CanonicalMarketTypeRepository.seed_canonical_market_types()

    by_sport_id = Counter()
    by_canonical_market_key = Counter()
    unsupported_examples = []
    supported_mapped = 0
    unsupported = 0
    outcomes_mapped = 0

    if dry_run:
        for item in market_items:
            canonical_market_key, reason = resolve_oddspapi_key(item)
            source_sport_id = normalize_source_id(item.get("sportId"))
            if canonical_market_key is None:
                unsupported += 1
                if len(unsupported_examples) < 20:
                    unsupported_examples.append(
                        {
                            "source_market_id": normalize_source_id(item.get("marketId")),
                            "marketName": item.get("marketName"),
                            "marketType": item.get("marketType"),
                            "period": item.get("period"),
                            "handicap": format_line(item.get("handicap")),
                            "reason": reason,
                        }
                    )
                continue

            supported_mapped += 1
            outcomes_mapped += _count_supported_outcomes(item, canonical_market_key)
            by_sport_id[source_sport_id or "null"] += 1
            by_canonical_market_key[canonical_market_key] += 1
    else:
        with db_manager.get_session() as session:
            CanonicalMarketTypeRepository.seed_canonical_market_types(session)
            for item in market_items:
                canonical_market_key, reason = resolve_oddspapi_key(item)
                source_sport_id = normalize_source_id(item.get("sportId"))
                if canonical_market_key is None:
                    unsupported += 1
                    if len(unsupported_examples) < 20:
                        unsupported_examples.append(
                            {
                                "source_market_id": normalize_source_id(item.get("marketId")),
                                "marketName": item.get("marketName"),
                                "marketType": item.get("marketType"),
                                "period": item.get("period"),
                                "handicap": format_line(item.get("handicap")),
                                "reason": reason,
                            }
                        )
                    continue

                mapping = upsert_market_source_mapping_from_catalog_item(
                    item,
                    source=normalized_source,
                    session=session,
                    include_unsupported=args.include_unsupported,
                )
                if mapping is None:
                    unsupported += 1
                    continue

                supported_mapped += 1
                outcomes_mapped += _count_supported_outcomes(item, canonical_market_key)
                by_sport_id[source_sport_id or "null"] += 1
                by_canonical_market_key[canonical_market_key] += 1

            session.add(
                SourceCatalogSync(
                    source=normalized_source,
                    catalog_type="markets",
                    language=args.language,
                    file_path=str(markets_path),
                    payload_hash=markets_hash,
                    item_count=len(market_items),
                )
            )
            session.flush()

    summary = {
        "mode": "dry-run" if dry_run else "commit",
        "source": normalized_source,
        "language": args.language,
        "markets_file": str(markets_path),
        "payload_hash": markets_hash,
        "total_markets": len(market_items),
        "supported_mapped": supported_mapped,
        "unsupported": unsupported,
        "outcomes_mapped": outcomes_mapped,
        "by_sport_id": dict(sorted(by_sport_id.items())),
        "by_canonical_market_key": dict(sorted(by_canonical_market_key.items())),
        "unsupported_examples": unsupported_examples,
        "bookmakers": _bookmaker_mapping_summary(bookmakers_payload, normalized_source),
    }
    print(_summary_payload(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
