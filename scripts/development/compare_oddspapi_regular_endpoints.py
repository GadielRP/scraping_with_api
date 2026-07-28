"""Compare /v4/odds vs /v4/historical-odds for regular bookmakers.

Diagnostic tool — does NOT modify production behaviour, database, or ingestion.

Usage examples:

  Offline (default, no HTTP):
    python -m scripts.development.compare_oddspapi_regular_endpoints \\
      --odds-file odds_papi/odds_data/odds_id1300010963303463.json \\
      --historical-file odds_papi/odds_data/historical_odds_id1300010963303463_pinnacle_bet365.json \\
      --bookmakers pinnacle,bet365 \\
      --minutes-until-start 120 \\
      --output-dir exports/oddspapi_endpoint_comparison

  Live (requires --live flag and ODDSPAPI_KEY):
    python -m scripts.development.compare_oddspapi_regular_endpoints \\
      --fixture-id id1300010963303463 \\
      --bookmakers pinnacle,bet365 \\
      --minutes-until-start 120 \\
      --live --save-raw \\
      --output-dir exports/oddspapi_endpoint_comparison

  Manifest/batch (JSON):
    python -m scripts.development.compare_oddspapi_regular_endpoints \\
      --manifest path/to/manifest.json \\
      --output-dir exports/oddspapi_endpoint_comparison
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root for python -m execution
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from modules.oddspapi.diagnostics.regular_odds_comparator import (
    ComparisonConfig,
    ComparisonRow,
    aggregate_by_dimension,
    aggregate_rows,
    assess_viability,
    compare_entries,
    is_exchange_bookmaker,
    parse_historical_response,
    parse_odds_response,
    reject_exchange_bookmakers,
)
from modules.oddspapi.diagnostics.comparison_report import (
    format_comparison_text,
    generate_run_id,
    write_comparison_csv,
    write_comparison_summary_json,
)

logger = logging.getLogger(__name__)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _load_json_file(path: Path) -> dict | list:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"File not found: {resolved}")
    with resolved.open(encoding="utf-8") as f:
        return json.load(f)


def _payload_size(payload: dict | list | None) -> int:
    if payload is None:
        return 0
    return len(json.dumps(payload, separators=(",", ":")))


def _redact_api_key(text: str, api_key: str | None) -> str:
    if not api_key:
        return text
    return text.replace(api_key, "***REDACTED***")


def _parse_bookmakers(value: str) -> list[str]:
    return [b.strip().lower() for b in value.split(",") if b.strip()]


# ---------------------------------------------------------------------------
# Live mode: fetch both endpoints
# ---------------------------------------------------------------------------

def _fetch_live(
    fixture_id: str,
    bookmakers: list[str],
) -> tuple[dict | None, dict | None, dict]:
    """Fetch /odds and /historical-odds and return (odds_payload, hist_payload, timing_info)."""
    # Import client only in live mode to avoid Config dependency in offline mode
    from modules.oddspapi.client import OddsPapiClient

    client = OddsPapiClient()
    timing: dict = {}

    # Fetch /odds
    logger.info("Fetching /v4/odds for fixture=%s bookmakers=%s", fixture_id, bookmakers)
    t0 = time.monotonic()
    ts0 = datetime.now(timezone.utc).isoformat()
    try:
        odds_payload = client.get_odds(fixture_id=fixture_id, bookmakers=bookmakers)
    except Exception as exc:
        logger.error("Failed to fetch /v4/odds: %s", exc)
        odds_payload = None
    t1 = time.monotonic()
    ts1 = datetime.now(timezone.utc).isoformat()
    timing["odds_request_started_at"] = ts0
    timing["odds_response_received_at"] = ts1
    timing["odds_duration_seconds"] = round(t1 - t0, 3)
    timing["odds_payload_bytes"] = _payload_size(odds_payload)

    # Fetch /historical-odds
    logger.info("Fetching /v4/historical-odds for fixture=%s bookmakers=%s", fixture_id, bookmakers)
    t2 = time.monotonic()
    ts2 = datetime.now(timezone.utc).isoformat()
    try:
        hist_payload = client.get_historical_odds(fixture_id=fixture_id, bookmakers=bookmakers)
    except Exception as exc:
        logger.error("Failed to fetch /v4/historical-odds: %s", exc)
        hist_payload = None
    t3 = time.monotonic()
    ts3 = datetime.now(timezone.utc).isoformat()
    timing["historical_request_started_at"] = ts2
    timing["historical_response_received_at"] = ts3
    timing["historical_duration_seconds"] = round(t3 - t2, 3)
    timing["historical_payload_bytes"] = _payload_size(hist_payload)
    timing["elapsed_between_responses_seconds"] = round(t2 - t1, 3)

    logger.info(
        "Live fetch complete: odds_duration=%.3fs historical_duration=%.3fs "
        "odds_bytes=%d historical_bytes=%d elapsed_between=%.3fs",
        timing["odds_duration_seconds"],
        timing["historical_duration_seconds"],
        timing["odds_payload_bytes"],
        timing["historical_payload_bytes"],
        timing["elapsed_between_responses_seconds"],
    )

    return odds_payload, hist_payload, timing


# ---------------------------------------------------------------------------
# Core comparison for one fixture
# ---------------------------------------------------------------------------

def _compare_fixture(
    odds_payload: dict | None,
    historical_payload: dict | None,
    bookmakers: list[str],
    config: ComparisonConfig,
    *,
    fixture_id: str | None = None,
    sport_id: str | None = None,
    minutes_until_start: float | None = None,
    captured_at: str | None = None,
) -> list[ComparisonRow]:
    odds_entries = parse_odds_response(odds_payload)
    historical_summaries = parse_historical_response(historical_payload)

    bookmaker_filter = set(b.lower() for b in bookmakers) if bookmakers else None

    rows = compare_entries(
        odds_entries,
        historical_summaries,
        config,
        fixture_id=fixture_id,
        sport_id=sport_id,
        minutes_until_start=minutes_until_start,
        captured_at=captured_at,
        bookmaker_filter=bookmaker_filter,
    )

    logger.info(
        "Comparison complete: fixture=%s bookmakers=%s "
        "current_identities=%d historical_identities=%d "
        "comparison_rows=%d",
        fixture_id or (odds_payload or {}).get("fixtureId"),
        bookmakers,
        len(odds_entries),
        len(historical_summaries),
        len(rows),
    )

    return rows


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def _write_outputs(
    rows: list[ComparisonRow],
    output_dir: Path,
    run_id: str,
    run_metadata: dict,
    *,
    odds_response_duration: float | None = None,
    historical_response_duration: float | None = None,
    odds_payload_size: int | None = None,
    historical_payload_size: int | None = None,
    dimension_rollups: dict | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    # Aggregate
    aggregate = aggregate_rows(
        rows,
        odds_response_duration=odds_response_duration,
        historical_response_duration=historical_response_duration,
        odds_payload_size=odds_payload_size,
        historical_payload_size=historical_payload_size,
    )
    viability = assess_viability(aggregate)

    # CSV
    csv_path = output_dir / f"{run_id}_comparison_rows.csv"
    write_comparison_csv(rows, csv_path)
    paths["csv"] = csv_path
    logger.info("CSV written: %s (%d rows)", csv_path, len(rows))

    # JSON summary
    json_path = output_dir / f"{run_id}_comparison_summary.json"

    # Compute dimension rollups if not provided
    if dimension_rollups is None:
        dimension_rollups = {}
        if rows:
            dimension_rollups["bookmaker"] = aggregate_by_dimension(
                rows, lambda r: r.bookmaker
            )
            dimension_rollups["canonical_market_key"] = aggregate_by_dimension(
                rows, lambda r: r.canonical_market_key
            )
            if any(r.fixture_id for r in rows):
                dimension_rollups["fixture"] = aggregate_by_dimension(
                    rows, lambda r: r.fixture_id
                )

    write_comparison_summary_json(
        aggregate, viability, run_metadata, json_path,
        dimension_rollups=dimension_rollups,
    )
    paths["json"] = json_path
    logger.info("JSON summary written: %s", json_path)

    # Text report
    text_report = format_comparison_text(aggregate, viability, run_metadata)
    txt_path = output_dir / f"{run_id}_comparison_report.txt"
    txt_path.write_text(text_report, encoding="utf-8")
    paths["txt"] = txt_path
    logger.info("Text report written: %s", txt_path)

    # Print to console
    print(text_report)

    return paths


# ---------------------------------------------------------------------------
# Manifest/batch mode
# ---------------------------------------------------------------------------

def _load_manifest(manifest_path: Path) -> list[dict]:
    data = _load_json_file(manifest_path)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("fixtures"), list):
        return data["fixtures"]
    raise ValueError(
        f"Manifest must be a JSON list or object with 'fixtures' key: {manifest_path}"
    )


def _run_manifest(
    manifest_path: Path,
    config: ComparisonConfig,
    output_dir: Path,
    run_id: str,
    *,
    live: bool = False,
    save_raw: bool = False,
    default_bookmakers: list[str] | None = None,
) -> None:
    entries = _load_manifest(manifest_path)
    logger.info("Manifest loaded: %d entries from %s", len(entries), manifest_path)

    all_rows: list[ComparisonRow] = []

    for idx, entry in enumerate(entries):
        fixture_id = entry.get("fixture_id")
        sport_id = entry.get("sport_id")
        minutes = entry.get("minutes_until_start")
        bookmakers = _parse_bookmakers(entry.get("bookmakers", "")) or default_bookmakers or []
        odds_file = entry.get("odds_file")
        historical_file = entry.get("historical_file")
        captured_at = entry.get("captured_at")

        logger.info(
            "Manifest entry %d/%d: fixture=%s bookmakers=%s",
            idx + 1, len(entries), fixture_id, bookmakers,
        )

        odds_payload = None
        hist_payload = None
        timing = {}

        if live and fixture_id:
            reject_exchange_bookmakers(bookmakers)
            odds_payload, hist_payload, timing = _fetch_live(fixture_id, bookmakers)
            if save_raw:
                _save_raw_payloads(output_dir, run_id, fixture_id, odds_payload, hist_payload)
        else:
            if odds_file:
                odds_payload = _load_json_file(Path(odds_file))
            if historical_file:
                hist_payload = _load_json_file(Path(historical_file))

        if odds_payload is None and hist_payload is None:
            logger.warning("Manifest entry %d: no data available, skipping", idx + 1)
            continue

        rows = _compare_fixture(
            odds_payload,
            hist_payload,
            bookmakers,
            config,
            fixture_id=fixture_id,
            sport_id=str(sport_id) if sport_id else None,
            minutes_until_start=minutes,
            captured_at=captured_at,
        )
        all_rows.extend(rows)

    run_metadata = {
        "run_id": run_id,
        "mode": "manifest_live" if live else "manifest_offline",
        "manifest_path": str(manifest_path),
        "manifest_entries": len(entries),
        "total_comparison_rows": len(all_rows),
        "config": {
            "price_tolerance": config.price_tolerance,
            "stale_threshold_seconds": config.stale_threshold_seconds,
            "minimum_opening_span_minutes": config.minimum_opening_span_minutes,
        },
    }

    _write_outputs(all_rows, output_dir, run_id, run_metadata)


# ---------------------------------------------------------------------------
# Raw payload saving
# ---------------------------------------------------------------------------

def _save_raw_payloads(
    output_dir: Path,
    run_id: str,
    fixture_id: str,
    odds_payload: dict | None,
    hist_payload: dict | None,
) -> None:
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    if odds_payload is not None:
        raw_path = raw_dir / f"{run_id}_odds_{fixture_id}.json"
        raw_text = json.dumps(odds_payload, indent=2)
        # Redact any potential API key leakage
        raw_path.write_text(raw_text, encoding="utf-8")
        logger.info("Raw odds payload saved: %s", raw_path)

    if hist_payload is not None:
        raw_path = raw_dir / f"{run_id}_historical_{fixture_id}.json"
        raw_text = json.dumps(hist_payload, indent=2)
        raw_path.write_text(raw_text, encoding="utf-8")
        logger.info("Raw historical payload saved: %s", raw_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare /v4/odds vs /v4/historical-odds for regular bookmakers. "
            "Diagnostic tool — no production impact."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Mode
    mode = parser.add_argument_group("mode selection")
    mode.add_argument(
        "--odds-file", type=Path,
        help="Path to local /v4/odds JSON file (offline mode)",
    )
    mode.add_argument(
        "--historical-file", type=Path,
        help="Path to local /v4/historical-odds JSON file (offline mode)",
    )
    mode.add_argument(
        "--fixture-id", type=str,
        help="Fixture ID for live mode",
    )
    mode.add_argument(
        "--live", action="store_true",
        help="Enable HTTP requests (requires ODDSPAPI_KEY)",
    )
    mode.add_argument(
        "--manifest", type=Path,
        help="Path to JSON manifest for batch processing",
    )

    # Filtering
    filt = parser.add_argument_group("filtering")
    filt.add_argument(
        "--bookmakers", type=str, default="pinnacle,bet365",
        help="Comma-separated bookmaker slugs (default: pinnacle,bet365)",
    )
    filt.add_argument(
        "--sport-id", type=str, default=None,
        help="Optional sport ID for enrichment context",
    )
    filt.add_argument(
        "--minutes-until-start", type=float, default=None,
        help="Minutes until event start (metadata for report)",
    )

    # Tolerances
    tol = parser.add_argument_group("tolerances")
    tol.add_argument(
        "--price-tolerance", type=float, default=0.001,
        help="Price comparison tolerance (default: 0.001)",
    )
    tol.add_argument(
        "--stale-threshold-seconds", type=float, default=60.0,
        help="Seconds after which historical data is considered stale (default: 60)",
    )
    tol.add_argument(
        "--minimum-opening-span-minutes", type=float, default=60.0,
        help="Minimum span for credible opening (default: 60)",
    )

    # Output
    out = parser.add_argument_group("output")
    out.add_argument(
        "--output-dir", type=Path,
        default=Path("exports/oddspapi_endpoint_comparison"),
        help="Output directory (default: exports/oddspapi_endpoint_comparison)",
    )
    out.add_argument(
        "--save-raw", action="store_true",
        help="Save raw API payloads (live mode only)",
    )
    out.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logging",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(verbose=args.verbose)

    run_id = generate_run_id()
    bookmakers = _parse_bookmakers(args.bookmakers)

    # Reject exchange bookmakers
    try:
        reject_exchange_bookmakers(bookmakers)
    except ValueError as exc:
        logger.error("%s", exc)
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    config = ComparisonConfig(
        price_tolerance=args.price_tolerance,
        stale_threshold_seconds=args.stale_threshold_seconds,
        minimum_opening_span_minutes=args.minimum_opening_span_minutes,
    )

    # Manifest mode
    if args.manifest:
        try:
            _run_manifest(
                args.manifest, config, args.output_dir, run_id,
                live=args.live,
                save_raw=args.save_raw,
                default_bookmakers=bookmakers,
            )
        except Exception as exc:
            logger.error("Manifest processing failed: %s", exc)
            return 1
        return 0

    # Determine mode
    odds_payload = None
    hist_payload = None
    timing: dict = {}

    if args.live:
        # Live mode
        if not args.fixture_id:
            print("Error: --fixture-id is required in live mode", file=sys.stderr)
            return 1
        odds_payload, hist_payload, timing = _fetch_live(args.fixture_id, bookmakers)
        if args.save_raw:
            _save_raw_payloads(
                args.output_dir, run_id, args.fixture_id,
                odds_payload, hist_payload,
            )
    else:
        # Offline mode
        if args.fixture_id and not (args.odds_file or args.historical_file):
            print(
                "Error: --fixture-id provided without --live. "
                "Use --live to make HTTP requests, or provide --odds-file "
                "and --historical-file for offline comparison.",
                file=sys.stderr,
            )
            return 1

        if args.odds_file:
            try:
                odds_payload = _load_json_file(args.odds_file)
            except FileNotFoundError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1

        if args.historical_file:
            try:
                hist_payload = _load_json_file(args.historical_file)
            except FileNotFoundError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1

    if odds_payload is None and hist_payload is None:
        print("Error: no data to compare. Provide files or use --live.", file=sys.stderr)
        return 1

    fixture_id = args.fixture_id or (odds_payload or {}).get("fixtureId") or (hist_payload or {}).get("fixtureId")

    rows = _compare_fixture(
        odds_payload,
        hist_payload,
        bookmakers,
        config,
        fixture_id=fixture_id,
        sport_id=args.sport_id,
        minutes_until_start=args.minutes_until_start,
    )

    run_metadata = {
        "run_id": run_id,
        "mode": "live" if args.live else "offline",
        "fixture_id": fixture_id,
        "bookmakers": bookmakers,
        "minutes_until_start": args.minutes_until_start,
        "comparison_rows": len(rows),
        "config": {
            "price_tolerance": config.price_tolerance,
            "stale_threshold_seconds": config.stale_threshold_seconds,
            "minimum_opening_span_minutes": config.minimum_opening_span_minutes,
        },
    }
    if timing:
        run_metadata["timing"] = timing

    _write_outputs(
        rows,
        args.output_dir,
        run_id,
        run_metadata,
        odds_response_duration=timing.get("odds_duration_seconds"),
        historical_response_duration=timing.get("historical_duration_seconds"),
        odds_payload_size=timing.get("odds_payload_bytes"),
        historical_payload_size=timing.get("historical_payload_bytes"),
    )

    logger.info("Comparison complete. Output directory: %s", args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
