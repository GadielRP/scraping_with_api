"""
Identify OddsPapi markets/outcomes in saved odds JSON files.

Matches raw market IDs and outcome IDs from odds responses against a markets
catalog dump (e.g. markets_data/markets_*.json).

Example:
  python identify_odds_markets.py odds_data
  python identify_odds_markets.py odds_data --markets-file markets_data/markets_20260623_141927.json
  python identify_odds_markets.py odds_data --bookmaker bet365 --json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_MARKETS_DIR = PACKAGE_DIR / "markets_data"
DEFAULT_ODDS_DIR = PACKAGE_DIR / "odds_data"


def resolve_user_path(path: Path, *, prefer_dirs: list[Path] | None = None) -> Path:
    """
    Resolve a user-supplied path.

    Tries, in order:
      1. as given (cwd-relative or absolute)
      2. under each prefer_dirs entry (e.g. odds_papi/)
    """
    candidates = [path]
    if not path.is_absolute():
        for base in prefer_dirs or []:
            candidates.append(base / path)

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.exists():
            return resolved

    return path.resolve()


def load_markets_catalog(markets_file: Path) -> dict[int, dict[str, Any]]:
    """Index markets catalog by marketId."""
    with markets_file.open(encoding="utf-8") as f:
        markets = json.load(f)

    if not isinstance(markets, list):
        raise ValueError(f"Expected markets catalog to be a list, got {type(markets).__name__}")

    catalog: dict[int, dict[str, Any]] = {}
    for market in markets:
        market_id = market.get("marketId")
        if market_id is None:
            continue
        catalog[int(market_id)] = market
    return catalog


def resolve_latest_markets_file(markets_dir: Path) -> Path:
    files = sorted(markets_dir.glob("markets_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No markets_*.json files found in {markets_dir}")
    return files[0]


def extract_odds_markets(
    odds_payload: dict[str, Any],
    bookmaker_filter: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Extract market/outcome structure from an odds response.

    Returns:
      {
        bookmaker: {
          market_id: {
            market_active: bool,
            outcomes: {
              outcome_id: {
                prices: [...],
                player_names: [...],
                active_count: int,
              }
            }
          }
        }
      }
    """
    bookmaker_odds = odds_payload.get("bookmakerOdds") or {}
    result: dict[str, dict[str, Any]] = {}

    for bookmaker, bm_data in bookmaker_odds.items():
        if bookmaker_filter and bookmaker != bookmaker_filter:
            continue

        markets = (bm_data or {}).get("markets") or {}
        parsed_markets: dict[str, Any] = {}

        for market_id, market_data in markets.items():
            outcomes_raw = (market_data or {}).get("outcomes") or {}
            outcomes: dict[str, Any] = {}
            market_is_mainline = False

            for outcome_id, outcome_data in outcomes_raw.items():
                players = (outcome_data or {}).get("players") or {}
                prices = []
                player_names = []
                active_count = 0

                for player in players.values():
                    if not isinstance(player, dict):
                        continue
                    if player.get("active"):
                        active_count += 1
                    price = player.get("price")
                    if price is not None:
                        prices.append(price)
                    name = player.get("playerName")
                    if name:
                        player_names.append(name)
                    if player.get("mainLine") is True:
                        market_is_mainline = True

                outcomes[str(outcome_id)] = {
                    "prices": prices,
                    "player_names": player_names,
                    "active_count": active_count,
                }

            parsed_markets[str(market_id)] = {
                "market_active": (market_data or {}).get("marketActive"),
                "outcomes": outcomes,
                "is_mainline": market_is_mainline,
            }

        if parsed_markets:
            result[bookmaker] = parsed_markets

    return result


def enrich_with_catalog(
    extracted: dict[str, dict[str, Any]],
    catalog: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build a flat, human-friendly list of resolved markets."""
    rows: list[dict[str, Any]] = []

    for bookmaker, markets in extracted.items():
        for market_id_str, market_data in markets.items():
            try:
                market_id = int(market_id_str)
            except ValueError:
                market_id = None

            catalog_market = catalog.get(market_id) if market_id is not None else None
            catalog_outcomes = {
                int(o["outcomeId"]): o.get("outcomeName")
                for o in (catalog_market or {}).get("outcomes") or []
                if o.get("outcomeId") is not None
            }

            resolved_outcomes = []
            for outcome_id_str, outcome_data in market_data["outcomes"].items():
                try:
                    outcome_id = int(outcome_id_str)
                except ValueError:
                    outcome_id = None

                resolved_outcomes.append(
                    {
                        "outcome_id": outcome_id_str,
                        "outcome_name": catalog_outcomes.get(outcome_id) if outcome_id is not None else None,
                        "prices": outcome_data["prices"],
                        "player_names": outcome_data["player_names"],
                        "active_count": outcome_data["active_count"],
                        "matched": outcome_id in catalog_outcomes if outcome_id is not None else False,
                    }
                )

            rows.append(
                {
                    "bookmaker": bookmaker,
                    "market_id": market_id_str,
                    "market_name": (catalog_market or {}).get("marketName"),
                    "market_type": (catalog_market or {}).get("marketType"),
                    "period": (catalog_market or {}).get("period"),
                    "handicap": (catalog_market or {}).get("handicap"),
                    "sport_id": (catalog_market or {}).get("sportId"),
                    "player_prop": (catalog_market or {}).get("playerProp"),
                    "market_active": market_data["market_active"],
                    "matched": catalog_market is not None,
                    "outcomes": resolved_outcomes,
                    "is_mainline": market_data.get("is_mainline", False),
                }
            )

    rows.sort(key=lambda r: (r["bookmaker"], int(r["market_id"]) if str(r["market_id"]).isdigit() else r["market_id"]))
    return rows


def format_report(
    file_path: Path,
    odds_payload: dict[str, Any],
    rows: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    fixture_id = odds_payload.get("fixtureId", "?")
    sport_id = odds_payload.get("sportId", "?")
    tournament_id = odds_payload.get("tournamentId", "?")

    matched = sum(1 for r in rows if r["matched"])
    unmatched = len(rows) - matched

    lines.append("=" * 80)
    lines.append(f"File: {file_path.name}")
    lines.append(
        f"Fixture: {fixture_id} | sportId={sport_id} | tournamentId={tournament_id} | "
        f"markets={len(rows)} (matched={matched}, unmatched={unmatched})"
    )
    lines.append("=" * 80)

    by_bookmaker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_bookmaker[row["bookmaker"]].append(row)

    for bookmaker, bm_rows in by_bookmaker.items():
        lines.append("")
        lines.append(f"[{bookmaker}] {len(bm_rows)} markets")
        lines.append("-" * 80)

        for row in bm_rows:
            status = "OK" if row["matched"] else "MISSING"
            handicap = row["handicap"]
            handicap_str = f" | handicap={handicap}" if handicap not in (None, 0) else ""
            period = row["period"]
            period_str = f" | period={period}" if period else ""
            mtype = row["market_type"] or "?"
            name = row["market_name"] or "<unknown market>"

            lines.append(
                f"  [{status}] marketId={row['market_id']}: {name} "
                f"(type={mtype}{period_str}{handicap_str})"
            )

            for outcome in row["outcomes"]:
                o_status = "" if outcome["matched"] else " [UNMATCHED OUTCOME]"
                o_name = outcome["outcome_name"] or "<unknown>"
                prices = ", ".join(str(p) for p in outcome["prices"]) or "-"
                players = ", ".join(outcome["player_names"])
                players_str = f" | players={players}" if players else ""
                lines.append(
                    f"      outcomeId={outcome['outcome_id']}: {o_name} "
                    f"| price={prices}{players_str}{o_status}"
                )

    return "\n".join(lines)


def collect_odds_files(odds_path: Path, odds_file_name: str | None = None) -> list[Path]:
    """Accept either a directory of odds JSON files or a single odds JSON file."""
    if odds_path.is_file():
        if odds_file_name and odds_path.name != odds_file_name:
            raise FileNotFoundError(
                f"--file {odds_file_name} does not match provided path {odds_path.name}"
            )
        return [odds_path]

    if not odds_path.is_dir():
        raise FileNotFoundError(f"Odds path not found: {odds_path}")

    files = sorted(p for p in odds_path.glob("*.json") if p.is_file())
    if odds_file_name:
        files = [p for p in files if p.name == odds_file_name]
        if not files:
            raise FileNotFoundError(f"File not found in {odds_path}: {odds_file_name}")

    if not files:
        raise FileNotFoundError(f"No .json files found in {odds_path}")
    return files


def build_summary(all_rows: list[dict[str, Any]]) -> str:
    unique_markets: dict[str, dict[str, Any]] = {}
    for row in all_rows:
        unique_markets[row["market_id"]] = row

    by_name: dict[str, list[str]] = defaultdict(list)
    unmatched_ids: list[str] = []
    for market_id, row in sorted(
        unique_markets.items(),
        key=lambda item: int(item[0]) if item[0].isdigit() else item[0],
    ):
        if row["matched"]:
            label = row["market_name"] or "<unnamed>"
            by_name[label].append(market_id)
        else:
            unmatched_ids.append(market_id)

    lines = [
        "",
        "=" * 80,
        "SUMMARY (unique market IDs across all files)",
        "=" * 80,
        f"Unique markets: {len(unique_markets)} | unmatched IDs: {len(unmatched_ids)}",
        "",
    ]

    for name, ids in sorted(by_name.items(), key=lambda item: item[0].lower()):
        lines.append(f"  {name}")
        lines.append(f"    marketIds: {', '.join(ids)}")

    if unmatched_ids:
        lines.append("")
        lines.append("  Unmatched market IDs:")
        lines.append(f"    {', '.join(unmatched_ids)}")

    return "\n".join(lines)


def build_mainline_summary(all_rows: list[dict[str, Any]]) -> str:
    by_bookmaker_markets: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in all_rows:
        if row.get("is_mainline"):
            bm = row["bookmaker"]
            mid = row["market_id"]
            by_bookmaker_markets[bm][mid] = row

    lines = [
        "",
        "=" * 80,
        "MAINLINE MARKETS (unique market IDs across all files)",
        "=" * 80,
    ]

    for bm in sorted(by_bookmaker_markets.keys()):
        bm_markets = by_bookmaker_markets[bm]
        
        by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
        unmatched_ids: list[str] = []
        for market_id, row in sorted(
            bm_markets.items(),
            key=lambda item: int(item[0]) if item[0].isdigit() else item[0],
        ):
            if row["matched"]:
                label = row["market_name"] or "<unnamed>"
                by_name[label].append(row)
            else:
                unmatched_ids.append(market_id)

        lines.append("")
        lines.append(f"[{bm}] {len(bm_markets)} mainline markets | unmatched IDs: {len(unmatched_ids)}")
        lines.append("-" * 80)

        for name, rows_for_name in sorted(by_name.items(), key=lambda item: item[0].lower()):
            lines.append(f"  {name}")
            id_strs = []
            for r in rows_for_name:
                hc = r.get("handicap")
                if hc not in (None, 0):
                    id_strs.append(f"{r['market_id']} (handicap={hc})")
                else:
                    id_strs.append(r['market_id'])
            lines.append(f"    marketIds: {', '.join(id_strs)}")

        if unmatched_ids:
            lines.append("")
            lines.append("  Unmatched market IDs:")
            lines.append(f"    {', '.join(unmatched_ids)}")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve OddsPapi odds market/outcome IDs against a markets catalog. "
            "Pass a directory of odds JSON files, or a single odds JSON file."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples (from repo root):\n"
            "  python -m odds_papi.identify_odds_markets "
            "--file odds_papi/odds_data/odds_id1300010963302195.json\n"
            "  python -m odds_papi.identify_odds_markets "
            "odds_papi/odds_data/odds_id1300010963302195.json\n"
            "  python -m odds_papi.identify_odds_markets odds_data "
            "--file odds_id1300010963302195.json\n"
            "\n"
            "Relative paths are resolved from the current directory first, then under odds_papi/."
        ),
    )
    parser.add_argument(
        "odds_path",
        type=Path,
        nargs="?",
        default=DEFAULT_ODDS_DIR,
        help="Odds JSON directory or single file (default: odds_papi/odds_data)",
    )
    parser.add_argument(
        "--markets-file",
        type=Path,
        default=None,
        help="Path to markets catalog JSON. Defaults to newest markets_*.json in markets_data/",
    )
    parser.add_argument(
        "--markets-dir",
        type=Path,
        default=DEFAULT_MARKETS_DIR,
        help="Directory used to auto-pick the newest markets catalog when --markets-file is omitted",
    )
    parser.add_argument(
        "--bookmaker",
        default=None,
        help="Only include this bookmaker key (e.g. bet365, pinnacle)",
    )
    parser.add_argument(
        "--file",
        dest="odds_file",
        default=None,
        help=(
            "Single odds JSON file path, or a filename inside odds_path "
            "(e.g. odds_id1300010963302195.json)"
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of the text report",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only print the unique-markets summary across files",
    )
    parser.add_argument(
        "--mainline",
        action="store_true",
        help="Include a section with mainline markets by market name in the text report",
    )

    args = parser.parse_args()

    # --file may be a full/relative path to one odds JSON, or just a basename filter.
    odds_file_filter: str | None = args.odds_file
    odds_path_arg = args.odds_path
    if args.odds_file:
        candidate = resolve_user_path(
            Path(args.odds_file),
            prefer_dirs=[PACKAGE_DIR, PACKAGE_DIR / "odds_data", DEFAULT_ODDS_DIR],
        )
        if candidate.is_file():
            odds_path_arg = candidate
            odds_file_filter = None

    odds_path = resolve_user_path(odds_path_arg, prefer_dirs=[PACKAGE_DIR])

    markets_dir = resolve_user_path(args.markets_dir, prefer_dirs=[PACKAGE_DIR])
    if args.markets_file is None:
        try:
            markets_file = resolve_latest_markets_file(markets_dir)
        except FileNotFoundError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
    else:
        markets_file = resolve_user_path(args.markets_file, prefer_dirs=[PACKAGE_DIR, markets_dir])

    if not markets_file.is_file():
        print(f"Error: markets file not found: {markets_file}", file=sys.stderr)
        return 1

    print(f"Using markets catalog: {markets_file}", file=sys.stderr)
    catalog = load_markets_catalog(markets_file)
    print(f"Loaded {len(catalog)} markets from catalog", file=sys.stderr)

    try:
        odds_files = collect_odds_files(odds_path, odds_file_name=odds_file_filter)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    reports: list[str] = []
    json_payload: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []

    for odds_file_path in odds_files:
        with odds_file_path.open(encoding="utf-8") as f:
            odds_payload = json.load(f)

        extracted = extract_odds_markets(odds_payload, bookmaker_filter=args.bookmaker)
        rows = enrich_with_catalog(extracted, catalog)
        all_rows.extend(rows)

        file_result = {
            "file": odds_file_path.name,
            "fixture_id": odds_payload.get("fixtureId"),
            "sport_id": odds_payload.get("sportId"),
            "tournament_id": odds_payload.get("tournamentId"),
            "markets": rows,
        }
        json_payload.append(file_result)

        if not args.summary_only:
            reports.append(format_report(odds_file_path, odds_payload, rows))

    if args.json:
        print(json.dumps({"markets_file": str(markets_file), "files": json_payload}, indent=2))
    else:
        if reports:
            print("\n\n".join(reports))
        print(build_summary(all_rows))
        if args.mainline:
            print(build_mainline_summary(all_rows))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
