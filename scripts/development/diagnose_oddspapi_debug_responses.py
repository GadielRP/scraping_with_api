"""
Modular diagnostic tool for analyzing OddsPAPI raw debug response JSONs across key moments.

Discovers response files in any directory, resolves markets using the OddsPapi
catalog, tracks mainline outcomes dynamically across all available moments (T-120,
T-30, T-5, T-1, T-0, etc.), and highlights line shifts.

Usage:
  python scripts/development/diagnose_oddspapi_debug_responses.py debug/oddspapi_odds_responses
  python scripts/development/diagnose_oddspapi_debug_responses.py debug/oddspapi_odds_responses/209277_Celta_Vigo_Osasuna
  python scripts/development/diagnose_oddspapi_debug_responses.py path/to/file.json --bookmaker pinnacle
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import sys
from typing import Any


def resolve_latest_catalog(markets_dir: Path | None = None) -> Path:
    """Find the newest markets_*.json catalog dump."""
    if markets_dir is None:
        markets_dir = Path("odds_papi") / "markets_data"
    
    files = sorted(markets_dir.glob("markets_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No markets_*.json files found in {markets_dir.resolve()}")
    return files[0]


def load_markets_catalog(catalog_path: Path) -> dict[int, dict[str, Any]]:
    """Index catalog markets by marketId."""
    with catalog_path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {catalog_path}, got {type(data).__name__}")
    return {int(m["marketId"]): m for m in data if m.get("marketId") is not None}


def parse_moment_from_filename(filename: str) -> int | None:
    """Extract t_X minute token from filename (e.g. 209277_..._t_5_odds_... -> 5)."""
    match = re.search(r"_t_(-?\d+)_", filename)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def collect_response_files(target_path: Path) -> list[tuple[int | str, Path]]:
    """
    Collect JSON files and sort them by moment in descending order.
    Returns: list of (moment_key, file_path).
    """
    if target_path.is_file():
        files = [target_path]
    elif target_path.is_dir():
        files = sorted(target_path.glob("*.json"))
        # Also check subdirectories recursively if root debug folder is passed
        if not files:
            files = sorted(target_path.glob("*/*.json"))
    else:
        raise FileNotFoundError(f"Path does not exist: {target_path}")

    collected: list[tuple[int | str, Path]] = []
    for idx, f in enumerate(files):
        moment = parse_moment_from_filename(f.name)
        # Use integer moment for sorting if available, else string
        sort_key = moment if moment is not None else f"file_{idx}_{f.stem}"
        collected.append((sort_key, f))

    # Sort descending by moment if numeric (e.g. 120 -> 30 -> 5 -> 1 -> 0)
    collected.sort(
        key=lambda item: (
            0 if isinstance(item[0], (int, float)) else 1,
            -item[0] if isinstance(item[0], (int, float)) else str(item[0]),
        )
    )
    return collected


def parse_response_payload(
    payload: dict[str, Any],
    catalog: dict[int, dict[str, Any]],
    bookmaker_filter: str | None = None,
) -> dict[str, dict[tuple[str, str], list[dict[str, Any]]]]:
    """
    Extract structured market entries grouped by bookmaker and (market_type, period).
    """
    bm_odds = payload.get("bookmakerOdds") or {}
    parsed: dict[str, dict[tuple[str, str], list[dict[str, Any]]]] = {}

    for bm_name, bm_data in bm_odds.items():
        if bookmaker_filter and bm_name.lower() != bookmaker_filter.lower():
            continue

        markets = (bm_data or {}).get("markets") or {}
        by_group = defaultdict(list)

        for mid_str, mdata in markets.items():
            try:
                mid = int(mid_str)
            except ValueError:
                continue

            cat_m = catalog.get(mid, {})
            mname = cat_m.get("marketName") or f"Unknown_{mid}"
            mtype = cat_m.get("marketType") or "unknown"
            period = cat_m.get("period") or "unknown"
            handicap = cat_m.get("handicap")
            market_active = mdata.get("marketActive")

            outcomes_raw = (mdata or {}).get("outcomes") or {}
            mainline_outcomes = []
            active_outcomes = []

            cat_outcomes = {
                int(o["outcomeId"]): o.get("outcomeName")
                for o in cat_m.get("outcomes", [])
                if o.get("outcomeId") is not None
            }

            for oid_str, odata in outcomes_raw.items():
                try:
                    oid = int(oid_str)
                except ValueError:
                    continue
                outcome_name = cat_outcomes.get(oid) or f"Outcome_{oid}"
                players = (odata or {}).get("players") or {}

                for pid, pdata in players.items():
                    price = pdata.get("price")
                    is_active = pdata.get("active") is True
                    is_mainline = pdata.get("mainLine") is True
                    pname = pdata.get("playerName") or outcome_name

                    if is_mainline:
                        mainline_outcomes.append({
                            "outcome_id": oid,
                            "name": pname,
                            "price": price,
                            "active": is_active,
                        })
                    if is_active:
                        active_outcomes.append({
                            "outcome_id": oid,
                            "name": pname,
                            "price": price,
                            "main_line": is_mainline,
                        })

            by_group[(mtype, period)].append({
                "market_id": mid,
                "market_name": mname,
                "market_type": mtype,
                "period": period,
                "handicap": handicap,
                "market_active": market_active,
                "has_mainline": len(mainline_outcomes) > 0,
                "mainline_outcomes": mainline_outcomes,
                "active_outcomes": active_outcomes,
            })

        if by_group:
            parsed[bm_name] = by_group

    return parsed


def diagnose_directory(
    target_path: Path,
    catalog_path: Path | None = None,
    bookmaker_filter: str | None = None,
) -> None:
    """Run full diagnostic report on target path."""
    catalog_file = catalog_path or resolve_latest_catalog()
    catalog = load_markets_catalog(catalog_file)
    files = collect_response_files(target_path)

    if not files:
        print(f"No JSON response files found in: {target_path}")
        return

    print("=" * 90)
    print(f"ODDSPAPI DEBUG RESPONSES DIAGNOSIS")
    print(f"Target Path  : {target_path}")
    print(f"Catalog File : {catalog_file.name} ({len(catalog)} markets indexed)")
    print(f"Files Found  : {len(files)}")
    print("=" * 90)

    # Structure: moment_key -> parsed_by_bookie
    moments_data: dict[int | str, tuple[Path, dict[str, dict[tuple[str, str], list[dict[str, Any]]]]]] = {}
    all_bookmakers: set[str] = set()

    for moment_key, fpath in files:
        try:
            with fpath.open(encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            print(f"[ERROR] Failed to load {fpath.name}: {exc}")
            continue

        parsed = parse_response_payload(payload, catalog, bookmaker_filter=bookmaker_filter)
        moments_data[moment_key] = (fpath, parsed)
        all_bookmakers.update(parsed.keys())

    if not all_bookmakers:
        print("No matching bookmaker odds data found in the provided files.")
        return

    moment_keys = list(moments_data.keys())

    for bm in sorted(all_bookmakers):
        print(f"\n{'#' * 35} BOOKMAKER: {bm.upper()} {'#' * 35}")

        # Collect all unique market families (mtype, period) across all moments
        all_families = sorted(
            set(
                fam
                for m_key in moment_keys
                for fam in moments_data[m_key][1].get(bm, {}).keys()
            )
        )

        mainlines_per_moment: dict[int | str, list[str]] = defaultdict(list)
        shifts_detected: list[str] = []

        for mtype, period in all_families:
            family_header_printed = False
            prev_main_line: dict[str, Any] | None = None
            prev_m_key: int | str | None = None

            for m_key in moment_keys:
                fpath, bm_parsed = moments_data[m_key]
                entries = bm_parsed.get(bm, {}).get((mtype, period), [])
                mainline_entries = [e for e in entries if e["has_mainline"]]

                m_label = f"T-{m_key} min" if isinstance(m_key, (int, float)) else str(m_key)

                if mainline_entries:
                    for me in mainline_entries:
                        outs_str = ", ".join(f"{o['name']}={o['price']}" for o in me["mainline_outcomes"])
                        h_str = f"h={me['handicap']}" if me["handicap"] is not None else "no-line"
                        entry_str = f"{me['market_name']} [{h_str}] -> {outs_str}"
                        mainlines_per_moment[m_key].append(f"{entry_str} (mid={me['market_id']})")

                        # Check for line shifts
                        if prev_main_line is not None and prev_main_line["market_id"] != me["market_id"]:
                            shift_msg = (
                                f"  [SHIFT DETECTED] {me['market_name']} ({mtype}/{period}): "
                                f"{prev_m_label} mid={prev_main_line['market_id']} (h={prev_main_line['handicap']}) "
                                f"-> {m_label} mid={me['market_id']} (h={me['handicap']})"
                            )
                            shifts_detected.append(shift_msg)

                        prev_main_line = me
                        prev_m_label = m_label

        # Print Executive Summary for this bookmaker
        print(f"\n--- Summary Across Key Moments for {bm.upper()} ---")
        for m_key in moment_keys:
            m_label = f"T-{m_key:3d} min" if isinstance(m_key, (int, float)) else str(m_key)
            m_count = len(mainlines_per_moment[m_key])
            print(f"  {m_label}: {m_count:2d} mainline markets active")

        if shifts_detected:
            print(f"\n--- Line Shifts in Mainlines ({len(shifts_detected)}) ---")
            for shift in shifts_detected:
                print(shift)
        else:
            print("\n--- Line Shifts: None (all mainline handicaps remained stable across moments) ---")

        # Detailed breakdown of mainline markets
        print(f"\n--- Mainline Markets Detail ({bm.upper()}) ---")
        for m_key in moment_keys:
            m_label = f"T-{m_key} min" if isinstance(m_key, (int, float)) else str(m_key)
            print(f"\n  [{m_label}] ({len(mainlines_per_moment[m_key])} markets):")
            for line in mainlines_per_moment[m_key]:
                print(f"    * {line}")

    print("\n" + "=" * 90)
    print("Diagnosis completed successfully.")
    print("=" * 90)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnose OddsPAPI raw JSON debug responses across arbitrary key moments."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to response JSON file or directory containing debug JSON responses.",
    )
    parser.add_argument(
        "--markets-file",
        type=Path,
        default=None,
        help="Optional path to OddsPapi markets catalog JSON dump.",
    )
    parser.add_argument(
        "-b",
        "--bookmaker",
        type=str,
        default=None,
        help="Filter diagnosis by specific bookmaker (e.g. pinnacle, bet365, betfair-ex).",
    )

    args = parser.parse_args()

    try:
        diagnose_directory(
            target_path=args.path,
            catalog_path=args.markets_file,
            bookmaker_filter=args.bookmaker,
        )
    except Exception as exc:
        print(f"Error running diagnosis: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
