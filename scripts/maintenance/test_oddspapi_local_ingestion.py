"""Offline-only CLI for validating a saved OddsPapi odds payload."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from dataclasses import asdict

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from infrastructure.persistence.database import db_manager  # noqa: E402
from infrastructure.persistence.repositories.event_repository import EventRepository  # noqa: E402
from infrastructure.persistence.repositories.event_source_mapping_repository import (  # noqa: E402
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.odds_trajectory_repository import (  # noqa: E402
    OddsTrajectoryRepository,
)
from infrastructure.persistence.repositories.result_repository import ResultRepository  # noqa: E402
from infrastructure.settings import Config  # noqa: E402
from modules.odds_ingestion import OddspapiMarketAdapter  # noqa: E402
from modules.odds_ingestion.market_odds_ingestion_service import (  # noqa: E402
    MarketOddsIngestionService,
)
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context  # noqa: E402


def _load_json(path: str | None):
    if not path:
        return None
    with Path(path).open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def _json_dump(data) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def _event_summary(event) -> dict:
    if event is None:
        return {}

    return {
        "id": event.id,
        "slug": event.slug,
        "custom_id": event.custom_id,
        "start_time_utc": event.start_time_utc,
        "sport": event.sport,
        "country": event.country,
        "competition_id": event.competition_id,
        "competition_display_name": getattr(event.competition_ref, "display_name", None),
        "home_participant_id": event.home_participant_id,
        "away_participant_id": event.away_participant_id,
        "home_participant_name": getattr(event.home_participant, "name", None),
        "away_participant_name": getattr(event.away_participant, "name", None),
        "season_id": event.season_id,
        "discovery_source": event.discovery_source,
        "legacy_home_team": event.home_team,
        "legacy_away_team": event.away_team,
        "legacy_competition": event.competition,
        "round": event.round,
        "alert_sent": event.alert_sent,
    }


def _result_summary(result) -> dict:
    if result is None:
        return {}

    return {
        "event_id": result.event_id,
        "home_score": result.home_score,
        "away_score": result.away_score,
        "winner": result.winner,
        "home_sets": result.home_sets,
        "away_sets": result.away_sets,
    }


def _trajectory_counts(context, raw_row_count: int | None = None) -> dict:
    market_groups = context.markets or {}
    market_period_count = 0
    market_name_count = 0
    choice_group_count = 0
    bookie_count = 0
    choice_count = 0
    odds_point_count = 0

    structure = {}
    for market_group, periods in market_groups.items():
        structure[market_group] = {}
        market_period_count += len(periods)
        for market_period, market_names in periods.items():
            structure[market_group][market_period] = sorted(market_names.keys())
            market_name_count += len(market_names)
            for choice_groups in market_names.values():
                choice_group_count += len(choice_groups)
                for market_line in choice_groups.values():
                    bookie_count += len(market_line.bookies)
                    for bookie in market_line.bookies.values():
                        choice_count += len(bookie.choices)
                        for choice in bookie.choices.values():
                            odds_point_count += len(choice.odds_values)

    summary = {
        "available": context.available,
        "event_id": context.event_id,
        "target_minutes_expected": context.target_minutes_expected,
        "target_minutes_present": context.target_minutes_present,
        "missing_target_minutes": context.missing_target_minutes,
        "market_group_count": len(market_groups),
        "market_period_count": market_period_count,
        "market_name_count": market_name_count,
        "choice_group_count": choice_group_count,
        "bookie_count": bookie_count,
        "choice_count": choice_count,
        "odds_point_count": odds_point_count,
        "structure": structure,
    }
    if raw_row_count is not None:
        summary["raw_row_count"] = raw_row_count
    return summary


def _trajectory_detail(context) -> dict:
    payload = asdict(context)
    payload["markets"] = payload.get("markets") or {}
    return payload


def _event_match_status(
    payload_source_event_id,
    service_event_id: int | None,
    resolved_canonical_event_id,
    db_source_event_id,
    event,
) -> dict:
    if service_event_id is None:
        return {
            "status": "failed",
            "reason": "oddspapi_event_not_resolved",
            "payload_source_event_id": payload_source_event_id,
            "resolved_canonical_event_id": resolved_canonical_event_id,
            "db_source_event_id": db_source_event_id,
            "canonical_event_id": None,
            "matched": False,
        }

    if resolved_canonical_event_id is None:
        return {
            "status": "failed",
            "reason": "source_event_not_resolved_in_db",
            "payload_source_event_id": payload_source_event_id,
            "resolved_canonical_event_id": resolved_canonical_event_id,
            "db_source_event_id": db_source_event_id,
            "canonical_event_id": service_event_id,
            "matched": False,
        }

    if event is None:
        return {
            "status": "failed",
            "reason": "canonical_event_row_missing",
            "payload_source_event_id": payload_source_event_id,
            "resolved_canonical_event_id": resolved_canonical_event_id,
            "db_source_event_id": db_source_event_id,
            "canonical_event_id": service_event_id,
            "matched": False,
        }

    if db_source_event_id is None:
        return {
            "status": "failed",
            "reason": "missing_sofascore_source_mapping",
            "payload_source_event_id": payload_source_event_id,
            "resolved_canonical_event_id": resolved_canonical_event_id,
            "db_source_event_id": db_source_event_id,
            "canonical_event_id": service_event_id,
            "matched": False,
        }

    if str(payload_source_event_id) != str(db_source_event_id):
        return {
            "status": "failed",
            "reason": "source_event_id_mismatch",
            "payload_source_event_id": payload_source_event_id,
            "resolved_canonical_event_id": resolved_canonical_event_id,
            "db_source_event_id": db_source_event_id,
            "canonical_event_id": service_event_id,
            "matched": False,
        }

    if int(service_event_id) != int(resolved_canonical_event_id):
        return {
            "status": "failed",
            "reason": "canonical_event_id_mismatch",
            "payload_source_event_id": payload_source_event_id,
            "resolved_canonical_event_id": resolved_canonical_event_id,
            "db_source_event_id": db_source_event_id,
            "canonical_event_id": service_event_id,
            "matched": False,
        }

    return {
        "status": "matched",
        "reason": "matched_via_sofascore_source_mapping",
        "payload_source_event_id": payload_source_event_id,
        "resolved_canonical_event_id": resolved_canonical_event_id,
        "db_source_event_id": db_source_event_id,
        "canonical_event_id": service_event_id,
        "matched": True,
    }


def _load_db_context(event_id: int, trajectory_market_groups, trajectory_market_periods):
    event = EventRepository.get_event_by_id(event_id)
    result = ResultRepository.get_result_by_event_id(event_id)
    source_event_id = EventSourceMappingRepository.get_source_event_id(event_id, "sofascore")
    trajectory_rows_by_event_id = OddsTrajectoryRepository.get_pre_start_trajectory_map(
        event_ids=[event_id],
        target_minutes=Config.PRE_START_ODDS_MOMENTS,
        tolerance_minutes=Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES,
    )
    trajectory_rows = [point.to_dict() for point in trajectory_rows_by_event_id.get(event_id, [])]
    trajectory_context = build_odds_trajectory_context(trajectory_rows)
    filtered_trajectory_context = trajectory_context.filter_by_market_groups(
        allowed_groups=set(trajectory_market_groups),
    ).filter_by_market_period(
        allowed_periods=set(trajectory_market_periods),
    )

    return {
        "event": event,
        "result": result,
        "source_event_id": source_event_id,
        "event_summary": _event_summary(event),
        "result_summary": _result_summary(result),
        "trajectory_rows": trajectory_rows,
        "trajectory_context": trajectory_context,
        "filtered_trajectory_context": filtered_trajectory_context,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate or persist a local OddsPapi odds response without HTTP calls."
    )
    parser.add_argument("--file", required=True, help="Local OddsPapi odds JSON file")
    parser.add_argument("--markets-catalog", help="Optional local /v4/markets JSON file")
    parser.add_argument("--bookmakers-catalog", help="Optional local /v4/bookmakers JSON file")
    parser.add_argument(
        "--trajectory-market-groups",
        nargs="+",
        default=None,
        help="Market groups to keep in the printed odds trajectory (default: 1X2 Home/Away ML)",
    )
    parser.add_argument(
        "--trajectory-market-periods",
        nargs="+",
        default=None,
        help="Market periods to keep in the printed odds trajectory (default: Full-time Match)",
    )
    parser.add_argument(
        "--show-raw-trajectory",
        action="store_true",
        help="Also print the unfiltered odds trajectory context",
    )
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
    trajectory_market_groups = args.trajectory_market_groups or ["1X2", "Home/Away", "ML"]
    trajectory_market_periods = args.trajectory_market_periods or ["Full-time", "Match"]

    schema_ready = db_manager.check_and_migrate_schema()
    print(f"schema_ready: {schema_ready}")
    if not schema_ready:
        print("schema_error: failed to synchronize database schema before ingestion")
        return 1

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
    payload_sofascore_id = external_providers.get("sofascoreId")
    bookmakers = adapted.get("bookmakers", [])
    markets_detected = sum(len(bookmaker.get("markets", [])) for bookmaker in bookmakers)
    resolved_canonical_event_id = EventSourceMappingRepository.get_event_id_by_source("sofascore", payload_sofascore_id)
    db_event_id = result.event_id if result.event_id is not None else resolved_canonical_event_id
    db_source_event_id = EventSourceMappingRepository.get_source_event_id(db_event_id, "sofascore") if db_event_id is not None else None

    db_context = _load_db_context(db_event_id, trajectory_market_groups, trajectory_market_periods) if db_event_id is not None else {
        "event": None,
        "result": None,
        "source_event_id": None,
        "event_summary": {},
        "result_summary": {},
        "trajectory_rows": [],
        "trajectory_context": build_odds_trajectory_context([]),
        "filtered_trajectory_context": build_odds_trajectory_context([]),
    }

    event_match = _event_match_status(
        payload_source_event_id=payload_sofascore_id,
        service_event_id=result.event_id,
        resolved_canonical_event_id=resolved_canonical_event_id,
        db_source_event_id=db_source_event_id,
        event=db_context["event"],
    )

    print(f"fixtureId: {odds_response.get('fixtureId')}")
    print(f"externalProviders.sofascoreId: {payload_sofascore_id}")
    print(f"canonical_event_id: {result.event_id}")
    print(f"resolved_canonical_event_id_from_db: {resolved_canonical_event_id}")
    print(f"db_source_event_id: {db_source_event_id}")
    print(f"db_event_id: {db_event_id}")
    print(f"resolved: {result.event_id is not None}")
    print(f"matched_in_db: {event_match['matched']}")
    print(f"match_reason: {event_match['reason']}")
    print(f"skipped: {result.skipped}")
    print(f"skipped_reason: {result.reason}")
    print(f"event_mappings_created: {result.event_mappings_created}")
    print(f"bookies_detected: {result.bookies_detected}")
    print(f"bookies_processed: {result.bookies_processed}")
    print(f"bookies_created: {result.bookies_created}")
    print(f"bookies_reused: {result.bookies_reused}")
    print(f"bookie_mappings_created: {result.bookie_mappings_created}")
    print(f"bookie_mappings_updated: {result.bookie_mappings_updated}")
    print(f"markets_detected: {markets_detected}")
    print(f"choices_detected: {result.choices_detected}")
    print(f"snapshots_detected: {result.snapshots_detected}")
    print(f"markets_saved: {result.markets_saved}")
    print(f"choices_saved: {result.choices_saved}")
    print(f"snapshots_saved: {result.snapshots_saved}")
    print(f"mode: {'dry-run' if dry_run else 'commit'}")
    print("db_event:")
    print(_json_dump(db_context["event_summary"] or {"status": "missing", "reason": event_match["reason"]}))
    print("db_result:")
    print(_json_dump(db_context["result_summary"] or {"status": "missing"}))
    print("odds_trajectory_query_filters:")
    print(_json_dump({
        "target_minutes_expected": Config.PRE_START_ODDS_MOMENTS,
        "tolerance_minutes": Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES,
        "config_markets": Config.PRE_START_ODDS_TRAJECTORY_MARKETS,
        "config_periods": Config.PRE_START_ODDS_TRAJECTORY_PERIODS,
        "selected_market_groups": trajectory_market_groups,
        "selected_market_periods": trajectory_market_periods,
    }))
    print("odds_trajectory_db_context_summary:")
    print(_json_dump(_trajectory_counts(db_context["trajectory_context"], raw_row_count=len(db_context["trajectory_rows"]))))
    print("odds_trajectory_pillar_filtered_context_summary:")
    print(_json_dump(_trajectory_counts(db_context["filtered_trajectory_context"])))
    print("odds_trajectory_filtered_context:")
    print(_json_dump(_trajectory_detail(db_context["filtered_trajectory_context"])))
    if args.show_raw_trajectory:
        print("odds_trajectory_db_context:")
        print(_json_dump(_trajectory_detail(db_context["trajectory_context"])))
    return 0 if event_match["matched"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
