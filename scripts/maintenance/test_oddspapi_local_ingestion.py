"""Offline CLI that exercises the real OddsPapi ingestion path on a saved odds payload.

Real flow components used (no parallel mapping logic in this script):
1. modules.oddspapi.OddspapiEventResolver
2. infrastructure...MarketMappingRepository.build_index / resolve_*
3. modules.odds_ingestion.OddspapiMarketAdapter  (uses format_utils + DB mappings)
4. modules.odds_ingestion.MarketOddsIngestionService.save_from_oddspapi_response

Note: CanonicalMarketNormalizer is SofaScore-only. OddsPapi canonicalization happens via
market_source_mappings loaded by MarketMappingRepository, not that normalizer.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from app.logging_setup import setup_logging  # noqa: E402
from infrastructure.persistence.database import db_manager  # noqa: E402
from infrastructure.persistence.repositories.event_repository import EventRepository  # noqa: E402
from infrastructure.persistence.repositories.event_source_mapping_repository import (  # noqa: E402
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.market_mapping_repository import (  # noqa: E402
    MarketMappingRepository,
)
from infrastructure.persistence.repositories.odds_trajectory_repository import (  # noqa: E402
    OddsTrajectoryRepository,
)
from infrastructure.persistence.repositories.result_repository import ResultRepository  # noqa: E402
from infrastructure.settings import Config  # noqa: E402
from modules.odds_ingestion import (  # noqa: E402
    CanonicalMarketNormalizer,
    MarketOddsIngestionService,
    OddspapiMarketAdapter,
)
from modules.oddspapi import OddspapiEventResolver  # noqa: E402
from modules.oddspapi.format_utils import normalize_source_id  # noqa: E402
from modules.oddspapi.sport_filters import is_allowed_sport_id  # noqa: E402
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context  # noqa: E402

logger = logging.getLogger("test_oddspapi_local_ingestion")


def _enable_runtime_module_logging(level: int = logging.INFO) -> None:
    """Keep the script's real-module logs visible during local validation."""
    for logger_name in ("modules", "infrastructure", "app", "shared"):
        module_logger = logging.getLogger(logger_name)
        module_logger.setLevel(level)
        module_logger.propagate = True


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


def _normalized_bookmakers_summary(adapted: dict) -> list[dict]:
    summary = []
    for bookmaker in adapted.get("bookmakers") or []:
        markets = bookmaker.get("markets") or []
        summary.append(
            {
                "slug": bookmaker.get("slug"),
                "name": bookmaker.get("name"),
                "market_count": len(markets),
                "canonical_market_names": sorted(
                    {
                        market.get("marketName")
                        for market in markets
                        if market.get("marketName")
                    }
                ),
                "market_groups": sorted(
                    {
                        market.get("marketGroup")
                        for market in markets
                        if market.get("marketGroup")
                    }
                ),
                "market_periods": sorted(
                    {
                        market.get("marketPeriod")
                        for market in markets
                        if market.get("marketPeriod")
                    }
                ),
                "choice_count": sum(len(market.get("choices", [])) for market in markets),
            }
        )
    return summary


def _raw_payload_market_stats(odds_response: dict) -> dict:
    bookmaker_odds = odds_response.get("bookmakerOdds") or {}
    if isinstance(bookmaker_odds, dict):
        bookmakers = list(bookmaker_odds.values())
    elif isinstance(bookmaker_odds, list):
        bookmakers = bookmaker_odds
    else:
        bookmakers = []

    market_ids = []
    for bookmaker in bookmakers:
        if not isinstance(bookmaker, dict):
            continue
        markets = bookmaker.get("markets") or {}
        if isinstance(markets, dict):
            market_ids.extend(str(key) for key in markets.keys())
        elif isinstance(markets, list):
            for market in markets:
                if isinstance(market, dict) and market.get("marketId") is not None:
                    market_ids.append(str(market.get("marketId")))

    return {
        "raw_bookmaker_count": len(bookmakers),
        "raw_market_id_count": len(market_ids),
        "raw_unique_market_ids": len(set(market_ids)),
    }


def _adapted_market_breakdown(adapted: dict) -> dict:
    by_name = Counter()
    by_group = Counter()
    by_period = Counter()
    for bookmaker in adapted.get("bookmakers") or []:
        for market in bookmaker.get("markets") or []:
            by_name[market.get("marketName") or "null"] += 1
            by_group[market.get("marketGroup") or "null"] += 1
            by_period[market.get("marketPeriod") or "null"] += 1
    return {
        "by_canonical_market_name": dict(sorted(by_name.items())),
        "by_canonical_market_group": dict(sorted(by_group.items())),
        "by_canonical_market_period": dict(sorted(by_period.items())),
    }


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
    filtered_trajectory_context = trajectory_context
    if trajectory_market_groups:
        filtered_trajectory_context = filtered_trajectory_context.filter_by_market_groups(
            allowed_groups=set(trajectory_market_groups),
        )
    if trajectory_market_periods:
        filtered_trajectory_context = filtered_trajectory_context.filter_by_market_period(
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


def _enrich_market_examples(examples: list[dict], markets_index: dict) -> list[dict]:
    if not markets_index:
        return examples
    enriched = []
    for ex in examples:
        item = dict(ex)
        m_id = str(item.get("sourceMarketId"))
        cat_m = markets_index.get(m_id)
        if cat_m:
            item["marketName"] = cat_m.get("marketName")
            item["marketType"] = cat_m.get("marketType")
            item["period"] = cat_m.get("period")
            item["handicap"] = cat_m.get("handicap")
        enriched.append(item)
    return enriched


def _enrich_outcome_examples(
    examples: list[dict], markets_index: dict, outcomes_index: dict
) -> list[dict]:
    if not markets_index and not outcomes_index:
        return examples
    enriched = []
    for ex in examples:
        item = dict(ex)
        m_id = str(item.get("sourceMarketId"))
        o_id = str(item.get("sourceOutcomeId"))
        cat_m = markets_index.get(m_id)
        if cat_m:
            item["marketName"] = cat_m.get("marketName")
            item["marketType"] = cat_m.get("marketType")
        if o_id in outcomes_index:
            item["outcomeName"] = outcomes_index[o_id]
        enriched.append(item)
    return enriched


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate or persist a local OddsPapi odds response using the real ingestion "
            "modules (no HTTP calls)."
        )
    )
    parser.add_argument("--file", required=True, help="Local OddsPapi odds JSON file")
    parser.add_argument("--bookmakers-catalog", help="Optional local /v4/bookmakers JSON file")
    parser.add_argument(
        "--markets-catalog",
        help="Optional local markets catalog JSON file to enrich unmapped logs",
    )
    parser.add_argument(
        "--trajectory-market-groups",
        nargs="+",
        default=None,
        help="Market groups to keep in selected processing / trajectory filters. Omit for all.",
    )
    parser.add_argument(
        "--trajectory-market-periods",
        nargs="+",
        default=None,
        help="Market periods to keep in selected processing / trajectory filters. Omit for all.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Validate without database writes (default)")
    mode.add_argument("--commit", action="store_true", help="Persist mappings, bookies, and markets")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    setup_logging()
    _enable_runtime_module_logging()

    odds_response = _load_json(args.file)
    if not isinstance(odds_response, dict):
        logger.error("odds file must contain a JSON object")
        return 1

    bookmaker_catalog = _load_json(args.bookmakers_catalog)
    markets_catalog_raw = _load_json(args.markets_catalog)
    
    # Load markets catalog and build indexes if provided
    markets_index = {}
    outcomes_index = {}
    if markets_catalog_raw:
        market_items = []
        if isinstance(markets_catalog_raw, list):
            market_items = markets_catalog_raw
        elif isinstance(markets_catalog_raw, dict):
            market_items = markets_catalog_raw.get("markets") or []
        
        for item in market_items:
            if isinstance(item, dict):
                m_id = str(item.get("marketId"))
                markets_index[m_id] = item
                for out in item.get("outcomes") or []:
                    if isinstance(out, dict):
                        out_id = str(out.get("outcomeId"))
                        outcomes_index[out_id] = out.get("outcomeName")

    dry_run = not args.commit
    trajectory_market_groups = args.trajectory_market_groups or []
    trajectory_market_periods = args.trajectory_market_periods or []

    # Keep the SofaScore normalizer imported so this script documents the shared package,
    # but OddsPapi does not call it (adapter + DB mappings are the OddsPapi path).
    _ = CanonicalMarketNormalizer

    schema_ready = db_manager.check_and_migrate_schema()
    if not schema_ready:
        report = {
            "mode": "dry-run" if dry_run else "commit",
            "file": str(Path(args.file)),
            "schema_ready": False,
            "error": "failed to synchronize database schema before ingestion",
        }
        print(_json_dump(report))
        return 1

    source_sport_id = normalize_source_id(odds_response.get("sportId"))
    sport_allowed = is_allowed_sport_id(source_sport_id)

    # 1) Real event resolution module
    event_resolution = OddspapiEventResolver.resolve_from_odds_response(
        odds_response,
        create_mappings=False,
    )

    # 2) Real DB-backed market mapping index
    market_mapping_index = MarketMappingRepository.build_index(
        source="oddspapi",
        enabled_only=True,
    )

    # 3) Real OddsPapi adapter (uses MarketMappingRepository.resolve_market/outcome)
    adapted = OddspapiMarketAdapter.from_odds_response(
        odds_response,
        bookmaker_catalog=bookmaker_catalog,
        market_mapping_index=market_mapping_index,
        source="oddspapi",
    )
    selected_adapted = MarketOddsIngestionService.filter_normalized_oddspapi_response_by_groups_and_periods(
        adapted,
        allowed_market_groups=trajectory_market_groups,
        allowed_market_periods=trajectory_market_periods,
    )

    # 4) Real ingestion service (same path automation will call)
    result = MarketOddsIngestionService.save_from_oddspapi_response(
        odds_response,
        bookmaker_catalog=bookmaker_catalog,
        dry_run=dry_run,
        allowed_market_groups=trajectory_market_groups,
        allowed_market_periods=trajectory_market_periods,
        market_mapping_index=market_mapping_index,
    )

    external_providers = odds_response.get("externalProviders") or {}
    payload_sofascore_id = external_providers.get("sofascoreId")
    bookmakers = adapted.get("bookmakers", [])
    selected_bookmakers = selected_adapted.get("bookmakers", [])
    diagnostics = adapted.get("diagnostics") or {}

    input_markets_detected = sum(len(bookmaker.get("markets", [])) for bookmaker in bookmakers)
    input_choices_detected = sum(
        len(market.get("choices", []))
        for bookmaker in bookmakers
        for market in bookmaker.get("markets", [])
    )
    selected_markets_detected = sum(
        len(bookmaker.get("markets", [])) for bookmaker in selected_bookmakers
    )
    selected_choices_detected = sum(
        len(market.get("choices", []))
        for bookmaker in selected_bookmakers
        for market in bookmaker.get("markets", [])
    )

    resolved_canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
        "sofascore",
        payload_sofascore_id,
    )
    db_event_id = result.event_id if result.event_id is not None else resolved_canonical_event_id
    db_source_event_id = (
        EventSourceMappingRepository.get_source_event_id(db_event_id, "sofascore")
        if db_event_id is not None
        else None
    )

    if db_event_id is not None:
        db_context = _load_db_context(
            db_event_id,
            trajectory_market_groups,
            trajectory_market_periods,
        )
    else:
        db_context = {
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

    raw_unmapped = diagnostics.get("unmapped_markets") or []
    enriched_unmapped = _enrich_market_examples(raw_unmapped, markets_index)
    filtered_unmapped = [
        m for m in enriched_unmapped
        if not ("handicap" in (m.get("marketName") or "").lower() 
                and "asian handicap" not in (m.get("marketName") or "").lower() 
                and "european handicap" not in (m.get("marketName") or "").lower())
    ]

    report = {
        "mode": "dry-run" if dry_run else "commit",
        "file": str(Path(args.file)),
        "components_used": {
            "event_resolver": "modules.oddspapi.OddspapiEventResolver",
            "market_mapping_repository": "infrastructure.persistence.repositories.MarketMappingRepository",
            "oddspapi_adapter": "modules.odds_ingestion.OddspapiMarketAdapter",
            "ingestion_service": "modules.odds_ingestion.MarketOddsIngestionService",
            "canonical_market_normalizer": (
                "modules.odds_ingestion.CanonicalMarketNormalizer "
                "(SofaScore-only; not invoked on OddsPapi path)"
            ),
            "oddspapi_helpers": [
                "modules.oddspapi.format_utils",
                "modules.oddspapi.sport_filters",
            ],
        },
        "schema_ready": True,
        "payload": {
            "fixtureId": odds_response.get("fixtureId"),
            "sportId": source_sport_id,
            "sport_allowed": sport_allowed,
            "sofascoreId": payload_sofascore_id,
            **_raw_payload_market_stats(odds_response),
        },
        "event_resolution": {
            "resolved": event_resolution.resolved,
            "canonical_event_id": event_resolution.canonical_event_id,
            "match_method": event_resolution.match_method,
            "skipped_reason": event_resolution.skipped_reason,
            "confidence": event_resolution.confidence,
        },
        "market_mapping_index": {
            "source": "oddspapi",
            "enabled_only": True,
            "mapping_count": len(market_mapping_index.market_mappings),
            "outcome_mapping_count": len(market_mapping_index.outcome_mappings),
            "loaded": bool(market_mapping_index.market_mappings),
        },
        "adapter_output": {
            "bookies_detected": len(bookmakers),
            "markets_detected": input_markets_detected,
            "choices_detected": input_choices_detected,
            "selected_bookies_detected": len(selected_bookmakers),
            "selected_markets_detected": selected_markets_detected,
            "selected_choices_detected": selected_choices_detected,
            "canonical_breakdown": _adapted_market_breakdown(adapted),
            "bookmakers": _normalized_bookmakers_summary(adapted),
            "selected_bookmakers": _normalized_bookmakers_summary(selected_adapted),
            "diagnostics": {
                "unmapped_markets": len(diagnostics.get("unmapped_markets") or []),
                "unmapped_outcomes": len(diagnostics.get("unmapped_outcomes") or []),
                "skipped_missing_handicap": len(
                    diagnostics.get("skipped_missing_handicap") or []
                ),
                "unmapped_market_examples": filtered_unmapped[:20],
                "unmapped_outcome_examples": _enrich_outcome_examples(
                    (diagnostics.get("unmapped_outcomes") or [])[:20],
                    markets_index,
                    outcomes_index,
                ),
                "skipped_missing_handicap_examples": _enrich_market_examples(
                    (diagnostics.get("skipped_missing_handicap") or [])[:20],
                    markets_index,
                ),
            },
        },
        "ingestion_result": asdict(result),
        "event_match": event_match,
        "db_event": db_context["event_summary"]
        or {"status": "missing", "reason": event_match["reason"]},
        "db_result": db_context["result_summary"] or {"status": "missing"},
        "odds_trajectory": {
            "filters": {
                "target_minutes_expected": Config.PRE_START_ODDS_MOMENTS,
                "tolerance_minutes": Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES,
                "selected_market_groups": trajectory_market_groups or None,
                "selected_market_periods": trajectory_market_periods or None,
            },
            "raw_summary": _trajectory_counts(
                db_context["trajectory_context"],
                raw_row_count=len(db_context["trajectory_rows"]),
            ),
            "filtered_summary": _trajectory_counts(db_context["filtered_trajectory_context"]),
        },
    }

    print(_json_dump(report))
    return 0 if event_match["matched"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
