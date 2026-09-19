"""Offline CLI that exercises the real OddsPapi event resolution flow on a saved payload.

This harness is intentionally narrow:
- it accepts only one CLI argument: a local OddsPapi fixture-response JSON file
- it calls the real resolver path used by production
- it verifies the persisted result by checking for either:
  - an `event_source_mapping` row, when the event is resolved, or
  - a `event_source_resolution_queue` row, when the event needs review

The goal is to validate the updated normalizer, matcher, resolver, and persistence
behavior without re-implementing any matching logic here.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from app.logging_setup import setup_logging  # noqa: E402

logger = logging.getLogger("test_oddspapi_event_resolution")
MAX_SINGLE_FIXTURE_CANDIDATES_TO_REPORT = 10
MAX_MULTI_FIXTURE_CANDIDATES_TO_REPORT = 2


def _load_json(path: str | None):
    if not path:
        return None
    with Path(path).open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def _json_dump(data) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def _configure_runtime_logging() -> None:
    """Let the real module logs reach the console during this standalone validation run."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for logger_name in (
        "modules",
        "modules.oddspapi",
        "modules.odds_ingestion",
        "infrastructure",
        "infrastructure.persistence",
        "infrastructure.persistence.repositories",
        "shared",
    ):
        module_logger = logging.getLogger(logger_name)
        module_logger.setLevel(logging.INFO)
        module_logger.propagate = True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate OddsPapi event resolution, candidate matching, and persistence "
            "from a local fixture-response or fixtures-response JSON file."
        )
    )
    parser.add_argument("--file", required=True, help="Local OddsPapi fixture-response JSON file")
    parser.add_argument(
        "--input-type",
        choices=("auto", "fixture_response", "fixtures_response"),
        default="auto",
        help=(
            "Override JSON shape detection. Use fixture_response for a single dict, "
            "fixtures_response for a list of dicts, or auto to infer it from the file."
        ),
    )
    return parser


def _truncate_candidate_scores(
    resolution_report: dict[str, object],
    *,
    max_candidates: int,
) -> dict[str, object]:
    candidate_scores = resolution_report.get("candidate_scores")
    if not isinstance(candidate_scores, list):
        return resolution_report

    trimmed = dict(resolution_report)
    trimmed["candidate_scores_total"] = len(candidate_scores)
    trimmed["candidate_scores"] = candidate_scores[:max_candidates]
    trimmed["candidate_scores_shown"] = min(len(candidate_scores), max_candidates)
    trimmed["candidate_scores_truncated"] = len(candidate_scores) > max_candidates
    return trimmed


def _format_values(values: list[str] | None) -> str:
    if not values:
        return "[]"
    return "[" + ", ".join(values) + "]"


def _log_success_resolution_details(
    *,
    index: int,
    odds_response: dict[str, object],
    resolution_report: dict[str, object],
) -> None:
    from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity  # noqa: E402

    fixture = OddspapiFixtureIdentity.from_payload(odds_response)
    best_candidate = (resolution_report.get("candidate_scores") or [{}])[0] or {}

    logger.info("Resolved item #%s fixture_id=%s", index, fixture.fixture_id)
    logger.info(
        "  Fixture participants: p1=%s p2=%s",
        _format_values([
            str(value)
            for value in [fixture.participant1_name, fixture.participant1_short_name, fixture.participant1_abbr]
            if value
        ]),
        _format_values([
            str(value)
            for value in [fixture.participant2_name, fixture.participant2_short_name, fixture.participant2_abbr]
            if value
        ]),
    )
    logger.info(
        "  Fixture tournament: tournament=%s category=%s",
        _format_values([value for value in [fixture.tournament_name, fixture.tournament_slug] if value]),
        _format_values([value for value in [fixture.category_name, fixture.category_slug] if value]),
    )
    logger.info(
        "  Matched candidate event_id=%s orientation=%s score=%.3f tournament_score=%.3f participant1_score=%.3f participant2_score=%.3f",
        best_candidate.get("event_id"),
        best_candidate.get("orientation"),
        float(best_candidate.get("score") or 0.0),
        float(best_candidate.get("tournament_score") or 0.0),
        float(best_candidate.get("participant1_score") or 0.0),
        float(best_candidate.get("participant2_score") or 0.0),
    )
    logger.info(
        "  Candidate strings: fixture_p1=%s fixture_p2=%s event_home=%s event_away=%s event_tournament=%s",
        _format_values(best_candidate.get("fixture_participant1_values") or []),
        _format_values(best_candidate.get("fixture_participant2_values") or []),
        _format_values(best_candidate.get("event_home_values") or []),
        _format_values(best_candidate.get("event_away_values") or []),
        _format_values(best_candidate.get("event_tournament_values") or []),
    )


def _coerce_input_payloads(raw_payload, input_type: str) -> list[dict]:
    if input_type == "fixture_response":
        if not isinstance(raw_payload, dict):
            raise ValueError("fixture_response input must be a single JSON object")
        return [raw_payload]

    if input_type == "fixtures_response":
        if not isinstance(raw_payload, list) or any(not isinstance(item, dict) for item in raw_payload):
            raise ValueError("fixtures_response input must be a JSON list of objects")
        return raw_payload

    if isinstance(raw_payload, dict):
        return [raw_payload]
    if isinstance(raw_payload, list) and all(isinstance(item, dict) for item in raw_payload):
        return raw_payload
    raise ValueError("input file must contain either a JSON object or a list of JSON objects")


def _resolution_persistence_status(
    fixture_id: str,
    *,
    db_manager,
    event_source_mapping_repository,
    event_source_resolution_queue,
) -> dict[str, object]:
    with db_manager.get_session() as session:
        mapping_event_id = event_source_mapping_repository.get_event_id_by_source(
            "oddspapi",
            fixture_id,
            session=session,
        )
        queue_row = (
            session.query(event_source_resolution_queue)
            .filter(
                event_source_resolution_queue.source == "oddspapi",
                event_source_resolution_queue.source_event_id == fixture_id,
            )
            .first()
        )

        return {
            "oddspapi_mapping_event_id": mapping_event_id,
            "queue_row_exists": queue_row is not None,
            "queue_row_status": queue_row.resolution_status if queue_row else None,
            "queue_row_best_candidate_event_id": queue_row.best_candidate_event_id if queue_row else None,
            "queue_row_best_candidate_confidence": float(queue_row.best_candidate_confidence)
            if queue_row and queue_row.best_candidate_confidence is not None
            else None,
        }


def main() -> int:
    args = build_parser().parse_args()
    setup_logging()
    _configure_runtime_logging()

    from infrastructure.persistence.database import db_manager  # noqa: E402
    from infrastructure.persistence.models import EventSourceResolutionQueue  # noqa: E402
    from infrastructure.persistence.repositories.event_source_mapping_repository import (  # noqa: E402
        EventSourceMappingRepository,
    )
    from modules.oddspapi import OddspapiEventResolver  # noqa: E402

    raw_payload = _load_json(args.file)
    try:
        payloads = _coerce_input_payloads(raw_payload, args.input_type)
    except ValueError as exc:
        logger.error(str(exc))
        return 1

    schema_ready = db_manager.verify_schema_at_head()
    if not schema_ready:
        report = {
            "mode": "validation",
            "file": str(Path(args.file)),
            "schema_ready": False,
            "error": "failed to synchronize database schema before event resolution",
        }
        print(_json_dump(report))
        return 1

    item_reports: list[dict[str, object]] = []
    all_successful = True
    candidate_cap = (
        MAX_MULTI_FIXTURE_CANDIDATES_TO_REPORT
        if isinstance(raw_payload, list)
        else MAX_SINGLE_FIXTURE_CANDIDATES_TO_REPORT
    )

    for index, odds_response in enumerate(payloads, start=1):
        resolution = OddspapiEventResolver.resolve_from_fixture_response(odds_response)
        persistence_status = _resolution_persistence_status(
            resolution.oddspapi_fixture_id,
            db_manager=db_manager,
            event_source_mapping_repository=EventSourceMappingRepository,
            event_source_resolution_queue=EventSourceResolutionQueue,
        )

        item_success = (
            (resolution.resolved and persistence_status["oddspapi_mapping_event_id"] is not None and persistence_status["queue_row_exists"] is False)
            or (not resolution.resolved and persistence_status["queue_row_exists"] is True)
        )
        all_successful = all_successful and item_success

        if resolution.resolved:
            _log_success_resolution_details(
                index=index,
                odds_response=odds_response,
                resolution_report=_truncate_candidate_scores(
                    asdict(resolution),
                    max_candidates=candidate_cap,
                ),
            )

        item_reports.append(
            {
                "index": index,
                "fixture_id": resolution.oddspapi_fixture_id,
                "resolution": _truncate_candidate_scores(
                    asdict(resolution),
                    max_candidates=candidate_cap,
                ),
                "persistence": persistence_status,
                "success": item_success,
            }
        )

    failed_persisted_items = [
        item
        for item in item_reports
        if (not item["resolution"]["resolved"]) and item["persistence"]["queue_row_exists"]
    ]

    report = {
        "mode": "validation",
        "file": str(Path(args.file)),
        "input_type": args.input_type,
        "effective_input_type": "fixtures_response" if isinstance(raw_payload, list) else "fixture_response",
        "processed_count": len(item_reports),
        "failed_persisted_count": len(failed_persisted_items),
        "failed_persisted_items": failed_persisted_items,
        "resolved_count": sum(1 for item in item_reports if item["resolution"]["resolved"]),
        "unresolved_count": sum(1 for item in item_reports if not item["resolution"]["resolved"]),
        "success": all_successful,
    }
    print(_json_dump(report))
    return 0 if all_successful else 1


if __name__ == "__main__":
    raise SystemExit(main())
