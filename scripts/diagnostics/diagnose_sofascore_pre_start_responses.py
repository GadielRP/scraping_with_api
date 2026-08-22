"""Audit consistency of captured SofaScore odds responses by event/moment.

The script is offline-only. It reads the raw files written by the pre-start
SofaScore debug capture and reports:

* missing or unexpected moment files per event;
* invalid JSON and event/source-id mismatches;
* market and choice counts, including live/non-live distribution;
* markets that disappear, appear, or change live state between moments;
* choices that are not present at every observed moment.

Examples::

    python -m scripts.diagnostics.diagnose_sofascore_pre_start_responses
    python -m scripts.diagnostics.diagnose_sofascore_pre_start_responses \
        --input-dir debug/sofascore_odds_responses \
        --moments 120,30,5,1,0,-5 \
        --event-id 191099
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable


_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


DEFAULT_INPUT_DIRECTORY = Path("debug") / "sofascore_odds_responses"
DEFAULT_MOMENTS = (120, 30, 5, 1, 0, -5)
REPORT_FILENAME_PREFIX = "sofascore_pre_start_consistency_report"
_FILENAME_RE = re.compile(
    r"^(?P<event_id>\d+)_(?P<source_event_id>\d+)_t_(?P<moment>-?\d+(?:\.\d+)?)\.json$",
    re.IGNORECASE,
)


def _coerce_moment(value: str | int | float) -> int | float:
    number = float(value)
    return int(number) if number.is_integer() else number


def _moment_sort_key(moment: int | float) -> tuple[int, float]:
    return (0, -float(moment))


def parse_moments(value: str) -> tuple[int | float, ...]:
    """Parse ``120,30,5,1,0,-5`` or a Python/JSON-style list."""

    text = str(value or "").strip()
    if not text:
        raise argparse.ArgumentTypeError("moments cannot be empty")
    try:
        parsed: Any = ast.literal_eval(text)
        values = parsed if isinstance(parsed, (list, tuple)) else [parsed]
    except (SyntaxError, ValueError):
        values = [item.strip() for item in text.split(",") if item.strip()]

    try:
        moments = tuple(dict.fromkeys(_coerce_moment(item) for item in values))
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            f"invalid moments value: {value!r}"
        ) from exc
    if not moments:
        raise argparse.ArgumentTypeError("moments cannot be empty")
    return moments


def _configured_moments(project_root: Path) -> tuple[int | float, ...]:
    """Read the configured list without importing the application settings."""

    raw_value = os.getenv("PRE_START_ODDS_MOMENTS")
    if raw_value is None:
        env_path = project_root / ".env"
        if env_path.is_file():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if line.strip().startswith("PRE_START_ODDS_MOMENTS="):
                    raw_value = line.split("=", 1)[1].strip()
                    break
    if raw_value:
        try:
            return parse_moments(raw_value)
        except argparse.ArgumentTypeError:
            pass
    return DEFAULT_MOMENTS


def _json_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _market_base_identity(market: dict[str, Any]) -> tuple[Any, ...]:
    """Identify a logical market while ignoring provider-generated IDs/state."""

    return (
        _json_scalar(market.get("marketId")),
        _text(market.get("marketName")),
        _text(market.get("marketPeriod")),
        _text(market.get("marketGroup")),
        _text(market.get("choiceGroup")),
    )


def _market_state_identity(market: dict[str, Any]) -> tuple[Any, ...]:
    return _market_base_identity(market) + (bool(market.get("isLive")),)


def _choice_identity(market: dict[str, Any], choice: dict[str, Any]) -> tuple[Any, ...]:
    return _market_base_identity(market) + (_text(choice.get("name")),)


def _choice_state_identity(
    market: dict[str, Any],
    choice: dict[str, Any],
) -> tuple[Any, ...]:
    return _market_state_identity(market) + (_text(choice.get("name")),)


def _key(value: tuple[Any, ...]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _key_label(value: str) -> list[Any]:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return [value]


def _payload_event_id(payload: dict[str, Any]) -> int | None:
    value = payload.get("eventId")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class CapturedResponse:
    event_id: int
    source_event_id: int
    moment: int | float
    path: Path
    payload: dict[str, Any]
    file_size_bytes: int
    market_count: int
    choice_count: int
    invalid_market_count: int
    invalid_choice_count: int
    live_market_count: int
    non_live_market_count: int
    market_base_keys: frozenset[str]
    market_state_keys: frozenset[str]
    choice_keys: frozenset[str]
    duplicate_market_state_keys: tuple[str, ...]
    duplicate_choice_keys: tuple[str, ...]


def _load_response(path: Path, event_id: int, source_event_id: int, moment: int | float) -> CapturedResponse:
    with path.open(encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    if not isinstance(payload, dict):
        raise ValueError("top-level JSON value is not an object")

    raw_markets = payload.get("markets")
    markets = raw_markets if isinstance(raw_markets, list) else []
    market_base_keys: list[str] = []
    market_state_keys: list[str] = []
    choice_keys: list[str] = []
    choice_state_keys: list[str] = []
    invalid_market_count = 0
    invalid_choice_count = 0
    live_market_count = 0
    non_live_market_count = 0
    choice_count = 0

    for market in markets:
        if not isinstance(market, dict):
            invalid_market_count += 1
            continue
        market_base_keys.append(_key(_market_base_identity(market)))
        market_state_keys.append(_key(_market_state_identity(market)))
        if bool(market.get("isLive")):
            live_market_count += 1
        else:
            non_live_market_count += 1

        raw_choices = market.get("choices")
        choices = raw_choices if isinstance(raw_choices, list) else []
        for choice in choices:
            if not isinstance(choice, dict):
                invalid_choice_count += 1
                continue
            choice_count += 1
            choice_keys.append(_key(_choice_identity(market, choice)))
            choice_state_keys.append(_key(_choice_state_identity(market, choice)))

    return CapturedResponse(
        event_id=event_id,
        source_event_id=source_event_id,
        moment=moment,
        path=path,
        payload=payload,
        file_size_bytes=path.stat().st_size,
        market_count=len(markets),
        choice_count=choice_count,
        invalid_market_count=invalid_market_count,
        invalid_choice_count=invalid_choice_count,
        live_market_count=live_market_count,
        non_live_market_count=non_live_market_count,
        market_base_keys=frozenset(market_base_keys),
        market_state_keys=frozenset(market_state_keys),
        choice_keys=frozenset(choice_keys),
        duplicate_market_state_keys=tuple(
            sorted(key for key, count in Counter(market_state_keys).items() if count > 1)
        ),
        duplicate_choice_keys=tuple(
            sorted(key for key, count in Counter(choice_state_keys).items() if count > 1)
        ),
    )


def _relative_path(path: Path) -> str:
    try:
        return str(path.relative_to(_PROJECT_ROOT))
    except ValueError:
        return str(path)


def discover_responses(
    input_directory: Path,
    *,
    event_id_filter: int | None = None,
    source_event_id_filter: int | None = None,
) -> tuple[dict[int, dict[int | float, list[CapturedResponse]]], list[dict[str, Any]]]:
    responses: dict[int, dict[int | float, list[CapturedResponse]]] = defaultdict(
        lambda: defaultdict(list)
    )
    errors: list[dict[str, Any]] = []
    if not input_directory.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {input_directory}")

    for path in sorted(input_directory.glob("*.json")):
        if path.stem.startswith(REPORT_FILENAME_PREFIX):
            continue
        match = _FILENAME_RE.match(path.name)
        if not match:
            errors.append({"path": _relative_path(path), "error": "unrecognized_filename"})
            continue
        parsed_event_id = int(match.group("event_id"))
        parsed_source_event_id = int(match.group("source_event_id"))
        moment = _coerce_moment(match.group("moment"))
        if event_id_filter is not None and parsed_event_id != event_id_filter:
            continue
        if (
            source_event_id_filter is not None
            and parsed_source_event_id != source_event_id_filter
        ):
            continue
        try:
            response = _load_response(
                path,
                parsed_event_id,
                parsed_source_event_id,
                moment,
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            errors.append({"path": _relative_path(path), "error": str(exc)})
            continue
        responses[parsed_event_id][moment].append(response)
    return responses, errors


def _presence_map(
    responses: Iterable[CapturedResponse],
    attribute: str,
) -> dict[str, set[int | float]]:
    presence: dict[str, set[int | float]] = defaultdict(set)
    for response in responses:
        for key in getattr(response, attribute):
            presence[key].add(response.moment)
    return presence


def _sorted_moments(values: Iterable[int | float]) -> list[int | float]:
    return sorted(values, key=_moment_sort_key)


def _response_summary(response: CapturedResponse, expected_moments: tuple[int | float, ...]) -> dict[str, Any]:
    payload_event_id_int = _payload_event_id(response.payload)
    return {
        "path": _relative_path(response.path),
        "source_event_id_from_filename": response.source_event_id,
        "payload_event_id": payload_event_id_int,
        "payload_event_id_matches_filename": payload_event_id_int == response.source_event_id,
        "moment": response.moment,
        "expected_moment": response.moment in expected_moments,
        "file_size_bytes": response.file_size_bytes,
        "market_count": response.market_count,
        "choice_count": response.choice_count,
        "live_market_count": response.live_market_count,
        "non_live_market_count": response.non_live_market_count,
        "invalid_market_count": response.invalid_market_count,
        "invalid_choice_count": response.invalid_choice_count,
        "duplicate_market_state_count": len(response.duplicate_market_state_keys),
        "duplicate_choice_count": len(response.duplicate_choice_keys),
    }


def _event_report(
    event_id: int,
    by_moment: dict[int | float, list[CapturedResponse]],
    expected_moments: tuple[int | float, ...],
) -> dict[str, Any]:
    observed_moments = set(by_moment)
    all_responses = [response for items in by_moment.values() for response in items]
    source_event_ids = sorted({response.source_event_id for response in all_responses})
    duplicate_moment_files = {
        moment: len(items) for moment, items in by_moment.items() if len(items) > 1
    }
    valid_moments = set(expected_moments).intersection(observed_moments)

    market_presence = _presence_map(all_responses, "market_base_keys")
    choice_presence = _presence_map(all_responses, "choice_keys")
    market_state_presence = _presence_map(all_responses, "market_state_keys")

    market_keys_by_moment = {
        moment: set().union(*(response.market_base_keys for response in items))
        for moment, items in by_moment.items()
    }
    choice_keys_by_moment = {
        moment: set().union(*(response.choice_keys for response in items))
        for moment, items in by_moment.items()
    }
    union_market_keys = set(market_presence)
    union_choice_keys = set(choice_presence)

    market_coverage = {
        key: {
            "identity": _key_label(key),
            "present_moments": _sorted_moments(market_presence[key]),
            "missing_expected_moments": [
                moment for moment in expected_moments if moment not in market_presence[key]
            ],
            "live_states_observed": sorted(
                bool(_key_label(state_key)[-1])
                for state_key, moments in market_state_presence.items()
                if _key_label(state_key)[:-1] == _key_label(key)
                and moments
            ),
        }
        for key in sorted(union_market_keys)
    }
    choice_coverage = {
        key: {
            "identity": _key_label(key),
            "present_moments": _sorted_moments(choice_presence[key]),
            "missing_expected_moments": [
                moment for moment in expected_moments if moment not in choice_presence[key]
            ],
        }
        for key in sorted(union_choice_keys)
    }

    market_presence_by_moment = {}
    for moment in _sorted_moments(observed_moments):
        market_presence_by_moment[str(moment)] = {
            "market_count": len(market_keys_by_moment[moment]),
            "missing_vs_event_union": [
                _key_label(key)
                for key in sorted(union_market_keys - market_keys_by_moment[moment])
            ],
            "only_in_this_moment": [
                _key_label(key)
                for key in sorted(
                    market_keys_by_moment[moment]
                    - set().union(
                        *(
                            market_keys_by_moment[other]
                            for other in observed_moments
                            if other != moment
                        )
                    )
                )
            ],
        }

    choice_presence_by_moment = {}
    for moment in _sorted_moments(observed_moments):
        choice_presence_by_moment[str(moment)] = {
            "choice_count": len(choice_keys_by_moment[moment]),
            "missing_vs_event_union": [
                _key_label(key)
                for key in sorted(union_choice_keys - choice_keys_by_moment[moment])
            ],
        }

    transition_markets = [
        details
        for details in market_coverage.values()
        if len(details["live_states_observed"]) > 1
    ]
    payload_event_id_mismatches = [
        _relative_path(response.path)
        for response in all_responses
        if _payload_event_id(response.payload) != response.source_event_id
    ]

    return {
        "event_id": event_id,
        "source_event_ids": source_event_ids,
        "expected_moments": list(expected_moments),
        "observed_moments": _sorted_moments(observed_moments),
        "present_expected_moments": [
            moment for moment in expected_moments if moment in valid_moments
        ],
        "missing_expected_moments": [
            moment for moment in expected_moments if moment not in observed_moments
        ],
        "unexpected_observed_moments": [
            moment for moment in _sorted_moments(observed_moments)
            if moment not in expected_moments
        ],
        "complete_moment_file_set": set(expected_moments).issubset(observed_moments),
        "duplicate_moment_files": {
            str(moment): count for moment, count in sorted(duplicate_moment_files.items())
        },
        "payload_event_id_mismatches": payload_event_id_mismatches,
        "moment_summaries": {
            str(moment): [
                _response_summary(response, expected_moments)
                for response in by_moment[moment]
            ]
            for moment in _sorted_moments(observed_moments)
        },
        "market_consistency": {
            "event_union_market_count": len(union_market_keys),
            "markets_missing_at_some_expected_moment": sum(
                bool(details["missing_expected_moments"])
                for details in market_coverage.values()
            ),
            "markets_with_live_and_non_live_states": len(transition_markets),
            "market_presence_by_moment": market_presence_by_moment,
            "markets": market_coverage,
        },
        "choice_consistency": {
            "event_union_choice_count": len(union_choice_keys),
            "choices_missing_at_some_expected_moment": sum(
                bool(details["missing_expected_moments"])
                for details in choice_coverage.values()
            ),
            "choice_presence_by_moment": choice_presence_by_moment,
            "choices": choice_coverage,
        },
    }


def build_report(
    responses: dict[int, dict[int | float, list[CapturedResponse]]],
    errors: list[dict[str, Any]],
    expected_moments: tuple[int | float, ...],
    input_directory: Path,
) -> dict[str, Any]:
    event_reports = [
        _event_report(event_id, responses_by_moment, expected_moments)
        for event_id, responses_by_moment in sorted(responses.items())
    ]
    complete_events = sum(report["complete_moment_file_set"] for report in event_reports)
    return {
        "diagnostic": "sofascore_pre_start_response_consistency",
        "input_directory": _relative_path(input_directory),
        "expected_moments": list(expected_moments),
        "files_analyzed": sum(
            len(items)
            for responses_by_moment in responses.values()
            for items in responses_by_moment.values()
        ),
        "events_analyzed": len(event_reports),
        "events_with_complete_moment_file_set": complete_events,
        "events_with_missing_moments": len(event_reports) - complete_events,
        "files_with_errors": len(errors),
        "errors": errors,
        "events": event_reports,
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# SofaScore pre-start response consistency report",
        "",
        f"- Input directory: `{report['input_directory']}`",
        f"- Expected moments: `{report['expected_moments']}`",
        f"- Files analyzed: **{report['files_analyzed']}**",
        f"- Events analyzed: **{report['events_analyzed']}**",
        f"- Events with complete moment files: **{report['events_with_complete_moment_file_set']}**",
        f"- Events with missing moments: **{report['events_with_missing_moments']}**",
        f"- Files with errors: **{report['files_with_errors']}**",
        "",
        "## Event summary",
        "",
        "| Event | Source event | Present | Missing | Market union | Choice union | Live/non-live transitions |",
        "|---:|---:|---|---|---:|---:|---:|",
    ]
    for event in report["events"]:
        lines.append(
            "| {event_id} | {source_event} | {present} | {missing} | {markets} | {choices} | {transitions} |".format(
                event_id=event["event_id"],
                source_event=",".join(str(value) for value in event["source_event_ids"]),
                present=",".join(str(value) for value in event["observed_moments"]),
                missing=",".join(str(value) for value in event["missing_expected_moments"]) or "—",
                markets=event["market_consistency"]["event_union_market_count"],
                choices=event["choice_consistency"]["event_union_choice_count"],
                transitions=event["market_consistency"]["markets_with_live_and_non_live_states"],
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- A missing moment file means the job did not save a response for that event/moment.",
            "- A market or choice missing inside an existing raw response is provider-level absence, not a database persistence failure.",
            "- A market with both live and non-live states changed representation across moments; this is expected around kickoff and is reported explicitly.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    default_moments = _configured_moments(_PROJECT_ROOT)
    parser = argparse.ArgumentParser(
        description="Audit raw SofaScore odds responses across configured pre-start moments."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIRECTORY,
        help="Directory containing event_source_t_moment.json files.",
    )
    parser.add_argument(
        "--moments",
        type=parse_moments,
        default=default_moments,
        help=f"Expected moments, e.g. 120,30,5,1,0,-5 (default: {default_moments}).",
    )
    parser.add_argument("--event-id", type=int, help="Analyze only one canonical event id.")
    parser.add_argument(
        "--source-event-id",
        type=int,
        help="Analyze only one SofaScore source event id.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="JSON report path (default: <input-dir>/sofascore_pre_start_consistency_report.json).",
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        help="Markdown summary path (default: <input-dir>/sofascore_pre_start_consistency_report.md).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Return exit code 1 when files/moments are missing or malformed.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    input_directory = args.input_dir.resolve()
    output_json = (
        args.output_json
        or input_directory / "sofascore_pre_start_consistency_report.json"
    ).resolve()
    output_markdown = (
        args.output_markdown
        or input_directory / "sofascore_pre_start_consistency_report.md"
    ).resolve()

    responses, errors = discover_responses(
        input_directory,
        event_id_filter=args.event_id,
        source_event_id_filter=args.source_event_id,
    )
    report = build_report(responses, errors, tuple(args.moments), input_directory)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    _write_markdown(report, output_markdown)

    print(
        "SofaScore diagnostic completed: "
        f"files={report['files_analyzed']} "
        f"events={report['events_analyzed']} "
        f"complete_events={report['events_with_complete_moment_file_set']} "
        f"events_with_missing_moments={report['events_with_missing_moments']} "
        f"file_errors={report['files_with_errors']}"
    )
    print(f"JSON report: {output_json}")
    print(f"Markdown report: {output_markdown}")

    has_issues = bool(
        report["files_with_errors"]
        or report["events_with_missing_moments"]
        or any(
            event["duplicate_moment_files"]
            or event["payload_event_id_mismatches"]
            for event in report["events"]
        )
    )
    return 1 if args.fail_on_issues and has_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
