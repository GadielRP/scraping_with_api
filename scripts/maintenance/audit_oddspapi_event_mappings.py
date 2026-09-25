"""Read-only audit of Oddspapi event mappings against Sofascore participants.

Run from the repository root. By default the script streams rows from the
configured database. --input-csv also accepts a PostgreSQL COPY snapshot, so
the scoring policy can be revised without querying the database again.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
from pathlib import Path
import random
import re
import sys
import tempfile
import textwrap
import unicodedata
from typing import Iterable, Mapping


AUDIT_SQL = """
SELECT o.mapping_id, o.event_id, o.source_event_id, e.sport, e.starts_at,
       o.match_method, o.confidence,
       o.source_participant_home_id AS odd_home_fk,
       o.source_participant_away_id AS odd_away_fk,
       oh.source AS odd_home_source,
       oa.source AS odd_away_source,
       oh.source_participant_id AS odd_home_source_id,
       oa.source_participant_id AS odd_away_source_id,
       oh.name AS odd_home_name, oh.short_name AS odd_home_short_name,
       oh.code_name AS odd_home_code_name,
       oa.name AS odd_away_name, oa.short_name AS odd_away_short_name,
       oa.code_name AS odd_away_code_name,
       sm.mapping_id AS sofa_mapping_id,
       sm.source_participant_home_id AS sofa_mapping_home_fk,
       sm.source_participant_away_id AS sofa_mapping_away_fk,
       e.home_participant_id AS event_home_fk,
       e.away_participant_id AS event_away_fk,
       COALESCE(sh.source_participant_id, eh.source_participant_id) AS sofa_home_source_id,
       COALESCE(sa.source_participant_id, ea.source_participant_id) AS sofa_away_source_id,
       COALESCE(sh.name, eh.name) AS sofa_home_name,
       COALESCE(sh.short_name, eh.short_name) AS sofa_home_short_name,
       COALESCE(sh.code_name, eh.code_name) AS sofa_home_code_name,
       COALESCE(sa.name, ea.name) AS sofa_away_name,
       COALESCE(sa.short_name, ea.short_name) AS sofa_away_short_name,
       COALESCE(sa.code_name, ea.code_name) AS sofa_away_code_name
FROM event_source_mappings AS o
JOIN events AS e ON e.id = o.event_id
LEFT JOIN participants AS oh ON oh.participant_id = o.source_participant_home_id
LEFT JOIN participants AS oa ON oa.participant_id = o.source_participant_away_id
LEFT JOIN event_source_mappings AS sm
       ON sm.event_id = o.event_id AND sm.source = 'sofascore'
LEFT JOIN participants AS sh
       ON sh.participant_id = sm.source_participant_home_id AND sh.source = 'sofascore'
LEFT JOIN participants AS sa
       ON sa.participant_id = sm.source_participant_away_id AND sa.source = 'sofascore'
LEFT JOIN participants AS eh
       ON eh.participant_id = e.home_participant_id AND eh.source = 'sofascore'
LEFT JOIN participants AS ea
       ON ea.participant_id = e.away_participant_id AND ea.source = 'sofascore'
WHERE o.source = 'oddspapi'
  AND (CAST(:sport AS text) IS NULL OR e.sport = :sport)
  AND (CAST(:start_from AS timestamptz) IS NULL OR e.starts_at >= :start_from)
  AND (CAST(:start_to AS timestamptz) IS NULL OR e.starts_at < :start_to)
  AND (CAST(:match_method AS text) IS NULL OR o.match_method = :match_method)
ORDER BY o.mapping_id
""".strip()

REVIEW_FIELDS = (
    "mapping_id", "event_id", "source_event_id", "sport", "starts_at",
    "match_method", "confidence", "confidence_signal", "classification", "reason",
    "direct_home_score", "direct_away_score", "reverse_home_score",
    "reverse_away_score", "odd_home_source_id", "odd_away_source_id",
    "sofa_home_source_id", "sofa_away_source_id", "odd_home_name",
    "odd_home_short_name", "odd_home_code_name", "odd_away_name",
    "odd_away_short_name", "odd_away_code_name", "sofa_home_name",
    "sofa_home_short_name", "sofa_home_code_name", "sofa_away_name",
    "sofa_away_short_name", "sofa_away_code_name",
    "direct_home_primary_score", "direct_away_primary_score",
    "reverse_home_primary_score", "reverse_away_primary_score",
    "code_name_contributed",
)
PROBLEM_FIELDS = ("priority", "problem_codes", *REVIEW_FIELDS)


def confidence_signal(method: object, value: object) -> str:
    """Describe provenance of the stored score; it is not calibrated accuracy."""
    if method == "external_provider_sofascore_id":
        return "provider_reference"
    if method != "deterministic_candidate_match":
        return "other_method"
    if not _nonempty(value):
        return "candidate_missing_score"
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "candidate_invalid_score"
    if score >= 0.95:
        return "candidate_high_composite"
    if score >= 0.91:
        return "candidate_above_auto_link_floor"
    # The identity path can bypass 0.91, so this is not intrinsically invalid.
    return "candidate_below_floor_identity_possible"


def normalize_name(value: object) -> str:
    raw = str(value or "").strip()
    if "," in raw and len(raw.split(",")) == 2:
        family, given = raw.split(",", 1)
        raw = f"{given.strip()} {family.strip()}"
    raw = unicodedata.normalize("NFKD", raw.casefold())
    raw = "".join(char for char in raw if not unicodedata.combining(char))
    return " ".join(re.findall(r"[a-z0-9]+", raw))


def name_score(left: object, right: object) -> float:
    a, b = normalize_name(left), normalize_name(right)
    if not a or not b:
        return 0.0
    if a == b or sorted(a.split()) == sorted(b.split()):
        return 1.0
    return max(
        SequenceMatcher(None, a, b).ratio(),
        SequenceMatcher(None, " ".join(sorted(a.split())), " ".join(sorted(b.split()))).ratio(),
    )


def participant_score(row: Mapping, left_prefix: str, right_prefix: str) -> float:
    left = [row.get(f"{left_prefix}_{field}") for field in ("name", "short_name", "code_name")]
    right = [row.get(f"{right_prefix}_{field}") for field in ("name", "short_name", "code_name")]
    return max((name_score(a, b) for a in left for b in right), default=0.0)


def primary_participant_score(row: Mapping, left_prefix: str, right_prefix: str) -> float:
    left = [row.get(f"{left_prefix}_{field}") for field in ("name", "short_name")]
    right = [row.get(f"{right_prefix}_{field}") for field in ("name", "short_name")]
    return max((name_score(a, b) for a in left for b in right), default=0.0)


def evaluate(row: Mapping, *, strong: float, weak: float, reversal_margin: float) -> dict:
    result = dict(row)
    reasons = []
    for side in ("home", "away"):
        fk = row.get(f"odd_{side}_fk")
        source = row.get(f"odd_{side}_source")
        if _nonempty(fk) and source != "oddspapi":
            reasons.append(f"odd_{side}_wrong_source")
    if not _nonempty(row.get("sofa_mapping_id")):
        reasons.append("missing_sofascore_mapping")
    for side in ("home", "away"):
        mapping_fk = row.get(f"sofa_mapping_{side}_fk")
        event_fk = row.get(f"event_{side}_fk")
        if _nonempty(mapping_fk) and _nonempty(event_fk) and str(mapping_fk) != str(event_fk):
            reasons.append(f"sofascore_{side}_fk_disagrees_with_event")

    complete = all(
        any(_nonempty(row.get(f"{source}_{side}_{field}")) for field in ("name", "short_name", "code_name"))
        for source in ("odd", "sofa") for side in ("home", "away")
    )
    direct_home = participant_score(row, "odd_home", "sofa_home")
    direct_away = participant_score(row, "odd_away", "sofa_away")
    reverse_home = participant_score(row, "odd_home", "sofa_away")
    reverse_away = participant_score(row, "odd_away", "sofa_home")
    direct_home_primary = primary_participant_score(row, "odd_home", "sofa_home")
    direct_away_primary = primary_participant_score(row, "odd_away", "sofa_away")
    reverse_home_primary = primary_participant_score(row, "odd_home", "sofa_away")
    reverse_away_primary = primary_participant_score(row, "odd_away", "sofa_home")
    direct_min = min(direct_home, direct_away)
    reverse_min = min(reverse_home, reverse_away)
    direct_primary_min = min(direct_home_primary, direct_away_primary)
    reverse_primary_min = min(reverse_home_primary, reverse_away_primary)
    code_name_only_match = direct_min >= strong and direct_primary_min < weak

    if reasons and any("wrong_source" in reason for reason in reasons):
        classification = "integrity_error"
    elif not complete:
        classification = "unverifiable"
        reasons.append("missing_participant_names")
    elif reverse_min >= strong and reverse_min - direct_min >= reversal_margin and reverse_primary_min < weak:
        classification = "review"
        reasons.append("code_name_only_reversed_match")
    elif reverse_min >= strong and reverse_primary_min >= weak and reverse_min - direct_min >= reversal_margin:
        classification = "reversed"
    elif code_name_only_match:
        classification = "review"
        reasons.append("code_name_only_match")
    elif direct_home_primary == direct_away_primary == 1.0:
        classification = "exact"
    elif direct_min >= strong:
        classification = "strong"
    elif direct_home < weak and direct_away < weak and reverse_min < weak:
        classification = "mismatch"
    else:
        classification = "review"

    result.update(
        classification=classification,
        reason=",".join(reasons),
        confidence_signal=confidence_signal(row.get("match_method"), row.get("confidence")),
        direct_home_score=round(direct_home, 3),
        direct_away_score=round(direct_away, 3),
        reverse_home_score=round(reverse_home, 3),
        reverse_away_score=round(reverse_away, 3),
        direct_home_primary_score=round(direct_home_primary, 3),
        direct_away_primary_score=round(direct_away_primary, 3),
        reverse_home_primary_score=round(reverse_home_primary, 3),
        reverse_away_primary_score=round(reverse_away_primary, 3),
        code_name_contributed=any((
            direct_home > direct_home_primary,
            direct_away > direct_away_primary,
            reverse_home > reverse_home_primary,
            reverse_away > reverse_away_primary,
        )),
    )
    return result


def _nonempty(value: object) -> bool:
    return value is not None and str(value).strip() != ""


def _remember_link(links: dict, left: object, right: object) -> None:
    if _nonempty(left) and _nonempty(right):
        links[str(left)].add(str(right))


def problem_codes(
    row: Mapping, *, odd_conflicts: set[str], sofa_conflicts: set[str],
    confidence_min: float, weak: float, include_incomplete: bool,
) -> list[str]:
    codes = []
    category = row.get("classification")
    if category in {"mismatch", "reversed", "integrity_error"}:
        codes.append(str(category))
    reasons = set(filter(None, str(row.get("reason") or "").split(",")))
    if "missing_sofascore_mapping" in reasons:
        codes.append("missing_sofascore_mapping")
    if "code_name_only_match" in reasons:
        codes.append("code_name_only_match")
    if "code_name_only_reversed_match" in reasons:
        codes.append("code_name_only_reversed_match")
    if any(reason.endswith("_fk_disagrees_with_event") for reason in reasons):
        codes.append("sofascore_participant_fk_disagreement")
    if include_incomplete and category == "unverifiable":
        codes.append("incomplete_participants")
    if any(str(row.get(f"odd_{side}_source_id")) in odd_conflicts for side in ("home", "away")):
        codes.append("oddspapi_participant_identity_conflict")
    if any(str(row.get(f"sofa_{side}_source_id")) in sofa_conflicts for side in ("home", "away")):
        codes.append("sofascore_participant_identity_conflict")
    if row.get("confidence_signal") in {"candidate_missing_score", "candidate_invalid_score"}:
        codes.append("candidate_confidence_missing_or_invalid")
    if category == "review" and min(
        float(row["direct_home_score"]), float(row["direct_away_score"])
    ) < weak:
        try:
            score = float(row.get("confidence"))
        except (TypeError, ValueError):
            score = 0.0
        if score >= confidence_min:
            if row.get("match_method") == "external_provider_sofascore_id":
                codes.append("provider_reference_weak_name_side")
            elif row.get("match_method") == "deterministic_candidate_match":
                codes.append("high_composite_weak_name_side")
    return codes


def iter_problematic_records(
    rows: Iterable[Mapping], *, odd_conflicts: set[str],
    sofa_conflicts: set[str], confidence_min: float, weak: float,
    include_incomplete: bool,
):
    for row in rows:
        codes = problem_codes(
            row, odd_conflicts=odd_conflicts, sofa_conflicts=sofa_conflicts,
            confidence_min=confidence_min, weak=weak,
            include_incomplete=include_incomplete,
        )
        if not codes:
            continue
        priority = "high" if any(code in {
            "mismatch", "reversed", "integrity_error",
            "oddspapi_participant_identity_conflict",
            "sofascore_participant_identity_conflict",
            "sofascore_participant_fk_disagreement",
        } for code in codes) else "medium"
        yield {"priority": priority, "problem_codes": ",".join(codes),
               **{key: row.get(key) for key in REVIEW_FIELDS}}


def problematic_summary(rows: Iterable[Mapping], **kwargs) -> dict:
    counts = Counter()
    total = 0
    for row in iter_problematic_records(rows, **kwargs):
        total += 1
        counts.update(row["problem_codes"].split(","))
    return {"rows": total, "by_code": dict(counts)}


class JsonArrayWriter:
    """Small streaming writer compatible with csv.DictWriter.writerow()."""

    def __init__(self, handle):
        self.handle = handle
        self.first = True

    def writeheader(self):
        return None

    def writerow(self, row):
        if not self.first:
            self.handle.write(",\n")
        rendered = json.dumps(row, ensure_ascii=False, indent=2, default=str)
        self.handle.write(textwrap.indent(rendered, "    "))
        self.first = False


def open_json_records(handle, metadata: Mapping) -> JsonArrayWriter:
    handle.write("{\n")
    for key, value in metadata.items():
        rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
        rendered = rendered.replace("\n", "\n  ")
        handle.write(f"  {json.dumps(key)}: {rendered},\n")
    handle.write('  "records": [\n')
    return JsonArrayWriter(handle)


def close_json_records(handle, writer: JsonArrayWriter) -> None:
    handle.write("\n  ]\n}\n")


def audit(
    rows: Iterable[Mapping], *, strong: float, weak: float,
    reversal_margin: float, max_examples: int, review_writer=None,
    sample_per_stratum: int = 0, seed: int = 0, all_writer=None,
) -> dict:
    classes = Counter()
    methods: dict[str, Counter] = defaultdict(Counter)
    sports: dict[str, Counter] = defaultdict(Counter)
    reasons = Counter()
    confidence_signals = Counter()
    classes_by_confidence_signal: dict[str, Counter] = defaultdict(Counter)
    coverage = Counter()
    odd_to_sofa: dict[str, set[str]] = defaultdict(set)
    sofa_to_odd: dict[str, set[str]] = defaultdict(set)
    examples: dict[str, list[dict]] = defaultdict(list)
    sample_counts = Counter()
    sample_rows: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    rng = random.Random(seed)
    for row in rows:
        result = evaluate(row, strong=strong, weak=weak, reversal_margin=reversal_margin)
        category = result["classification"]
        classes[category] += 1
        confidence_signals[result["confidence_signal"]] += 1
        classes_by_confidence_signal[result["confidence_signal"]][category] += 1
        methods[str(row.get("match_method") or "unknown")][category] += 1
        sports[str(row.get("sport") or "unknown")][category] += 1
        for reason in filter(None, result["reason"].split(",")):
            reasons[reason] += 1
        if all_writer is not None:
            all_writer.writerow({key: result.get(key) for key in REVIEW_FIELDS})
        if sample_per_stratum:
            stratum = (category, str(row.get("match_method") or "unknown"), str(row.get("sport") or "unknown"))
            sample_counts[stratum] += 1
            bucket = sample_rows[stratum]
            item = {key: result.get(key) for key in REVIEW_FIELDS}
            item.update(stratum_size=None, manual_label="", reviewer_notes="")
            if len(bucket) < sample_per_stratum:
                bucket.append(item)
            else:
                position = rng.randrange(sample_counts[stratum])
                if position < sample_per_stratum:
                    bucket[position] = item
        for side in ("home", "away"):
            if _nonempty(row.get(f"odd_{side}_source_id")):
                coverage[f"odd_{side}_present"] += 1
            if _nonempty(row.get(f"sofa_{side}_source_id")):
                coverage[f"sofa_{side}_present"] += 1
            if row.get(f"odd_{side}_source") == "oddspapi":
                sofa_side = ("away" if side == "home" else "home") if category == "reversed" else side
                _remember_link(odd_to_sofa, row.get(f"odd_{side}_source_id"), row.get(f"sofa_{sofa_side}_source_id"))
                _remember_link(sofa_to_odd, row.get(f"sofa_{sofa_side}_source_id"), row.get(f"odd_{side}_source_id"))
        if category not in {"exact", "strong"} or result["reason"]:
            if review_writer is not None:
                review_writer.writerow({key: result.get(key) for key in REVIEW_FIELDS})
            if len(examples[category]) < max_examples:
                examples[category].append({key: result.get(key) for key in REVIEW_FIELDS})

    total = sum(classes.values())
    evaluated = total - classes["unverifiable"] - classes["integrity_error"]
    def rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    sampled = []
    for stratum, bucket in sorted(sample_rows.items()):
        for item in bucket:
            item["stratum_size"] = sample_counts[stratum]
            sampled.append(item)

    odd_conflicts = {key for key, ids in odd_to_sofa.items() if len(ids) > 1}
    sofa_conflicts = {key for key, ids in sofa_to_odd.items() if len(ids) > 1}
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "thresholds": {"strong": strong, "weak": weak, "reversal_margin": reversal_margin},
        "total_mappings": total,
        "evaluated_name_pairs": evaluated,
        "classes": dict(classes),
        "strong_or_exact_rate_among_evaluated": rate(classes["exact"] + classes["strong"], evaluated),
        "suspicious_rate_among_evaluated": rate(classes["reversed"] + classes["mismatch"], evaluated),
        "coverage": dict(coverage),
        "reasons": dict(reasons),
        "confidence_signals": dict(confidence_signals),
        "classes_by_confidence_signal": {
            key: dict(value) for key, value in sorted(classes_by_confidence_signal.items())
        },
        "by_match_method": {key: dict(value) for key, value in sorted(methods.items())},
        "by_sport": {key: dict(value) for key, value in sorted(sports.items())},
        "participant_identity_conflicts": {
            "oddspapi_ids_linked_to_multiple_sofascore_ids": sum(len(ids) > 1 for ids in odd_to_sofa.values()),
            "sofascore_ids_linked_to_multiple_oddspapi_ids": sum(len(ids) > 1 for ids in sofa_to_odd.values()),
            "oddspapi_examples": [
                {"oddspapi_source_participant_id": key, "sofascore_source_participant_ids": sorted(ids)}
                for key, ids in odd_to_sofa.items() if len(ids) > 1
            ][:max_examples],
        },
        "review_examples_by_class": dict(examples),
        "_manual_review_sample": sampled,
        "_odd_conflict_ids": odd_conflicts,
        "_sofa_conflict_ids": sofa_conflicts,
        "interpretation": "Name agreement is a proxy for mapping quality, not measured ground-truth accuracy. Manual labels are required for precision/recall.",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, help="CSV with AUDIT_SQL columns; use - for stdin")
    parser.add_argument("--print-sql", action="store_true", help="Print the read-only query for COPY export")
    parser.add_argument("--sport")
    parser.add_argument("--start-from", help="Inclusive ISO UTC start time")
    parser.add_argument("--start-to", help="Exclusive ISO UTC start time")
    parser.add_argument("--match-method")
    parser.add_argument("--strong", type=float, default=0.90)
    parser.add_argument("--weak", type=float, default=0.70)
    parser.add_argument("--reversal-margin", type=float, default=0.15)
    parser.add_argument("--max-examples", type=int, default=20)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--review-json", "--review-csv", dest="review_json", type=Path)
    parser.add_argument("--problematic-json", "--problematic-csv", dest="problematic_json", type=Path,
                        default=Path("exports/oddspapi_event_mapping_problematic.json"),
                        help="Focused risk queue; JSON created by default")
    parser.add_argument("--problem-confidence-min", type=float, default=0.95,
                        help="Composite score floor for a weak-side review flag")
    parser.add_argument("--include-incomplete-problems", action="store_true",
                        help="Also place rows without a full participant pair in the focused JSON")
    parser.add_argument("--sample-json", "--sample-csv", dest="sample_json", type=Path,
                        help="Stratified random sample for human labels, written as JSON")
    parser.add_argument("--sample-per-stratum", type=int, default=20)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--estimate-from-labels", type=Path,
                        help="Score completed manual labels from sample JSON (or legacy CSV)")
    return parser


def _parse_datetime(value: str | None):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("date/time filters must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def estimate_from_labels(rows: Iterable[Mapping]) -> dict:
    """Estimate mapped-event precision from complete, stratified human labels."""
    strata: dict[tuple[str, str, str], list[Mapping]] = defaultdict(list)
    for row in rows:
        key = (str(row["classification"]), str(row["match_method"]), str(row["sport"]))
        strata[key].append(row)
    covered_population = 0
    all_population = 0
    weighted_correct = 0.0
    labeled_rows = 0
    excluded = []
    valid_labels = {"correct_event", "wrong_event", "orientation_only"}
    for key, sampled in sorted(strata.items()):
        sizes = {int(row["stratum_size"]) for row in sampled}
        if len(sizes) != 1:
            raise ValueError(f"Inconsistent stratum_size for {key}")
        population = sizes.pop()
        all_population += population
        labels = [str(row.get("manual_label") or "").strip() for row in sampled]
        if not all(label in valid_labels for label in labels):
            excluded.append({"stratum": key, "population": population,
                             "sampled": len(sampled),
                             "labeled": sum(label in valid_labels for label in labels)})
            continue
        covered_population += population
        labeled_rows += len(sampled)
        weighted_correct += population * sum(
            label in {"correct_event", "orientation_only"} for label in labels
        ) / len(sampled)
    return {
        "estimated_mapped_event_precision": round(weighted_correct / covered_population, 4) if covered_population else None,
        "covered_mapping_population": covered_population,
        "sample_population": all_population,
        "coverage_rate": round(covered_population / all_population, 4) if all_population else None,
        "labeled_sample_rows": labeled_rows,
        "excluded_strata": excluded,
        "interpretation": "Stratified point estimate of precision for existing mappings only; it does not estimate recall or account for reviewer error.",
    }


def _database_rows(args):
    from sqlalchemy import text
    from infrastructure.persistence.database import db_manager

    params = {
        "sport": args.sport, "match_method": args.match_method,
        "start_from": _parse_datetime(args.start_from),
        "start_to": _parse_datetime(args.start_to),
    }
    with db_manager.engine.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(text(AUDIT_SQL), params)
        try:
            for row in result.mappings():
                yield row
        finally:
            result.close()


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.print_sql:
        print(AUDIT_SQL)
        return 0
    if args.estimate_from_labels:
        with args.estimate_from_labels.open(newline="", encoding="utf-8") as handle:
            if args.estimate_from_labels.suffix.casefold() == ".csv":
                labeled_rows = csv.DictReader(handle)
            else:
                labeled_rows = json.load(handle)["records"]
            report = estimate_from_labels(labeled_rows)
        rendered = json.dumps(report, ensure_ascii=True, indent=2) + "\n"
        if args.output_json:
            args.output_json.parent.mkdir(parents=True, exist_ok=True)
            args.output_json.write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0
    if not 0 < args.weak <= args.strong <= 1 or not 0 <= args.reversal_margin <= 1 or not 0 <= args.problem_confidence_min <= 1:
        raise SystemExit("Require 0 < weak <= strong <= 1 and 0 <= reversal-margin, problem-confidence-min <= 1")
    if args.max_examples < 0 or args.sample_per_stratum < 0:
        raise SystemExit("--max-examples and --sample-per-stratum must be nonnegative")
    if args.input_csv and any((args.sport, args.start_from, args.start_to, args.match_method)):
        raise SystemExit("Database filters are unavailable with --input-csv; filter the SQL snapshot first")
    start_from, start_to = _parse_datetime(args.start_from), _parse_datetime(args.start_to)
    if start_from and start_to and start_from >= start_to:
        raise SystemExit("--start-from must precede --start-to")

    input_handle = None
    stage_handle = tempfile.SpooledTemporaryFile(
        max_size=2_000_000, mode="w+", newline="", encoding="utf-8"
    )
    try:
        if args.input_csv:
            input_handle = sys.stdin if str(args.input_csv) == "-" else args.input_csv.open(newline="", encoding="utf-8")
            rows = csv.DictReader(input_handle)
        else:
            rows = _database_rows(args)
        stage_writer = csv.DictWriter(stage_handle, fieldnames=REVIEW_FIELDS)
        stage_writer.writeheader()
        report = audit(rows, strong=args.strong, weak=args.weak,
                       reversal_margin=args.reversal_margin,
                       max_examples=args.max_examples,
                       sample_per_stratum=args.sample_per_stratum if args.sample_json else 0,
                       seed=args.seed, all_writer=stage_writer)
        odd_conflicts = report.pop("_odd_conflict_ids")
        sofa_conflicts = report.pop("_sofa_conflict_ids")
        sample_rows = report.pop("_manual_review_sample")

        problem_kwargs = {
            "odd_conflicts": odd_conflicts,
            "sofa_conflicts": sofa_conflicts,
            "confidence_min": args.problem_confidence_min,
            "weak": args.weak,
            "include_incomplete": args.include_incomplete_problems,
        }
        stage_handle.seek(0)
        report["problematic"] = problematic_summary(csv.DictReader(stage_handle), **problem_kwargs)
        report["problematic"]["confidence_min"] = args.problem_confidence_min
        report["problematic"]["includes_incomplete"] = args.include_incomplete_problems

        args.problematic_json.parent.mkdir(parents=True, exist_ok=True)
        stage_handle.seek(0)
        with args.problematic_json.open("w", encoding="utf-8") as handle:
            writer = open_json_records(handle, {
                "generated_at_utc": report["generated_at_utc"],
                "thresholds": report["thresholds"],
                "filters": {"sport": args.sport, "start_from": args.start_from,
                            "start_to": args.start_to, "match_method": args.match_method},
                "summary": report["problematic"],
            })
            for item in iter_problematic_records(csv.DictReader(stage_handle), **problem_kwargs):
                writer.writerow(item)
            close_json_records(handle, writer)

        if args.review_json:
            args.review_json.parent.mkdir(parents=True, exist_ok=True)
            stage_handle.seek(0)
            review_count = sum(
                1 for item in csv.DictReader(stage_handle)
                if item["classification"] not in {"exact", "strong"} or item["reason"]
            )
            stage_handle.seek(0)
            with args.review_json.open("w", encoding="utf-8") as handle:
                writer = open_json_records(handle, {
                    "generated_at_utc": report["generated_at_utc"],
                    "purpose": "Broad review set: non-strong classes and rows with integrity reasons",
                    "records_count": review_count,
                })
                for item in csv.DictReader(stage_handle):
                    if item["classification"] not in {"exact", "strong"} or item["reason"]:
                        writer.writerow(item)
                close_json_records(handle, writer)
            report["review_rows"] = review_count

        if args.sample_json:
            args.sample_json.parent.mkdir(parents=True, exist_ok=True)
            with args.sample_json.open("w", encoding="utf-8") as handle:
                json.dump({
                    "generated_at_utc": report["generated_at_utc"],
                    "sampling": {
                        "method": "reservoir sample stratified by classification, match_method and sport",
                        "per_stratum": args.sample_per_stratum,
                        "seed": args.seed,
                        "label_values": ["correct_event", "wrong_event", "orientation_only"],
                        "blank_label_means": "not reviewed or insufficient evidence",
                    },
                    "records_count": len(sample_rows),
                    "records": sample_rows,
                }, handle, ensure_ascii=False, indent=2, default=str)
    finally:
        if input_handle is not None and input_handle is not sys.stdin:
            input_handle.close()
        stage_handle.close()
    rendered = json.dumps(report, ensure_ascii=True, indent=2, default=str) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(rendered, encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
