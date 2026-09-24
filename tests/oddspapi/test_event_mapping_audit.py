"""Pure scoring tests; no database or provider calls."""

import csv
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.maintenance.audit_oddspapi_event_mappings import (
    PROBLEM_FIELDS, audit, confidence_signal, estimate_from_labels, evaluate, main,
    write_problematic_csv,
)


def row(**overrides):
    value = {
        "mapping_id": 1, "event_id": 2, "source_event_id": "fixture-1",
        "sport": "Football", "match_method": "deterministic_candidate_match",
        "odd_home_fk": 11, "odd_away_fk": 12,
        "odd_home_source": "oddspapi", "odd_away_source": "oddspapi",
        "odd_home_source_id": 101, "odd_away_source_id": 102,
        "odd_home_name": "Warta Poznan", "odd_away_name": "Miedz Legnica",
        "sofa_mapping_id": 20,
        "sofa_mapping_home_fk": 21, "sofa_mapping_away_fk": 22,
        "event_home_fk": 21, "event_away_fk": 22,
        "sofa_home_source_id": 201, "sofa_away_source_id": 202,
        "sofa_home_name": "Warta Poznań", "sofa_away_name": "Miedź Legnica",
    }
    value.update(overrides)
    return value


def score(value):
    return evaluate(value, strong=0.90, weak=0.70, reversal_margin=0.15)


def test_exact_accent_normalization():
    assert score(row())["classification"] == "exact"


def test_reversed_pair_is_not_a_wrong_event_claim():
    result = score(row(sofa_home_name="Miedź Legnica", sofa_away_name="Warta Poznań"))
    assert result["classification"] == "reversed"


def test_single_low_name_is_review_not_mismatch():
    result = score(row(sofa_home_name="Different club"))
    assert result["classification"] == "review"


def test_both_low_names_are_mismatch():
    result = score(row(sofa_home_name="Team Alpha", sofa_away_name="Team Beta"))
    assert result["classification"] == "mismatch"


def test_csv_empty_foreign_keys_are_missing_not_wrong_source():
    result = score(row(odd_home_fk="", odd_home_source="", odd_home_name=""))
    assert result["classification"] == "unverifiable"


def test_integrity_error_and_identity_conflict():
    first = row()
    second = row(mapping_id=2, sofa_home_source_id=999)
    invalid = row(mapping_id=3, odd_home_source="sofascore")
    report = audit([first, second, invalid], strong=0.9, weak=0.7,
                   reversal_margin=0.15, max_examples=2)
    assert report["classes"]["integrity_error"] == 1
    assert report["participant_identity_conflicts"]["oddspapi_ids_linked_to_multiple_sofascore_ids"] == 1


def test_stratified_precision_uses_population_weights():
    labels = [
        {"classification": "exact", "match_method": "direct", "sport": "Football",
         "stratum_size": "90", "manual_label": "correct_event"},
        {"classification": "mismatch", "match_method": "direct", "sport": "Football",
         "stratum_size": "10", "manual_label": "wrong_event"},
    ]
    estimate = estimate_from_labels(labels)
    assert estimate["estimated_mapped_event_precision"] == 0.9
    assert estimate["covered_mapping_population"] == 100


def test_confidence_provenance_is_not_accuracy():
    assert confidence_signal("external_provider_sofascore_id", 1) == "provider_reference"
    assert confidence_signal("deterministic_candidate_match", 0.98) == "candidate_high_composite"
    assert confidence_signal("deterministic_candidate_match", 0.89) == "candidate_below_floor_identity_possible"


def test_problematic_csv_includes_high_composite_weak_side_and_identity_conflict():
    evaluated = score(row(sofa_home_name="Different club", confidence=0.99))
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=PROBLEM_FIELDS)
    writer.writeheader()
    summary = write_problematic_csv(
        [evaluated], writer, odd_conflicts={"101"}, sofa_conflicts=set(),
        confidence_min=0.95, weak=0.70, include_incomplete=False,
    )
    saved = next(csv.DictReader(StringIO(output.getvalue())))
    assert summary["rows"] == 1
    assert "high_composite_weak_name_side" in saved["problem_codes"]
    assert "oddspapi_participant_identity_conflict" in saved["problem_codes"]


def test_cli_writes_focused_problematic_csv():
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        input_csv = root / "input.csv"
        output_csv = root / "problematic.csv"
        rows = [
            row(mapping_id=1, confidence=0.99),
            row(mapping_id=2, confidence=0.99, sofa_home_name="Different club"),
            row(mapping_id=3, confidence=0.99,
                sofa_home_name="Miedź Legnica", sofa_away_name="Warta Poznań",
                sofa_home_source_id=202, sofa_away_source_id=201),
        ]
        with input_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        with redirect_stdout(StringIO()):
            assert main(["--input-csv", str(input_csv),
                         "--problematic-csv", str(output_csv),
                         "--max-examples", "0"]) == 0
        with output_csv.open(newline="", encoding="utf-8") as handle:
            problems = list(csv.DictReader(handle))
        assert {item["mapping_id"] for item in problems} == {"2", "3"}
