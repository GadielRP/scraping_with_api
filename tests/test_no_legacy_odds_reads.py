from __future__ import annotations

from scripts.maintenance.check_no_legacy_odds_reads import (
    ROOT,
    main,
    scan_legacy_odds_reads,
)


def test_repository_has_no_unallowlisted_legacy_reads():
    violations = scan_legacy_odds_reads(
        [ROOT / "app", ROOT / "infrastructure", ROOT / "modules", ROOT / "scripts"]
    )
    assert violations == ()


def test_guard_rejects_orm_model_alias_and_snapshot_sql(tmp_path):
    fixture = tmp_path / "violating_reader.py"
    fixture.write_text(
        """
from infrastructure.persistence.models import MarketChoice as MC

def read_it():
    field = MC.current_odds
    sql = 'SELECT mcs.source_market_id FROM market_choice_snapshots mcs'
    return field, sql
""",
        encoding="utf-8",
    )

    violations = scan_legacy_odds_reads([fixture])

    assert {item.rule for item in violations} == {
        "legacy_orm_choice_state",
        "legacy_sql_snapshot_identity",
    }
    assert main([str(fixture)]) == 1


def test_guard_does_not_reject_new_trajectory_domain_contract(tmp_path):
    fixture = tmp_path / "valid_reader.py"
    fixture.write_text(
        """
def read_it(choice):
    return choice.initial_odds
""",
        encoding="utf-8",
    )
    assert scan_legacy_odds_reads([fixture]) == ()
    assert main([str(fixture)]) == 0
