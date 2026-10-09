from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from openpyxl import load_workbook
import pytest

from modules.jobs.pre_start_check_job.debug_exports.odds_trajectory_excel import (
    export_odds_trajectory_context_xlsx,
)
from modules.pillars.context import EventIdentity
from modules.pillars.odds_trajectory_context import (
    OddsTrajectoryContext, MarketLineOddsTrajectory, BookieOddsTrajectory,
    ChoiceOddsTrajectory, OddsPointMeta, OddsSnapshotPoint,
)

UTC = timezone.utc
EARLY = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
CURRENT = EARLY + timedelta(hours=2)


def choice(name, initial="1.90", current="1.85"):
    initial_value = Decimal(initial) if initial is not None else None
    value = Decimal(current)
    snapshots = [
        OddsSnapshotPoint(1, 10, Decimal("1.90"), EARLY + timedelta(minutes=10), EARLY, Decimal(120)),
        OddsSnapshotPoint(2, 10, Decimal("1.90"), EARLY + timedelta(minutes=20), EARLY + timedelta(minutes=1), Decimal(119)),
        OddsSnapshotPoint(3, 10, value, CURRENT + timedelta(minutes=10), CURRENT, Decimal(5)),
        # Later raw snapshot must not replace the selected target's source timestamp.
        OddsSnapshotPoint(4, 10, Decimal("1.75"), CURRENT + timedelta(minutes=20), CURRENT + timedelta(minutes=15), Decimal(0)),
    ]
    return ChoiceOddsTrajectory(
        name, 1, initial_value, 10,
        odds_values={120: Decimal("2.50"), 5: value},
        meta_by_minute={5: OddsPointMeta(3, CURRENT + timedelta(minutes=10), 5, 5, Decimal(0))},
        snapshots=snapshots,
    )


def context(specs, targets=(120, 5)):
    markets = {}
    for index, (group, period, line, name, side, choices) in enumerate(specs):
        bookie = BookieOddsTrajectory(index, name, exchange_side=side, choices={c.choice_name: c for c in choices})
        market = MarketLineOddsTrajectory(index, f"{group} {period}", group, period, line, {str(index): bookie})
        lines = markets.setdefault(group, {}).setdefault(period, {}).setdefault(market.market_name, {})
        existing = lines.get(line or "default")
        if existing is not None:
            existing.bookies[str(index)] = bookie
        else:
            lines[line or "default"] = market
    return OddsTrajectoryContext(True, 4004, list(targets), list(targets), [], markets)


def export(tmp_path, ctx):
    path = tmp_path / "trajectory.xlsx"
    event = EventIdentity(
        4004, "Home vs Away", datetime(2026, 8, 31, 18, 0, tzinfo=UTC),
        5, "Football",
    )
    export_odds_trajectory_context_xlsx(ctx, path, event_context=event)
    return load_workbook(path)


def test_market_sheets_pivot_choices_and_have_excel_features(tmp_path):
    ctx = context([
        ("1X2", "Full Time", None, "SofaScore", None, [choice("1"), choice("x"), choice("2")]),
        ("Asian Handicap", "Full Time", "0", "Pinnacle Sports", None, [choice("1"), choice("2")]),
        ("Over/Under", "1st Half", "2.5", "bet365", None, [choice("over"), choice("under")]),
    ])
    workbook = export(tmp_path, ctx)
    assert workbook.sheetnames == ["1X2", "Asian Handicap", "Over-Under"]
    for sheet, names in (("1X2", ("1", "X", "2")), ("Asian Handicap", ("1", "2")), ("Over-Under", ("Over", "Under"))):
        ws = workbook[sheet]
        assert ws.max_row == 4
        assert [cell.value for cell in ws[3]][:4] == ["Periodo", "Línea", "Bookmaker", "Lado Exchange"]
        assert [cell.value for cell in ws[3]][4:] == [f"{name} {label}" for name in names for label in ("Inicial", "Fecha y hora del cambio (Inicial)", "Actual", "Fecha y hora del cambio (Actual)")]
        assert ws.freeze_panes == "E4"
        assert ws.auto_filter.ref.startswith("A3:")
        assert ws["E4"].number_format == "0.###"
        assert ws["F4"].number_format == "yyyy-mm-dd hh:mm:ss"
        assert "Inicio: 2026-08-31 18:00:00 UTC" in ws["A1"].value
        assert "source_collected_at (UTC)" in ws["A2"].value
        assert "Fecha y hora del cambio" in " ".join(cell.value for cell in ws[3])
    assert workbook["Asian Handicap"]["B4"].value == 0
    workbook.close()


def test_initial_and_current_use_exact_source_timestamps(tmp_path):
    workbook = export(tmp_path, context([("1X2", "Full Time", None, "SofaScore", None, [choice("1")])]))
    ws = workbook["1X2"]
    assert ws["E4"].value == 1.9  # initial_odds, not T120's 2.5
    assert ws["F4"].value == EARLY.replace(tzinfo=None)
    assert ws["G4"].value == 1.85
    assert ws["H4"].value == CURRENT.replace(tzinfo=None)
    workbook.close()


def test_current_is_most_recent_present_target_even_when_it_is_not_five(tmp_path):
    c = choice("1")
    c.odds_values[30] = Decimal("2.10")
    c.meta_by_minute[30] = replace(c.meta_by_minute[5], target_minute=30, snapshot_id=2)
    c.odds_values[1] = Decimal("1.75")
    c.meta_by_minute[1] = replace(c.meta_by_minute[5], target_minute=1, snapshot_id=4)
    for targets, expected, timestamp in (((120, 30), 2.1, EARLY + timedelta(minutes=1)), ((5, 120, 1), 1.75, CURRENT + timedelta(minutes=15))):
        workbook = export(tmp_path, context([("1X2", "Full Time", None, "SofaScore", None, [c])], targets))
        assert workbook["1X2"]["G4"].value == expected
        assert workbook["1X2"]["H4"].value == timestamp.replace(tzinfo=None)
        assert f"T{min(targets)}" in workbook["1X2"]["A2"].value
        workbook.close()


def test_missing_prices_and_missing_source_dates_stay_blank(tmp_path):
    c = choice("1", initial=None)
    c.odds_values.pop(5)
    workbook = export(tmp_path, context([("1X2", "Full Time", None, "SofaScore", None, [c])]))
    assert [workbook["1X2"].cell(4, col).value for col in range(5, 9)] == [None] * 4
    workbook.close()
    c = choice("1", initial="1.90000000000000000001")
    c.snapshots[2] = replace(c.snapshots[2], source_collected_at=None)
    workbook = export(tmp_path, context([("1X2", "Full Time", None, "SofaScore", None, [c])]))
    assert workbook["1X2"]["F4"].value is None  # Decimal inequality; no float matching
    assert workbook["1X2"]["H4"].value is None  # Never substitute collected_at
    workbook.close()


def test_bookmakers_periods_lines_and_exchange_sides_have_deterministic_order(tmp_path):
    specs = [("Asian Handicap", "Full Time", "0", name, side, [choice("1"), choice("2")]) for name, side in (
        ("Zulu", None), ("Betfair Exchange", "lay"), ("bet365", None),
        ("Pinnacle Sports", None), ("SofaScore", None), ("Alpha", None), ("Betfair Exchange", "back"),
    )]
    specs.extend(("Asian Handicap", period, line, "SofaScore", None, [choice("1"), choice("2")]) for period, line in (("1st Half", "10"), ("1st Half", "2")))
    workbook = export(tmp_path, context(specs))
    rows = list(workbook["Asian Handicap"].iter_rows(min_row=4, values_only=True))
    assert [row[2] for row in rows] == ["SofaScore"] * 3 + ["Pinnacle Sports", "bet365", "Betfair Exchange", "Betfair Exchange", "Alpha", "Zulu"]
    assert [(row[0], row[1]) for row in rows[:3]] == [("1st Half", 2), ("1st Half", 10), ("Full Time", 0)]
    assert [row[3] for row in rows[5:7]] == ["Back", "Lay"]
    workbook.close()


def test_unknown_market_sheet_names_are_safe_and_unique(tmp_path):
    group = "Future/Market:*?[With]VeryLongNameMoreThan31"
    workbook = export(tmp_path, context([
        (group, "New Period", None, "New Book", None, [choice("Yes")]),
        (group.replace("/", "-"), "New Period", None, "New Book", None, [choice("No")]),
    ]))
    assert len(workbook.sheetnames) == 2
    assert len(set(workbook.sheetnames)) == 2
    assert all(len(name) <= 31 and not any(char in name for char in "[]:*?/\\") for name in workbook.sheetnames)
    assert all(ws.max_row == 4 for ws in workbook)
    workbook.close()


def test_empty_context_writes_a_readable_workbook(tmp_path):
    workbook = export(tmp_path, context([], targets=()))
    assert workbook.sheetnames == ["Sin mercados"]
    assert "sin target presente" in workbook.active["A2"].value
    workbook.close()
