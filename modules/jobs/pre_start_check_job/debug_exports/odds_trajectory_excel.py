"""Tabular XLSX view of the already-built odds trajectory; no fetching or sampling."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import re

from modules.pillars.context import EventIdentity
from modules.pillars.odds_trajectory_context import ChoiceOddsTrajectory, OddsTrajectoryContext
from shared.temporal import as_utc

MARKET_CHOICES = {
    "1X2": ("1", "X", "2"),
    "Asian Handicap": ("1", "2"),
    "Over/Under": ("Over", "Under"),
}
BOOKMAKER_ORDER = {
    "sofascore": 0, "pinnacle sports": 1, "pinnacle": 1,
    "bet365": 2, "betfair exchange": 3, "betfair-ex": 3, "betfair": 3,
}
PERIOD_ORDER = {"1st Half": 0, "Full Time": 1}
BASE_HEADERS = ("Periodo", "Línea", "Bookmaker", "Lado Exchange")
CHOICE_HEADERS = ("Inicial", "Fecha y hora del cambio (Inicial)", "Actual", "Fecha y hora del cambio (Actual)")


@dataclass
class MarketRow:
    period: str
    line: Decimal | None
    bookmaker: str
    exchange_side: str
    choices: dict[str, ChoiceOddsTrajectory] = field(default_factory=dict)


def build_odds_trajectory_excel_rows(context: OddsTrajectoryContext) -> dict[str, list[MarketRow]]:
    """Pivot choices into a single row per period/line/bookmaker/exchange side."""
    sheets: dict[str, list[MarketRow]] = {}
    for group, periods in context.markets.items():
        grouped_rows: dict[tuple, MarketRow] = {}
        for period, market_names in periods.items():
            for lines in market_names.values():
                for market_line in lines.values():
                    line = Decimal(market_line.line_value) if market_line.line_value is not None else None
                    for bookie in market_line.bookies.values():
                        side = (bookie.exchange_side or "").title()
                        key = (period, line, bookie.bookie_name, side)
                        row = grouped_rows.setdefault(key, MarketRow(period, line, bookie.bookie_name, side))
                        for choice in bookie.choices.values():
                            name = choice.choice_name
                            if group in MARKET_CHOICES:
                                name = next((label for label in MARKET_CHOICES[group] if label.casefold() == name.casefold()), name)
                            row.choices[name] = choice
        sheets[group] = sorted(grouped_rows.values(), key=_row_sort_key)
    return sheets


def _row_sort_key(row: MarketRow) -> tuple:
    name = row.bookmaker.casefold()
    return (
        BOOKMAKER_ORDER.get(name, len(BOOKMAKER_ORDER)), name,
        PERIOD_ORDER.get(row.period, len(PERIOD_ORDER)), row.period,
        row.line is not None, row.line if row.line is not None else Decimal(0),
        row.exchange_side,
    )


def _excel_timestamp(value: datetime | None) -> datetime | None:
    # Excel datetime cells have no timezone. Convert explicitly to UTC before
    # removing the offset; the sheet note documents that representation.
    return as_utc(value).replace(tzinfo=None) if value is not None else None


def _choice_values(
    choice: ChoiceOddsTrajectory | None, current_minute: int | None,
) -> list[Decimal | datetime | None]:
    if choice is None:
        return [None] * 4
    initial_times = [
        snapshot.source_collected_at for snapshot in choice.snapshots
        if choice.initial_odds is not None and snapshot.odds_value == choice.initial_odds
        and snapshot.source_collected_at is not None
    ]
    initial_at = min(initial_times) if initial_times else None
    current = choice.odds_values.get(current_minute)
    meta = choice.meta_by_minute.get(current_minute)
    snapshots = {point.snapshot_id: point for point in choice.snapshots if point.snapshot_id is not None}
    snapshot = snapshots.get(meta.snapshot_id) if meta is not None else None
    current_at = snapshot.source_collected_at if current is not None and snapshot is not None else None
    return [choice.initial_odds, _excel_timestamp(initial_at), current, _excel_timestamp(current_at)]


def _sheet_name(group: str, used: set[str]) -> str:
    base = re.sub(r"[\\/*?:\[\]]", "-", group).strip("'")[:31] or "Mercado"
    name = base
    suffix = 2
    while name.casefold() in used:
        label = f" ({suffix})"
        name = base[:31 - len(label)] + label
        suffix += 1
    used.add(name.casefold())
    return name


def export_odds_trajectory_context_xlsx(
    context: OddsTrajectoryContext,
    output_path: Path,
    *,
    event_context: EventIdentity | None = None,
) -> None:
    """Write market sheets from the same in-memory context used by JSON debug."""
    # Loading Excel support only here keeps it out of runs with debug disabled.
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    current_minute = min(context.target_minutes_present, default=None)
    current_label = f"cuota T{current_minute}" if current_minute is not None else "sin target presente"
    note = (
        f'Actual = {current_label}. Todas las columnas "Fecha y hora del cambio" '
        "utilizan source_collected_at (UTC), nunca collected_at."
    )
    if event_context is None:
        title = f"Evento {context.event_id} | Inicio: no disponible"
    else:
        starts_at_utc = as_utc(event_context.starts_at)
        title = (
            f"Evento {event_context.event_id}: {event_context.participants_label} | "
            f"Inicio: {starts_at_utc:%Y-%m-%d %H:%M:%S} UTC"
        )
    sheets = build_odds_trajectory_excel_rows(context)
    workbook = Workbook()
    workbook.remove(workbook.active)
    used_names: set[str] = set()
    for group, rows in sorted(sheets.items()) or [("Sin mercados", [])]:
        choices = MARKET_CHOICES.get(group)
        if choices is None:
            choices = tuple(sorted({name for row in rows for name in row.choices}, key=str.casefold))
        headers = [*BASE_HEADERS, *(f"{choice} {label}" for choice in choices for label in CHOICE_HEADERS)]
        worksheet = workbook.create_sheet(_sheet_name(group, used_names))
        worksheet.append([title])
        worksheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(headers))
        worksheet.row_dimensions[1].height = 28
        worksheet["A1"].font = Font(bold=True, size=14, color="24476A")
        worksheet["A1"].alignment = Alignment(vertical="center")
        worksheet.append([note])
        worksheet.merge_cells(start_row=2, start_column=1, end_row=2, end_column=len(headers))
        worksheet.row_dimensions[2].height = 30
        worksheet["A2"].alignment = Alignment(wrap_text=True, vertical="center")
        worksheet.append(headers)
        worksheet.row_dimensions[3].height = 36
        for cell in worksheet[3]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="24476A")
            cell.alignment = Alignment(wrap_text=True, vertical="center")
        for row in rows:
            values = [row.period, row.line, row.bookmaker, row.exchange_side or None]
            for choice in choices:
                values.extend(_choice_values(row.choices.get(choice), current_minute))
            worksheet.append(values)
        cell_border = Border(
            left=Side(style="thin", color="D9E2F3"),
            right=Side(style="thin", color="D9E2F3"),
            top=Side(style="thin", color="D9E2F3"),
            bottom=Side(style="thin", color="D9E2F3"),
        )
        body_fill = PatternFill("solid", fgColor="F7F9FC")
        for cells in worksheet.iter_rows(min_row=3):
            for cell in cells:
                cell.border = cell_border
                if cell.row >= 4:
                    cell.fill = body_fill
                if isinstance(cell.value, datetime):
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"
                elif isinstance(cell.value, (Decimal, int, float)):
                    cell.number_format = "0.###"
        for index, header in enumerate(headers, start=1):
            width = 34 if "Fecha y hora del cambio" in header else 18
            if header in ("Periodo", "Bookmaker"):
                width = 28
            worksheet.column_dimensions[get_column_letter(index)].width = width
        worksheet.freeze_panes = "E4"
        worksheet.auto_filter.ref = f"A3:{get_column_letter(len(headers))}{worksheet.max_row}"
        worksheet.sheet_view.showGridLines = False
    try:
        workbook.save(output_path)
    finally:
        workbook.close()
