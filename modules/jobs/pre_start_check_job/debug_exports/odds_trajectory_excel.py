"""Tabular XLSX views of the in-memory odds trajectory."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_CEILING
import os
from pathlib import Path
import re
import tempfile
from uuid import uuid4

from modules.pillars.context import EventIdentity
from modules.pillars.odds_trajectory_context import (
    ChoiceOddsTrajectory,
    OddsSnapshotPoint,
    OddsTrajectoryContext,
)
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
CHOICE_HEADERS = (
    "Inicial", "Fecha y hora del cambio (Inicial)",
    "Actual", "Fecha y hora del cambio (Actual)",
)

# Manual Excel layout settings. Edit these values to resize the generated sheets.
# Column widths are approximate character units, not pixels or centimeters.
FINAL_COLUMN_WIDTHS = {
    "Periodo": 16,
    "Línea": 6,
    "Bookmaker": 18,
    "Lado Exchange": 10,
    # Every price column such as "1 Inicial", "X Actual" or "Over Actual".
    "choice_odds": 12,
    # Every timestamp column such as "1 Fecha y hora del cambio (Inicial)".
    "change_timestamp": 24,
    "other": 20,
}
TRAJECTORY_COLUMN_WIDTHS = {
    "Cambio": 12,
    "Minutos antes del inicio": 32,
    "Cuota": 25,
}
# Title/note width is the sum of the widths of the merged columns. These values
# are the last column numbers: 6 means merge from A through F. Keep title and
# note separate so either span can be widened without changing the data table.
TRAJECTORY_TEXT_MERGE_END_COLUMNS = {
    "title": 10,
    "note": 10,
}
TRAJECTORY_CHART_SIZE_CM = {
    "width": 12,
    "height": 6.5,
}
TRAJECTORY_CHART_LINE_WIDTH_EMU = 19050
TRAJECTORY_CHART_ANCHOR_COLUMN = "E"
TRAJECTORY_SEPARATOR_END_COLUMN = 12
# Reserve enough default-height worksheet rows for a 6.5 cm chart, plus a small gap.
TRAJECTORY_CHART_RESERVED_ROWS = 13
TRAJECTORY_ODDS_AXIS_MIN = Decimal("1.0")
TRAJECTORY_ODDS_AXIS_MAX_STEP = Decimal("0.5")
TRAJECTORY_ODDS_AXIS_MIN_RANGE = Decimal("1.5")

# Row heights are points (pt); 72 pt = 1 inch.
ROW_HEIGHTS_PT = {
    "final_title": 28,
    "trajectory_title": 54,
    "final_note": 45,
    "trajectory_note": 45,
    "final_header": 36,
    "trajectory_section": 23,
    "bookmaker_separator": 32,
    "trajectory_header": 30,
}


@dataclass
class MarketRow:
    period: str
    line: Decimal | None
    bookmaker: str
    exchange_side: str
    choices: dict[str, ChoiceOddsTrajectory] = field(default_factory=dict)


@dataclass
class ChoiceTrajectoryTable:
    period: str
    line: Decimal | None
    bookmaker: str
    exchange_side: str
    source: str | None
    exchange_level: int
    choice_name: str
    choice_id: int | None
    quote_id: int | None
    snapshots: list[OddsSnapshotPoint]

    @property
    def title(self) -> str:
        line = self.line if self.line is not None else "sin línea"
        side = f" | {self.exchange_side}" if self.exchange_side else ""
        level = f" | Nivel {self.exchange_level}" if self.exchange_side else ""
        source = (
            f" | Fuente {self.source}"
            if self.source and self.source.casefold() != self.bookmaker.casefold()
            else ""
        )
        return (
            f"{self.period} | Línea {line} | {self.bookmaker}{side}{level}{source} "
            f"| Choice {self.choice_name}"
        )


def _normalize_choice_name(group: str, name: str) -> str:
    for market_group, labels in MARKET_CHOICES.items():
        normalized_group = group.casefold().replace("-", "/")
        if normalized_group == market_group.casefold():
            return next((label for label in labels if label.casefold() == name.casefold()), name)
    return name


def build_odds_trajectory_excel_rows(context: OddsTrajectoryContext) -> dict[str, list[MarketRow]]:
    """Pivot choices into final-price rows, one per market/bookmaker identity."""
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
                            name = _normalize_choice_name(group, choice.choice_name)
                            row.choices[name] = choice
        sheets[group] = sorted(grouped_rows.values(), key=_row_sort_key)
    return sheets


def build_significant_trajectory_tables(
    context: OddsTrajectoryContext,
) -> dict[str, list[ChoiceTrajectoryTable]]:
    """Build one independently ordered snapshot table for each quote choice."""
    sheets: dict[str, dict[tuple, ChoiceTrajectoryTable]] = {}
    for group, periods in context.markets.items():
        tables = sheets.setdefault(group, {})
        for period, market_names in periods.items():
            for lines in market_names.values():
                for market_line in lines.values():
                    line = Decimal(market_line.line_value) if market_line.line_value is not None else None
                    for bookie in market_line.bookies.values():
                        side = (bookie.exchange_side or "").title()
                        for choice in bookie.choices.values():
                            if not choice.snapshots:
                                continue
                            name = _normalize_choice_name(group, choice.choice_name)
                            identity = (
                                period, line, bookie.bookie_name, side, bookie.source,
                                bookie.bookie_id, bookie.exchange_level, name,
                                choice.choice_id, choice.quote_id,
                            )
                            initial, *changes = choice.snapshots
                            ordered = [
                                initial,
                                *sorted(
                                    changes,
                                    key=lambda snapshot: (
                                        snapshot.minutes_before_start is not None,
                                        snapshot.minutes_before_start or Decimal(0),
                                    ),
                                    reverse=True,
                                ),
                            ]
                            tables.setdefault(
                                identity,
                                ChoiceTrajectoryTable(
                                    period, line, bookie.bookie_name, side, bookie.source,
                                    bookie.exchange_level, name, choice.choice_id,
                                    choice.quote_id, ordered,
                                ),
                            )
        sheets[group] = tables
    return {
        group: sorted(tables.values(), key=lambda table: _trajectory_table_sort_key(group, table))
        for group, tables in sheets.items()
    }


def _shared_trajectory_odds_axis_bounds(
    tables: list[ChoiceTrajectoryTable],
) -> tuple[Decimal, Decimal]:
    """Return the odds range shared by choices of one type/period/line market."""
    snapshots = [
        snapshot
        for table in tables
        for snapshot in table.snapshots
    ]
    odds = [snapshot.odds_value for snapshot in snapshots]
    max_odds = max(odds, default=TRAJECTORY_ODDS_AXIS_MIN_RANGE)
    axis_max = (
        max_odds / TRAJECTORY_ODDS_AXIS_MAX_STEP
    ).to_integral_value(rounding=ROUND_CEILING) * TRAJECTORY_ODDS_AXIS_MAX_STEP
    axis_max = max(axis_max, TRAJECTORY_ODDS_AXIS_MIN_RANGE)
    return TRAJECTORY_ODDS_AXIS_MIN, axis_max


def _row_sort_key(row: MarketRow) -> tuple:
    name = row.bookmaker.casefold()
    return (
        BOOKMAKER_ORDER.get(name, len(BOOKMAKER_ORDER)), name,
        PERIOD_ORDER.get(row.period, len(PERIOD_ORDER)), row.period,
        row.line is not None, row.line if row.line is not None else Decimal(0),
        row.exchange_side,
    )


def _trajectory_table_sort_key(group: str, table: ChoiceTrajectoryTable) -> tuple:
    name = table.bookmaker.casefold()
    normalized_group = group.casefold().replace("-", "/")
    choices = next(
        (labels for market, labels in MARKET_CHOICES.items() if market.casefold() == normalized_group),
        (),
    )
    choice_rank = next(
        (index for index, label in enumerate(choices) if label.casefold() == table.choice_name.casefold()),
        len(choices),
    )
    return (
        BOOKMAKER_ORDER.get(name, len(BOOKMAKER_ORDER)), name,
        PERIOD_ORDER.get(table.period, len(PERIOD_ORDER)), table.period,
        table.line is not None, table.line if table.line is not None else Decimal(0),
        table.exchange_side,
        choice_rank, table.choice_name.casefold(),
    )


def _excel_timestamp(value: datetime | None) -> datetime | None:
    # Excel datetime cells have no timezone. Convert explicitly to UTC before
    # removing the offset; the sheet note documents that representation.
    return as_utc(value).replace(tzinfo=None) if value is not None else None


def _final_column_width(header: str) -> int:
    if header in FINAL_COLUMN_WIDTHS:
        return FINAL_COLUMN_WIDTHS[header]
    if "Fecha y hora del cambio" in header:
        return FINAL_COLUMN_WIDTHS["change_timestamp"]
    if header.endswith(" Inicial") or header.endswith(" Actual"):
        return FINAL_COLUMN_WIDTHS["choice_odds"]
    return FINAL_COLUMN_WIDTHS["other"]


def _choice_values(choice: ChoiceOddsTrajectory | None, current_minute: int | None) -> list:
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


def _sheet_name(prefix: str, group: str, used: set[str]) -> str:
    market = "Over-Under" if group.casefold().replace("-", "/") == "over/under" else group
    base = re.sub(r"[\\/*?:\[\]]", "-", f"{prefix} {market}").strip("'")[:31] or "Mercado"
    name = base
    suffix = 2
    while name.casefold() in used:
        label = f" ({suffix})"
        name = base[:31 - len(label)] + label
        suffix += 1
    used.add(name.casefold())
    return name


def _event_title(prefix: str, group: str, context: OddsTrajectoryContext, event_context: EventIdentity | None) -> str:
    if event_context is None:
        event = f"Evento {context.event_id} | Inicio: no disponible"
    else:
        starts_at_utc = as_utc(event_context.starts_at)
        event = (
            f"Evento {event_context.event_id}: {event_context.participants_label} | "
            f"Inicio: {starts_at_utc:%Y-%m-%d %H:%M:%S} UTC"
        )
    return f"{prefix} — {group} | {event}"


def _write_base_title(
    worksheet,
    title: str,
    note: str,
    end_column: int,
    *,
    title_end_column: int | None = None,
    note_end_column: int | None = None,
) -> None:
    from openpyxl.styles import Alignment, Font

    worksheet.append([title])
    worksheet.merge_cells(
        start_row=1, start_column=1, end_row=1, end_column=title_end_column or end_column,
    )
    worksheet.row_dimensions[1].height = (
        ROW_HEIGHTS_PT["trajectory_title"] if end_column == 2 else ROW_HEIGHTS_PT["final_title"]
    )
    worksheet["A1"].font = Font(bold=True, size=14, color="24476A")
    worksheet["A1"].alignment = Alignment(wrap_text=True, vertical="center")
    worksheet.append([note])
    worksheet.merge_cells(
        start_row=2, start_column=1, end_row=2, end_column=note_end_column or end_column,
    )
    worksheet.row_dimensions[2].height = (
        ROW_HEIGHTS_PT["trajectory_note"] if end_column == 2 else ROW_HEIGHTS_PT["final_note"]
    )
    worksheet["A2"].alignment = Alignment(wrap_text=True, vertical="center")


def _format_market_sheet(worksheet, headers: list[str], *, final_prices: bool) -> None:
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    worksheet.row_dimensions[3].height = ROW_HEIGHTS_PT["final_header"]
    for cell in worksheet[3]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="24476A")
        cell.alignment = Alignment(wrap_text=True, vertical="center")

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
            elif isinstance(cell.value, Decimal):
                cell.number_format = "0" if cell.value == cell.value.to_integral_value() else "0.###"

    for index, header in enumerate(headers, start=1):
        worksheet.column_dimensions[get_column_letter(index)].width = _final_column_width(header)

    worksheet.freeze_panes = "E4" if final_prices else "A3"
    if final_prices:
        worksheet.auto_filter.ref = f"A3:{get_column_letter(len(headers))}{worksheet.max_row}"
    worksheet.sheet_view.showGridLines = False


def _write_final_sheet(worksheet, *, title: str, note: str, headers: list[str], rows: list[list]) -> None:
    _write_base_title(worksheet, title, note, len(headers))
    worksheet.append(headers)
    for row in rows:
        worksheet.append(row)
    _format_market_sheet(worksheet, headers, final_prices=True)


def _write_trajectory_sheet(
    worksheet,
    *,
    title: str,
    note: str,
    market_group: str,
    tables: list[ChoiceTrajectoryTable],
) -> None:
    from openpyxl.chart import LineChart, Reference
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    _write_base_title(
        worksheet,
        title,
        note,
        2,
        title_end_column=TRAJECTORY_TEXT_MERGE_END_COLUMNS["title"],
        note_end_column=TRAJECTORY_TEXT_MERGE_END_COLUMNS["note"],
    )
    section_fill = PatternFill("solid", fgColor="DCE8F4")
    bookmaker_fill = PatternFill("solid", fgColor="24476A")
    separator_fill = PatternFill("solid", fgColor="000000")
    header_fill = PatternFill("solid", fgColor="24476A")
    body_fill = PatternFill("solid", fgColor="F7F9FC")
    thin = Side(style="thin", color="D9E2F3")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    tables_by_market: dict[tuple[str, Decimal | None], list[ChoiceTrajectoryTable]] = {}
    for table in tables:
        market_key = (table.period, table.line)
        tables_by_market.setdefault(market_key, []).append(table)
    axis_bounds_by_market = {
        market_key: _shared_trajectory_odds_axis_bounds(market_tables)
        for market_key, market_tables in tables_by_market.items()
    }
    row_number = 4
    current_bookmaker: str | None = None
    for table in tables:
        if table.bookmaker != current_bookmaker:
            if current_bookmaker is not None:
                worksheet.merge_cells(
                    start_row=row_number,
                    start_column=1,
                    end_row=row_number,
                    end_column=TRAJECTORY_SEPARATOR_END_COLUMN,
                )
                for column in range(1, TRAJECTORY_SEPARATOR_END_COLUMN + 1):
                    separator_cell = worksheet.cell(row_number, column)
                    separator_cell.fill = separator_fill
                separator = worksheet.cell(row_number, 1, "BOOKMAKER SEPARATOR")
                separator.font = Font(bold=True, color="FFFFFF", size=18)
                separator.alignment = Alignment(horizontal="center", vertical="center")
                separator.border = Border(
                    top=Side(style="medium", color="000000"),
                    bottom=Side(style="medium", color="000000"),
                )
                worksheet.row_dimensions[row_number].height = ROW_HEIGHTS_PT["bookmaker_separator"]
                row_number += 1
            worksheet.merge_cells(
                start_row=row_number,
                start_column=1,
                end_row=row_number,
                end_column=3,
            )
            bookmaker_header = worksheet.cell(
                row_number,
                1,
                f"Bookmaker: {table.bookmaker}",
            )
            bookmaker_header.font = Font(bold=True, color="FFFFFF")
            bookmaker_header.fill = bookmaker_fill
            bookmaker_header.alignment = Alignment(vertical="center")
            bookmaker_header.border = border
            worksheet.row_dimensions[row_number].height = ROW_HEIGHTS_PT["trajectory_section"]
            row_number += 1
            current_bookmaker = table.bookmaker

        section_row = row_number
        worksheet.merge_cells(start_row=row_number, start_column=1, end_row=row_number, end_column=3)
        section = worksheet.cell(row_number, 1, table.title)
        section.font = Font(bold=True, color="24476A")
        section.fill = section_fill
        section.alignment = Alignment(vertical="center")
        section.border = border
        worksheet.row_dimensions[row_number].height = ROW_HEIGHTS_PT["trajectory_section"]
        row_number += 1

        for column, header in enumerate(("Cambio", "Minutos antes del inicio", "Cuota"), start=1):
            cell = worksheet.cell(row_number, column, header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = header_fill
            cell.alignment = Alignment(wrap_text=True, vertical="center")
            cell.border = border
        worksheet.row_dimensions[row_number].height = ROW_HEIGHTS_PT["trajectory_header"]
        row_number += 1
        data_start_row = row_number

        for change_number, snapshot in enumerate(table.snapshots):
            minute = snapshot.minutes_before_start
            odds = snapshot.odds_value
            change_cell = worksheet.cell(
                row_number,
                1,
                "Inicial" if change_number == 0 else change_number,
            )
            minute_cell = worksheet.cell(row_number, 2, minute)
            odds_cell = worksheet.cell(row_number, 3, odds)
            for cell in (change_cell, minute_cell, odds_cell):
                cell.fill = body_fill
                cell.border = border
            if minute is not None:
                minute_cell.number_format = "0" if minute == minute.to_integral_value() else "0.###"
            odds_cell.number_format = "0" if odds == odds.to_integral_value() else "0.###"
            row_number += 1

        data_end_row = row_number - 1
        chart = LineChart()
        line_label = table.line if table.line is not None else "sin línea"
        chart.title = (
            f"{market_group} · {table.period} · Línea {line_label} · "
            f"Choice {table.choice_name} · Cuota"
        )
        chart.style = 13
        chart.y_axis.title = "Cuota"
        chart.x_axis.title = "Secuencia de cambios"
        chart.y_axis.numFmt = "0.###"
        y_min, y_max = axis_bounds_by_market[(table.period, table.line)]
        chart.y_axis.scaling.min = float(y_min)
        chart.y_axis.scaling.max = float(y_max)
        chart.legend = None
        chart.width = TRAJECTORY_CHART_SIZE_CM["width"]
        chart.height = TRAJECTORY_CHART_SIZE_CM["height"]
        chart.add_data(
            Reference(worksheet, min_col=3, min_row=data_start_row - 1, max_row=data_end_row),
            titles_from_data=True,
        )
        chart.set_categories(
            Reference(worksheet, min_col=1, min_row=data_start_row, max_row=data_end_row),
        )
        series = chart.series[0]
        series.marker.symbol = "circle"
        series.marker.size = 5
        series.graphicalProperties.line.solidFill = "4472C4"
        series.graphicalProperties.line.width = TRAJECTORY_CHART_LINE_WIDTH_EMU
        worksheet.add_chart(chart, f"{TRAJECTORY_CHART_ANCHOR_COLUMN}{section_row}")

        next_after_data = data_end_row + 2
        next_after_chart = section_row + TRAJECTORY_CHART_RESERVED_ROWS + 1
        row_number = max(next_after_data, next_after_chart)

    worksheet.column_dimensions["A"].width = TRAJECTORY_COLUMN_WIDTHS["Cambio"]
    worksheet.column_dimensions["B"].width = TRAJECTORY_COLUMN_WIDTHS["Minutos antes del inicio"]
    worksheet.column_dimensions["C"].width = TRAJECTORY_COLUMN_WIDTHS["Cuota"]
    worksheet.freeze_panes = "A3"
    worksheet.sheet_view.showGridLines = False


def _save_workbook(workbook, output_path: Path) -> Path:
    """Save atomically and use a new filename if Windows has locked the target."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.stem}_",
        suffix=output_path.suffix,
        dir=output_path.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        workbook.save(temporary_path)
        try:
            os.replace(temporary_path, output_path)
            return output_path
        except PermissionError:
            # An open workbook can prevent replacement on Windows. Preserve it
            # and publish this complete export under a unique alternate name.
            alternate_path = output_path.with_name(
                f"{output_path.stem}_updated_{uuid4().hex[:8]}{output_path.suffix}"
            )
            os.replace(temporary_path, alternate_path)
            return alternate_path
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def export_odds_trajectory_context_xlsx(
    context: OddsTrajectoryContext,
    output_path: Path,
    *,
    event_context: EventIdentity | None = None,
) -> None:
    """Write final-price and per-choice significant trajectory views."""
    from openpyxl import Workbook

    final_sheets = build_odds_trajectory_excel_rows(context)
    trajectory_tables = build_significant_trajectory_tables(context)
    current_minute = min(context.target_minutes_present, default=None)
    current_label = f"cuota T{current_minute}" if current_minute is not None else "sin target presente"
    final_note = (
        f'Actual = {current_label}. Todas las columnas "Fecha y hora del cambio" '
        "utilizan source_collected_at (UTC), nunca collected_at."
    )
    trajectory_note = (
        "Cada choice conserva Inicial y enumera sus cambios significativos. La gráfica "
        "espacia los puntos por orden de cambio; la tabla mantiene los minutos reales."
    )

    workbook = Workbook()
    workbook.remove(workbook.active)
    used_names: set[str] = set()
    groups = sorted(set(final_sheets) | set(trajectory_tables)) or ["Sin mercados"]
    for group in groups:
        final_rows = final_sheets.get(group, [])
        choices = MARKET_CHOICES.get(group)
        if choices is None:
            choices = tuple(sorted({name for row in final_rows for name in row.choices}, key=str.casefold))
        final_headers = [
            *BASE_HEADERS,
            *(f"{choice} {label}" for choice in choices for label in CHOICE_HEADERS),
        ]
        final_sheet = workbook.create_sheet(_sheet_name("Forma actual", group, used_names))
        _write_final_sheet(
            final_sheet,
            title=_event_title("Forma actual", group, context, event_context),
            note=final_note,
            headers=final_headers,
            rows=[
                [row.period, row.line, row.bookmaker, row.exchange_side or None,
                 *(value for choice_name in choices for value in _choice_values(row.choices.get(choice_name), current_minute))]
                for row in final_rows
            ],
        )

        trajectory_sheet = workbook.create_sheet(_sheet_name("Trayectoria", group, used_names))
        _write_trajectory_sheet(
            trajectory_sheet,
            title=_event_title("Trayectoria significativa", group, context, event_context),
            note=trajectory_note,
            market_group=group,
            tables=trajectory_tables.get(group, []),
        )
    try:
        return _save_workbook(workbook, output_path)
    finally:
        workbook.close()
