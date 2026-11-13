"""Adapt table rows from the standings pipeline to the berth engine."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from .final_outcome import ParticipantIdentity


def standing_identity(row: dict | None, participant_name: str | None = None) -> ParticipantIdentity | None:
    """Participant identity stored on a standings row, if it has one."""
    value = row.get("participant_identity") if row is not None else None
    if isinstance(value, ParticipantIdentity):
        return value if value.source_participant_name else replace(
            value, source_participant_name=participant_name
        )
    if isinstance(value, dict) and value.get("source") and value.get("source_participant_id") is not None:
        return ParticipantIdentity(
            value["source"], int(value["source_participant_id"]),
            value.get("source_participant_name") or participant_name,
        )
    return None


@dataclass(frozen=True, slots=True)
class TableRow:
    participant: ParticipantIdentity
    rank: int | None
    ranking_reason: str | None = None


def table_rows(standings: Mapping[str, dict] | Sequence[dict] | None) -> tuple[TableRow, ...] | None:
    """Rows ordered by position, or None when the table cannot allocate berths.

    A club without identity or without any position (unresolved) makes the table
    unusable. Clubs that share a position keep no rank and carry that reason, so
    the engine leaves only the berths that depend on them pending.
    """
    named = list(standings.items()) if isinstance(standings, Mapping) else [(None, row) for row in standings or ()]
    if not named or any(not isinstance(row, dict) for _, row in named):
        return None
    rows = []
    for name, row in named:
        identity = standing_identity(row, name)
        position = row.get("rank")
        if identity is None or not isinstance(position, int):
            return None
        shared = row.get("ranking_status") == "SHARED_POSITION"
        rows.append((position, TableRow(identity, None if shared else position, "shared_position" if shared else None)))
    if len({row.participant for _, row in rows}) != len(rows):
        return None
    return tuple(row for _, row in sorted(rows, key=lambda item: item[0]))
