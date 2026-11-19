"""Adapt table rows from the standings pipeline to the berth engine."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from .final_outcome import ParticipantIdentity

from .rule_terms import BerthIssue, RankingIssue, RankingScope, RankingStatus


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


def standing_group(row: Mapping | None, ranking_scope: RankingScope) -> str | None:
    """Normalized group membership for the requested ranking scope."""
    if not isinstance(row, Mapping) or ranking_scope == RankingScope.GLOBAL:
        return None
    group_fields = {
        RankingScope.GROUP: ("group",),
        RankingScope.CONFERENCE: ("conference", "group"),
        RankingScope.DIVISION: ("division",),
        RankingScope.LEAGUE: ("league", "group"),
    }[ranking_scope]
    group = next((str(row[field]).strip().casefold() for field in group_fields if row.get(field)), None)
    return None if group == "unknown" else group


@dataclass(frozen=True, slots=True)
class TableRow:
    participant: ParticipantIdentity
    rank: int | None
    ranking_reason: RankingIssue | BerthIssue | None = None
    ranking_order: int | None = None
    ranking_group: str | None = None


def _ordered_rows(entries) -> tuple[TableRow, ...] | None:
    """Rows sorted by ranking order, preserving their group and unresolved positions.

    A row that is not resolved keeps no rank and carries its reason, so the engine leaves only the
    berths that depend on it pending. The order of a tie group stays usable even without a rank.
    """
    rows = []
    for identity, order, rank, status, reason, group in entries:
        if identity is None or not isinstance(order, int) or reason == RankingIssue.MISSING_GROUP_MEMBERSHIP:
            return None
        if status in (RankingStatus.SHARED_POSITION, RankingStatus.UNRESOLVED) or not isinstance(rank, int):
            pending = BerthIssue.SHARED_POSITION if status == RankingStatus.SHARED_POSITION else reason or BerthIssue.RANKING_UNRESOLVED
            rows.append((order, TableRow(identity, None, pending, order, group)))
        else:
            rows.append((order, TableRow(identity, rank, ranking_order=order, ranking_group=group)))
    if len({row.participant for _, row in rows}) != len(rows):
        return None
    return tuple(row for _, row in sorted(rows, key=lambda item: item[0]))


def table_rows(standings: Mapping[str, dict] | Sequence[dict] | None) -> tuple[TableRow, ...] | None:
    """Rows ordered by ranking order, or None when the table cannot allocate berths.

    A club without identity or ordering, a duplicated club, or a club whose group membership is
    unknown makes the table unusable. Clubs that share a position or wait for an official
    resolution keep no rank and carry the reason.
    """
    named = list(standings.items()) if isinstance(standings, Mapping) else [(None, row) for row in standings or ()]
    if not named or any(not isinstance(row, dict) for _, row in named):
        return None
    return _ordered_rows(
        (standing_identity(row, name),
         row.get("ranking_order") if isinstance(row.get("ranking_order"), int) else row.get("rank"),
         row.get("rank"), row.get("ranking_status"), row.get("ranking_reason"),
         standing_group(row, RankingScope.GROUP))
        for name, row in named
    )


def annual_table_rows(standings) -> tuple[TableRow, ...] | None:
    """Rows of an annual table, under the same contract as a league table."""
    if not standings:
        return None
    return _ordered_rows(
        (row.participant, row.rank if row.ranking_order is None else row.ranking_order,
         row.rank, row.ranking_status, row.ranking_reason, None)
        for row in standings
    )
