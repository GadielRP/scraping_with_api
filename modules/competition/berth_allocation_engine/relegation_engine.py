"""Standalone engine for league table relegation resolution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Protocol

from modules.competition.competitive_rules import RankSelection
from modules.competition.final_outcome import ParticipantIdentity
from modules.competition.rule_terms import SelectionMethod


class StandingRow(Protocol):
    @property
    def participant(self) -> ParticipantIdentity: ...
    @property
    def points(self) -> float: ...
    @property
    def rank(self) -> int | None: ...


@dataclass(frozen=True, slots=True)
class RelegationOutcome:
    """The result of evaluating relegation rules against standings and averages."""
    participants: tuple[ParticipantIdentity, ...]
    resolution: str = "RESOLVED"
    issue: str | None = None


def resolve_relegation(
    rules: Sequence[RankSelection],
    standings: Sequence[StandingRow],
    *,
    multiseason_averages: Mapping[ParticipantIdentity, Fraction] | None = None,
) -> RelegationOutcome:
    """Determine who would be relegated from the table as it stands, by points or multiseason average."""
    if not rules:
        return RelegationOutcome(participants=())

    remaining_points = {row.participant: row.points for row in standings}
    relegated: list[ParticipantIdentity] = []

    for rule in rules:
        if rule.selection_method == SelectionMethod.LOWEST_POINTS_AVERAGE:
            if multiseason_averages is None or not remaining_points.keys() <= multiseason_averages.keys():
                return RelegationOutcome(
                    participants=tuple(relegated),
                    resolution="UNRESOLVED",
                    issue="incomplete_multiseason_record",
                )
            scores = {club: multiseason_averages[club] for club in remaining_points}
            tie_reason = "average_relegation_decider_required"
        elif rule.selection_method == SelectionMethod.LOWEST_POINTS:
            scores = remaining_points
            tie_reason = "relegation_decider_required"
        else:
            raise ValueError(f"Unsupported relegation selection method: {rule.selection_method}")

        count = rule.places
        if count is None or len(scores) < count:
            return RelegationOutcome(
                participants=tuple(relegated),
                resolution="UNRESOLVED",
                issue="insufficient_relegation_candidates",
            )

        ordered = sorted(scores, key=scores.__getitem__)
        # Check for tied score at cut-off boundary
        if len(ordered) > count and scores[ordered[count - 1]] == scores[ordered[count]]:
            return RelegationOutcome(
                participants=tuple(relegated),
                resolution="UNRESOLVED",
                issue=tie_reason,
            )

        selected = ordered[:count]
        relegated.extend(selected)
        for club in selected:
            del remaining_points[club]

    return RelegationOutcome(participants=tuple(relegated), resolution="RESOLVED")

