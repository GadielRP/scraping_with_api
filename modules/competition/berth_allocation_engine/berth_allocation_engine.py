"""Universal Berth Allocation Engine.

Pure domain engine that allocates tournament and table berths dynamically in sporting cascade order.
Independent of whether standings come from a single league, conferences, or multi-tournament aggregates.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from typing import Protocol

from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.berth_allocation_engine.models import (
    AllocatedBerth,
    BerthAllocation,
    BerthResolution,
    QualificationRules,
    TournamentBerthRule,
)
from modules.competition.final_outcome import CupFinalEvidence, ParticipantIdentity
from modules.competition.rule_terms import Finalist


class StandingRow(Protocol):
    @property
    def participant(self) -> ParticipantIdentity: ...
    @property
    def rank(self) -> int | None: ...
    @property
    def ranking_reason(self) -> str | None: ...


def _resolve_tournament_candidate(
    rule: TournamentBerthRule,
    evidence: CupFinalEvidence | None,
    ranks: Mapping[ParticipantIdentity, int | None],
    eligible: Set[ParticipantIdentity],
    held: Set[ParticipantIdentity],
    eliminated: Mapping[str, tuple[ParticipantIdentity, ...]],
    roster_complete: bool,
) -> tuple[ParticipantIdentity | None, str | None]:
    if evidence is None:
        return None, "final_absent"
    if evidence.status != "DECIDED":
        return None, evidence.status
    if evidence.winner is None:
        return None, "incomplete_cup_evidence"

    for finalist in rule.finalists:
        club = evidence.winner if finalist == Finalist.WINNER else evidence.loser
        if club is None:
            return None, "incomplete_cup_evidence"
        if club in held:
            continue  # a club holds one berth only: try the next finalist, then the replacement rounds
        if club in eligible or (club not in ranks and rule.open_to_outsiders):
            return club, None
        if club not in ranks and not roster_complete:
            return None, "finalist_eligibility_unknown"

    for round_name, expected_matches in rule.replacement_rounds:
        losers = eliminated.get(round_name, ())
        if len(set(losers)) != expected_matches:
            return None, "incomplete_cup_bracket"
        candidates = set(losers) & eligible
        if len(candidates) == 1:
            return next(iter(candidates)), None
        if candidates:
            positions = {ranks.get(club) for club in candidates}
            if None in positions or len(positions) != len(candidates):
                return None, "cup_replacement_annual_tiebreak_pending"
            return min(candidates, key=ranks.__getitem__), None

    return None, None


def allocate_berths(
    rules: QualificationRules,
    standings: Sequence[StandingRow],
    *,
    season_year: int,
    tournament_evidence: Sequence[CupFinalEvidence] = (),
    bracket_eliminations: Mapping[CompetitionIdentity, Mapping[str, tuple[ParticipantIdentity, ...]]] | None = None,
    excluded_participants: Set[ParticipantIdentity] = frozenset(),
    next_season_roster: Set[ParticipantIdentity] | None = None,
    eligibility_unconfirmed: bool = False,
) -> BerthAllocation:
    """Allocate sporting qualification berths in priority order.

    Tournament and table berths are interleaved by ``QualificationRules.ordered_berths``.
    A tournament berth goes to its eligible winner; when none can take it, it is
    reassigned to the table if configured. A table berth goes to the best-ranked
    club that holds no berth yet; a shared or unresolved position leaves the berths
    that depend on it pending. Table berths always follow the current table, so they
    stay PROJECTED; a decided tournament berth is CONFIRMED unless an earlier berth
    is pending or the club's eligibility is unconfirmed.
    """
    ranks = {row.participant: row.rank for row in standings}
    roster_complete = next_season_roster is not None
    available: set[ParticipantIdentity] = (
        set(next_season_roster)
        if roster_complete
        else set(ranks).difference(excluded_participants)
    )

    evidence_by_comp = {
        item.competition: item
        for item in tournament_evidence
        if item.season_year == season_year
    }

    berths: list[AllocatedBerth] = []
    held: set[ParticipantIdentity] = set()
    earlier_berth_pending = False
    ranked_candidates = iter(row for row in standings if row.participant in available)
    table_pending_issue = None

    def record_berth(destination, club, *, source=None, resolution, issue=None, final=None):
        if club is not None:
            available.discard(club)
            held.add(club)
        if issue is None and resolution is BerthResolution.PROJECTED and earlier_berth_pending:
            issue = "earlier_berth_pending"
        berths.append(AllocatedBerth(
            destination, club, source, resolution, issue, ranks.get(club) if club is not None else None, final,
        ))

    def award_from_table(destination, *, source=None, issue=None, final=None):
        """Give the berth to the best remaining ranked club, or leave it pending."""
        nonlocal table_pending_issue
        if table_pending_issue is None:
            row = next(ranked_candidates, None)
            if row is None:
                table_pending_issue = "insufficient_eligible_participants"
            elif row.rank is None:
                table_pending_issue = row.ranking_reason or "ranking_unresolved"
            else:
                record_berth(destination, row.participant, source=source, issue=issue, final=final,
                             resolution=BerthResolution.PROJECTED)
                return
        record_berth(destination, None, source=source, resolution=BerthResolution.UNRESOLVED,
                     issue=table_pending_issue, final=final)

    for destination, rule in rules.ordered_berths:
        if rule is None:
            award_from_table(destination)
            continue
        evidence = evidence_by_comp.get(rule.competition)
        # An absent final adds nothing beyond the berth's issue.
        final = evidence if evidence is not None and evidence.status != "final_absent" else None
        club, issue = _resolve_tournament_candidate(
            rule,
            evidence,
            ranks,
            available,
            held,
            (bracket_eliminations or {}).get(rule.competition, {}),
            roster_complete,
        )
        if club is not None:
            resolution = BerthResolution.PROJECTED if (eligibility_unconfirmed or earlier_berth_pending) else BerthResolution.CONFIRMED
        elif issue is not None:
            resolution = BerthResolution.UNRESOLVED
            earlier_berth_pending = True
        elif rule.reallocate_to_table:
            award_from_table(destination, source=rule.competition, final=final,
                             issue="tournament_winner_already_qualified")
            continue
        elif rule.replacement_rounds:
            resolution, issue = BerthResolution.UNRESOLVED, "no_eligible_cup_participant"
            earlier_berth_pending = True
        else:
            resolution, issue = BerthResolution.UNAWARDED, "winner_not_available"
        record_berth(destination, club, source=rule.competition, resolution=resolution, issue=issue, final=final)

    return BerthAllocation(
        destination_season_year=season_year + rules.destination_season_offset,
        berths=tuple(berths),
    )
