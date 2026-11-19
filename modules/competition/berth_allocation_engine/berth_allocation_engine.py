"""Universal Berth Allocation Engine.

Pure domain engine that allocates tournament and table berths dynamically in sporting cascade order.
Independent of whether standings come from a single league, conferences, or multi-tournament aggregates.
"""

from __future__ import annotations


from collections.abc import Mapping, Sequence, Set
from dataclasses import replace
from typing import Protocol

from modules.competition import competition_ids
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.berth_allocation_engine.models import (
    AllocatedBerth,
    BerthAllocation,
    BerthResolution,
    QualificationRules,
    TableBerthRule,
    TournamentBerthRule,
)
from modules.competition.final_outcome import TournamentFinalEvidence, ParticipantIdentity
from modules.competition.rule_terms import (
    BerthIssue,
    BerthType,
    DestinationRound,
    Finalist,
    RankingIssue,
    RankingScope,
    TournamentFinalStatus,
)
from modules.competition.standings_rows import TableRow


class StandingRow(Protocol):
    @property
    def participant(self) -> ParticipantIdentity: ...
    @property
    def rank(self) -> int | None: ...
    @property
    def ranking_reason(self) -> RankingIssue | BerthIssue | None: ...


def _resolve_tournament_candidate(
    rule: TournamentBerthRule,
    evidence: TournamentFinalEvidence | None,
    ranks: Mapping[ParticipantIdentity, int | None],
    eligible: Set[ParticipantIdentity],
    held: Set[ParticipantIdentity],
    eliminated: Mapping[str, tuple[ParticipantIdentity, ...]],
    roster_complete: bool,
) -> tuple[ParticipantIdentity | None, BerthIssue | TournamentFinalStatus | None]:
    if evidence is None:
        return None, TournamentFinalStatus.FINAL_ABSENT
    if evidence.status != TournamentFinalStatus.DECIDED:
        return None, evidence.status
    if evidence.winner is None:
        return None, BerthIssue.INCOMPLETE_CUP_EVIDENCE

    for finalist in rule.finalists:
        club = evidence.winner if finalist == Finalist.WINNER else evidence.loser
        if club is None:
            return None, BerthIssue.INCOMPLETE_CUP_EVIDENCE
        if club in held:
            continue  # a club holds one berth only: try the next finalist, then the replacement rounds
        if club in eligible or (club not in ranks and rule.open_to_outsiders):
            return club, None
        if club not in ranks and not roster_complete:
            return None, BerthIssue.FINALIST_ELIGIBILITY_UNKNOWN

    for round_name, expected_matches in rule.replacement_rounds:
        losers = eliminated.get(round_name, ())
        if len(set(losers)) != expected_matches:
            return None, BerthIssue.INCOMPLETE_CUP_BRACKET
        candidates = set(losers) & eligible
        if len(candidates) == 1:
            return next(iter(candidates)), None
        if candidates:
            positions = {ranks.get(club) for club in candidates}
            if None in positions or len(positions) != len(candidates):
                return None, BerthIssue.CUP_REPLACEMENT_ANNUAL_TIEBREAK_PENDING
            return min(candidates, key=ranks.__getitem__), None

    return None, None


def allocate_berths(
    rules: QualificationRules,
    standings: Sequence[StandingRow],
    *,
    season_year: int,
    profile_identity: CompetitionIdentity,
    tournament_evidence: Sequence[TournamentFinalEvidence] = (),
    scoped_standings: Mapping[RankingScope, Sequence[TableRow]] | None = None,
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
    liga_mx_finals = tuple(evidence_by_comp.get(identity) for identity in (
        competition_ids.LIGA_MX_APERTURA_PLAYOFFS, competition_ids.LIGA_MX_CLAUSURA_PLAYOFFS,
    )) if profile_identity in (competition_ids.LIGA_MX_APERTURA, competition_ids.LIGA_MX_CLAUSURA) else ()
    liga_mx_finalists = {
        club for final in liga_mx_finals if final and final.status == TournamentFinalStatus.DECIDED
        for club in (final.winner, final.loser) if club is not None
    }
    concacaf_bye_holder = None
    champions = {final.winner for final in liga_mx_finals
                 if final and final.status == TournamentFinalStatus.DECIDED and final.winner is not None}
    if liga_mx_finals and all(final and final.status == TournamentFinalStatus.DECIDED
                             and final.winner in champions for final in liga_mx_finals):
        if len(champions) == 1:
            concacaf_bye_holder = next(iter(champions))
        elif all(ranks.get(club) is not None for club in champions):
            concacaf_bye_holder = min(champions, key=ranks.__getitem__)

    berths: list[AllocatedBerth] = []
    held: set[ParticipantIdentity] = set()
    earlier_berth_pending = False
    ranked_candidates = iter(row for row in standings
                             if row.participant in available and row.participant not in liga_mx_finalists)
    scoped_candidates = {}
    for table_rule in rules.table_berths:
        selection = table_rule.rank_selection
        if selection is None or selection.ranking_scope == RankingScope.GLOBAL:
            continue
        scoped_candidates[table_rule] = tuple(
            row for row in (scoped_standings or {}).get(selection.ranking_scope, ())
            if selection.rank_start <= row.ranking_order <= selection.rank_end
            and (selection.ranking_group is None
                 or row.ranking_group == selection.ranking_group.strip().casefold())
        )
    table_pending_issue = None

    def record_berth(destination, club, *, source, berth_type, resolution, issue=None, final_evidence=None):
        if club is not None:
            available.discard(club)
            held.add(club)
        if issue is None and resolution is BerthResolution.PROJECTED and earlier_berth_pending:
            issue = BerthIssue.EARLIER_BERTH_PENDING
        berths.append(AllocatedBerth(
            destination=destination,
            participant=club,
            source=source,
            berth_type=berth_type,
            resolution=resolution,
            issue=issue,
            table_rank=ranks.get(club) if club is not None else None,
            tournament_final_evidence=final_evidence,
        ))

    def award_from_table(destination, *, source, berth_type, table_rule=None, issue=None, final_evidence=None):
        """Select by the rule's scope, then pass duplicate berths to the overall table."""
        nonlocal table_pending_issue
        if table_pending_issue is None:
            selection = table_rule.rank_selection if table_rule is not None else None
            if (selection is not None and selection.ranking_scope != RankingScope.GLOBAL
                    and selection.ranking_scope not in (scoped_standings or {})):
                table_pending_issue = RankingIssue.MISSING_GROUP_MEMBERSHIP
            else:
                row = next((candidate for candidate in scoped_candidates.get(table_rule, ())
                            if candidate.participant in available
                            and candidate.participant not in liga_mx_finalists), None)
                if row is None:
                    row = next(ranked_candidates, None)
                if row is None:
                    table_pending_issue = BerthIssue.INSUFFICIENT_ELIGIBLE_PARTICIPANTS
                elif row.rank is None:
                    table_pending_issue = row.ranking_reason or BerthIssue.RANKING_UNRESOLVED
                else:
                    record_berth(destination, row.participant, source=source, berth_type=berth_type, issue=issue, final_evidence=final_evidence,
                                 resolution=BerthResolution.PROJECTED)
                    return
        record_berth(destination, None, source=source, berth_type=berth_type, resolution=BerthResolution.UNRESOLVED,
                     issue=table_pending_issue, final_evidence=final_evidence)

    for destination, rule in rules.ordered_berths:
        if isinstance(rule, TableBerthRule):
            award_from_table(destination, source=profile_identity, berth_type=BerthType.TABLE, table_rule=rule)
            continue
        evidence = evidence_by_comp.get(rule.competition)
        # An absent final adds nothing beyond the berth's issue.
        final_evidence = evidence if evidence is not None and evidence.status != TournamentFinalStatus.FINAL_ABSENT else None
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
            if liga_mx_finals and destination.destination_round is None and concacaf_bye_holder is not None:
                destination = replace(destination, destination_round=(
                    DestinationRound.ROUND_OF_16 if club == concacaf_bye_holder else DestinationRound.ROUND_OF_32
                ))
            resolution = BerthResolution.PROJECTED if (eligibility_unconfirmed or earlier_berth_pending) else BerthResolution.CONFIRMED
            if liga_mx_finals:
                resolution = BerthResolution.PROJECTED
        elif issue is not None:
            resolution = BerthResolution.UNRESOLVED
            earlier_berth_pending = True
        elif rule.reallocate_to_table:
            if liga_mx_finals:
                destination = replace(destination, destination_round=DestinationRound.ROUND_OF_32)
            award_from_table(destination, source=rule.competition, berth_type=BerthType.TOURNAMENT, final_evidence=final_evidence,
                             issue=BerthIssue.TOURNAMENT_WINNER_ALREADY_QUALIFIED)
            continue
        elif rule.replacement_rounds:
            resolution, issue = BerthResolution.UNRESOLVED, BerthIssue.NO_ELIGIBLE_CUP_PARTICIPANT
            earlier_berth_pending = True
        else:
            resolution, issue = BerthResolution.UNAWARDED, BerthIssue.WINNER_NOT_AVAILABLE
        record_berth(destination, club, source=rule.competition, berth_type=BerthType.TOURNAMENT, resolution=resolution,
                     issue=issue, final_evidence=final_evidence)

    return BerthAllocation(
        destination_season_year=season_year + rules.destination_season_offset,
        berths=tuple(berths),
    )
